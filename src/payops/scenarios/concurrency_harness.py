"""Run isolated serial/parallel/serial stages and restore only journaled payments specs."""

import asyncio
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.trace_span import PodIdentity
from payops.scenarios.concurrency_evidence import parse_events, validate_events
from payops.scenarios.concurrency_gateway import ConcurrencyGateway
from payops.scenarios.concurrency_lifetime import capture_concurrency_oom
from payops.scenarios.concurrency_specs import RUNTIME_IMAGE_DIGEST, concurrency_spec
from payops.scenarios.concurrency_traffic import (
    TrafficWindow,
    concurrency_workload,
    validate_traffic,
)
from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
    utc_timestamp,
)
from payops.scenarios.memory_provenance import current_pods
from payops.scenarios.protocol_gateway import protocol_identities
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner, sample_healthy
from payops.scenarios.sampling_gateway import deployment_map, validate_runtime_baseline
from payops.scenarios.traffic import TrafficDriver


@dataclass
class ConcurrencyRun:
    """Keep the pre-write restoration source and cross-stage sample uniqueness in memory."""

    original: JsonObject
    enabled: JsonObject
    peers: dict[str, PodIdentity]
    requested: str = ""
    samples: set[str] = field(default_factory=set[str])
    processes: set[str] = field(default_factory=set[str])


class ConcurrencyHarness(LocalScenarioRunner):
    """An operator-owned experiment with a cluster latch and exact-state recovery."""

    def __init__(
        self,
        kubeconfig: Path,
        evidence_root: Path,
        gateway: ConcurrencyGateway | None = None,
        timeout_seconds: float = 90,
        poll_seconds: float = 2,
    ) -> None:
        """Reuse bounded transport and reject timing beyond the reviewed stage limits."""
        if timeout_seconds > 90 or poll_seconds > 2:
            raise ValueError("concurrency timing exceeds frozen bounds")
        self.access = gateway or ConcurrencyGateway(kubeconfig)
        self.kubeconfig = kubeconfig
        super().__init__(kubeconfig, evidence_root, self.access, timeout_seconds, poll_seconds)

    def _prepare(self, directory: Path, receipt: ScenarioReceipt) -> ConcurrencyRun:
        """Validate originals before returning context; the caller journals before mutation."""
        self._save(directory, receipt, "scope", self.access.verify_scope())
        state = self.access.state()
        self._save(directory, receipt, "baseline", state)
        validate_runtime_baseline(state)
        documents = deployment_map(state)
        identities = protocol_identities(
            state,
            state,
            object_value(documents["payments-api"]["spec"]),
            object_value(documents["risk-sim"]["spec"]),
        )
        enabled = concurrency_spec(documents["payments-api"])
        self._wait(directory, receipt, "healthy-before", self.access.healthy, sample_healthy)
        return ConcurrencyRun(state, enabled, identities)

    def _identities(
        self, context: ConcurrencyRun, state: JsonObject, enabled: bool
    ) -> dict[str, PodIdentity]:
        """Verify pinned worker code and every unchanged peer before accepting a ready stage."""
        original = deployment_map(context.original)
        spec = context.enabled if enabled else object_value(original["payments-api"]["spec"])
        image: str | None = None
        if enabled:
            matching = [
                p
                for p in object_items(state["pods"])
                if str(object_value(p["metadata"]).get("name", "")).startswith("payments-api-")
            ]
            if len(matching) != 1:
                raise ValueError("payments pod multiplicity changed")
            statuses = object_items(object_value(matching[0]["status"])["containerStatuses"])
            if len(statuses) != 1:
                raise ValueError("payments container multiplicity changed")
            image = str(statuses[0].get("imageID", ""))
            if image.split("@")[-1] != RUNTIME_IMAGE_DIGEST:
                raise ValueError("payments worker image differs from calibrated import")
        identities = protocol_identities(
            state,
            context.original,
            spec,
            object_value(original["risk-sim"]["spec"]),
            payments_image_id=image,
        )
        if any(
            identities[name] != prior
            for name, prior in context.peers.items()
            if name != "payments-api"
        ):
            raise ValueError("unmodified concurrency peer changed")
        return identities

    def _settle(
        self, context: ConcurrencyRun, enabled: bool, directory: Path, receipt: ScenarioReceipt
    ) -> PodIdentity:
        """Poll retained full runtime snapshots until exact specs and isolation are established."""

        def accept(state: JsonObject) -> bool:
            """Transient rollout states are retained but cannot satisfy readiness."""
            try:
                self._identities(context, state, enabled)
                return True
            except (ValueError, KeyError):
                return False

        state = self._wait(directory, receipt, "runtime", self.access.state, accept)
        return self._identities(context, state, enabled)["payments-api"]

    def _enable(
        self, context: ConcurrencyRun, directory: Path, receipt: ScenarioReceipt
    ) -> PodIdentity:
        """A fresh original-to-enabled transition prevents earlier stage logs from leaking in."""
        original = deployment_map(context.original)["payments-api"]
        current = self.access.deployment("payments-api")
        if (
            object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]
            or current["spec"] != original["spec"]
        ):
            raise ValueError("concurrency stage did not start from the captured original")
        context.requested = utc_timestamp()
        self._save(
            directory,
            receipt,
            "transition",
            {"expected": current, "next_spec": context.enabled, "requested_at": context.requested},
        )
        self.access.replace_spec("payments-api", current, context.enabled)
        identity = self._settle(context, True, directory, receipt)
        snapshot = self.access.snapshot()
        self._save(directory, receipt, "fresh-process", snapshot)
        pods = current_pods(snapshot, original, context.enabled, context.requested)
        if (
            len(pods) != 1
            or object_value(pods[0]["metadata"])["uid"] != identity.pod_uid
            or identity.container_id in context.processes
        ):
            raise ValueError("concurrency stage lacks a fresh owned process")
        context.processes.add(identity.container_id)
        return identity

    def _traffic(
        self, context: ConcurrencyRun, stage: str, directory: Path, receipt: ScenarioReceipt
    ) -> TrafficWindow:
        """Retain both the driver's original plan and receipt, including failed batch evidence."""
        parallel = stage == "parallel"
        output = directory / "traffic" / stage
        driver = TrafficDriver(self.kubeconfig, output)
        observed = asyncio.run(driver.run(concurrency_workload(parallel=parallel)))
        plan = JSON_OBJECT.validate_json((output / observed.run_id / "plan.json").read_bytes())
        self._save(directory, receipt, stage + "-plan", plan)
        self._save(
            directory,
            receipt,
            stage + "-traffic",
            JSON_OBJECT.validate_json(observed.model_dump_json()),
        )
        if observed.mode != self.access.mode:
            raise ValueError("traffic and Kubernetes acquisition modes differ")
        window = validate_traffic(observed, plan, parallel=parallel)
        if context.samples.intersection(window.samples):
            raise ValueError("sample identity reused across concurrency stages")
        context.samples.update(window.samples)
        return window

    def _capture(
        self, context: ConcurrencyRun, previous: bool, directory: Path, receipt: ScenarioReceipt
    ) -> tuple[JsonObject, JsonObject, str]:
        """Persist raw logs between ownership snapshots before any semantic validation."""
        before = self.access.snapshot()
        self._save(directory, receipt, "capture-before", before)
        original = deployment_map(context.original)["payments-api"]
        pods = current_pods(before, original, context.enabled, context.requested)
        if len(pods) != 1:
            raise ValueError("payments log source is not uniquely owned")
        raw = self.access.read_log(pods[0], previous)
        self._save(directory, receipt, "raw-log", raw)
        after = self.access.snapshot()
        self._save(directory, receipt, "capture-after", after)
        return before, after, str(raw["text"])

    def _stage(
        self,
        context: ConcurrencyRun,
        stage: str,
        directory: Path,
        receipt: ScenarioReceipt,
        after_activation: Callable[[], None] | None = None,
    ) -> None:
        """Each stage consumes its own process and eight fresh requests at identical limits."""
        identity = self._enable(context, directory, receipt)
        if stage == "parallel":
            receipt.injection_requested_at = utc_timestamp()
        window = self._traffic(context, stage, directory, receipt)
        if stage == "parallel":
            self._oom(context, identity, window, directory, receipt)
            receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
            self._investigate(receipt, after_activation)
        else:
            if self._settle(context, True, directory, receipt) != identity:
                raise ValueError("serial process changed during traffic")
            _, _, raw = self._capture(context, False, directory, receipt)
            progress = validate_events(
                parse_events(raw), window.samples, window.started, window.completed, parallel=False
            )
            if self._settle(context, True, directory, receipt) != identity:
                raise ValueError("serial process changed during log capture")
            self._save(directory, receipt, stage + "-memory", object_value(asdict(progress)))
        self._restore_original(context, directory, receipt)

    def _oom(
        self,
        context: ConcurrencyRun,
        identity: PodIdentity,
        window: TrafficWindow,
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Wait for kubelet termination propagation without rerunning the traffic batch."""
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                before, after, raw = self._capture(context, True, directory, receipt)
                result = capture_concurrency_oom(
                    before,
                    after,
                    deployment_map(context.original)["payments-api"],
                    context.enabled,
                    context.requested,
                    identity,
                    window,
                    raw,
                )
                self._save(
                    directory,
                    receipt,
                    "oom-proof",
                    {
                        "container_id": result.lifetime.container_id,
                        "log_sha256": result.lifetime.log_sha256,
                        "memory": object_value(asdict(result.memory)),
                    },
                )
                return
            except ValueError as error:
                self._save(directory, receipt, "oom-rejected", {"reason": str(error)})
            time.sleep(self.poll)
        raise TimeoutError("owned concurrency OOM was not verified")

    def _restore_original(
        self, context: ConcurrencyRun, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Restore before writing audit data so an unavailable evidence sink cannot strand work."""
        original = deployment_map(context.original)["payments-api"]
        current = self.access.deployment("payments-api")
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("payments deployment identity changed")
        if current["spec"] != original["spec"]:
            if current["spec"] != context.enabled:
                raise CleanupUnverified("payments deployment spec changed outside this run")
            self.access.replace_spec("payments-api", current, object_value(original["spec"]))
        self._settle(context, False, directory, receipt)
        self._wait(directory, receipt, "healthy-restored", self.access.healthy, sample_healthy)

    def run(
        self, case_id: CaseId = "OOM-04", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Qualification requires both serial controls, the actual OOM and original restoration."""
        if case_id != "OOM-04":
            raise ValueError("concurrency harness accepts only OOM-04")
        directory, receipt = self._start(case_id)
        context: ConcurrencyRun | None = None
        try:
            context = self._prepare(directory, receipt)
            self._save(
                directory,
                receipt,
                "journal",
                {"original": context.original, "enabled": context.enabled},
            )
            for stage in ("control", "parallel", "recovered"):
                self._stage(context, stage, directory, receipt, after_activation)
                if stage == "control":
                    receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt)
        return receipt

    def _recover(
        self, context: ConcurrencyRun | None, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Unverified recovery or receipt persistence retains the cross-harness latch."""
        errors: list[str] = []
        if context is not None:
            receipt.cleanup_started_at = utc_timestamp()
            try:
                self._restore_original(context, directory, receipt)
                receipt.cleanup_verified, receipt.cleanup_verified_at = True, utc_timestamp()
            except BaseException as error:
                errors.append(f"cleanup: {type(error).__name__}: {error}")
        receipt.completed_at, receipt.cleanup_failure = utc_timestamp(), "; ".join(errors) or None
        try:
            with (directory / "receipt.json").open("x", encoding="utf-8") as stream:
                stream.write(receipt.model_dump_json(indent=2))
            if not errors:
                self.block_file.unlink()
        except BaseException as error:
            errors.append(f"receipt/latch: {type(error).__name__}: {error}")
        if errors:
            receipt.cleanup_verified, receipt.cleanup_verified_at = False, None
            receipt.cleanup_failure = "; ".join(errors)
            self.block_file.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
