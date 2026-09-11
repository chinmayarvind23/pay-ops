"""Journal a release control and two retained-allocation OOMs before exact risk restoration."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
    utc_timestamp,
)
from payops.scenarios.leak_evidence import parse_records, validate_progression
from payops.scenarios.leak_gateway import LeakGateway
from payops.scenarios.leak_lifetime import OomLifetime, capture_lifetime, repeated_oom, termination
from payops.scenarios.leak_specs import leak_specs
from payops.scenarios.memory_provenance import current_pods, timestamp
from payops.scenarios.protocol_gateway import protocol_identities
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner, sample_healthy
from payops.scenarios.sampling_gateway import deployment_map, validate_runtime_baseline


@dataclass
class LeakRun:
    """Keep both recognized variants available even when a mutation response is lost."""

    original: JsonObject
    control: JsonObject
    retained: JsonObject
    requested: str = ""
    control_image: str = ""


class LeakHarness(LocalScenarioRunner):
    """Only an operator can run this fixed risk experiment; the agent receives no mutation tool."""

    def __init__(
        self,
        kubeconfig: Path,
        evidence_root: Path,
        gateway: LeakGateway | None = None,
        timeout_seconds: float = 120,
        poll_seconds: float = 2,
    ) -> None:
        """Bound each experiment stage and retain the cross-harness cluster latch."""
        self.access = gateway or LeakGateway(kubeconfig)
        super().__init__(kubeconfig, evidence_root, self.access, timeout_seconds, poll_seconds)

    def _prepare(self, directory: Path, receipt: ScenarioReceipt) -> LeakRun:
        """Require a healthy five-service baseline and complete variant specs before writes."""
        self._save(directory, receipt, "scope", self.access.verify_scope())
        state = self.access.state()
        self._save(directory, receipt, "baseline", state)
        validate_runtime_baseline(state)
        control, retained = leak_specs(deployment_map(state)["risk-sim"])
        self._wait(directory, receipt, "healthy-before", self.access.healthy, sample_healthy)
        return LeakRun(state, control, retained)

    def _settle(
        self, context: LeakRun, expected: JsonObject, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Require all five exact specs and unchanged non-risk processes after every recovery."""
        original = deployment_map(context.original)
        payments = object_value(original["payments-api"]["spec"])
        baseline = protocol_identities(
            context.original, context.original, payments, object_value(original["risk-sim"]["spec"])
        )

        def accept(state: JsonObject) -> bool:
            """A replaced peer invalidates isolation even when the replacement is healthy."""
            try:
                identities = protocol_identities(state, context.original, payments, expected)
                return all(
                    identities[name] == prior
                    for name, prior in baseline.items()
                    if name != "risk-sim"
                )
            except ValueError:
                return False

        self._wait(directory, receipt, "settled-runtime", self.access.state, accept)

    def _transition(
        self, context: LeakRun, expected: JsonObject, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Journal the requested time before CAS; unknown current states never become baselines."""
        current = self.access.deployment("risk-sim")
        original = deployment_map(context.original)["risk-sim"]
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise ValueError("risk deployment identity changed")
        if current["spec"] not in (original["spec"], context.control, context.retained):
            raise ValueError("risk deployment has an unrecognized spec")
        context.requested = utc_timestamp()
        self._save(
            directory,
            receipt,
            "transition",
            {"requested_at": context.requested, "expected": expected},
        )
        self.access.replace_spec("risk-sim", current, expected)

    def _capture(
        self,
        context: LeakRun,
        expected: JsonObject,
        previous: bool,
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> tuple[JsonObject, JsonObject, str]:
        """Persist both observations and raw logs before judging semantic acceptance."""
        before = self.access.snapshot()
        self._save(directory, receipt, "capture-before", before)
        original = deployment_map(context.original)["risk-sim"]
        pods = current_pods(before, original, expected, context.requested, "risk-sim")
        if len(pods) != 1:
            raise ValueError("risk rollout not yet uniquely owned")
        if previous:
            termination(pods[0])
        log = self.access.read_log(pods[0], previous)
        self._save(directory, receipt, "raw-log", log)
        after = self.access.snapshot()
        self._save(directory, receipt, "capture-after", after)
        return before, after, str(log["text"])

    def _control(self, context: LeakRun, directory: Path, receipt: ScenarioReceipt) -> None:
        """A complete release control must survive without a hidden restart or changed container."""
        original = deployment_map(context.original)["risk-sim"]
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                before, after, raw = self._capture(
                    context, context.control, False, directory, receipt
                )
                frames = [
                    control_identity(frame, original, context.control, context.requested)
                    for frame in (before, after)
                ]
                if frames[0] != frames[1]:
                    raise ValueError("risk control process changed during log read")
                validate_progression(
                    parse_records(raw), "released-v1", frames[0][1], datetime.now(UTC)
                )
                if time.monotonic() >= deadline:
                    break
                context.control_image = frames[0][2]
                self._settle(context, context.control, directory, receipt)
                self._wait(
                    directory, receipt, "healthy-control", self.access.healthy, sample_healthy
                )
                receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
                return
            except ValueError as error:
                self._save(directory, receipt, "control-rejected", {"reason": str(error)})
            time.sleep(self.poll)
        raise TimeoutError("risk release control did not complete")

    def _fault(self, context: LeakRun, directory: Path, receipt: ScenarioReceipt) -> None:
        """Count distinct adjacent OOMs while retaining rejected observations."""
        original = deployment_map(context.original)["risk-sim"]
        first: OomLifetime | None = None
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                before, after, raw = self._capture(
                    context, context.retained, True, directory, receipt
                )
                frame = capture_lifetime(
                    before, after, original, context.retained, context.requested, raw
                )
                if frame.image_id != context.control_image:
                    raise ValueError("retained image differs from the release control")
                self._save(
                    directory,
                    receipt,
                    "oom-lifetime",
                    {
                        "pod_uid": frame.pod_uid,
                        "container_id": frame.container_id,
                        "image_id": frame.image_id,
                        "restart_count": frame.restart_count,
                        "started": frame.started.isoformat(),
                        "finished": frame.finished.isoformat(),
                        "log_sha256": frame.log_sha256,
                    },
                )
                if first is None:
                    first = frame
                elif repeated_oom(first, frame) and time.monotonic() < deadline:
                    receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
                    return
            except ValueError as error:
                self._save(directory, receipt, "oom-rejected", {"reason": str(error)})
            time.sleep(self.poll)
        raise TimeoutError("two owned risk OOM lifetimes were not verified")

    def run(
        self, case_id: CaseId = "OOM-02", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Restore original state after interrupted or ambiguous writes."""
        if case_id != "OOM-02":
            raise ValueError("leak harness accepts only OOM-02")
        directory, receipt = self._start(case_id)
        context: LeakRun | None = None
        try:
            context = self._prepare(directory, receipt)
            self._save(
                directory,
                receipt,
                "journal",
                {
                    "original": context.original,
                    "control": context.control,
                    "retained": context.retained,
                },
            )
            self._transition(context, context.control, directory, receipt)
            self._control(context, directory, receipt)
            self._transition(context, context.retained, directory, receipt)
            receipt.injection_requested_at = context.requested
            self._fault(context, directory, receipt)
            self._investigate(receipt, after_activation)
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt)
        return receipt

    def _undo(self, context: LeakRun) -> None:
        """Restore known variants while preserving foreign state for review."""
        original = deployment_map(context.original)["risk-sim"]
        current = self.access.deployment("risk-sim")
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("risk identity changed")
        if current["spec"] == original["spec"]:
            return
        if current["spec"] not in (context.control, context.retained):
            raise CleanupUnverified("risk spec changed outside the experiment")
        self.access.replace_spec("risk-sim", current, object_value(original["spec"]))

    def _recover(self, context: LeakRun | None, directory: Path, receipt: ScenarioReceipt) -> None:
        """Restore before audit writes; retain the latch if cleanup or persistence fails."""
        errors: list[str] = []
        if context is not None:
            receipt.cleanup_started_at = utc_timestamp()
            try:
                self._undo(context)
                self._settle(
                    context,
                    object_value(deployment_map(context.original)["risk-sim"]["spec"]),
                    directory,
                    receipt,
                )
                self._wait(directory, receipt, "healthy-final", self.access.healthy, sample_healthy)
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


def control_identity(
    observed: JsonObject, original: JsonObject, expected: JsonObject, requested: str
) -> tuple[str, datetime, str]:
    """Require a new owned control pod with exactly one healthy, never-restarted container."""
    pods = current_pods(observed, original, expected, requested, "risk-sim")
    if len(pods) != 1:
        raise ValueError("risk control pod ownership changed")
    statuses = object_items(object_value(pods[0]["status"]).get("containerStatuses", []))
    if len(statuses) != 1:
        raise ValueError("risk control container multiplicity changed")
    status = statuses[0]
    started = timestamp(
        object_value(object_value(status.get("state", {})).get("running", {})).get("startedAt")
    )
    if (
        status.get("restartCount") != 0
        or status.get("ready") is not True
        or not status.get("imageID")
        or not status.get("containerID")
        or started is None
    ):
        raise ValueError("risk release control is not healthy and uninterrupted")
    return str(status["containerID"]), started, str(status["imageID"])
