"""Journal the five-stage CPU experiment and restore only recognized owned deployment states."""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from payops.contracts import utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_value,
    utc_timestamp,
)
from payops.scenarios.cpu_contract import PLAN, CpuStage, compare, cpu_specs, stage_means
from payops.scenarios.cpu_observer import CpuObserver, RuntimeCpuObserver, verify_observation
from payops.scenarios.cpu_sources import CpuGateway
from payops.scenarios.protocol_gateway import fresh_protocol_process, protocol_identities
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner
from payops.scenarios.sampling_gateway import deployment_map, validate_runtime_baseline


@dataclass
class CpuRun:
    """Hold recognized full specs and progress; the saved journal survives process failure."""

    original: JsonObject
    latest: JsonObject
    control: JsonObject
    restricted: JsonObject
    identities: dict[str, PodIdentity]
    samples: set[str] = field(default_factory=set[str])
    traces: set[str] = field(default_factory=set[str])
    means: dict[str, tuple[float, float]] = field(default_factory=dict[str, tuple[float, float]])
    requested_at: str | None = None


class CpuHarness(LocalScenarioRunner):
    """The CPU workload stays operator-owned and cannot be selected through model tools."""

    def __init__(
        self,
        kubeconfig: Path,
        evidence_root: Path,
        gateway: CpuGateway | None = None,
        observer: CpuObserver | None = None,
        timeout_seconds: float = 90,
        poll_seconds: float = 2,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """Retain the shared latch and fixed runtime limits across every stage and recovery."""
        if timeout_seconds > 90 or poll_seconds > 2:
            raise ValueError("CPU timing exceeds frozen limits")
        self.access = gateway or CpuGateway(kubeconfig)
        super().__init__(kubeconfig, evidence_root, self.access, timeout_seconds, poll_seconds)
        self.observer = observer or RuntimeCpuObserver(kubeconfig, self.access)
        self.clock = clock

    @staticmethod
    def _spec(context: CpuRun, stage: CpuStage) -> JsonObject:
        """Select from the two journaled variants or the exact captured original."""
        if stage == "restricted":
            return context.restricted
        if stage in {"control", "recovered"}:
            return context.control
        return object_value(deployment_map(context.original)["payments-api"]["spec"])

    def _settle(
        self,
        context: CpuRun,
        stage: CpuStage,
        directory: Path,
        receipt: ScenarioReceipt,
        changed: bool = False,
        fresh: bool = False,
    ) -> datetime:
        """Require all five exact specs and unchanged peers; forward writes need a new process."""
        deadline = time.monotonic() + self.timeout
        risk = object_value(deployment_map(context.original)["risk-sim"]["spec"])
        for _ in range(46):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            state = self.access.state(min(30, remaining))
            self._save(directory, receipt, "runtime", state)
            try:
                current = protocol_identities(
                    state, context.original, self._spec(context, stage), risk
                )
                if any(
                    current[name] != identity
                    for name, identity in context.identities.items()
                    if not (changed and name == "payments-api")
                ):
                    raise ValueError("unmodified CPU experiment peer changed")
                if fresh:
                    fresh_protocol_process(
                        state,
                        "payments-api",
                        current["payments-api"],
                        context.identities["payments-api"],
                        context.requested_at or "",
                    )
            except ValueError:
                time.sleep(min(self.poll, max(0, deadline - time.monotonic())))
                continue
            if time.monotonic() > deadline:
                break
            context.latest, context.identities = state, current
            return self.clock()
        raise TimeoutError("CPU runtime did not settle within fixed limits")

    def _stage(
        self,
        context: CpuRun,
        stage: CpuStage,
        directory: Path,
        receipt: ScenarioReceipt,
        store: ArtifactStore,
        changed: bool = False,
        fresh: bool = False,
    ) -> None:
        """Every retained request must be fresh, in scope and backed by its complete path."""
        ready = self._settle(context, stage, directory, receipt, changed, fresh)
        identities = context.identities.copy()
        risk = object_value(deployment_map(context.original)["risk-sim"]["spec"])
        observed = self.observer.collect(
            stage,
            receipt.run_id,
            identities,
            directory,
            store,
            context.original,
            self._spec(context, stage),
            risk,
        )
        self._save(directory, receipt, stage, JSON_OBJECT.validate_json(observed.model_dump_json()))
        previous_end = ready
        for path in observed.paths:
            probe, trace_id = path.probe, path.probe.traceparent.split("-")[1]
            if (
                path.incident_id != receipt.run_id
                or path.identities != identities
                or probe.mode != self.access.mode
                or probe.started_at < previous_end
                or probe.sample.sample_id in context.samples
                or trace_id in context.traces
            ):
                raise ValueError("CPU request scope or fresh identity differs")
            context.samples.add(probe.sample.sample_id)
            context.traces.add(trace_id)
            previous_end = probe.completed_at
        records = verify_observation(stage, observed, store)
        if records:
            maximum = (
                PLAN.restricted_quota_usec if stage == "restricted" else PLAN.control_quota_usec
            )
            context.means[stage] = stage_means(
                records, receipt.run_id, identities["payments-api"], maximum
            )
        self._settle(context, stage, directory, receipt)

    def _prepare(self, directory: Path, receipt: ScenarioReceipt) -> CpuRun:
        """Persist originals and both variants before any write can take place."""
        self._save(directory, receipt, "scope", self.access.verify_scope())
        original = self.access.state()
        self._save(directory, receipt, "original", original)
        validate_runtime_baseline(original)
        documents = deployment_map(original)
        control, restricted = cpu_specs(documents["payments-api"])
        identities = protocol_identities(
            original,
            original,
            object_value(documents["payments-api"]["spec"]),
            object_value(documents["risk-sim"]["spec"]),
        )
        self._save(
            directory,
            receipt,
            "cpu-journal",
            {
                "original": original,
                "control": control,
                "restricted": restricted,
                "plan": JSON_OBJECT.validate_json(PLAN.model_dump_json()),
            },
        )
        return CpuRun(original, original, control, restricted, identities)

    def _transition(
        self, context: CpuRun, stage: CpuStage, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Persist the exact expected UID/version/spec and next spec before invoking CAS."""
        current = deployment_map(context.latest)["payments-api"]
        context.requested_at = self.clock().isoformat()
        spec = self._spec(context, stage)
        self._save(
            directory,
            receipt,
            "transition-" + stage,
            {"expected": current, "next_spec": spec, "requested_at": context.requested_at},
        )
        self.access.replace_spec("payments-api", current, spec)

    def run(
        self, case_id: CaseId = "OOM-03", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Any error still attempts exact restoration; only a completed contrast activates."""
        if case_id != "OOM-03":
            raise ValueError("CPU harness accepts only OOM-03")
        directory, receipt = self._start(case_id)
        context: CpuRun | None = None
        store: ArtifactStore | None = None
        try:
            store = ArtifactStore(directory / "artifacts")
            context = self._prepare(directory, receipt)
            self._stage(context, "original", directory, receipt, store)
            for stage in ("control", "restricted", "recovered"):
                receipt.injection_requested_at = receipt.injection_requested_at or utc_timestamp()
                self._transition(context, stage, directory, receipt)
                self._stage(context, stage, directory, receipt, store, True, True)
                if stage == "control":
                    receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
                elif stage == "restricted":
                    compare(
                        context.means["control"],
                        context.means["restricted"],
                        context.means["control"],
                    )
                    receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
                    self._investigate(receipt, after_activation)
            compare(
                context.means["control"], context.means["restricted"], context.means["recovered"]
            )
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt, store)
        return receipt

    def _undo(self, context: CpuRun) -> None:
        """Recognized variants allow ambiguous-write recovery; foreign UID/spec blocks overwrite."""
        current = self.access.deployment("payments-api")
        original = deployment_map(context.original)["payments-api"]
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("CPU deployment identity changed")
        if current["spec"] == original["spec"]:
            return
        if current["spec"] not in (context.control, context.restricted):
            raise CleanupUnverified("CPU deployment has an unknown spec")
        self.access.replace_spec("payments-api", current, object_value(original["spec"]))

    def _recover(
        self,
        context: CpuRun | None,
        directory: Path,
        receipt: ScenarioReceipt,
        store: ArtifactStore | None,
    ) -> None:
        """Restoration precedes audit writes so a failed evidence sink cannot strand the fault."""
        errors: list[str] = []
        if context is not None and store is not None:
            receipt.cleanup_started_at = utc_timestamp()
            try:
                self._undo(context)
                self._stage(context, "final", directory, receipt, store, True)
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
