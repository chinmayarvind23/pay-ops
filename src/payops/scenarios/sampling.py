"""Journal all sampling states before mutation and restore independently of audit failures."""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Protocol

from payops.contracts import utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import (
    CaseId,
    ClusterGateway,
    JsonObject,
    ScenarioReceipt,
    object_value,
    utc_timestamp,
)
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner
from payops.scenarios.sampling_contract import PLAN, sampling_specs
from payops.scenarios.sampling_gateway import (
    SamplingGateway,
    deployment_map,
    runtime_identities,
    validate_runtime_baseline,
)
from payops.scenarios.sampling_observation import (
    SamplingObservation,
    Stage,
    verify_sampling_counterfactual,
    verify_sampling_stage,
)
from payops.scenarios.sampling_observer import RuntimeSamplingObserver, SamplingObserver


class SamplingAccess(ClusterGateway, Protocol):
    """Only the operator owns the fixed namespace runtime snapshot and inherited closed CAS."""

    def state(self, timeout_seconds: float = 30) -> JsonObject:
        """Return bounded current deployments, pods and ReplicaSets from the sandbox namespace."""
        ...


@dataclass
class SamplingRun:
    """In-memory progress never replaces the immutable full-spec journal written before mutation."""

    original: JsonObject
    suppressed: JsonObject
    restored_sampling: JsonObject
    latest: JsonObject
    identities: tuple[PodIdentity, PodIdentity]
    observations: dict[Stage, SamplingObservation] = field(
        default_factory=dict[Stage, SamplingObservation]
    )
    requested_at: str | None = None
    previous_processor: PodIdentity | None = None


def _new_requests(observations: dict[Stage, SamplingObservation]) -> None:
    """All four stages require globally distinct request and trace identities."""
    samples = [
        row.planned.sample.sample_id
        for item in observations.values()
        for row in item.traffic.attempts
    ]
    traces = [
        row.planned.traceparent.split("-")[1]
        for item in observations.values()
        for row in item.traffic.attempts
    ]
    if len(set(samples)) != len(samples) or len(set(traces)) != len(traces):
        raise ValueError("sampling stages reuse sample or trace identity")


class SamplingHarness(LocalScenarioRunner):
    """This specialized lifecycle never delegates mutation or case acceptance to the model."""

    def __init__(
        self,
        kubeconfig: Path,
        evidence_root: Path,
        gateway: SamplingAccess | None = None,
        observer: SamplingObserver | None = None,
        timeout_seconds: float = 90,
        poll_seconds: float = 2,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """Share the canonical cluster latch with an independent cleanup implementation."""
        if timeout_seconds > 90 or poll_seconds > 2:
            raise ValueError("sampling timing exceeds frozen runtime limits")
        self.access = gateway or SamplingGateway(kubeconfig)
        super().__init__(kubeconfig, evidence_root, self.access, timeout_seconds, poll_seconds)
        self.observer = observer or RuntimeSamplingObserver(kubeconfig)
        self.clock = clock

    def _record(
        self,
        directory: Path,
        receipt: ScenarioReceipt,
        name: str,
        value: JsonObject,
        errors: list[str] | None = None,
    ) -> None:
        """Cleanup audit errors are retained but cannot interrupt restoration or readiness reads."""
        try:
            self._save(directory, receipt, name, value)
        except BaseException as error:
            if errors is None:
                raise
            errors.append(f"{name} persistence: {type(error).__name__}: {error}")

    def _settle(
        self,
        context: SamplingRun,
        spec: JsonObject,
        directory: Path,
        receipt: ScenarioReceipt,
        errors: list[str] | None = None,
        fresh: bool = False,
    ) -> datetime:
        """Wait for five exact owned healthy services; persistence cannot suppress cleanup reads."""
        deadline = time.monotonic() + self.timeout
        for _ in range(46):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("sampling runtime did not settle")
            state = self.access.state(min(30, remaining))
            self._record(
                directory,
                receipt,
                "runtime",
                {**state, "captured_at": self.clock().isoformat()},
                errors,
            )
            if time.monotonic() > deadline:
                raise TimeoutError("sampling runtime did not settle")
            try:
                identities = runtime_identities(
                    state,
                    context.original,
                    spec,
                    context.requested_at if fresh else None,
                    context.previous_processor if fresh else None,
                )
            except ValueError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("sampling runtime did not settle") from None
                time.sleep(min(self.poll, max(0, deadline - time.monotonic())))
                continue
            context.latest, context.identities = state, identities
            return self.clock()
        raise TimeoutError("sampling runtime snapshot count exceeded")

    def _stage(
        self,
        stage: Stage,
        context: SamplingRun,
        spec: JsonObject,
        directory: Path,
        receipt: ScenarioReceipt,
        store: ArtifactStore,
        fresh: bool = False,
    ) -> None:
        """Pin process identity across traffic, both captures and verified acceptance."""
        ready_at = self._settle(context, spec, directory, receipt, fresh=fresh)
        identities = context.identities
        observed = self.observer.collect(
            stage, receipt.run_id, identities, ready_at, directory, store
        )
        if (
            observed.traffic.mode != self.access.mode
            or observed.traffic.started_at < ready_at
            or (observed.payments_identity, observed.processor_identity) != identities
        ):
            raise ValueError("sampling observations disagree with runtime mode or readiness")
        self._save(directory, receipt, stage, JSON_OBJECT.validate_json(observed.model_dump_json()))
        verify_sampling_stage(stage, observed, store)
        self._settle(context, spec, directory, receipt)
        if context.identities != identities:
            raise ValueError("sampling process changed across stage measurements")
        context.observations[stage] = observed
        _new_requests(context.observations)

    def _prepare(
        self, directory: Path, receipt: ScenarioReceipt, store: ArtifactStore
    ) -> SamplingRun:
        """Derive and persist every future full spec before any fault mutation is possible."""
        self._save(directory, receipt, "scope", self.access.verify_scope())
        original = self.access.state()
        self._save(directory, receipt, "original", original)
        validate_runtime_baseline(original)
        document = deployment_map(original)["processor-adapter"]
        suppressed, restored = sampling_specs(document)
        identities = runtime_identities(original, original, object_value(document["spec"]))
        context = SamplingRun(original, suppressed, restored, original, identities)
        self._stage("original", context, object_value(document["spec"]), directory, receipt, store)
        receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
        self._save(
            directory,
            receipt,
            "sampling-journal",
            {
                "original": original,
                "suppressed": suppressed,
                "restored_sampling": restored,
                "plan": JSON_OBJECT.validate_json(PLAN.model_dump_json()),
            },
        )
        return context

    def _transition(
        self,
        context: SamplingRun,
        spec: JsonObject,
        directory: Path,
        receipt: ScenarioReceipt,
        stage: Stage,
    ) -> None:
        """Only the three journaled specs are reachable through exact processor-only CAS."""
        current = deployment_map(context.latest)["processor-adapter"]
        context.requested_at, context.previous_processor = (
            self.clock().isoformat(),
            context.identities[1],
        )
        self._save(
            directory,
            receipt,
            "transition-" + stage,
            {
                "requested_at": context.requested_at,
                "expected": current,
                "next_spec": spec,
            },
        )
        self.access.replace_spec("processor-adapter", current, spec)

    def run(
        self, case_id: CaseId = "TELEM-03", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Investigate while sampling is suppressed; callback and source failures still restore."""
        if case_id != "TELEM-03":
            raise ValueError("sampling harness accepts only TELEM-03")
        directory, receipt = self._start(case_id)
        context: SamplingRun | None = None
        store: ArtifactStore | None = None
        try:
            store = ArtifactStore(directory / "artifacts")
            context = self._prepare(directory, receipt, store)
            receipt.injection_requested_at = utc_timestamp()
            self._transition(context, context.suppressed, directory, receipt, "suppressed")
            self._stage("suppressed", context, context.suppressed, directory, receipt, store, True)
            receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
            self._investigate(receipt, after_activation)
            self._transition(
                context, context.restored_sampling, directory, receipt, "restored_sampling"
            )
            self._stage(
                "restored_sampling",
                context,
                context.restored_sampling,
                directory,
                receipt,
                store,
                True,
            )
            verify_sampling_counterfactual(
                context.observations["suppressed"], context.observations["restored_sampling"], store
            )
            self._save(directory, receipt, "sampling-counterfactual", {"verified": True})
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt, store)
        return receipt

    def _undo(self, context: SamplingRun) -> None:
        """Unknown UID/spec is never overwritten, including after an ambiguous failed write."""
        current = self.access.deployment("processor-adapter")
        original = deployment_map(context.original)["processor-adapter"]
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("sampling Deployment identity changed")
        if current["spec"] == original["spec"]:
            return
        if current["spec"] not in (context.suppressed, context.restored_sampling):
            raise CleanupUnverified("sampling Deployment has an unknown spec")
        self.access.replace_spec("processor-adapter", current, object_value(original["spec"]))

    def _final_observation(
        self,
        context: SamplingRun,
        directory: Path,
        receipt: ScenarioReceipt,
        store: ArtifactStore,
        errors: list[str],
    ) -> None:
        """The final eight requests supply fresh success without adding a ninth census sample."""
        spec = object_value(deployment_map(context.original)["processor-adapter"]["spec"])
        ready_at = self._settle(context, spec, directory, receipt, errors)
        identities = context.identities
        final = self.observer.collect(
            "final", receipt.run_id, identities, ready_at, directory, store
        )
        if (
            final.traffic.mode != self.access.mode
            or final.traffic.started_at < ready_at
            or (final.payments_identity, final.processor_identity) != identities
        ):
            raise ValueError("sampling final observations disagree with runtime readiness")
        self._record(
            directory, receipt, "final", JSON_OBJECT.validate_json(final.model_dump_json()), errors
        )
        verify_sampling_stage("final", final, store)
        context.observations["final"] = final
        _new_requests(context.observations)
        self._settle(context, spec, directory, receipt, errors)
        if context.identities != identities:
            raise CleanupUnverified("sampling process changed during final proof")

    def _recover(
        self,
        context: SamplingRun | None,
        directory: Path,
        receipt: ScenarioReceipt,
        store: ArtifactStore | None,
    ) -> None:
        """Attempt restoration and verification independently despite audit storage errors."""
        errors: list[str] = []
        if context is not None and store is not None:
            receipt.cleanup_started_at = utc_timestamp()
            operations = (
                ("restore", lambda: self._undo(context)),
                (
                    "verify",
                    lambda: self._final_observation(context, directory, receipt, store, errors),
                ),
            )
            for name, operation in operations:
                try:
                    operation()
                except BaseException as error:
                    errors.append(f"{name}: {type(error).__name__}: {error}")
                self._record(
                    directory, receipt, "cleanup-" + name, {"errors": list(errors)}, errors
                )
            if not errors:
                receipt.cleanup_verified, receipt.cleanup_verified_at = True, utc_timestamp()
        self._final_receipt(directory, receipt, errors)

    def _final_receipt(self, directory: Path, receipt: ScenarioReceipt, errors: list[str]) -> None:
        """A successful saved final receipt is required before the cluster latch can be released."""
        receipt.completed_at, receipt.cleanup_failure = utc_timestamp(), "; ".join(errors) or None
        try:
            with (directory / "receipt.json").open("x", encoding="utf-8") as output:
                output.write(receipt.model_dump_json(indent=2))
        except BaseException as error:
            errors.append(f"receipt persistence: {type(error).__name__}: {error}")
        if not errors:
            try:
                self.block_file.unlink()
            except BaseException as error:
                errors.append(f"latch release: {type(error).__name__}: {error}")
        if errors:
            self._retain_block(receipt, errors)

    def _retain_block(self, receipt: ScenarioReceipt, errors: list[str]) -> None:
        """The canonical latch remains authoritative if a saved receipt predates release failure."""
        receipt.cleanup_verified, receipt.cleanup_verified_at = False, None
        receipt.cleanup_failure = "; ".join(errors)
        try:
            self.block_file.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        except BaseException as error:
            raise CleanupUnverified(
                f"{receipt.cleanup_failure}; latch persistence failed"
            ) from error
