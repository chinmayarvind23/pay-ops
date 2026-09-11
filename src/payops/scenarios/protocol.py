"""Journal both wire-version Deployments and restore each independently on every exit path."""

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
from payops.scenarios.protocol_contract import PLAN, ProtocolStage, ProtocolTarget, protocol_spec
from payops.scenarios.protocol_gateway import (
    ProtocolGateway,
    fresh_protocol_process,
    protocol_identities,
)
from payops.scenarios.protocol_observation import ProtocolObservation, verify_protocol_observation
from payops.scenarios.protocol_observer import (
    ProtocolLogReader,
    ProtocolObserver,
    RuntimeProtocolObserver,
)
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner
from payops.scenarios.sampling_gateway import deployment_map, validate_runtime_baseline


class ProtocolAccess(ClusterGateway, ProtocolLogReader, Protocol):
    """The operator owns only fixed runtime reads, risk logs and exact two-target CAS."""

    def state(self, timeout_seconds: float = 30) -> JsonObject:
        """Read five-service runtime under shared subprocess byte and time limits."""
        ...


@dataclass
class ProtocolRun:
    """The immutable on-disk journal remains authoritative over mutable execution progress."""

    original: JsonObject
    latest: JsonObject
    variants: dict[str, JsonObject]
    identities: dict[str, PodIdentity]
    observations: dict[ProtocolStage, ProtocolObservation] = field(
        default_factory=dict[ProtocolStage, ProtocolObservation]
    )
    requested_at: str | None = None


class ProtocolHarness(LocalScenarioRunner):
    """One local case demonstrates request wire mismatch without a fabricated outage flag."""

    def __init__(
        self,
        kubeconfig: Path,
        evidence_root: Path,
        gateway: ProtocolAccess | None = None,
        observer: ProtocolObserver | None = None,
        timeout_seconds: float = 90,
        poll_seconds: float = 2,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """Share the canonical latch and retain the fixed ninety-second readiness allowance."""
        if timeout_seconds > 90 or poll_seconds > 2:
            raise ValueError("protocol timing exceeds frozen limits")
        self.access = gateway or ProtocolGateway(kubeconfig)
        super().__init__(kubeconfig, evidence_root, self.access, timeout_seconds, poll_seconds)
        self.observer = observer or RuntimeProtocolObserver(kubeconfig, self.access)
        self.clock = clock

    def _record(
        self,
        directory: Path,
        receipt: ScenarioReceipt,
        name: str,
        data: JsonObject,
        errors: list[str] | None = None,
    ) -> None:
        """Cleanup persistence failure must never suppress the other resource's restoration."""
        try:
            self._save(directory, receipt, name, data)
        except BaseException as error:
            if errors is None:
                raise
            errors.append(f"{name} persistence: {type(error).__name__}: {error}")

    def _specs(self, context: ProtocolRun, stage: ProtocolStage) -> dict[str, JsonObject]:
        """Only four fixed states may be selected by the closed internal lifecycle."""
        documents = deployment_map(context.original)
        return {
            "payments-api": context.variants["payments-api"]
            if stage == "matched"
            else object_value(documents["payments-api"]["spec"]),
            "risk-sim": context.variants["risk-sim"]
            if stage in {"mismatch", "matched"}
            else object_value(documents["risk-sim"]["spec"]),
        }

    def _settle(
        self,
        context: ProtocolRun,
        stage: ProtocolStage,
        directory: Path,
        receipt: ScenarioReceipt,
        changed: frozenset[str] = frozenset(),
        errors: list[str] | None = None,
    ) -> datetime:
        """Pin unchanged peers and require the intended new process for each forward rollout."""
        deadline = time.monotonic() + self.timeout
        specs, previous = self._specs(context, stage), context.identities
        for _ in range(46):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("protocol readiness deadline exceeded")
            state = self.access.state(min(30, remaining))
            self._record(
                directory,
                receipt,
                "runtime",
                {**state, "captured_at": self.clock().isoformat()},
                errors,
            )
            try:
                current = protocol_identities(
                    state, context.original, specs["payments-api"], specs["risk-sim"]
                )
                if any(
                    current[name] != identity
                    for name, identity in previous.items()
                    if name not in changed
                ):
                    raise ValueError("unmodified protocol peer changed process")
                if len(changed) == 1:
                    name = next(iter(changed))
                    fresh_protocol_process(
                        state, name, current[name], previous[name], context.requested_at or ""
                    )
            except ValueError:
                time.sleep(min(self.poll, max(0, deadline - time.monotonic())))
                continue
            if time.monotonic() > deadline:
                raise TimeoutError("protocol runtime read exceeded deadline")
            context.latest, context.identities = state, current
            return self.clock()
        raise TimeoutError("protocol readiness snapshot count exceeded")

    def _stage(
        self,
        context: ProtocolRun,
        stage: ProtocolStage,
        directory: Path,
        receipt: ScenarioReceipt,
        store: ArtifactStore,
        changed: frozenset[str] = frozenset(),
        errors: list[str] | None = None,
    ) -> None:
        """Saved observations must match runtime readiness and fresh request identity."""
        ready_at = self._settle(context, stage, directory, receipt, changed, errors)
        identities = context.identities.copy()
        observed = self.observer.collect(stage, receipt.run_id, identities, directory, store)
        self._record(
            directory, receipt, stage, JSON_OBJECT.validate_json(observed.model_dump_json()), errors
        )
        if (
            observed.incident_id != receipt.run_id
            or observed.identities != identities
            or observed.probe.mode != self.access.mode
            or observed.probe.started_at < ready_at
        ):
            raise ValueError("protocol observations disagree with runtime scope")
        if any(
            item.probe.sample.sample_id == observed.probe.sample.sample_id
            or item.probe.traceparent.split("-")[1] == observed.probe.traceparent.split("-")[1]
            for item in context.observations.values()
        ):
            raise ValueError("protocol stages reused sample or trace identity")
        verify_protocol_observation(stage, observed, store)
        self._settle(context, stage, directory, receipt, errors=errors)
        context.observations[stage] = observed

    def _prepare(
        self, directory: Path, receipt: ScenarioReceipt, store: ArtifactStore
    ) -> ProtocolRun:
        """Capture both originals and every future full spec before the first fault write."""
        self._save(directory, receipt, "scope", self.access.verify_scope())
        original = self.access.state()
        self._save(directory, receipt, "original", original)
        validate_runtime_baseline(original)
        documents = deployment_map(original)
        variants = {
            name: protocol_spec(documents[name], name) for name in ("payments-api", "risk-sim")
        }
        identities = protocol_identities(
            original,
            original,
            object_value(documents["payments-api"]["spec"]),
            object_value(documents["risk-sim"]["spec"]),
        )
        context = ProtocolRun(original, original, variants, identities)
        self._stage(context, "original", directory, receipt, store)
        receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
        self._save(
            directory,
            receipt,
            "protocol-journal",
            JSON_OBJECT.validate_python(
                {
                    "original": original,
                    "v2_specs": variants,
                    "plan": PLAN.model_dump(mode="json"),
                }
            ),
        )
        return context

    def _transition(
        self,
        context: ProtocolRun,
        target: ProtocolTarget,
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Persist exact expected UID/version/spec and transition time before CAS."""
        current = deployment_map(context.latest)[target]
        context.requested_at = self.clock().isoformat()
        self._save(
            directory,
            receipt,
            "transition-" + target,
            {
                "requested_at": context.requested_at,
                "expected": current,
                "next_spec": context.variants[target],
            },
        )
        self.access.replace_spec(target, current, context.variants[target])

    def run(
        self, case_id: CaseId = "ROLLOUT-04", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Match both versions before qualification; any earlier failure still restores both."""
        if case_id != "ROLLOUT-04":
            raise ValueError("protocol harness accepts only ROLLOUT-04")
        directory, receipt = self._start(case_id)
        context: ProtocolRun | None = None
        store: ArtifactStore | None = None
        try:
            store = ArtifactStore(directory / "artifacts")
            context = self._prepare(directory, receipt, store)
            receipt.injection_requested_at = utc_timestamp()
            self._transition(context, "risk-sim", directory, receipt)
            self._stage(context, "mismatch", directory, receipt, store, frozenset({"risk-sim"}))
            receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
            self._investigate(receipt, after_activation)
            self._transition(context, "payments-api", directory, receipt)
            self._stage(context, "matched", directory, receipt, store, frozenset({"payments-api"}))
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt, store)
        return receipt

    def _undo(self, context: ProtocolRun, target: ProtocolTarget) -> None:
        """Unknown state on one resource never authorizes overwriting it or skipping the other."""
        current = self.access.deployment(target)
        original = deployment_map(context.original)[target]
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("protocol Deployment identity changed")
        if current["spec"] == original["spec"]:
            return
        if current["spec"] != context.variants[target]:
            raise CleanupUnverified("protocol Deployment has unknown spec")
        self.access.replace_spec(target, current, object_value(original["spec"]))

    def _recover(
        self,
        context: ProtocolRun | None,
        directory: Path,
        receipt: ScenarioReceipt,
        store: ArtifactStore | None,
    ) -> None:
        """Always attempt both restorations before final v1 health and evidence verification."""
        errors: list[str] = []
        if context is not None and store is not None:
            receipt.cleanup_started_at = utc_timestamp()
            for target in ("payments-api", "risk-sim"):
                try:
                    self._undo(context, target)
                except BaseException as error:
                    errors.append(f"{target} restore: {type(error).__name__}: {error}")
                self._record(
                    directory, receipt, "cleanup-" + target, {"errors": list(errors)}, errors
                )
            try:
                self._stage(
                    context,
                    "final",
                    directory,
                    receipt,
                    store,
                    frozenset({"payments-api", "risk-sim"}),
                    errors,
                )
            except BaseException as error:
                errors.append(f"final verification: {type(error).__name__}: {error}")
            self._record(directory, receipt, "cleanup-verify", {"errors": list(errors)}, errors)
            if not errors:
                receipt.cleanup_verified, receipt.cleanup_verified_at = True, utc_timestamp()
        self._final_receipt(directory, receipt, errors)

    def _final_receipt(self, directory: Path, receipt: ScenarioReceipt, errors: list[str]) -> None:
        """A saved receipt and released canonical latch are both required for clean completion."""
        receipt.completed_at, receipt.cleanup_failure = utc_timestamp(), "; ".join(errors) or None
        try:
            with (directory / "receipt.json").open("x", encoding="utf-8") as stream:
                stream.write(receipt.model_dump_json(indent=2))
        except BaseException as error:
            errors.append(f"receipt persistence: {type(error).__name__}: {error}")
        if not errors:
            try:
                self.block_file.unlink()
            except BaseException as error:
                errors.append(f"latch release: {type(error).__name__}: {error}")
        if errors:
            receipt.cleanup_verified, receipt.cleanup_verified_at = False, None
            receipt.cleanup_failure = "; ".join(errors)
            try:
                self.block_file.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
            except BaseException as error:
                raise CleanupUnverified(
                    "protocol cleanup and latch persistence are unverified"
                ) from error
