"""Capture, inject, observe and restore before another scenario may start."""

import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

from pydantic import TypeAdapter

from payops.sandbox.models import SimulationResult
from payops.scenarios.contracts import (
    Artifact,
    CaseId,
    ClusterGateway,
    JsonObject,
    ScenarioReceipt,
    object_value,
    utc_timestamp,
)
from payops.scenarios.kubectl import KubectlGateway
from payops.scenarios.recipes import VARIANTS, activation, fault_spec, target, validate_baseline


class CleanupUnverified(RuntimeError):
    """Continuing would contaminate later incidents and invalidate their benchmark results."""


def sample_healthy(observation: JsonObject) -> bool:
    """A 200 alone is insufficient; require a validated accepted synthetic payment result."""
    if observation.get("sample_status") != 200:
        return False
    result = SimulationResult.model_validate_json(str(observation.get("sample_body", "")))
    return result.role == "payments" and result.status == "accepted"


class LocalScenarioRunner:
    """This operator-only runner has no model-facing tool registration or generic patch API."""

    def __init__(
        self,
        kubeconfig: Path,
        evidence_root: Path,
        gateway: ClusterGateway | None = None,
        timeout_seconds: float = 90,
        poll_seconds: float = 2,
    ) -> None:
        """Bound total activation/cleanup waits and persist a cross-process contamination latch."""
        if not 0 < timeout_seconds <= 180 or not 0 < poll_seconds <= 5:
            raise ValueError("scenario timing outside bounded local limits")
        self.gateway = gateway or KubectlGateway(kubeconfig)
        self.root = evidence_root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout_seconds
        self.poll = poll_seconds
        self.block_file = kubeconfig.resolve().with_name("payops-dev-scenario-lock.json")

    def _save(self, directory: Path, receipt: ScenarioReceipt, name: str, data: JsonObject) -> None:
        """Exclusive files and hashes preserve failed observations instead of rewriting history."""
        content = json.dumps(data, sort_keys=True, indent=2).encode()
        filename = f"{len(receipt.artifacts):03d}-{name}.json"
        with (directory / filename).open("xb") as output:
            output.write(content)
        receipt.artifacts.append(
            Artifact(name=filename, sha256=hashlib.sha256(content).hexdigest())
        )

    def _wait(
        self,
        directory: Path,
        receipt: ScenarioReceipt,
        name: str,
        collect: Callable[[], JsonObject],
        accept: Callable[[JsonObject], bool],
    ) -> JsonObject:
        """Every sampled state is retained, including the final non-activating timeout."""
        deadline = time.monotonic() + self.timeout
        while True:
            observed = collect()
            self._save(directory, receipt, name, observed)
            if accept(observed):
                return observed
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{name} deadline exceeded")
            time.sleep(self.poll)

    def _activation(self, case_id: CaseId) -> JsonObject:
        """Only dependency outages need a live payment failure in addition to pod evidence."""
        observed = self.gateway.observe(target(case_id))
        if case_id == "DEP-01":
            observed.update(self.gateway.healthy())
        return observed

    def _restore(
        self,
        case_id: CaseId,
        original: JsonObject,
        injected: JsonObject,
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Restore exact captured spec only if no concurrent operator changed the resource."""
        current = self.gateway.deployment(target(case_id))
        current_spec = object_value(current["spec"])
        original_spec = object_value(original["spec"])
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("Deployment identity changed")
        if current_spec != original_spec:
            if current_spec != injected:
                raise CleanupUnverified("Deployment spec changed outside this run")
            self.gateway.replace_spec(target(case_id), current, original_spec)
        self._wait(
            directory,
            receipt,
            "restored",
            lambda: self.gateway.observe(target(case_id)),
            lambda observed: deployment_ready(object_value(observed["deployment"]), original_spec),
        )
        self._wait(directory, receipt, "healthy-after", self.gateway.healthy, sample_healthy)
        receipt.cleanup_verified = True
        receipt.cleanup_verified_at = utc_timestamp()

    def run(
        self, case_id: CaseId, after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Receipt failures are returned after cleanup; an unverified cleanup blocks later runs."""
        case_id = TypeAdapter[CaseId](CaseId).validate_python(case_id)
        directory, receipt = self._start(case_id)
        original: JsonObject | None = None
        injected: JsonObject | None = None
        try:
            original = self._baseline(case_id, directory, receipt)
            injected = fault_spec(case_id, object_value(original["spec"]))
            self._inject(case_id, original, injected, directory, receipt)
            self._investigate(receipt, after_activation)
        except BaseException as exc:
            receipt.failure = f"{type(exc).__name__}: {exc}"
            if not isinstance(exc, Exception):
                raise
        finally:
            self._finish(case_id, original, injected, directory, receipt)
        return receipt

    def _start(self, case_id: CaseId) -> tuple[Path, ScenarioReceipt]:
        """A kubeconfig-sibling latch coordinates one cluster across different evidence roots."""
        if self.block_file.exists():
            raise CleanupUnverified("prior cleanup unresolved; inspect persisted receipt")
        receipt = ScenarioReceipt(
            run_id=uuid4().hex,
            case_id=case_id,
            mode=self.gateway.mode,
            implementation_variant=VARIANTS[case_id],
        )
        try:
            with self.block_file.open("x", encoding="utf-8") as latch:
                latch.write(receipt.model_dump_json(indent=2))
        except FileExistsError as exc:
            raise CleanupUnverified("another scenario owns the sandbox") from exc
        directory = self.root / receipt.run_id
        directory.mkdir()
        return directory, receipt

    def _baseline(self, case_id: CaseId, directory: Path, receipt: ScenarioReceipt) -> JsonObject:
        """Capture reviewed state and prove workload health before deriving any mutation."""
        self._save(directory, receipt, "scope", self.gateway.verify_scope())
        original = self.gateway.deployment(target(case_id))
        spec = validate_baseline(original, target(case_id))
        self._save(directory, receipt, "original", original)
        self._wait(
            directory,
            receipt,
            "baseline-ready",
            lambda: self.gateway.observe(target(case_id)),
            lambda observed: deployment_ready(object_value(observed["deployment"]), spec),
        )
        self._wait(directory, receipt, "healthy-before", self.gateway.healthy, sample_healthy)
        return original

    def _inject(
        self,
        case_id: CaseId,
        original: JsonObject,
        injected: JsonObject,
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """The caller holds restoration state before the first possibly ambiguous API write."""
        self._save(directory, receipt, "injection-plan", {"spec": injected})
        receipt.injection_requested_at = utc_timestamp()
        self.gateway.replace_spec(target(case_id), original, injected)
        self._wait(
            directory,
            receipt,
            "activation",
            lambda: self._activation(case_id),
            lambda observed: activation(case_id, observed),
        )
        receipt.activated = True
        receipt.activation_observed_at = utc_timestamp()

    @staticmethod
    def _investigate(receipt: ScenarioReceipt, callback: Callable[[], None] | None) -> None:
        """A label-free bound collector runs while the fault is active, before finally cleanup."""
        if callback is not None:
            try:
                callback()
                receipt.investigation_status = "completed"
            except BaseException as exc:
                receipt.investigation_status = "failed"
                receipt.investigation_failure = f"{type(exc).__name__}: {exc}"
                raise

    def _finish(
        self,
        case_id: CaseId,
        original: JsonObject | None,
        injected: JsonObject | None,
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Even ambiguous patch failures attempt recovery and retain an auditable final receipt."""
        if original is not None and injected is not None:
            receipt.cleanup_started_at = utc_timestamp()
            try:
                self._restore(case_id, original, injected, directory, receipt)
            except Exception as exc:
                receipt.cleanup_failure = f"{type(exc).__name__}: {exc}"
                self.block_file.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        receipt.completed_at = utc_timestamp()
        (directory / "receipt.json").write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        if injected is None or receipt.cleanup_verified:
            self.block_file.unlink()


def deployment_ready(document: JsonObject, expected_spec: JsonObject) -> bool:
    """Controller observation, replica convergence and exact restoration must all agree."""
    metadata = object_value(document["metadata"])
    status = object_value(document.get("status", {}))
    return (
        document.get("spec") == expected_spec
        and status.get("observedGeneration") == metadata.get("generation")
        and status.get("replicas") == expected_spec.get("replicas")
        and status.get("updatedReplicas") == expected_spec.get("replicas")
        and status.get("readyReplicas") == expected_spec.get("replicas")
        and status.get("availableReplicas") == expected_spec.get("replicas")
    )
