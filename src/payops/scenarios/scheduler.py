"""Journal and restore every admission object even when another restoration fails."""

from collections.abc import Callable
from pathlib import Path

from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
    utc_timestamp,
)
from payops.scenarios.recipes import VARIANTS
from payops.scenarios.runner import (
    CleanupUnverified,
    LocalScenarioRunner,
    deployment_ready,
    sample_healthy,
)
from payops.scenarios.scheduler_gateway import SchedulerAccess, SchedulerGateway, Slot
from payops.scenarios.scheduler_specs import (
    activated,
    capture,
    healthy_users,
    node_identity,
    plans,
    quota_converged,
    quota_usage_restored,
)


class SchedulerHarness(LocalScenarioRunner):
    """The shared latch covers three fixed objects with independently attempted restoration."""

    def __init__(
        self,
        kubeconfig: Path,
        evidence_root: Path,
        gateway: SchedulerAccess | None = None,
        timeout_seconds: float = 120,
        poll_seconds: float = 2,
    ) -> None:
        """Reuse receipt storage and bounded waits while retaining a separate mutation interface."""
        if not 0 < timeout_seconds <= 120:
            raise ValueError("scheduler wait outside reviewed bounds")
        self.access = gateway or SchedulerGateway(kubeconfig)
        super().__init__(kubeconfig, evidence_root, self.access, timeout_seconds, poll_seconds)

    def run(
        self, case_id: CaseId = "SCHED-01", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """No high-resource write can precede the complete journal of all possible specs."""
        if case_id != "SCHED-01":
            raise ValueError("scheduler harness accepts only SCHED-01")
        directory, receipt = self._start(case_id)
        original: dict[Slot, JsonObject] = {}
        injected: dict[Slot, JsonObject] = {}
        try:
            before = self._preflight(directory, receipt)
            original = capture(before)
            injected = plans(original)
            self._save(
                directory,
                receipt,
                "scheduler-journal",
                {
                    "variant": VARIANTS[case_id],
                    "original": {str(key): value for key, value in original.items()},
                    "injected": {str(key): value for key, value in injected.items()},
                    "node_identity": node_identity(before),
                    "order": ["quota", "limits", "payments"],
                },
            )
            self._apply(original, injected, before, directory, receipt)
            self._investigate(receipt, after_activation)
        except BaseException as exc:
            receipt.failure = f"{type(exc).__name__}: {exc}"
            if not isinstance(exc, Exception):
                raise
        finally:
            self._recover(original, injected, directory, receipt)
        return receipt

    def _preflight(self, directory: Path, receipt: ScenarioReceipt) -> JsonObject:
        """Capture healthy namespace users and current node/admission state before mutation."""
        self._save(directory, receipt, "scope", self.access.verify_scope())
        observed = self.access.snapshot()
        self._save(directory, receipt, "scheduler-before", observed)
        capture(observed)
        original = object_value(observed["deployment"])
        if not deployment_ready(original, object_value(original["spec"])):
            raise ValueError("payments baseline controller is not converged")
        self._wait(directory, receipt, "healthy-before", self.access.healthy, sample_healthy)
        return observed

    def _apply(
        self,
        original: dict[Slot, JsonObject],
        injected: dict[Slot, JsonObject],
        before: JsonObject,
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Converged quota and exact LimitRange permit admission before the scheduler experiment."""
        self.access.replace_resource("quota", original["quota"], injected["quota"])
        self._wait(
            directory,
            receipt,
            "quota-expanded",
            lambda: self.access.resource("quota"),
            lambda item: quota_converged(item, injected["quota"]),
        )
        self.access.replace_resource("limits", original["limits"], injected["limits"])
        self._wait(
            directory,
            receipt,
            "limits-expanded",
            lambda: self.access.resource("limits"),
            lambda item: item.get("spec") == injected["limits"],
        )
        receipt.injection_requested_at = utc_timestamp()
        self.access.replace_resource("payments", original["payments"], injected["payments"])
        old_uids = tuple(
            str(object_value(pod["metadata"])["uid"]) for pod in object_items(before["pods"])
        )
        self._wait(
            directory,
            receipt,
            "activation",
            self.access.snapshot,
            lambda item: activated(
                item,
                original["payments"],
                injected["payments"],
                str(receipt.injection_requested_at),
                node_identity(before),
                old_uids,
            ),
        )
        receipt.activated = True
        receipt.activation_observed_at = utc_timestamp()

    def _undo(self, slot: Slot, original: JsonObject, injected: JsonObject) -> None:
        """Never overwrite an unknown spec or another object, even during best-effort recovery."""
        current = self.access.resource(slot)
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified(f"{slot} identity changed")
        if current["spec"] == original["spec"]:
            return
        if current["spec"] != injected:
            raise CleanupUnverified(f"{slot} spec changed outside this run")
        self.access.replace_resource(slot, current, object_value(original["spec"]))

    def _deployment_recovery(
        self,
        original: dict[Slot, JsonObject],
        injected: dict[Slot, JsonObject],
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Restore service first and wait until the pending pod no longer occupies quota."""
        self._undo("payments", original["payments"], injected["payments"])
        expected = object_value(original["payments"]["spec"])
        self._wait(
            directory,
            receipt,
            "payments-restored",
            self.access.snapshot,
            lambda item: (
                deployment_ready(object_value(item["deployment"]), expected) and healthy_users(item)
            ),
        )
        self._wait(directory, receipt, "healthy-after", self.access.healthy, sample_healthy)
        self._wait(
            directory,
            receipt,
            "quota-drained",
            lambda: self.access.resource("quota"),
            quota_usage_restored,
        )

    def _recover(
        self,
        original: dict[Slot, JsonObject],
        injected: dict[Slot, JsonObject],
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Attempt each restoration despite earlier API, health or accounting failures."""
        failures: list[str] = []
        if original and injected:
            receipt.cleanup_started_at = utc_timestamp()
            operations: tuple[tuple[str, Callable[[], None]], ...] = (
                (
                    "payments",
                    lambda: self._deployment_recovery(original, injected, directory, receipt),
                ),
                ("limits", lambda: self._undo("limits", original["limits"], injected["limits"])),
                ("quota", lambda: self._undo("quota", original["quota"], injected["quota"])),
                ("verification", lambda: self._verify_restored(original, directory, receipt)),
            )
            for name, operation in operations:
                self._attempt_restore(name, operation, failures, directory, receipt)
            if not failures:
                receipt.cleanup_verified = True
                receipt.cleanup_verified_at = utc_timestamp()
        self._persist_final(failures, directory, receipt)

    def _attempt_restore(
        self,
        name: str,
        operation: Callable[[], None],
        failures: list[str],
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Audit storage failure must never prevent another independent resource restoration."""
        outcome: JsonObject = {"status": "completed"}
        try:
            operation()
        except BaseException as exc:
            failures.append(f"{name}: {type(exc).__name__}: {exc}")
            outcome = {"status": "failed", "error": failures[-1]}
        try:
            self._save(directory, receipt, f"cleanup-{name}", outcome)
        except BaseException as exc:
            failures.append(f"{name} persistence: {type(exc).__name__}: {exc}")

    def _persist_final(
        self,
        failures: list[str],
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Release only after final evidence is saved; otherwise retain or surface the latch."""
        receipt.cleanup_failure = "; ".join(failures) or None
        receipt.completed_at = utc_timestamp()
        try:
            (directory / "receipt.json").write_text(
                receipt.model_dump_json(indent=2), encoding="utf-8"
            )
        except BaseException as exc:
            failures.append(f"receipt persistence: {type(exc).__name__}: {exc}")
        if failures:
            receipt.cleanup_verified = False
            receipt.cleanup_verified_at = None
            receipt.cleanup_failure = "; ".join(failures)
            try:
                self.block_file.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
            except BaseException as exc:
                raise CleanupUnverified(
                    f"{receipt.cleanup_failure}; latch persistence: {exc}"
                ) from exc
        else:
            self.block_file.unlink()

    def _verify_restored(
        self,
        original: dict[Slot, JsonObject],
        directory: Path,
        receipt: ScenarioReceipt,
    ) -> None:
        """Require exact identities/specs, quota convergence and all five healthy users."""
        for slot in ("payments", "limits", "quota"):
            current = self.access.resource(slot)
            self._save(directory, receipt, f"final-{slot}", current)
            if (
                current["spec"] != original[slot]["spec"]
                or object_value(current["metadata"])["uid"]
                != object_value(original[slot]["metadata"])["uid"]
            ):
                raise CleanupUnverified(f"{slot} exact restoration not verified")
        self._wait(
            directory,
            receipt,
            "quota-restored",
            lambda: self.access.resource("quota"),
            lambda item: (
                quota_converged(item, object_value(original["quota"]["spec"]))
                and quota_usage_restored(item)
            ),
        )
        self._wait(directory, receipt, "all-services-restored", self.access.snapshot, healthy_users)
