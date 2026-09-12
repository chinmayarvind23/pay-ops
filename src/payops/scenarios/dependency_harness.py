"""Run dependency controls, actual outages and recovery under the shared cluster latch."""

import asyncio
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path

from payops.evidence.artifacts import JSON_OBJECT
from payops.scenarios.concurrency_harness import ConcurrencyHarness, ConcurrencyRun
from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_value,
    utc_timestamp,
)
from payops.scenarios.dependency_evidence import (
    dependency_workload,
    validate_archived_error,
    validate_delayed_metrics,
    validate_dependency_traffic,
)
from payops.scenarios.dependency_gateway import DependencyGateway, data_deployments
from payops.scenarios.dependency_pressure import postgres_pressure
from payops.scenarios.dependency_specs import RUNTIME_IMAGE_DIGEST, dependency_kind, dependency_spec
from payops.scenarios.protocol_gateway import protocol_identities
from payops.scenarios.runner import CleanupUnverified, sample_healthy
from payops.scenarios.sampling_gateway import deployment_map, validate_runtime_baseline
from payops.scenarios.traffic import TrafficDriver


class DependencyHarness(ConcurrencyHarness):
    """Share strict payments process ownership and exact restoration with memory experiments."""

    runtime_image_digest = RUNTIME_IMAGE_DIGEST
    access: DependencyGateway

    def __init__(self, kubeconfig: Path, evidence_root: Path, credentials: Path) -> None:
        """Credential files remain private inputs and are never included in evidence."""
        super().__init__(kubeconfig, evidence_root, DependencyGateway(kubeconfig))
        self.credentials = credentials
        self.data_original: JsonObject | None = None
        self.after_activation: Callable[[], None] | None = None

    def _prepare(self, directory: Path, receipt: ScenarioReceipt) -> ConcurrencyRun:
        """Capture both namespaces before any mutation and validate healthy payment flow."""
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
        data = self.access.data_state()
        self._save(directory, receipt, "data-baseline", data)
        for document in data_deployments(data).values():
            if (
                object_value(document["spec"]).get("replicas") != 1
                or object_value(document["status"]).get("readyReplicas") != 1
            ):
                raise ValueError("data services must start ready at one replica")
        self.data_original = data
        self._wait(directory, receipt, "healthy-before", self.access.healthy, sample_healthy)
        return ConcurrencyRun(
            state, dependency_spec(documents["payments-api"], receipt.case_id), identities
        )

    def _batch(
        self, context: ConcurrencyRun, stage: str, directory: Path, receipt: ScenarioReceipt
    ) -> str:
        """Persist every request and raw log before validating the three-request denominator."""
        identity = self._identities(context, self.access.state(), True)["payments-api"]
        output = directory / "traffic" / stage
        observed = asyncio.run(TrafficDriver(self.kubeconfig, output).run(dependency_workload()))
        plan = JSON_OBJECT.validate_json((output / observed.run_id / "plan.json").read_bytes())
        self._save(directory, receipt, stage + "-plan", plan)
        self._save(
            directory,
            receipt,
            stage + "-traffic",
            JSON_OBJECT.validate_json(observed.model_dump_json()),
        )
        _, _, raw = self._capture(context, False, directory, receipt)
        if (
            self._identities(context, self.access.state(), True)["payments-api"] != identity
            or observed.mode != self.access.mode
        ):
            raise ValueError("dependency process or acquisition mode changed")
        samples = validate_dependency_traffic(
            observed, plan, raw, dependency_kind(receipt.case_id), stage == "fault"
        )
        if context.samples.intersection(samples):
            raise ValueError("dependency sample reused")
        context.samples.update(samples)
        if receipt.case_id == "TELEM-02":
            validate_archived_error(raw, observed.started_at)
        return raw

    def _sessions(self, count: int, directory: Path, receipt: ScenarioReceipt) -> None:
        """Retain server-side role counts during pressure and after owned sessions close."""
        if count == 0:
            self._wait(
                directory,
                receipt,
                "postgres-drained",
                lambda: self.access.postgres_sessions(receipt.run_id),
                postgres_drained,
            )
            return
        state = self.access.postgres_sessions(receipt.run_id)
        self._save(directory, receipt, "postgres-sessions", state)
        if state.get("limit") != 4 or state.get("total") != count or state.get("owned") != count:
            raise ValueError("PostgreSQL role pressure differs from owned sessions")

    def _fault(self, context: ConcurrencyRun, directory: Path, receipt: ScenarioReceipt) -> None:
        """No fault can qualify without current HTTP and dependency evidence."""
        self._batch(context, "fault", directory, receipt)
        receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
        self._investigate(receipt, self.after_activation)

    def _experiment(
        self, context: ConcurrencyRun, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Hold one process constant across healthy, unavailable and recovered dependency states."""
        self._enable(context, directory, receipt)
        self._batch(context, "control", directory, receipt)
        receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
        metrics = self.access.metrics()
        self._save(directory, receipt, "metrics-control", metrics)
        receipt.injection_requested_at = utc_timestamp()
        if dependency_kind(receipt.case_id) == "postgres":
            self._sessions(0, directory, receipt)
            with postgres_pressure(self.access, self.credentials, receipt.run_id):
                self._sessions(4, directory, receipt)
                self._fault(context, directory, receipt)
                self._sessions(4, directory, receipt)
                current = self.access.metrics()
                self._save(directory, receipt, "metrics-fault", current)
                if receipt.case_id == "TELEM-04":
                    validate_delayed_metrics(metrics, current)
            self._sessions(0, directory, receipt)
        else:
            original = self.access.redis()
            if self.data_original is None:
                raise ValueError("missing data restoration source")
            expected = data_deployments(self.data_original)["redis"]
            if (
                original["spec"] != expected["spec"]
                or object_value(original["metadata"])["uid"]
                != object_value(expected["metadata"])["uid"]
            ):
                raise ValueError("Redis changed before injection")
            self._save(directory, receipt, "redis-transition", original)
            self.access.redis_replicas(original, 0)
            self._wait(directory, receipt, "redis-down", self.access.redis, redis_down)
            self._fault(context, directory, receipt)
            self._restore_data(directory, receipt)
        self._batch(context, "recovered", directory, receipt)

    def _restore_data(self, directory: Path, receipt: ScenarioReceipt) -> None:
        """Restore only the journaled Redis replica change and verify all data specs."""
        if self.data_original is None:
            return
        originals = data_deployments(self.data_original)
        prior = originals["redis"]
        current = self.access.redis()
        off = deepcopy(object_value(prior["spec"]))
        off["replicas"] = 0
        if object_value(current["metadata"])["uid"] != object_value(prior["metadata"])["uid"]:
            raise CleanupUnverified("Redis identity changed")
        if current["spec"] != prior["spec"]:
            if current["spec"] != off:
                raise CleanupUnverified("Redis changed outside the journal")
            self.access.redis_replicas(current, 1)
        self._wait(directory, receipt, "redis-restored", self.access.redis, redis_ready)
        state = self.access.data_state()
        self._save(directory, receipt, "data-restored", state)
        for name, document in data_deployments(state).items():
            if (
                document["spec"] != originals[name]["spec"]
                or object_value(document["metadata"])["uid"]
                != object_value(originals[name]["metadata"])["uid"]
            ):
                raise CleanupUnverified("data deployment changed outside run")

    def _restore_original(
        self, context: ConcurrencyRun, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Attempt both restorations independently; any unverified cleanup retains the latch."""
        errors: list[Exception] = []
        try:
            self._restore_data(directory, receipt)
            if dependency_kind(receipt.case_id) == "postgres":
                self._sessions(0, directory, receipt)
        except Exception as error:
            errors.append(error)
        try:
            super()._restore_original(context, directory, receipt)
        except Exception as error:
            errors.append(error)
        if errors:
            raise CleanupUnverified("; ".join(str(error) for error in errors))

    def run(
        self, case_id: CaseId = "DEP-03", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Run the full experiment and finalize exact-state recovery even after rejection."""
        dependency_kind(case_id)
        self.after_activation = after_activation
        directory, receipt = self._start(case_id)
        context: ConcurrencyRun | None = None
        try:
            context = self._prepare(directory, receipt)
            self._save(
                directory,
                receipt,
                "journal",
                {
                    "original": context.original,
                    "enabled": context.enabled,
                    "data": self.data_original,
                },
            )
            self._experiment(context, directory, receipt)
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt)
        return receipt


def redis_down(document: JsonObject) -> bool:
    """The controller must observe zero replicas before payment failures are sampled."""
    status = object_value(document.get("status", {}))
    return (
        object_value(document["spec"]).get("replicas") == 0
        and status.get("replicas", 0) == 0
        and status.get("observedGeneration") == object_value(document["metadata"]).get("generation")
    )


def postgres_drained(document: JsonObject) -> bool:
    """Backend disappearance can lag client close; only observed zero sessions proves cleanup."""
    return document.get("limit") == 4 and document.get("total") == 0 and document.get("owned") == 0


def redis_ready(document: JsonObject) -> bool:
    """Recovery needs an observed ready replacement; HTTP control separately verifies TLS PING."""
    status = object_value(document.get("status", {}))
    return (
        object_value(document["spec"]).get("replicas") == 1
        and status.get("readyReplicas") == 1
        and status.get("observedGeneration") == object_value(document["metadata"]).get("generation")
    )
