"""Contrast processor availability while independent measured CPU noise remains active."""

import asyncio
import time
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path

from payops.evidence.artifacts import JSON_OBJECT
from payops.scenarios.concurrency_harness import ConcurrencyHarness, ConcurrencyRun
from payops.scenarios.concurrency_traffic import payment_outcome, started_request_time
from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
    utc_timestamp,
)
from payops.scenarios.dependency_evidence import dependency_workload
from payops.scenarios.noise_contract import noise_window
from payops.scenarios.noise_gateway import NoiseGateway, noise_metadata, noise_pod
from payops.scenarios.runner import CleanupUnverified, sample_healthy
from payops.scenarios.sampling_gateway import (
    deployment_map,
    runtime_identities,
    validate_runtime_baseline,
)
from payops.scenarios.traffic import TrafficDriver, TrafficReceipt


class NoiseHarness(ConcurrencyHarness):
    """Reuse immutable receipts and finally cleanup; no model can select this workload."""

    access: NoiseGateway

    def __init__(self, kubeconfig: Path, evidence_root: Path) -> None:
        """Hold the same canonical latch as every other local experiment."""
        super().__init__(kubeconfig, evidence_root, NoiseGateway(kubeconfig))
        self.job: JsonObject | None = None
        self.noise_uid: str | None = None

    def _prepare(self, directory: Path, receipt: ScenarioReceipt) -> ConcurrencyRun:
        """Require a healthy baseline and no existing experiment controllers before writes."""
        self._save(directory, receipt, "scope", self.access.verify_scope())
        experiments = self.access.experiment_resources()
        self._save(directory, receipt, "existing-experiments", experiments)
        if experiments["jobs"] or experiments["hpas"]:
            raise ValueError("existing controller conflicts with CPU noise experiment")
        state = self.access.state()
        self._save(directory, receipt, "baseline", state)
        validate_runtime_baseline(state)
        self._wait(directory, receipt, "healthy-before", self.access.healthy, sample_healthy)
        off = deepcopy(object_value(deployment_map(state)["processor-adapter"]["spec"]))
        off["replicas"] = 0
        return ConcurrencyRun(state, off, {})

    def _snapshot(
        self, context: ConcurrencyRun, fault: bool, directory: Path, receipt: ScenarioReceipt
    ) -> JsonObject:
        """Retain the complete namespace then exclude only the verified owned Job pod."""
        state = self.access.state()
        self._save(directory, receipt, "runtime", state)
        if self.job is None:
            raise ValueError("missing owned CPU job")
        pod = noise_pod(state, self.job, receipt.run_id)
        uid = str(object_value(pod["metadata"])["uid"])
        if self.noise_uid is not None and self.noise_uid != uid:
            raise ValueError("CPU noise process changed")
        self.noise_uid = uid
        checked = deepcopy(state)
        checked["pods"] = [
            p for p in object_items(state["pods"]) if object_value(p["metadata"])["uid"] != uid
        ]
        verify_peers(checked, context.original, fault, context.enabled)
        return pod

    def _capture_noise(
        self, pod: JsonObject, observed: TrafficReceipt, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Wait for a later real CPU sample to cover the entire observed HTTP interval."""
        deadline = time.monotonic() + 6
        while True:
            raw = self.access.noise_log(pod, receipt.run_id)
            self._save(directory, receipt, "noise-log", raw)
            try:
                window = noise_window(str(raw["text"]), observed.started_at, observed.completed_at)
                self._save(directory, receipt, "noise-window", window)
                return
            except ValueError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)

    def _batch(
        self, context: ConcurrencyRun, stage: str, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """All three stages keep the noise running while only processor availability changes."""
        fault = stage == "fault"
        deadline = time.monotonic() + 15
        while True:
            try:
                self._snapshot(context, fault, directory, receipt)
                break
            except ValueError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
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
        validate_noise_traffic(observed, plan, fault)
        samples = {a.planned.sample.sample_id for a in observed.attempts}
        if context.samples.intersection(samples):
            raise ValueError("noise experiment reused a sample")
        context.samples.update(samples)
        pod = self._snapshot(context, fault, directory, receipt)
        self._capture_noise(pod, observed, directory, receipt)
        self._snapshot(context, fault, directory, receipt)

    def _restore_processor(self, context: ConcurrencyRun) -> None:
        """Only the exact zero-replica state may be replaced by the captured original."""
        original = deployment_map(context.original)["processor-adapter"]
        current = self.access.deployment("processor-adapter")
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("processor identity changed")
        if current["spec"] != original["spec"]:
            if current["spec"] != context.enabled:
                raise CleanupUnverified("processor changed outside journal")
            self.access.replace_spec("processor-adapter", current, object_value(original["spec"]))

    def _restore_original(
        self, context: ConcurrencyRun, directory: Path, receipt: ScenarioReceipt
    ) -> None:
        """Always attempt processor and owned Job cleanup independently; retain latch on failure."""
        errors: list[str] = []
        try:
            self._restore_processor(context)
            self._wait(directory, receipt, "healthy-restored", self.access.healthy, sample_healthy)
        except Exception as error:
            errors.append(str(error))
        try:
            state = self.access.noise_state(receipt.run_id)
            self._save(directory, receipt, "cleanup-inventory", state)
            for job in object_items(state["jobs"]):
                noise_metadata(job, receipt.run_id)
                self.access.remove_noise(job, receipt.run_id)
            self._wait(
                directory,
                receipt,
                "noise-removed",
                lambda: self.access.noise_state(receipt.run_id),
                noise_absent,
            )
            final = self.access.state()
            self._save(directory, receipt, "restored-runtime", final)
            verify_peers(final, context.original, False, context.enabled)
        except Exception as error:
            errors.append(str(error))
        if errors:
            raise CleanupUnverified("; ".join(errors))

    def _experiment(
        self,
        context: ConcurrencyRun,
        directory: Path,
        receipt: ScenarioReceipt,
        callback: Callable[[], None] | None,
    ) -> None:
        """Healthy and recovered payments with sustained noise separate correlation from cause."""
        self.job = self.access.create_noise(receipt.run_id)
        self._save(directory, receipt, "noise-created", self.job)
        self._wait(
            directory,
            receipt,
            "noise-started",
            lambda: self.access.noise_state(receipt.run_id),
            lambda state: noise_running(state, self.job, receipt.run_id),
        )
        time.sleep(2)
        self._batch(context, "control", directory, receipt)
        receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
        original = deployment_map(context.original)["processor-adapter"]
        receipt.injection_requested_at = utc_timestamp()
        self.access.replace_spec("processor-adapter", original, context.enabled)
        self._wait(
            directory,
            receipt,
            "processor-down",
            lambda: self.access.observe("processor-adapter"),
            processor_down,
        )
        self._batch(context, "fault", directory, receipt)
        receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
        self._investigate(receipt, callback)
        self._restore_processor(context)
        self._wait(directory, receipt, "processor-recovered", self.access.healthy, sample_healthy)
        self._batch(context, "recovered", directory, receipt)

    def run(
        self, case_id: CaseId = "TELEM-01", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Qualification requires sustained measured noise in every stage and full recovery."""
        if case_id != "TELEM-01":
            raise ValueError("noise harness accepts only TELEM-01")
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
            self._experiment(context, directory, receipt, after_activation)
        except BaseException as error:
            receipt.failure = f"{type(error).__name__}: {error}"
            if not isinstance(error, Exception):
                raise
        finally:
            self._recover(context, directory, receipt)
        return receipt


def processor_down(state: JsonObject) -> bool:
    """Zero ready replicas alone is insufficient; require no remaining processor pod."""
    document = object_value(state["deployment"])
    return (
        not state["pods"]
        and object_value(document["spec"]).get("replicas") == 0
        and object_value(document["status"]).get("observedGeneration")
        == object_value(document["metadata"]).get("generation")
    )


def noise_running(state: JsonObject, job: JsonObject | None, run_id: str) -> bool:
    """Pending startup observations remain evidence but cannot establish a running worker."""
    try:
        if job is None:
            return False
        noise_pod(state, job, run_id)
        return True
    except (KeyError, ValueError):
        return False


def noise_absent(state: JsonObject) -> bool:
    """No Job or leftover noise pod may remain when the latch is released."""
    return not state["jobs"] and not any(
        str(object_value(p["metadata"])["name"]).startswith("cpu-noise-")
        for p in object_items(state["pods"])
    )


def verify_peers(state: JsonObject, original: JsonObject, fault: bool, off: JsonObject) -> None:
    """Preserve four peer processes and all deployment specs while the processor is absent."""
    documents, prior = deployment_map(state), deployment_map(original)
    for name, document in documents.items():
        expected = off if fault and name == "processor-adapter" else prior[name]["spec"]
        if (
            document["spec"] != expected
            or object_value(document["metadata"])["uid"]
            != object_value(prior[name]["metadata"])["uid"]
        ):
            raise ValueError("noise experiment deployment drift")
    before = {
        object_value(p["metadata"])["uid"]: p
        for p in object_items(original["pods"])
        if not str(object_value(p["metadata"])["name"]).startswith("processor-adapter-")
    }
    after = {
        object_value(p["metadata"])["uid"]: p
        for p in object_items(state["pods"])
        if not str(object_value(p["metadata"])["name"]).startswith("processor-adapter-")
    }
    if set(before) != set(after) or len(before) != 4:
        raise ValueError("unrelated peer pod changed")
    for uid, pod in before.items():
        statuses = object_items(object_value(after[uid]["status"])["containerStatuses"])
        initial = object_items(object_value(pod["status"])["containerStatuses"])
        if (
            after[uid]["spec"] != pod["spec"]
            or len(statuses) != 1
            or statuses[0].get("ready") is not True
            or any(
                statuses[0].get(key) != initial[0].get(key)
                for key in ("containerID", "restartCount", "imageID")
            )
        ):
            raise ValueError("unrelated peer process changed")
    if fault:
        if len(object_items(state["pods"])) != 4:
            raise ValueError("processor still has a pod")
    else:
        runtime_identities(state, original, object_value(prior["processor-adapter"]["spec"]))


def validate_noise_traffic(receipt: TrafficReceipt, plan: JsonObject, fault: bool) -> None:
    """Require the fixed three-request denominator and actual 503 availability failures."""
    if (
        receipt.status != "completed"
        or receipt.failure
        or receipt.mode != "local_kind"
        or len(receipt.attempts) != 3
        or receipt.role != "payments"
        or plan.get("run_id") != receipt.run_id
        or plan.get("probe") is not False
        or plan.get("workload") != dependency_workload().model_dump(mode="json")
        or plan.get("attempts") != [a.planned.model_dump(mode="json") for a in receipt.attempts]
    ):
        raise ValueError("noise traffic plan mismatch")
    for index, attempt in enumerate(receipt.attempts):
        started_request_time(attempt, receipt)
        if (
            attempt.planned.sample.sample_id != f"synthetic-{receipt.run_id}-{index}"
            or payment_outcome(attempt) == fault
            or (fault and attempt.http_status != 503)
        ):
            raise ValueError("noise request outcome mismatch")
