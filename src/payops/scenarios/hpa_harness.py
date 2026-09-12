"""One journaled HPA cap experiment: idle, sustained demand, scale-out, and exact cleanup."""

import json
import subprocess
import time
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
    utc_timestamp,
)
from payops.scenarios.cpu_contract import cpu_specs
from payops.scenarios.cpu_sources import CpuRecord, record_delta, select_record
from payops.scenarios.hpa_contract import hpa_demand
from payops.scenarios.hpa_gateway import HpaGateway, Kind, owned_metadata
from payops.scenarios.hpa_job_identity import LoadProcess, split_load_pod
from payops.scenarios.hpa_metrics import validate_cpu_metrics
from payops.scenarios.hpa_receipt import validate_load_receipt
from payops.scenarios.hpa_runtime import hpa_identities, service_pods
from payops.scenarios.memory_provenance import timestamp
from payops.scenarios.recipes import container
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner, sample_healthy
from payops.scenarios.sampling_gateway import deployment_map, validate_runtime_baseline
from payops.scenarios.traffic import TrafficReceipt


class HpaHarness(LocalScenarioRunner):
    """Journal originals before creating experiment resources."""

    def __init__(
        self, kubeconfig: Path, evidence_root: Path, access: HpaGateway | None = None
    ) -> None:
        """Use existing local scope, exclusive latch, immutable artifacts and bounded transport."""
        self.access = access or HpaGateway(kubeconfig)
        super().__init__(kubeconfig, evidence_root, self.access, 180, 2)
        self.original: JsonObject | None = None
        self.enabled: JsonObject = {}
        self.directory = evidence_root
        self.receipt: ScenarioReceipt | None = None
        self.run_id = ""
        self.hpa_uid = ""
        self.load: LoadProcess | None = None

    def save(self, name: str, data: JsonObject) -> None:
        """Preserve every observation before evaluating it, including rejected metric windows."""
        assert self.receipt is not None
        self._save(self.directory, self.receipt, name, data)

    def spec(self, replicas: Literal[1, 2]) -> JsonObject:
        """Scaling may change only replicas; CPU work and every other field remain constant."""
        result = deepcopy(self.enabled)
        result["replicas"] = replicas
        return result

    def runtime(
        self, replicas: Literal[1, 2], with_load: bool
    ) -> tuple[JsonObject, tuple[PodIdentity, ...]]:
        """Count all service pods and exclude only an independently verified owned load process."""
        assert self.original is not None
        state = self.access.state()
        self.save("runtime", state)
        services = state
        if with_load:
            job = self.access.read_resource("job", self.run_id)
            self.save("job", job)
            services, observed = split_load_pod(state, job, self.run_id)
            if self.load is not None and self.load != observed:
                raise ValueError("load process was replaced during the experiment")
            self.load = observed
        identities = hpa_identities(services, self.original, self.spec(replicas), replicas)
        return state, identities["payments-api"]

    def demand(self, replicas: Literal[1, 2], since: datetime, saturated: bool) -> None:
        """Require fresh metrics between unchanged identities plus independent HPA cap status."""
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                _, before = self.runtime(replicas, saturated)
                raw = self.access.cpu_metrics()
                captured = datetime.now(UTC)
                self.save(
                    "cpu-metrics", {"captured_at": captured.isoformat(), "text": raw.decode()}
                )
                _, after = self.runtime(replicas, saturated)
                metric = validate_cpu_metrics(raw, before, after, since, captured)
                hpa = self.access.read_resource("hpa", self.run_id)
                self.save("hpa-demand", hpa)
                report = hpa_demand(hpa, self.hpa_uid, replicas, saturated=saturated)
                if (saturated and metric.average_utilization < 100) or (
                    not saturated and metric.average_utilization > 50
                ):
                    raise ValueError("independent CPU window does not support controller demand")
                self.save(
                    "demand-verified",
                    {
                        "replicas": replicas,
                        "saturated": saturated,
                        "since": since.isoformat(),
                        "raw_utilization": str(metric.average_utilization),
                        "hpa_utilization": report.cpu_utilization,
                    },
                )
                return
            except (ValueError, KeyError) as error:
                self.save("demand-rejected", {"reason": str(error)})
            time.sleep(2)
        raise TimeoutError("HPA demand did not converge within the frozen observation bound")

    def prepare(self) -> None:
        """Refuse active faults and journal the original before enabling CPU work."""
        self.save("scope", self.access.verify_scope())
        resources = self.access.experiment_resources()
        self.save("resources-before", resources)
        if resources["hpas"] or resources["jobs"]:
            raise ValueError("sandbox already has an HPA or Job")
        state = self.access.state()
        validate_runtime_baseline(state)
        original = deployment_map(state)["payments-api"]
        enabled, _ = cpu_specs(original)
        requests = object_value(object_value(container(enabled)["resources"])["requests"])
        if requests.get("cpu") != "50m":
            raise ValueError("HPA utilization requires the frozen 50m request")
        if not sample_healthy(self.access.healthy()):
            raise ValueError("baseline payment is unhealthy")
        self.save("journal", {"original": state, "enabled": enabled})
        self.original, self.enabled = state, enabled
        self.access.replace_spec("payments-api", original, enabled)
        self.settle(1, False)
        created = self.access.create_hpa(self.run_id)
        self.save("hpa-created", created)
        self.hpa_uid = str(owned_metadata("hpa", created, self.run_id)["uid"])

    def settle(self, replicas: Literal[1, 2], with_load: bool) -> datetime:
        """Wait for real converged replicas; do not project or fake Kubernetes controller counts."""
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                self.runtime(replicas, with_load)
                return datetime.now(UTC)
            except (ValueError, KeyError) as error:
                self.save("runtime-rejected", {"reason": str(error)})
            time.sleep(2)
        raise TimeoutError("HPA service runtime did not converge")

    def raise_cap(self) -> None:
        """Retry only controller resource-version conflicts while preserving the owned HPA UID."""
        for _ in range(8):
            current = self.access.read_resource("hpa", self.run_id)
            if object_value(current["metadata"])["uid"] != self.hpa_uid:
                raise ValueError("HPA was replaced")
            self.save("cap-transition", {"expected": current, "maximum": 2})
            try:
                self.save("cap-raised", self.access.set_cap(current, self.run_id, 2))
                return
            except subprocess.CalledProcessError as error:
                if "Conflict" not in str(error.stderr):
                    raise
        raise TimeoutError("HPA cap resource version remained unstable")

    def completed_load(self) -> TrafficReceipt:
        """Job success requires its full receipt within the observed process lifetime."""
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            job = self.access.read_resource("job", self.run_id)
            self.save("job-completion", job)
            status = object_value(job.get("status", {}))
            conditions = object_items(status.get("conditions", []))
            if any(c.get("type") == "Failed" and c.get("status") == "True" for c in conditions):
                raise ValueError("load Job failed")
            if any(c.get("type") == "Complete" and c.get("status") == "True" for c in conditions):
                return self.capture_load(job)
            time.sleep(2)
        raise TimeoutError("load Job completion was not observed")

    def capture_load(self, job: JsonObject) -> TrafficReceipt:
        """Bind exit zero and the receipt to an unchanged load process."""
        before = self.access.state()
        self.save("load-capture-before", before)
        _, process = split_load_pod(before, job, self.run_id)
        if self.load != process:
            raise ValueError("completed load differs from the observed process")
        pod = next(
            p
            for p in object_items(before["pods"])
            if object_value(p["metadata"])["uid"] == process.pod_uid
        )
        row = object_items(object_value(pod["status"])["containerStatuses"])[0]
        ended = object_value(object_value(row["state"]).get("terminated", {}))
        finished = timestamp(ended.get("finishedAt"))
        if type(ended.get("exitCode")) is not int or ended.get("exitCode") != 0 or finished is None:
            raise ValueError("load process did not exit successfully")
        raw = self.access.load_log(process.pod_name, self.run_id)
        self.save("load-raw", {"text": raw.decode(), "pod_uid": process.pod_uid})
        after = self.access.state()
        self.save("load-capture-after", after)
        if (
            split_load_pod(after, self.access.read_resource("job", self.run_id), self.run_id)[1]
            != process
        ):
            raise ValueError("load process changed during receipt acquisition")
        window = validate_load_receipt(raw, process, finished)
        self.save(
            "load-verified",
            {
                "samples": len(window.samples),
                "failures": window.failures,
                "started": window.started.isoformat(),
                "completed": window.completed.isoformat(),
            },
        )
        return TrafficReceipt.model_validate_json(json.dumps(json.loads(raw)["receipt"]))

    def replica_work(self, traffic: TrafficReceipt, since: datetime) -> None:
        """Prove that both owned replicas actually executed this Job's samples after scale-out."""
        state, before = self.runtime(2, True)
        counts: JsonObject = {}
        for identity in before:
            pod = next(
                p
                for p in service_pods(state, "payments-api")
                if object_value(p["metadata"])["uid"] == identity.pod_uid
            )
            raw = self.access.read_log(pod, False)
            self.save("replica-work-raw", raw)
            count = self.count_work(str(raw["text"]), traffic, identity, since)
            if count < 3:
                raise ValueError(
                    "each scaled replica needs three actual post-scale CPU completions"
                )
            counts[identity.pod_uid] = count
        if self.runtime(2, True)[1] != before:
            raise ValueError("payments processes changed during work capture")
        self.save("replica-work-verified", {"since": since.isoformat(), "counts": counts})

    def count_work(
        self, raw: str, traffic: TrafficReceipt, identity: PodIdentity, since: datetime
    ) -> int:
        """Join retained kernel completions to real HTTP attempt windows and positive CPU deltas."""
        attempts = {a.planned.sample.sample_id: a for a in traffic.attempts}
        accepted: set[str] = set()
        for line in raw.splitlines():
            _, _, payload = line.partition(" ")
            if '"synthetic.cpu"' not in payload:
                continue
            record = CpuRecord.model_validate_json(payload)
            attempt = attempts.get(record.sample_id)
            if attempt is None or attempt.started_at is None or record.before.started_at < since:
                continue
            verified = select_record(
                (line + "\n").encode(),
                attempt.planned.sample,
                attempt.started_at,
                attempt.completed_at,
            )
            if record_delta(verified, self.run_id, identity).usage_usec <= 0:
                raise ValueError("replica has no positive consumed CPU")
            accepted.add(record.sample_id)
        return len(accepted)

    def remove(self, kind: Kind) -> None:
        """Delete with fresh UID/version guards; reject foreign objects."""
        key = "hpas" if kind == "hpa" else "jobs"
        for _ in range(12):
            rows = object_items(self.access.experiment_resources()[key])
            if not rows:
                return
            if len(rows) != 1:
                raise CleanupUnverified("unexpected experiment resources during cleanup")
            owned_metadata(kind, rows[0], self.run_id)
            try:
                self.access.remove_owned(kind, rows[0], self.run_id)
            except subprocess.CalledProcessError as error:
                if "Conflict" not in str(error.stderr) and "NotFound" not in str(error.stderr):
                    raise
            time.sleep(2)
        raise CleanupUnverified("experiment resource deletion not observed")

    def cleanup(self) -> None:
        """Remove Job/HPA first, then restore the original runtime and payment health."""
        if self.original is None:
            return
        errors: list[str] = []
        for kind in ("job", "hpa"):
            try:
                self.remove(kind)
            except Exception as error:
                errors.append(str(error))
        if errors:
            raise CleanupUnverified("; ".join(errors))
        original = deployment_map(self.original)["payments-api"]
        current = self.access.deployment("payments-api")
        if object_value(current["metadata"])["uid"] != object_value(original["metadata"])["uid"]:
            raise CleanupUnverified("payments Deployment was replaced")
        if current["spec"] not in (original["spec"], self.spec(1), self.spec(2)):
            raise CleanupUnverified("payments spec changed outside the experiment")
        if current["spec"] != original["spec"]:
            self.access.replace_spec("payments-api", current, object_value(original["spec"]))
        self.enabled = object_value(original["spec"])
        self.settle(1, False)
        assert self.receipt is not None
        self._wait(
            self.directory, self.receipt, "healthy-restored", self.access.healthy, sample_healthy
        )
        self.save("resources-after", self.access.experiment_resources())

    def run(
        self, case_id: CaseId = "SCHED-03", after_activation: Callable[[], None] | None = None
    ) -> ScenarioReceipt:
        """Persist failures and cleanup independently across all experimental stages."""
        if case_id != "SCHED-03":
            raise ValueError("HPA harness accepts only SCHED-03")
        self.directory, receipt = self._start(case_id)
        self.receipt, self.run_id = receipt, receipt.run_id
        try:
            self.prepare()
            self.demand(1, datetime.now(UTC), False)
            receipt.control_verified, receipt.control_verified_at = True, utc_timestamp()
            receipt.injection_requested_at = utc_timestamp()
            self.save("job-created", self.access.create_load(self.run_id))
            loaded = self.settle(1, True)
            self.demand(1, loaded, True)
            receipt.activated, receipt.activation_observed_at = True, utc_timestamp()
            self._investigate(receipt, after_activation)
            self.raise_cap()
            scaled = self.settle(2, True)
            self.demand(2, scaled, True)
            traffic = self.completed_load()
            self.replica_work(traffic, scaled)
        except Exception as error:
            receipt.failure = f"{type(error).__name__}: {error}"
        finally:
            receipt.cleanup_started_at = utc_timestamp()
            try:
                self.cleanup()
                receipt.cleanup_verified, receipt.cleanup_verified_at = True, utc_timestamp()
            except Exception as error:
                receipt.cleanup_failure = f"{type(error).__name__}: {error}"
            receipt.completed_at = utc_timestamp()
            (self.directory / "receipt.json").write_text(
                receipt.model_dump_json(indent=2), encoding="utf-8"
            )
            if receipt.cleanup_verified:
                self.block_file.unlink()
        return receipt
