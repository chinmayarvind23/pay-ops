"""Fixture-only scheduler proof and every ambiguous three-resource recovery boundary."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from pydantic import JsonValue
from test_memory import MemoryCluster, add_provenance, memory_observation, replace_field

from payops.scenarios.contracts import JsonObject, ScenarioReceipt, object_items, object_value
from payops.scenarios.recipes import container, fault_spec
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner
from payops.scenarios.scheduler import SchedulerHarness
from payops.scenarios.scheduler_gateway import SchedulerGateway, Slot
from payops.scenarios.scheduler_specs import (
    LIMITS,
    QUOTA,
    SERVICES,
    activated,
    capture,
    node_identity,
    plans,
    quota_usage_restored,
)


def nodes() -> list[JsonValue]:
    """Model real22CPU nodes with the observed control-plane taint and eligible worker."""
    return [
        {
            "metadata": {"name": "payops-dev-" + suffix, "uid": suffix + "-uid"},
            "spec": {
                "taints": [{"key": "node-role.kubernetes.io/control-plane", "effect": "NoSchedule"}]
                if suffix == "control-plane"
                else []
            },
            "status": {
                "allocatable": {"cpu": "22", "memory": "16124080Ki"},
                "conditions": [
                    {"type": name, "status": "True" if name == "Ready" else "False"}
                    for name in ("Ready", "MemoryPressure", "DiskPressure", "PIDPressure")
                ],
            },
        }
        for suffix in ("control-plane", "worker")
    ]


class SchedulerFixture(MemoryCluster):
    """The fake records each write and can lose a response before or after any apply."""

    def __init__(self, fail_at: int = 0, after_apply: bool = False) -> None:
        """Keep independent full resource documents so recovery cannot silently rewrite baseline."""
        super().__init__()
        resources = object_value(container(object_value(self.original["spec"]))["resources"])
        object_value(resources["requests"])["cpu"] = "50m"
        object_value(resources["limits"])["cpu"] = "500m"
        self.current = deepcopy(self.original)
        self.documents: dict[Slot, JsonObject] = {"payments": self.current}
        for slot, name, spec in (
            ("quota", "sandbox-budget", QUOTA),
            ("limits", "sandbox-container-bounds", LIMITS),
        ):
            self.documents[cast(Slot, slot)] = {
                "metadata": {
                    "name": name,
                    "namespace": "payops-sandbox",
                    "uid": slot + "-uid",
                    "resourceVersion": "1",
                },
                "spec": deepcopy(spec),
                "status": {
                    "hard": deepcopy(QUOTA["hard"]),
                    "used": {
                        "requests.cpu": "250m",
                        "limits.cpu": "2500m",
                        "requests.memory": "480Mi",
                        "limits.memory": "1280Mi",
                        "pods": "5",
                        "services.nodeports": "0",
                        "services.loadbalancers": "0",
                    },
                },
            }
        self.originals = deepcopy(self.documents)
        self.fail_at, self.after_apply = fail_at, after_apply
        self.writes: list[Slot] = []
        self.stale = False
        self.lag = False
        self.pending_quota = False

    def resource(self, slot: Slot) -> JsonObject:
        """Each API read returns an independent document, including controller quota accounting."""
        return deepcopy(self.documents[slot])

    def replace_resource(self, slot: Slot, expected: JsonObject, spec: JsonObject) -> None:
        """Any request may fail after the server accepted it, requiring journal-based recovery."""
        assert expected == self.documents[slot]
        self.writes.append(slot)
        failing = len(self.writes) == self.fail_at
        if failing and not self.after_apply:
            raise RuntimeError("fixture response rejected before apply")
        self.documents[slot]["spec"] = deepcopy(spec)
        if slot == "payments":
            self.current = self.documents[slot]
            metadata = object_value(self.current["metadata"])
            metadata["generation"] = int(str(metadata["generation"])) + 1
            object_value(self.current["status"])["observedGeneration"] = metadata["generation"]
        if slot == "quota" and not self.lag:
            object_value(self.documents[slot]["status"])["hard"] = deepcopy(spec["hard"])
        if failing:
            raise RuntimeError("fixture response lost after apply")

    def snapshot(self) -> JsonObject:
        """A request23 fixture has real-shaped Pending/Unschedulable state, never a live claim."""
        observed = memory_observation()
        observed["deployment"] = deepcopy(self.documents["payments"])
        cpu = object_value(
            object_value(container(object_value(self.current["spec"]))["resources"])["requests"]
        )["cpu"]
        memory = object_value(
            object_value(container(object_value(self.current["spec"]))["resources"])["requests"]
        )["memory"]
        if cpu == "23" or memory == "16Gi":
            pod = object_items(observed["pods"])[0]
            object_value(pod["metadata"])["uid"] = "pending-pod"
            pod["status"] = {
                "phase": "Pending",
                "containerStatuses": [],
                "conditions": [
                    {
                        "type": "PodScheduled",
                        "status": "False",
                        "reason": "Unschedulable",
                        "lastTransitionTime": datetime.now(UTC).replace(microsecond=0).isoformat(),
                    }
                ],
            }
        observed = add_provenance(observed)
        object_value(object_items(observed["pods"])[0]["metadata"])["name"] = (
            "payments-api-fixture-pod"
        )
        observed["events"] = [
            {
                "metadata": {
                    "name": "payments-api-fixture-pod.event",
                    "namespace": "payops-sandbox",
                },
                "involvedObject": {
                    "name": "payments-api-fixture-pod",
                    "uid": "pending-pod",
                    "kind": "Pod",
                    "namespace": "payops-sandbox",
                },
                "reason": "FailedScheduling",
                "source": {"component": "default-scheduler"},
                "lastTimestamp": "2020-01-01T00:00:00Z"
                if self.stale
                else datetime.now(UTC).isoformat(),
                "message": "Insufficient memory" if memory == "16Gi" else "Insufficient cpu",
            }
        ]
        observed.update(
            {
                "nodes": nodes(),
                "quotas": [self.resource("quota")],
                "limit_ranges": [self.resource("limits")],
                "other_workloads": [],
            }
        )
        return namespace_users(observed, self.original)


def namespace_users(observed: JsonObject, original: JsonObject) -> JsonObject:
    """Build the remaining four normal service users around the actual fixture target state."""
    pods: list[JsonValue] = list(object_items(observed["pods"]))
    deployments: list[JsonValue] = [observed["deployment"]]
    object_value(object_value(pods[0])["metadata"])["labels"] = {
        "app.kubernetes.io/name": "payments-api"
    }
    for name in sorted(SERVICES - {"payments-api"}):
        document = deepcopy(original)
        object_value(document["metadata"]).update({"name": name, "uid": name + "-uid"})
        deployments.append(document)
        pod: JsonObject = {
            "metadata": {"uid": name + "-pod", "labels": {"app.kubernetes.io/name": name}},
            "spec": deepcopy(object_value(original["spec"])["template"]),
            "status": {"containerStatuses": [{"ready": True, "state": {"running": {}}}]},
        }
        pod["spec"] = deepcopy(object_value(object_value(original["spec"])["template"])["spec"])
        object_value(pod["metadata"]).update(
            {
                "namespace": "payops-sandbox",
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": name + "-rs",
                        "uid": name + "-rs-uid",
                        "controller": True,
                    }
                ],
            }
        )
        replicas = object_items(observed["replica_sets"])
        replicas.append(
            {
                "metadata": {
                    "name": name + "-rs",
                    "uid": name + "-rs-uid",
                    "namespace": "payops-sandbox",
                    "ownerReferences": [
                        {
                            "apiVersion": "apps/v1",
                            "kind": "Deployment",
                            "name": name,
                            "uid": name + "-uid",
                            "controller": True,
                        }
                    ],
                },
                "spec": {"template": deepcopy(object_value(document["spec"])["template"])},
            }
        )
        observed["replica_sets"] = list(replicas)
        pods.append(pod)
    observed["namespace_pods"], observed["deployments"] = pods, deployments
    return observed


def task(tmp_path: Path, fixture: SchedulerFixture) -> SchedulerHarness:
    """Millisecond fixture waits exercise rejection quickly without real cluster calls."""
    return SchedulerHarness(tmp_path / "kubeconfig", tmp_path / "evidence", fixture, 0.005, 0.001)


def test_complete_three_resource_journal_and_exact_cleanup(tmp_path: Path) -> None:
    """All resources are journaled before expansion and restored in deliberate order."""
    fixture = SchedulerFixture()
    runner = task(tmp_path, fixture)
    receipt = runner.run()
    assert receipt.activated and receipt.cleanup_verified and not receipt.failure
    assert fixture.writes == ["quota", "limits", "payments", "payments", "limits", "quota"]
    assert all(
        fixture.documents[slot]["spec"] == original["spec"]
        for slot, original in fixture.originals.items()
    )
    assert not runner.block_file.exists()
    journal = json.loads(
        next((runner.root / receipt.run_id).glob("*-scheduler-journal.json")).read_text()
    )
    assert set(journal["original"]) == {"payments", "quota", "limits"}


@pytest.mark.parametrize("fail_at", [1, 2, 3])
@pytest.mark.parametrize("after_apply", [False, True])
def test_every_ambiguous_injection_write_recovers(
    tmp_path: Path, fail_at: int, after_apply: bool
) -> None:
    """Rejected and lost responses at all three writes converge to the exact originals."""
    fixture = SchedulerFixture(fail_at, after_apply)
    receipt = task(tmp_path, fixture).run()
    assert receipt.failure and not receipt.activated and receipt.cleanup_verified
    assert all(
        fixture.documents[slot]["spec"] == original["spec"]
        for slot, original in fixture.originals.items()
    )


@pytest.mark.parametrize("fail_at", [4, 5, 6])
@pytest.mark.parametrize("after_apply", [False, True])
def test_cleanup_attempts_every_resource_after_failure(
    tmp_path: Path, fail_at: int, after_apply: bool
) -> None:
    """Cleanup failures retain the latch even if a lost response actually restored the object."""
    fixture = SchedulerFixture(fail_at, after_apply)
    runner = task(tmp_path, fixture)
    receipt = runner.run()
    assert receipt.activated and not receipt.cleanup_verified and receipt.cleanup_failure
    assert fixture.writes[-2:] == ["limits", "quota"] and runner.block_file.exists()
    with pytest.raises(CleanupUnverified):
        runner.run()


@pytest.mark.parametrize("slot", ["payments", "quota", "limits"])
@pytest.mark.parametrize("change", ["uid", "spec"])
def test_foreign_resource_is_never_overwritten(tmp_path: Path, slot: Slot, change: str) -> None:
    """Concurrent changes preserve their state while independent known resources still restore."""
    fixture = SchedulerFixture()

    def tamper() -> None:
        """Model a separate operator replacing identity or an unplanned spec field."""
        if change == "uid":
            object_value(fixture.documents[slot]["metadata"])["uid"] = "foreign"
        else:
            object_value(fixture.documents[slot]["spec"])["foreign"] = True

    runner = task(tmp_path, fixture)
    receipt = runner.run(after_activation=tamper)
    assert receipt.cleanup_failure and not receipt.cleanup_verified and runner.block_file.exists()
    assert len(fixture.writes) == 5


def test_stale_event_and_quota_lag_never_activate(tmp_path: Path) -> None:
    """A old scheduler event or unacknowledged allowance cannot count as a current reproduction."""
    for attribute in ("stale", "lag"):
        fixture = SchedulerFixture()
        setattr(fixture, attribute, True)
        runner = task(tmp_path / attribute, fixture)
        receipt = runner.run()
        assert not receipt.activated and receipt.failure and receipt.cleanup_verified


def test_pending_quota_does_not_skip_admission_restoration(tmp_path: Path) -> None:
    """Stale quota usage blocks release but must not skip either admission-object undo."""
    fixture = SchedulerFixture()

    def retained_usage() -> None:
        """The API can retain old pod accounting even after the service has recovered."""
        object_value(object_value(fixture.documents["quota"]["status"])["used"])["requests.cpu"] = (
            "23"
        )

    runner = task(tmp_path, fixture)
    receipt = runner.run(after_activation=retained_usage)
    assert receipt.activated and not receipt.cleanup_verified
    assert fixture.writes[-2:] == ["limits", "quota"] and runner.block_file.exists()


def test_callback_interrupt_still_restores_all_three(tmp_path: Path) -> None:
    """KeyboardInterrupt is re-raised only after the finally block attempts complete cleanup."""
    fixture = SchedulerFixture()
    runner = task(tmp_path, fixture)

    def interrupted() -> None:
        """An operator interruption must not strand expanded admission policy."""
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        runner.run(after_activation=interrupted)
    assert fixture.writes[-3:] == ["payments", "limits", "quota"] and not runner.block_file.exists()


@pytest.mark.parametrize(
    "path,value",
    [
        (("quotas", 0, "spec", "hard", "limits.memory"), "8Gi"),
        (("limit_ranges", 0, "spec", "limits", 0, "max", "memory"), "2Gi"),
        (("nodes", 1, "status", "allocatable", "cpu"), "24"),
        (("namespace_pods", 0, "spec", "containers", 0, "image"), "other"),
        (("other_workloads",), [{"kind": "Job"}]),
        (("quotas", 0, "metadata", "namespace"), "other"),
    ],
)
def test_changed_preflight_is_rejected(path: tuple[str | int, ...], value: JsonValue) -> None:
    """Scope or non-CPU allowance changes cannot be silently incorporated into this fixed recipe."""
    observed = SchedulerFixture().snapshot()
    replace_field(observed, path, value)
    with pytest.raises(ValueError):
        capture(observed)


def pending_evidence() -> tuple[JsonObject, JsonObject, JsonObject, str, JsonObject]:
    """Return a valid owned Pending positive control before independently corrupting one signal."""
    fixture = SchedulerFixture()
    before = fixture.snapshot()
    original = capture(before)
    injected = plans(original)
    fixture.replace_resource("payments", original["payments"], injected["payments"])
    return (
        fixture.snapshot(),
        original["payments"],
        injected["payments"],
        (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        node_identity(before),
    )


@pytest.mark.parametrize(
    "path,value",
    [
        (("pods", 0, "status", "phase"), "Running"),
        (("pods", 0, "spec", "nodeName"), "payops-dev-worker"),
        (("pods", 0, "status", "conditions", 0, "reason"), "SchedulingGated"),
        (("pods", 0, "status", "conditions", 0, "lastTransitionTime"), "2020-01-01T00:00:00Z"),
        (("events", 0, "reason"), "FailedCreate"),
        (("events", 0, "metadata", "namespace"), "foreign"),
        (("events", 0, "involvedObject", "name"), "foreign-pod"),
        (("events", 0, "message"), "node selector mismatch"),
        (("events", 0, "involvedObject", "uid"), "foreign"),
        (("events", 0, "source", "component"), "untrusted-logger"),
        (("events", 0, "lastTimestamp"), "2020-01-01T00:00:00Z"),
        (("events", 0, "series"), {"lastObservedTime": "2099-01-01T00:00:00Z"}),
        (("deployment", "metadata", "uid"), "foreign"),
        (("replica_sets", 0, "metadata", "ownerReferences", 0, "uid"), "foreign"),
    ],
)
def test_false_scheduler_proofs_reject(path: tuple[str | int, ...], value: JsonValue) -> None:
    """A wrong reason, ownership or timestamp invalidates otherwise plausible Pending evidence."""
    observed, original, injected, requested, identity = pending_evidence()
    assert activated(observed, original, injected, requested, identity, ("current-pod",))
    replace_field(observed, path, value)
    assert not activated(observed, original, injected, requested, identity, ("current-pod",))


def test_gateway_cas_has_only_fixed_object_and_atomic_preconditions(tmp_path: Path) -> None:
    """The real gateway emits structured fixed kubectl argv and tests UID/version/full spec."""
    config = tmp_path / "config"
    config.write_text("fixture")
    expected = SchedulerFixture().resource("quota")
    injected = deepcopy(object_value(expected["spec"]))
    object_value(injected["hard"])["requests.cpu"] = "24"
    with (
        patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"),
        patch.object(SchedulerGateway, "verify_scope", return_value={}),
        patch.object(SchedulerGateway, "resource", return_value=expected),
        patch.object(SchedulerGateway, "_invoke", return_value="") as invoke,
    ):
        SchedulerGateway(config).replace_resource("quota", expected, injected)
    args = invoke.call_args.args[0]
    assert args[:3] == ("patch", "resourcequota", "sandbox-budget")
    operations = json.loads(args[-1])
    assert [item["path"] for item in operations] == [
        "/metadata/uid",
        "/metadata/resourceVersion",
        "/spec",
        "/spec",
    ]


def test_wrong_harness_and_invalid_quantities_fail_closed(tmp_path: Path) -> None:
    """No generic rollout fallback or malformed quota quantity can bypass scheduler safeguards."""
    fixture = SchedulerFixture()
    with pytest.raises(ValueError):
        LocalScenarioRunner(tmp_path / "config", tmp_path / "evidence", fixture).run("SCHED-01")
    with pytest.raises(ValueError):
        fault_spec("SCHED-01", object_value(fixture.current["spec"]))
    with pytest.raises(ValueError):
        task(tmp_path, fixture).run("OOM-01")
    for invalid in ("NaN", "-1", "unknown", None):
        quota = fixture.resource("quota")
        object_value(object_value(quota["status"])["used"])["requests.cpu"] = invalid
        with pytest.raises(ValueError):
            quota_usage_restored(quota)


@pytest.mark.parametrize(
    "path,value",
    [
        (("nodes",), []),
        (("namespace_pods", 0, "metadata", "namespace"), "foreign"),
        (("namespace_pods", 0, "metadata", "ownerReferences", 0, "uid"), "foreign"),
        (("deployments", 1, "metadata", "namespace"), "foreign"),
        (("namespace_pods", 1, "spec", "initContainers"), [{"name": "extra"}]),
        (("namespace_pods", 1, "spec", "containers", 0, "command"), ["other"]),
        (
            ("namespace_pods", 1, "spec", "containers", 0, "env"),
            [{"name": "UNREVIEWED", "value": "true"}],
        ),
        (("quotas", 0, "status", "used", "requests.cpu"), "5"),
    ],
)
def test_preflight_rejection_never_changes_any_resource(
    tmp_path: Path, path: tuple[str | int, ...], value: JsonValue
) -> None:
    """Bad namespace users or baseline accounting abort before the journal's first API write."""
    fixture = SchedulerFixture()
    snapshot = fixture.snapshot()
    replace_field(snapshot, path, value)
    runner = task(tmp_path, fixture)
    with patch.object(fixture, "snapshot", return_value=snapshot):
        receipt = runner.run()
    assert receipt.failure and not receipt.activated and not fixture.writes
    assert not runner.block_file.exists()


def test_unconverged_baseline_and_timing_bounds_reject(tmp_path: Path) -> None:
    """A healthy-looking pod cannot hide an unobserved baseline Deployment generation."""
    fixture = SchedulerFixture()
    object_value(fixture.current["status"])["observedGeneration"] = 0
    receipt = task(tmp_path, fixture).run()
    assert receipt.failure and not fixture.writes
    with pytest.raises(ValueError):
        SchedulerHarness(tmp_path / "config", tmp_path / "other", fixture, 121)


def test_node_identity_drift_and_nonreviewed_request_reject() -> None:
    """Same names with replaced node UIDs or a different request do not prove the fixed case."""
    observed, original, injected, requested, identity = pending_evidence()
    replace_field(observed, ("nodes", 1, "metadata", "uid"), "new-node")
    assert not activated(observed, original, injected, requested, identity, ())
    observed, original, injected, requested, identity = pending_evidence()
    object_value(object_value(container(injected)["resources"])["requests"])["cpu"] = "1"
    assert not activated(observed, original, injected, requested, identity, ())


def test_fixed_gateway_read_bounds_and_runtime_slot_rejection(tmp_path: Path) -> None:
    """Every read target is fixed; namespace inventory cannot grow without an explicit failure."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with (
        patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"),
        patch.object(SchedulerGateway, "_json", return_value={"items": []}) as query,
        patch.object(SchedulerGateway, "observe", return_value={"pods": []}),
    ):
        gateway = SchedulerGateway(config)
        gateway.resource("quota")
        assert query.call_args.args[0] == ("get", "resourcequota", "sandbox-budget")
        with pytest.raises(ValueError):
            gateway.resource(cast(Slot, "nodes"))
        result = gateway.snapshot()
        assert result["other_workloads"] == [] and result["nodes"] == []
        assert all(call.args[0][0] == "get" for call in query.call_args_list)
        query.return_value = {"items": [{}] * 33}
        with pytest.raises(ValueError):
            gateway.snapshot()


def test_gateway_refuses_concurrent_cas_change(tmp_path: Path) -> None:
    """No patch is issued when a fresh API read differs from the expected captured spec."""
    config = tmp_path / "config"
    config.write_text("fixture")
    expected = SchedulerFixture().resource("quota")
    changed = deepcopy(expected)
    object_value(changed["metadata"])["uid"] = "foreign"
    with (
        patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"),
        patch.object(SchedulerGateway, "verify_scope", return_value={}),
        patch.object(SchedulerGateway, "resource", return_value=changed),
        patch.object(SchedulerGateway, "_invoke") as invoke,
    ):
        with pytest.raises(ValueError):
            SchedulerGateway(config).replace_resource("quota", expected, QUOTA)
    invoke.assert_not_called()


def test_shared_latch_blocks_other_output_root(tmp_path: Path) -> None:
    """Scheduler and earlier harnesses coordinate the same cluster independently of output path."""
    fixture = SchedulerFixture()
    runner = task(tmp_path, fixture)
    runner.block_file.write_text("unresolved other run")
    with pytest.raises(CleanupUnverified):
        SchedulerHarness(tmp_path / "kubeconfig", tmp_path / "different", fixture).run()


class AuditFailureHarness(SchedulerHarness):
    """A fixture storage adapter fails only the receipt after the payments restore attempt."""

    def _save(self, directory: Path, receipt: ScenarioReceipt, name: str, data: JsonObject) -> None:
        """API operations remain real fixture calls while this one persistence boundary fails."""
        if name == "cleanup-payments":
            raise OSError("fixture audit volume unavailable")
        super()._save(directory, receipt, name, data)


@pytest.mark.parametrize("api_failure", [False, True])
def test_cleanup_record_failure_cannot_skip_remaining_restores(
    tmp_path: Path, api_failure: bool
) -> None:
    """Success-record and failure-record disk errors still attempt both admission restorations."""
    fixture = SchedulerFixture(4 if api_failure else 0)
    runner = AuditFailureHarness(
        tmp_path / "kubeconfig", tmp_path / "evidence", fixture, 0.005, 0.001
    )
    receipt = runner.run()
    assert fixture.writes[-2:] == ["limits", "quota"]
    assert not receipt.cleanup_verified and "persistence" in str(receipt.cleanup_failure)
    assert runner.block_file.exists()
    assert fixture.documents["limits"]["spec"] == fixture.originals["limits"]["spec"]
    assert fixture.documents["quota"]["spec"] == fixture.originals["quota"]["spec"]


@pytest.mark.parametrize("latch_fails", [False, True])
def test_final_receipt_failure_never_releases_latch(tmp_path: Path, latch_fails: bool) -> None:
    """Final disk failures are exposed after all restoration calls and leave the original latch."""
    fixture = SchedulerFixture()
    runner = task(tmp_path, fixture)
    write_text = Path.write_text

    def disk_error(
        path: Path,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        """The initial exclusive latch write succeeds; only final receipt persistence is broken."""
        if path.name == "receipt.json" or (latch_fails and path == runner.block_file):
            raise OSError("fixture final disk failure")
        return write_text(path, data, encoding=encoding, errors=errors, newline=newline)

    with patch.object(Path, "write_text", autospec=True, side_effect=disk_error):
        if latch_fails:
            with pytest.raises(CleanupUnverified, match="latch persistence"):
                runner.run()
        else:
            receipt = runner.run()
            assert not receipt.cleanup_verified and "receipt persistence" in str(
                receipt.cleanup_failure
            )
    assert runner.block_file.exists() and fixture.writes[-3:] == ["payments", "limits", "quota"]


class SurgeQuotaFixture(SchedulerFixture):
    """Model observed RollingUpdate overlap: pending23 plus peers2 plus new replacement0.5CPU."""

    def __init__(self) -> None:
        """The overlap exists only after an actual fault-template apply in this fixture."""
        super().__init__()
        self.fault_seen = False
        self.surge_blocked = False
        self.pending_limit = Decimal(0)

    def replace_resource(self, slot: Slot, expected: JsonObject, spec: JsonObject) -> None:
        """A successful restore patch can still fail to create its replacement due to quota."""
        if slot == "payments":
            request = object_value(object_value(container(spec)["resources"])["requests"])["cpu"]
            quota = object_value(object_value(self.documents["quota"]["spec"])["hard"])
            if request == "23":
                self.fault_seen = True
                self.pending_limit = cpu_limit(spec)
            elif self.fault_seen:
                required = (
                    self.pending_limit
                    + (len(SERVICES) - 1) * cpu_limit(object_value(self.original["spec"]))
                    + cpu_limit(spec)
                )
                self.surge_blocked = Decimal(str(quota["limits.cpu"])) < required
        super().replace_resource(slot, expected, spec)
        if self.surge_blocked:
            object_value(self.documents["payments"]["status"])["readyReplicas"] = 0
            object_value(object_value(self.documents["quota"]["status"])["used"])["limits.cpu"] = (
                "25"
            )


def cpu_limit(spec: JsonObject) -> Decimal:
    """Derive replica limit in cores from actual fixture specs, including Kubernetes millicores."""
    value = str(object_value(object_value(container(spec)["resources"])["limits"])["cpu"])
    return Decimal(value[:-1]) / 1000 if value.endswith("m") else Decimal(value)


@pytest.mark.parametrize("insufficient", ["25", "25.4"])
def test_actual_rolling_update_overlap_requires_recovery_headroom(
    tmp_path: Path, insufficient: str
) -> None:
    """The failed live25CPU allowance reproduces cleanup failure; reviewed26CPU permits recovery."""
    original_plans = plans

    def without_headroom(original: dict[Slot, JsonObject]) -> dict[Slot, JsonObject]:
        """Reproduce the previous committed recipe without rewriting its retained live artifacts."""
        result = original_plans(original)
        object_value(result["quota"]["hard"])["limits.cpu"] = insufficient
        return result

    failed_fixture = SurgeQuotaFixture()
    failed_runner = task(tmp_path / "old", failed_fixture)
    with patch("payops.scenarios.scheduler.plans", side_effect=without_headroom):
        failed = failed_runner.run()
    assert failed.activated and not failed.cleanup_verified and failed_runner.block_file.exists()
    assert failed_fixture.writes[-2:] == ["limits", "quota"]
    corrected_fixture = SurgeQuotaFixture()
    corrected_runner = task(tmp_path / "corrected", corrected_fixture)
    corrected = corrected_runner.run()
    assert (
        corrected.activated
        and corrected.cleanup_verified
        and not corrected_runner.block_file.exists()
    )
