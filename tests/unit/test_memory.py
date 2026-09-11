"""Dry memory-workload and journal tests never claim actual kernel OOM execution."""

import asyncio
import json
import subprocess
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from pydantic import JsonValue
from test_scenarios import FakeCluster

from payops.scenarios.contracts import DeploymentName, JsonObject, object_items, object_value
from payops.scenarios.kubectl import KubectlGateway
from payops.scenarios.memory import MemoryRead, control_holding
from payops.scenarios.memory_provenance import current_pods, fault_activated
from payops.scenarios.memory_workload import (
    CHUNK_BYTES,
    MIB,
    TARGET_BYTES,
    allocate_workload,
    container_memory,
    create_app,
    main,
    record_phase,
    touched_chunk,
    validate_container,
)
from payops.scenarios.recipes import container, fault_spec, memory_control_spec, oom_killed
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner


def memory_observation(phase: str = "holding", age: int = 0) -> JsonObject:
    """A labeled fixture log models actual recorder fields for parser counterfactual tests."""
    stamp = (datetime.now(UTC) - timedelta(seconds=age)).isoformat()
    event = {
        "event": "synthetic.memory",
        "phase": phase,
        "allocated_bytes": 128 * MIB,
        "cgroup_current_bytes": 192 * MIB,
        "cgroup_limit_bytes": 256 * MIB,
        "hold_seconds": 20,
    }
    return {
        "pods": [
            {
                "metadata": {"uid": "current-pod"},
                "status": {
                    "containerStatuses": [
                        {
                            "name": "sandbox",
                            "ready": True,
                            "restartCount": 0,
                            "state": {"running": {}},
                        }
                    ]
                },
            }
        ],
        "memory_logs": [
            {"pod_uid": "current-pod", "previous": False, "text": stamp + " " + json.dumps(event)}
        ],
    }


class MemoryCluster(FakeCluster):
    """Three-state fake API exercises CAS/cleanup without manufacturing live Kubernetes claims."""

    def __init__(self, failure_at: int = 0, after_apply: bool = False) -> None:
        """Use the reviewed256Mi baseline and optionally interrupt one specific write."""
        super().__init__("OOM-01")
        container(object_value(self.original["spec"]))["resources"] = {
            "requests": {"memory": "96Mi"},
            "limits": {"memory": "256Mi"},
        }
        self.current = deepcopy(self.original)
        self.failure_at, self.after_apply = failure_at, after_apply

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """An API error can happen before or after the desired state actually reached the server."""
        failing = self.patches + 1 == self.failure_at
        if failing and not self.after_apply:
            self.patches += 1
            raise RuntimeError("write rejected before apply")
        super().replace_spec(name, expected, spec)
        if spec == self.original["spec"]:
            self.current = deepcopy(self.original)
        else:
            metadata = object_value(self.current["metadata"])
            metadata["generation"] = int(str(metadata["generation"])) + 1
            object_value(self.current["status"])["observedGeneration"] = metadata["generation"]
        if failing:
            raise RuntimeError("response lost after apply")


class MemoryFixture:
    """Explicit fixture observations are separate from the real fixed kubectl adapter."""

    def __init__(self, cluster: MemoryCluster, hold: bool = True, oom: bool = True) -> None:
        """Disable a real acceptance predicate to exercise timeout and known-state recovery."""
        self.cluster, self.hold, self.oom = cluster, hold, oom

    def collect(self) -> JsonObject:
        """Produce control-hold or OOM-shaped unit data according to the fake API spec."""
        observed = memory_observation("holding" if self.hold else "released")
        observed["deployment"] = deepcopy(self.cluster.current)
        item = container(object_value(self.cluster.current["spec"]))
        limit = object_value(object_value(item["resources"])["limits"])["memory"]
        if limit == "128Mi":
            observed["pods"] = [
                {
                    "metadata": {"uid": "fault-pod"},
                    "status": {
                        "containerStatuses": [
                            {
                                "name": "sandbox",
                                "restartCount": 1,
                                "lastState": {
                                    "terminated": {
                                        "reason": "OOMKilled" if self.oom else "Error",
                                        "exitCode": 137,
                                    }
                                },
                            }
                        ]
                    },
                }
            ]
        return add_provenance(observed)


def add_provenance(observed: JsonObject) -> JsonObject:
    """Unit-only ownership and lifetime fields model the API chain the real collector reads."""
    deployment = object_value(observed["deployment"])
    spec = object_value(deployment["spec"])
    stamp = datetime.now(UTC).replace(microsecond=0).isoformat()
    pod = object_items(observed["pods"])[0]
    metadata = object_value(pod["metadata"])
    metadata.update(
        {
            "namespace": "payops-sandbox",
            "creationTimestamp": stamp,
            "ownerReferences": [
                {
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "payments-api-rs",
                    "uid": "replica-uid",
                    "controller": True,
                }
            ],
        }
    )
    pod["spec"] = deepcopy(object_value(spec["template"])["spec"])
    for status in object_items(object_value(pod["status"])["containerStatuses"]):
        if "lastState" in status:
            object_value(object_value(status["lastState"])["terminated"]).update(
                {"startedAt": stamp, "finishedAt": stamp}
            )
    observed["replica_sets"] = [
        {
            "metadata": {
                "name": "payments-api-rs",
                "uid": "replica-uid",
                "namespace": "payops-sandbox",
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "payments-api",
                        "uid": "fixture-uid",
                        "controller": True,
                    }
                ],
            },
            "spec": {"template": deepcopy(spec["template"])},
        }
    ]
    return observed


def runner(
    tmp_path: Path, cluster: MemoryCluster, hold: bool = True, oom: bool = True
) -> LocalScenarioRunner:
    """No real memory collector or network path exists in these journal tests."""
    return LocalScenarioRunner(
        tmp_path / "config",
        tmp_path / "evidence",
        cluster,
        0.01,
        0.001,
        memory=MemoryFixture(cluster, hold, oom),
    )


def test_control_changes_only_memory_limit_for_fault(tmp_path: Path) -> None:
    """The same workload/image is used; only the control-to-fault memory limit changes."""
    cluster = MemoryCluster()
    original = object_value(cluster.original["spec"])
    control = memory_control_spec(original)
    fault = fault_spec("OOM-01", original)
    limits = object_value(object_value(container(control)["resources"])["limits"])
    limits["memory"] = "128Mi"
    assert control == fault and original == cluster.original["spec"]
    task = runner(tmp_path, cluster)
    receipt = task.run("OOM-01")
    assert receipt.control_verified and receipt.activated and receipt.cleanup_verified
    assert cluster.current == cluster.original and cluster.patches == 3
    assert not task.block_file.exists()
    journal = next((task.root / receipt.run_id).glob("*-mutation-journal.json"))
    saved = json.loads(journal.read_text())
    assert (
        saved["original"] == original and saved["fault"] == fault and saved["uid"] == "fixture-uid"
    )


@pytest.mark.parametrize("failure_at,after_apply", [(1, False), (1, True), (2, False), (2, True)])
def test_journal_recovers_every_known_intermediate(
    tmp_path: Path, failure_at: int, after_apply: bool
) -> None:
    """Control and fault writes may fail ambiguously without leaving a reviewed state unhandled."""
    cluster = MemoryCluster(failure_at, after_apply)
    task = runner(tmp_path, cluster)
    receipt = task.run("OOM-01")
    assert receipt.failure is not None and receipt.cleanup_verified and not receipt.activated
    assert cluster.current == cluster.original and not task.block_file.exists()
    assert receipt.control_verified is (failure_at == 2)


@pytest.mark.parametrize("hold,oom", [(False, True), (True, False)])
def test_missing_control_or_real_oom_predicate_cannot_pass(
    tmp_path: Path, hold: bool, oom: bool
) -> None:
    """A ready pod or generic137 cannot replace the actual control and kernel OOM predicates."""
    cluster = MemoryCluster()
    task = runner(tmp_path, cluster, hold, oom)
    receipt = task.run("OOM-01")
    assert not receipt.activated and receipt.cleanup_verified and receipt.failure
    assert cluster.current == cluster.original
    assert cluster.patches == (2 if not hold else 3)


def test_cleanup_failure_retains_cluster_latch(tmp_path: Path) -> None:
    """An accepted OOM fixture cannot allow the next case when restoration is unverified."""
    cluster = MemoryCluster(3)
    task = runner(tmp_path, cluster)
    receipt = task.run("OOM-01")
    assert receipt.activated and not receipt.cleanup_verified and receipt.cleanup_failure
    assert task.block_file.exists()
    with pytest.raises(CleanupUnverified):
        task.run("OOM-01")


def test_unknown_concurrent_spec_is_not_restored(tmp_path: Path) -> None:
    """A spec outside the persisted three-state journal remains untouched for operator review."""
    cluster = MemoryCluster()
    task = runner(tmp_path, cluster)

    def changed() -> None:
        """Model an independent operator changing a different memory limit during investigation."""
        item = container(object_value(cluster.current["spec"]))
        object_value(object_value(item["resources"])["limits"])["memory"] = "192Mi"

    receipt = task.run("OOM-01", changed)
    assert receipt.activated and not receipt.cleanup_verified and task.block_file.exists()
    assert cluster.patches == 2


def test_control_logs_reject_stale_released_or_wrong_pod() -> None:
    """A matching word in old or unrelated logs never proves current resident workload pressure."""
    assert control_holding(memory_observation())
    assert not control_holding(memory_observation(age=16))
    assert not control_holding(memory_observation(age=-5))
    observed = memory_observation()
    logs = observed["memory_logs"]
    assert isinstance(logs, list)
    entry = object_value(logs[0])
    released = memory_observation("released")["memory_logs"]
    assert isinstance(released, list)
    entry["text"] = str(entry["text"]) + "\n" + str(object_value(released[0])["text"])
    assert not control_holding(observed)
    entry["pod_uid"] = "other-pod"
    assert not control_holding(observed)
    assert not control_holding({"pods": []})


@pytest.mark.parametrize(
    "line",
    [
        "holding",
        "bad-time {}",
        "2026-01-01T00:00:00 {}",
        "2026-01-01T00:00:00+00:00 not-json",
        '2026-01-01T00:00:00+00:00 {"event":"unrelated"}',
    ],
)
def test_control_log_parser_rejects_unstructured_text(line: str) -> None:
    """Only timestamped structured runtime events can satisfy the control proof."""
    observed = memory_observation()
    logs = observed["memory_logs"]
    assert isinstance(logs, list)
    object_value(logs[0])["text"] = line
    assert not control_holding(observed)


def test_kernel_oom_reason_and_restart_are_required() -> None:
    """OOMKilled is distinct from ordinary exit137 or a process with no observed restart."""
    assert not oom_killed({"status": {"containerStatuses": []}})
    base: JsonObject = {
        "name": "sandbox",
        "restartCount": 1,
        "lastState": {"terminated": {"reason": "OOMKilled", "exitCode": 137}},
    }
    assert oom_killed({"status": {"containerStatuses": [base]}})
    base["restartCount"] = 0
    assert not oom_killed({"status": {"containerStatuses": [base]}})


def test_fixed_worker_touches_pages_and_releases_on_stop() -> None:
    """One small chunk verifies physical page touching; the full workload uses mocked allocation."""
    chunk = touched_chunk()
    assert len(chunk) == CHUNK_BYTES and sum(chunk) == CHUNK_BYTES // 4096
    stop = MagicMock(spec=Event)
    stop.wait.return_value = False
    stop.is_set.return_value = False
    with (
        patch(
            "payops.scenarios.memory_workload.touched_chunk", return_value=bytearray(1)
        ) as allocate,
        patch("payops.scenarios.memory_workload.record_phase") as record,
    ):
        allocate_workload(stop)
    assert allocate.call_count == TARGET_BYTES // CHUNK_BYTES
    assert record.call_args_list[-2].args == ("holding", TARGET_BYTES)
    assert record.call_args_list[-1].args == ("released", 0)


@pytest.mark.parametrize("where", ["grace", "before_chunk", "between_chunks"])
def test_worker_cancellation_is_bounded(where: str) -> None:
    """Cancellation prevents further allocation and releases already allocated blocks."""
    stop = MagicMock(spec=Event)
    stop.wait.side_effect = [True] if where == "grace" else [False, True]
    stop.is_set.return_value = where == "before_chunk"
    with (
        patch(
            "payops.scenarios.memory_workload.touched_chunk", return_value=bytearray(1)
        ) as allocate,
        patch("payops.scenarios.memory_workload.record_phase") as record,
    ):
        allocate_workload(stop)
    assert allocate.call_count == (1 if where == "between_chunks" else 0)
    if where != "grace":
        assert record.call_args_list[-1].args == ("released", 0)


def test_container_guard_and_real_cgroup_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    """The entrypoint refuses host allocation or a container outside the reviewed memory bounds."""
    with patch("payops.scenarios.memory_workload.sys.platform", "win32"):
        with pytest.raises(ValueError):
            validate_container()
    monkeypatch.setenv("PAYOPS_SANDBOX_ROLE", "payments")
    monkeypatch.setenv("PAYOPS_SYNTHETIC_MEMORY_WORKLOAD", "bounded-v1")
    with (
        patch("payops.scenarios.memory_workload.sys.platform", "linux"),
        patch(
            "payops.scenarios.memory_workload.container_memory", return_value=(64 * MIB, 256 * MIB)
        ),
    ):
        validate_container()
    with (
        patch("payops.scenarios.memory_workload.sys.platform", "linux"),
        patch(
            "payops.scenarios.memory_workload.container_memory", return_value=(64 * MIB, 1024 * MIB)
        ),
    ):
        with pytest.raises(ValueError):
            validate_container()
    with patch.object(Path, "read_text", side_effect=[str(64 * MIB), str(256 * MIB)]):
        assert container_memory() == (64 * MIB, 256 * MIB)


def test_runtime_record_contains_actual_accounting(capsys: pytest.CaptureFixture[str]) -> None:
    """The printed record uses kernel-reader results rather than repeating an allocation target."""
    with patch(
        "payops.scenarios.memory_workload.container_memory", return_value=(192 * MIB, 256 * MIB)
    ):
        record_phase("holding", 128 * MIB)
    record = json.loads(capsys.readouterr().out)
    assert record["allocated_bytes"] == 128 * MIB and record["cgroup_current_bytes"] == 192 * MIB


@pytest.mark.parametrize("stuck", [False, True])
def test_lifespan_stops_owned_worker(stuck: bool) -> None:
    """Server teardown signals and joins only its own worker, surfacing a failed join."""

    async def lifespan(app: FastAPI) -> None:
        """Exercise the actual wrapper without allocating memory or opening a server port."""
        async with app.router.lifespan_context(app):
            pass

    with (
        patch("payops.scenarios.memory_workload.validate_container"),
        patch("payops.scenarios.memory_workload.sandbox_app", return_value=FastAPI()),
        patch("payops.scenarios.memory_workload.Thread") as thread,
    ):
        thread.return_value.is_alive.return_value = stuck
        app = create_app()
        if stuck:
            with pytest.raises(RuntimeError):
                asyncio.run(lifespan(app))
        else:
            asyncio.run(lifespan(app))
    thread.return_value.start.assert_called_once()
    thread.return_value.join.assert_called_once_with(timeout=5)


def test_entrypoint_and_memory_reader_have_fixed_targets(tmp_path: Path) -> None:
    """Both startup and reads remain closed operator capabilities, not command forwarding."""
    with (
        patch("payops.scenarios.memory_workload.create_app", return_value=FastAPI()),
        patch("payops.scenarios.memory_workload.uvicorn.run") as serve,
    ):
        main()
    assert serve.call_args.kwargs == {"host": "0.0.0.0", "port": 8080}
    with patch("payops.scenarios.memory.KubectlGateway") as gateway:
        gateway.return_value.memory_observation.return_value = {"pods": []}
        assert MemoryRead(tmp_path / "config").collect() == {"pods": []}


def test_bounded_current_and_previous_memory_logs(tmp_path: Path) -> None:
    """The real adapter uses only validated pod names and fixed byte/line/time bounds."""
    config = tmp_path / "config"
    config.write_text("fixture")
    pod: JsonObject = {
        "metadata": {"uid": "pod-one", "name": "payments-api-abc-123"},
        "status": {"containerStatuses": [{"restartCount": 1}]},
    }
    with (
        patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"),
        patch.object(KubectlGateway, "observe", return_value={"pods": [pod]}),
        patch.object(KubectlGateway, "_json", return_value={"items": []}),
        patch.object(
            KubectlGateway,
            "_invoke",
            side_effect=["current", subprocess.CalledProcessError(1, "kubectl")],
        ) as invoke,
    ):
        result = KubectlGateway(config).memory_observation()
    logs = result["memory_logs"]
    assert isinstance(logs, list) and len(logs) == 2
    assert object_value(logs[1])["error_type"] == "CalledProcessError"
    assert "--limit-bytes=32768" in invoke.call_args.args[0]
    assert "--previous=true" in invoke.call_args.args[0]


@pytest.mark.parametrize("unsafe", ["count", "name", "bytes"])
def test_memory_log_bounds_reject_unknown_targets(tmp_path: Path, unsafe: str) -> None:
    """Unexpected pod counts, names and oversized responses fail before activation."""
    config = tmp_path / "config"
    config.write_text("fixture")
    pod: JsonObject = {
        "metadata": {"uid": "one", "name": "other" if unsafe == "name" else "payments-api-a-b"}
    }
    pods: list[JsonValue] = [pod] * (3 if unsafe == "count" else 1)
    with (
        patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"),
        patch.object(KubectlGateway, "observe", return_value={"pods": pods}),
        patch.object(KubectlGateway, "_json", return_value={"items": []}),
        patch.object(KubectlGateway, "_invoke", return_value="x" * 65537),
    ):
        with pytest.raises(ValueError):
            KubectlGateway(config).memory_observation()


def fault_observation() -> tuple[JsonObject, JsonObject, JsonObject, str]:
    """Produce an owned new-generation fault pod with a current kernel-termination fixture."""
    cluster = MemoryCluster()
    spec = object_value(cluster.original["spec"])
    cluster.replace_spec("payments-api", cluster.current, memory_control_spec(spec))
    previous = deepcopy(cluster.current)
    fault = fault_spec("OOM-01", spec)
    cluster.replace_spec("payments-api", previous, fault)
    observed = MemoryFixture(cluster).collect()
    requested = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    return observed, previous, fault, requested


def test_owned_fault_accepts_only_new_pod_and_actual_lifetime() -> None:
    """The positive control includes the complete ownership chain, template and event order."""
    observed, previous, fault, requested = fault_observation()
    assert fault_activated(observed, previous, fault, requested, ("current-pod",))
    assert not fault_activated(observed, previous, fault, requested, ("fault-pod",))
    assert not current_pods(observed, previous, fault, "invalid-time")
    assert not current_pods(observed, previous, fault, "2026-01-01T00:00:00")
    # Admission-added fields do not change the reviewed workload.
    pod = object_items(observed["pods"])[0]
    object_value(pod["spec"])["nodeName"] = "payops-dev-worker"
    assert fault_activated(observed, previous, fault, requested, ())


@pytest.mark.parametrize(
    "path,value",
    [
        (("deployment", "metadata", "uid"), "foreign-deployment"),
        (("deployment", "metadata", "namespace"), "other-namespace"),
        (("deployment", "metadata", "generation"), 2),
        (("deployment", "metadata", "generation"), "3"),
        (("deployment", "status", "observedGeneration"), 2),
        (("deployment", "spec", "replicas"), 2),
        (("replica_sets", 0, "metadata", "ownerReferences", 0, "uid"), "foreign-deployment"),
        (("replica_sets", 0, "metadata", "ownerReferences", 0, "controller"), False),
        (("replica_sets", 0, "metadata", "namespace"), "other-namespace"),
        (("replica_sets", 0, "spec", "template", "spec", "containers", 0, "image"), "old-image"),
        (("pods", 0, "metadata", "ownerReferences", 0, "uid"), "foreign-replicaset"),
        (("pods", 0, "metadata", "ownerReferences", 0, "name"), "foreign-name"),
        (("pods", 0, "metadata", "ownerReferences", 0, "kind"), "Deployment"),
        (("pods", 0, "metadata", "namespace"), "other-namespace"),
        (("pods", 0, "metadata", "creationTimestamp"), "2020-01-01T00:00:00Z"),
        (("pods", 0, "metadata", "creationTimestamp"), "2099-01-01T00:00:00Z"),
        (("pods", 0, "metadata", "creationTimestamp"), None),
        (("pods", 0, "spec", "containers", 0, "image"), "old-image"),
        (("pods", 0, "spec", "containers", 0, "env"), []),
        (("pods", 0, "spec", "containers", 0, "command"), ["python", "other.py"]),
        (("pods", 0, "spec", "containers", 0, "args"), ["other.py"]),
        (("pods", 0, "spec", "containers", 0, "envFrom"), [{"configMapRef": {"name": "other"}}]),
        (
            ("pods", 0, "spec", "containers", 0, "lifecycle"),
            {"postStart": {"exec": {"command": ["other"]}}},
        ),
        (("pods", 0, "spec", "initContainers"), [{"name": "other", "image": "other"}]),
        (("pods", 0, "spec", "ephemeralContainers"), [{"name": "other", "image": "other"}]),
        (("pods", 0, "spec", "containers", 0, "resources", "limits", "memory"), "256Mi"),
        (
            ("pods", 0, "status", "containerStatuses", 0, "lastState", "terminated", "startedAt"),
            "2020-01-01T00:00:00Z",
        ),
        (
            ("pods", 0, "status", "containerStatuses", 0, "lastState", "terminated", "finishedAt"),
            "2020-01-01T00:00:00Z",
        ),
        (
            ("pods", 0, "status", "containerStatuses", 0, "lastState", "terminated", "finishedAt"),
            "2099-01-01T00:00:00Z",
        ),
        (
            ("pods", 0, "status", "containerStatuses", 0, "lastState", "terminated", "finishedAt"),
            None,
        ),
    ],
)
def test_foreign_stale_or_changed_fault_cannot_activate(
    path: tuple[str | int, ...], value: JsonValue
) -> None:
    """Each counterfactual keeps genuine OOM-shaped signals while breaking one causal link."""
    observed, previous, fault, requested = fault_observation()
    replace_field(observed, path, value)
    assert not fault_activated(observed, previous, fault, requested, ())


def replace_field(root: JsonObject, path: tuple[str | int, ...], value: JsonValue) -> None:
    """Mutate exactly one nested fixture field so each negative isolates its missing proof."""
    current = cast(JsonValue, root)
    for key in path[:-1]:
        if isinstance(key, int):
            assert isinstance(current, list)
            current = current[key]
        else:
            current = object_value(current)[key]
    final = path[-1]
    if isinstance(final, int):
        assert isinstance(current, list)
        current[final] = value
    else:
        object_value(current)[final] = value


def test_control_provenance_rejects_foreign_owner_before_fault(tmp_path: Path) -> None:
    """A healthy hold from a foreign ReplicaSet cannot advance the two-stage runner."""
    cluster = MemoryCluster()
    task = runner(tmp_path, cluster)
    fixture = MemoryFixture(cluster)

    def foreign() -> JsonObject:
        """Retain valid control health/logs while severing its actual Deployment ownership."""
        observed = fixture.collect()
        replace_field(
            observed, ("replica_sets", 0, "metadata", "ownerReferences", 0, "uid"), "foreign"
        )
        return observed

    with patch.object(task.memory, "collect", side_effect=foreign):
        receipt = task.run("OOM-01")
    assert not receipt.control_verified and not receipt.activated
    assert receipt.cleanup_verified and cluster.patches == 2


def test_generic_oom_recipe_abstains_without_rollout_context() -> None:
    """The old context-free entry point cannot accept the reviewer's historical foreign pod."""
    from payops.scenarios.recipes import activation

    observed, _, _, _ = fault_observation()
    assert not activation("OOM-01", observed)
