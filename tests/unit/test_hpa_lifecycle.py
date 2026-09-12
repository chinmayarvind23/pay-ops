"""HPA orchestration must reject stale demand and recover only its owned controllers."""

# pyright: reportPrivateUsage=false

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from test_hpa_gateway import RUN, document
from test_hpa_harness import harness
from test_hpa_runtime import runtime
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.contracts import JsonObject, object_value
from payops.scenarios.hpa_job_identity import LoadProcess
from payops.scenarios.recipes import container
from payops.scenarios.runner import CleanupUnverified
from payops.scenarios.traffic import TrafficReceipt

MODULE = "payops.scenarios.hpa_harness"
pytest_plugins = ["test_hpa_receipt"]


@pytest.mark.parametrize("failure", [None, "foreign", "conflict", "forbidden"])
def test_cap_change_retries_only_version_conflicts(tmp_path: Path, failure: str | None) -> None:
    """A replacement HPA or forbidden write cannot be hidden by retrying the cap transition."""
    runner = harness(tmp_path)
    runner.hpa_uid = "foreign" if failure == "foreign" else "owned"
    error = subprocess.CalledProcessError(
        1, "kubectl", stderr="Conflict" if failure == "conflict" else "Forbidden"
    )
    with (
        patch.object(runner, "save"),
        patch.object(runner.access, "read_resource", return_value=document()),
        patch.object(
            runner.access,
            "set_cap",
            side_effect=error if failure in {"conflict", "forbidden"} else None,
        ) as write,
    ):
        if failure:
            with pytest.raises((ValueError, TimeoutError, subprocess.CalledProcessError)):
                runner.raise_cap()
        else:
            runner.raise_cap()
    assert write.call_count == (0 if failure == "foreign" else 8 if failure == "conflict" else 1)


@pytest.mark.parametrize("failure", [None, "foreign", "conflict", "forbidden", "stuck"])
def test_controller_removal_requires_observed_absence(tmp_path: Path, failure: str | None) -> None:
    """Deletion needs observed absence; foreign or undeletable HPAs retain the latch."""
    runner = harness(tmp_path)
    runner.run_id = RUN
    item = document()
    if failure == "foreign":
        object_value(item["metadata"])["uid"] = ""
    inventories: list[JsonObject] = [{"hpas": [item]}, {"hpas": []}]
    error = subprocess.CalledProcessError(
        1, "kubectl", stderr="Conflict" if failure == "conflict" else "Forbidden"
    )
    with (
        patch.object(
            runner.access,
            "experiment_resources",
            side_effect=None if failure == "stuck" else inventories,
            return_value={"hpas": [item]},
        ),
        patch.object(
            runner.access,
            "remove_owned",
            side_effect=error if failure in {"conflict", "forbidden"} else None,
        ) as remove,
        patch(MODULE + ".time.sleep"),
    ):
        if failure in {"foreign", "forbidden", "stuck"}:
            with pytest.raises((ValueError, CleanupUnverified, subprocess.CalledProcessError)):
                runner.remove("hpa")
        else:
            runner.remove("hpa")
    assert remove.call_count == (0 if failure == "foreign" else 12 if failure == "stuck" else 1)


@pytest.mark.parametrize("timeout", [False, True])
def test_settle_retries_rejected_runtime_with_fixed_deadline(tmp_path: Path, timeout: bool) -> None:
    """A pending controller gets bounded observation retries, never an invented ready state."""
    runner = harness(tmp_path)
    with (
        patch.object(runner, "runtime", side_effect=[ValueError("pending"), None]),
        patch.object(runner, "save") as save,
        patch(MODULE + ".time.monotonic", side_effect=[0, 0, 91] if timeout else [0, 0, 1]),
        patch(MODULE + ".time.sleep"),
    ):
        if timeout:
            with pytest.raises(TimeoutError):
                runner.settle(1, False)
        else:
            assert runner.settle(1, False).tzinfo == UTC
    save.assert_called_once_with("runtime-rejected", {"reason": "pending"})


@pytest.mark.parametrize(
    "saturated,utilization", [(False, 20), (True, 150), (False, 80), (True, 40)]
)
def test_demand_requires_independent_cpu_agreement(
    tmp_path: Path, saturated: bool, utilization: int
) -> None:
    """Controller claims cannot override a contradictory independent CPU measurement."""
    runner = harness(tmp_path)
    valid = utilization >= 100 if saturated else utilization <= 50
    with (
        patch.object(runner, "runtime", return_value=({}, ())),
        patch.object(runner.access, "cpu_metrics", return_value=b"{}"),
        patch.object(runner.access, "read_resource", return_value={}),
        patch(MODULE + ".validate_cpu_metrics", return_value=Mock(average_utilization=utilization)),
        patch(MODULE + ".hpa_demand", return_value=Mock(cpu_utilization=utilization)),
        patch.object(runner, "save") as save,
        patch(MODULE + ".time.monotonic", side_effect=[0, 0, 91]),
        patch(MODULE + ".time.sleep"),
    ):
        if valid:
            runner.demand(1, datetime.now(UTC), saturated)
        else:
            with pytest.raises(TimeoutError):
                runner.demand(1, datetime.now(UTC), saturated)
    assert save.call_args.args[0] == ("demand-verified" if valid else "demand-rejected")


@pytest.mark.parametrize("condition", ["Complete", "Failed", "Pending"])
def test_job_completion_requires_terminal_success(tmp_path: Path, condition: str) -> None:
    """Only a completed Job can enter receipt validation; pending observations expire."""
    runner = harness(tmp_path)
    job = {"status": {"conditions": [{"type": condition, "status": "True"}]}}
    with (
        patch.object(runner.access, "read_resource", return_value=job),
        patch.object(runner, "capture_load") as capture,
        patch.object(runner, "save"),
        patch(MODULE + ".time.monotonic", side_effect=[0, 0, 181]),
        patch(MODULE + ".time.sleep"),
    ):
        if condition == "Complete":
            assert runner.completed_load() == capture.return_value
        else:
            with pytest.raises((TimeoutError, ValueError)):
                runner.completed_load()
    assert capture.call_count == (1 if condition == "Complete" else 0)


@pytest.mark.parametrize("failure", [None, "controller", "health"])
def test_prepare_journals_before_mutation(tmp_path: Path, failure: str | None) -> None:
    """The original and enabled specs must be durable before any payments configuration changes."""
    runner = harness(tmp_path)
    runner.run_id = RUN
    cluster = SamplingCluster(Clock())
    item = container(object_value(cluster.documents["payments-api"]["spec"]))
    object_value(item["resources"])["requests"] = {"cpu": "50m", "memory": "96Mi"}
    events: list[str] = []

    def save(name: str, data: object) -> None:
        """Track the journal boundary independently from mutation acknowledgement."""
        events.append(name)

    def record_write(*args: object) -> None:
        """Capture the mutation boundary without invoking Kubernetes."""
        events.append("write")

    with (
        patch.object(runner.access, "verify_scope", return_value={}),
        patch.object(
            runner.access,
            "experiment_resources",
            return_value={"hpas": [{}] if failure == "controller" else [], "jobs": []},
        ),
        patch.object(runner.access, "state", return_value=cluster.state()),
        patch.object(runner.access, "healthy", return_value={}),
        patch(MODULE + ".sample_healthy", return_value=failure != "health"),
        patch.object(runner.access, "create_hpa", return_value=document()),
        patch.object(runner.access, "replace_spec", side_effect=record_write) as write,
        patch.object(runner, "settle"),
        patch.object(runner, "save", side_effect=save),
    ):
        if failure:
            with pytest.raises(ValueError):
                runner.prepare()
            write.assert_not_called()
        else:
            runner.prepare()
            assert events.index("journal") < events.index("write")
            assert runner.hpa_uid == "owned"


def test_runtime_preserves_real_service_identities(tmp_path: Path) -> None:
    """The harness returns the independently checked two-replica state, not desired counts."""
    runner = harness(tmp_path)
    state, original, spec = runtime(2)
    runner.original, runner.enabled = original, spec
    with patch.object(runner.access, "state", return_value=state), patch.object(runner, "save"):
        observed, identities = runner.runtime(2, False)
    assert observed == state and len(identities) == 2


@pytest.mark.parametrize("failure", [None, "exit", "replacement"])
def test_capture_validates_full_driver_receipt_inside_process_lifetime(
    tmp_path: Path, evidence: tuple[JsonObject, LoadProcess, datetime], failure: str | None
) -> None:
    """Real receipt validation follows exit and before/after process identity checks."""
    runner = harness(tmp_path)
    raw, process, finished = evidence
    runner.load = process
    pod: JsonObject = {
        "metadata": {"uid": process.pod_uid},
        "status": {
            "containerStatuses": [
                {
                    "state": {
                        "terminated": {
                            "exitCode": 1 if failure == "exit" else 0,
                            "finishedAt": finished.isoformat(),
                        }
                    }
                }
            ]
        },
    }
    state: JsonObject = {"pods": [pod]}
    with (
        patch.object(runner.access, "state", return_value=state),
        patch.object(runner.access, "load_log", return_value=json.dumps(raw).encode()),
        patch.object(runner.access, "read_resource", return_value={}),
        patch.object(runner, "save"),
        patch(
            MODULE + ".split_load_pod",
            side_effect=[(state, process), (state, None if failure == "replacement" else process)],
        ),
    ):
        if failure:
            with pytest.raises(ValueError):
                runner.capture_load({})
        else:
            receipt = runner.capture_load({})
            assert len(receipt.attempts) == 256


@pytest.mark.parametrize("count", [2, 3])
def test_replica_work_requires_completions_on_each_owned_process(
    tmp_path: Path, count: int
) -> None:
    """One busy replica cannot stand in for observed work on both scaled processes."""
    runner = harness(tmp_path)
    state, original, spec = runtime(2)
    runner.original, runner.enabled = original, spec
    with (
        patch.object(runner.access, "state", return_value=state),
        patch.object(runner, "save") as save,
        patch.object(runner.access, "read_log", return_value={"text": "fixture"}),
        patch.object(runner.access, "read_resource", return_value={}),
        patch(MODULE + ".split_load_pod", return_value=(state, Mock(spec=LoadProcess))),
        patch.object(runner, "count_work", return_value=count),
    ):
        if count < 3:
            with pytest.raises(ValueError, match="three actual"):
                runner.replica_work(Mock(spec=TrafficReceipt), datetime.now(UTC))
        else:
            runner.replica_work(Mock(spec=TrafficReceipt), datetime.now(UTC))
            assert len(save.call_args.args[1]["counts"]) == 2
