"""Dependency experiments preserve control ordering, request identity and exact restoration."""

# pyright: reportPrivateUsage=false

import json
from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from test_dependency_scenarios import exercise
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.concurrency_harness import ConcurrencyHarness, ConcurrencyRun
from payops.scenarios.contracts import CaseId, JsonObject, ScenarioReceipt, object_value
from payops.scenarios.dependency_gateway import data_deployments
from payops.scenarios.dependency_harness import (
    DependencyHarness,
    postgres_drained,
    redis_down,
    redis_ready,
)
from payops.scenarios.recipes import container
from payops.scenarios.runner import CleanupUnverified


def data_state() -> JsonObject:
    """Each fixed data service has an independent immutable deployment identity."""
    return {
        "deployments": {
            "items": [
                {
                    "metadata": {"name": name, "uid": name, "generation": 1},
                    "spec": {"replicas": 1},
                    "status": {"readyReplicas": 1, "observedGeneration": 1},
                }
                for name in ("postgres", "redis", "elasticsearch")
            ]
        }
    }


def setup(
    tmp_path: Path, case: CaseId = "DEP-03"
) -> tuple[DependencyHarness, ConcurrencyRun, ScenarioReceipt]:
    """Only external transport is replaced; captured deployment and receipt structures stay real."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        runner = DependencyHarness(config, tmp_path / "evidence", tmp_path / "credentials")
    cluster = SamplingCluster(Clock())
    item = container(object_value(cluster.documents["payments-api"]["spec"]))
    object_value(item["resources"])["requests"] = {"cpu": "50m", "memory": "96Mi"}
    state = cluster.state()
    runner.data_original = data_state()
    receipt = ScenarioReceipt(
        run_id="a" * 32, case_id=case, mode="fixture_replay", implementation_variant="fixture"
    )
    return runner, ConcurrencyRun(state, {}, {}), receipt


@pytest.mark.parametrize("changed", [None, "uid", "spec", "peer", "off", "missing"])
def test_restore_data_refuses_unjournaled_changes(tmp_path: Path, changed: str | None) -> None:
    """Recovery changes only the known Redis zero-replica state and detects unrelated data drift."""
    runner, _, receipt = setup(tmp_path)
    original = data_state()
    current = deepcopy(data_deployments(original)["redis"])
    final = deepcopy(original)
    if changed == "uid":
        object_value(current["metadata"])["uid"] = "foreign"
    elif changed in {"spec", "off"}:
        object_value(current["spec"])["replicas"] = 0 if changed == "off" else 3
    elif changed == "peer":
        object_value(data_deployments(final)["postgres"]["metadata"])["uid"] = "foreign"
    elif changed == "missing":
        runner.data_original = None
    with (
        patch.object(runner.access, "redis", return_value=current),
        patch.object(runner.access, "redis_replicas") as write,
        patch.object(runner.access, "data_state", return_value=final),
        patch.object(runner, "_wait"),
        patch.object(runner, "_save"),
    ):
        if changed in {"uid", "spec", "peer"}:
            with pytest.raises(CleanupUnverified):
                runner._restore_data(tmp_path, receipt)
        else:
            runner._restore_data(tmp_path, receipt)
    assert write.call_count == (1 if changed == "off" else 0)


@pytest.mark.parametrize("unhealthy", [False, True])
def test_prepare_validates_both_namespaces(tmp_path: Path, unhealthy: bool) -> None:
    """A dependency outage cannot begin from an already degraded data deployment."""
    runner, context, receipt = setup(tmp_path)
    data = data_state()
    if unhealthy:
        object_value(data_deployments(data)["redis"]["status"])["readyReplicas"] = 0
    with (
        patch.object(runner.access, "verify_scope", return_value={}),
        patch.object(runner.access, "state", return_value=context.original),
        patch.object(runner.access, "data_state", return_value=data),
        patch.object(runner, "_wait"),
        patch.object(runner, "_save"),
    ):
        if unhealthy:
            with pytest.raises(ValueError, match="must start ready"):
                runner._prepare(tmp_path, receipt)
        else:
            prepared = runner._prepare(tmp_path, receipt)
            assert prepared.original == context.original and prepared.enabled
            assert runner.data_original == data


@pytest.mark.parametrize("case", ["DEP-03", "DEP-04"])
@pytest.mark.parametrize("failure", [None, "control", "fault", "recovered", "interrupt"])
def test_experiment_orders_controls_and_always_recovers(
    tmp_path: Path, case: CaseId, failure: str | None
) -> None:
    """Exercise actual PostgreSQL/Redis lifecycle branches with failures at each traffic phase."""
    runner, context, _ = setup(tmp_path, case)
    stages: list[str] = []

    def batch(*args: object) -> str:
        """A selected failure interrupts only the phase, leaving run/finally logic intact."""
        stage = str(args[1])
        stages.append(stage)
        if stage == failure:
            raise ValueError("stage failed")
        if stage == "fault" and failure == "interrupt":
            raise KeyboardInterrupt()
        return ""

    callback = Mock()
    with (
        patch.object(runner, "_prepare", return_value=context),
        patch.object(runner, "_enable"),
        patch.object(runner, "_batch", side_effect=batch),
        patch.object(runner, "_sessions"),
        patch.object(runner, "_restore_data"),
        patch.object(runner, "_restore_original") as restore,
        patch.object(runner, "_wait"),
        patch.object(runner.access, "metrics", return_value={}),
        patch.object(runner.access, "redis", return_value=data_deployments(data_state())["redis"]),
        patch.object(runner.access, "redis_replicas"),
        patch("payops.scenarios.dependency_harness.postgres_pressure", return_value=nullcontext()),
    ):
        if failure == "interrupt":
            with pytest.raises(KeyboardInterrupt):
                runner.run(case, callback)
            receipt = ScenarioReceipt.model_validate_json(
                next(runner.root.glob("*/receipt.json")).read_bytes()
            )
        else:
            receipt = runner.run(case, callback)
    restore.assert_called_once()
    assert receipt.cleanup_verified and not runner.block_file.exists()
    assert callback.call_count == (1 if failure in {None, "recovered"} else 0)
    assert bool(receipt.failure) is (failure is not None)
    if failure is None:
        assert stages == ["control", "fault", "recovered"] and receipt.activated


@pytest.mark.parametrize("fault", [False, True])
def test_batch_binds_real_driver_requests_and_rejects_reuse(tmp_path: Path, fault: bool) -> None:
    """Transport-fixture requests pass through the actual persisted-plan and log-join checks."""
    runner, context, receipt = setup(tmp_path)
    observed, plan, raw = exercise(tmp_path / "driver", "postgres", fault)
    stage = "fault" if fault else "control"
    output = tmp_path / "traffic" / stage / observed.run_id
    output.mkdir(parents=True)
    (output / "plan.json").write_text(json.dumps(plan))
    with (
        patch.object(runner.access, "mode", "fixture_replay"),
        patch.object(runner.access, "state", return_value=context.original),
        patch.object(runner, "_identities", return_value={"payments-api": "owned"}),
        patch.object(runner, "_capture", return_value=({}, {}, raw)),
        patch.object(runner, "_save"),
        patch(
            "payops.scenarios.dependency_harness.TrafficDriver.run",
            new_callable=AsyncMock,
            return_value=observed,
        ),
    ):
        assert runner._batch(context, stage, tmp_path, receipt) == raw
        assert len(context.samples) == 3
        with pytest.raises(ValueError, match="sample reused"):
            runner._batch(context, stage, tmp_path, receipt)


def test_pressure_counts_and_independent_restoration(tmp_path: Path) -> None:
    """Foreign role sessions cannot qualify pressure; payment cleanup survives data failure."""
    runner, context, receipt = setup(tmp_path)
    with (
        patch.object(runner, "_save"),
        patch.object(
            runner.access, "postgres_sessions", return_value={"limit": 4, "total": 4, "owned": 3}
        ),
    ):
        with pytest.raises(ValueError, match="owned sessions"):
            runner._sessions(4, tmp_path, receipt)
    with patch.object(runner, "_wait") as wait:
        runner._sessions(0, tmp_path, receipt)
        assert wait.call_args.args[-1] is postgres_drained
    with (
        patch.object(runner, "_restore_data"),
        patch.object(runner, "_sessions") as drain,
        patch.object(
            ConcurrencyHarness, "_restore_original", side_effect=ValueError("payments drift")
        ),
    ):
        with pytest.raises(CleanupUnverified, match="payments drift"):
            runner._restore_original(context, tmp_path, receipt)
        drain.assert_called_once_with(0, tmp_path, receipt)
    ready = data_deployments(data_state())["redis"]
    assert redis_ready(ready) and not redis_down(ready)
    object_value(ready["spec"])["replicas"] = 0
    assert redis_down(ready) and not redis_ready(ready)
    assert postgres_drained({"limit": 4, "total": 0, "owned": 0})
    assert not postgres_drained({"limit": 4, "total": 1, "owned": 0})
