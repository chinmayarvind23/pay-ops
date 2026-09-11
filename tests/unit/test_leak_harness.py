"""Exercise ambiguous writes and restoration without launching fault containers."""

from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from test_leak_evidence import END, START, sequence
from test_leak_gateway import baseline
from test_leak_lifetime import evidence
from test_scheduler import SchedulerFixture

from payops.scenarios.contracts import (
    DeploymentName,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
)
from payops.scenarios.leak_gateway import LeakGateway
from payops.scenarios.leak_harness import LeakHarness, LeakRun
from payops.scenarios.leak_specs import leak_specs


class FixtureGateway(LeakGateway):
    """A fixed deployment store models server apply followed by a lost response."""

    def __init__(self, fail_at: int = 0, after_apply: bool = False) -> None:
        """No kubectl configuration is constructed for lifecycle tests."""
        self.original = baseline()
        self.current = deepcopy(self.original)
        self.writes = 0
        self.fail_at, self.after_apply = fail_at, after_apply

    def deployment(self, name: DeploymentName) -> JsonObject:
        """Return a fresh object so test mutation cannot alter a captured baseline."""
        assert name == "risk-sim"
        return deepcopy(self.current)

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """Apply exact CAS and inject failures on either side of the write."""
        assert name == "risk-sim" and expected == self.current
        self.writes += 1
        failure = self.writes == self.fail_at
        if failure and not self.after_apply:
            raise OSError("fixture pre-apply failure")
        self.current["spec"] = deepcopy(spec)
        if failure:
            raise OSError("fixture response lost")

    def healthy(self) -> JsonObject:
        """Return the existing valid synthetic-payment fixture after restoration."""
        return SchedulerFixture().healthy()


def context(gateway: FixtureGateway) -> LeakRun:
    """Retain five named baseline documents for the same production context shape."""
    state = SchedulerFixture().snapshot()
    documents = state["deployments"]
    assert isinstance(documents, list)
    for index, document in enumerate(documents):
        if object_value(object_value(document)["metadata"])["name"] == "risk-sim":
            documents[index] = deepcopy(gateway.original)
    control, retained = leak_specs(gateway.original)
    return LeakRun({"deployments": documents}, control, retained)


def control_passes(run: LeakRun, directory: Path, receipt: ScenarioReceipt) -> None:
    """Separate evidence acceptance from the lifecycle's mutation/recovery tests."""
    run.control_image = "sha256:fixed-image"
    receipt.control_verified = True


def fault_passes(run: LeakRun, directory: Path, receipt: ScenarioReceipt) -> None:
    """Only the accepted fault callback marks activation in these lifecycle fixtures."""
    receipt.activated = True


@pytest.mark.parametrize("fail_at", range(4))
@pytest.mark.parametrize("after_apply", [False, True])
def test_ambiguous_writes_restore_or_hold_latch(
    tmp_path: Path, fail_at: int, after_apply: bool
) -> None:
    """Both forward writes and recovery are independently exposed to ambiguous API failure."""
    gateway = FixtureGateway(fail_at, after_apply)
    runner = LeakHarness(tmp_path / "config", tmp_path / "evidence", gateway, 0.01, 0.001)
    with (
        patch.object(runner, "_prepare", return_value=context(gateway)),
        patch.object(runner, "_control", side_effect=control_passes),
        patch.object(runner, "_fault", side_effect=fault_passes),
        patch.object(runner, "_settle"),
    ):
        receipt = runner.run()
    if fail_at == 3:
        assert receipt.activated and not receipt.cleanup_verified and runner.block_file.exists()
    else:
        assert receipt.cleanup_verified and not runner.block_file.exists()
        assert gateway.current["spec"] == gateway.original["spec"]
        assert receipt.activated is (fail_at == 0)


@pytest.mark.parametrize("foreign", [False, True])
def test_interrupt_restores_only_recognized_state(tmp_path: Path, foreign: bool) -> None:
    """Keyboard interruption still cleans up; a concurrent foreign spec is preserved."""
    gateway = FixtureGateway()
    runner = LeakHarness(tmp_path / "config", tmp_path / "evidence", gateway, 0.01, 0.001)

    def interrupt() -> None:
        """Model operator interruption after successful evidence collection."""
        if foreign:
            object_value(gateway.current["spec"])["foreign"] = True
        raise KeyboardInterrupt

    with (
        patch.object(runner, "_prepare", return_value=context(gateway)),
        patch.object(runner, "_control", side_effect=control_passes),
        patch.object(runner, "_fault", side_effect=fault_passes),
        patch.object(runner, "_settle"),
        pytest.raises(KeyboardInterrupt),
    ):
        runner.run(after_activation=interrupt)
    assert runner.block_file.exists() is foreign
    assert gateway.writes == (2 if foreign else 3)


def test_control_failure_cannot_apply_retained_fault(tmp_path: Path) -> None:
    """A failed control must restore immediately without allowing the fault transition."""
    gateway = FixtureGateway()
    runner = LeakHarness(tmp_path / "config", tmp_path / "evidence", gateway, 0.01, 0.001)
    with (
        patch.object(runner, "_prepare", return_value=context(gateway)),
        patch.object(runner, "_control", side_effect=TimeoutError("control incomplete")),
        patch.object(runner, "_fault") as fault,
        patch.object(runner, "_settle"),
    ):
        receipt = runner.run()
    fault.assert_not_called()
    assert receipt.failure and not receipt.activated and receipt.cleanup_verified
    assert gateway.writes == 2 and gateway.current["spec"] == gateway.original["spec"]


def test_real_fault_stage_requires_two_distinct_captures(tmp_path: Path) -> None:
    """Exercise the actual raw-log validator and aggregator, including a duplicate poll."""
    gateway = FixtureGateway()
    runner = LeakHarness(tmp_path / "config", tmp_path / "evidence", gateway, 1, 0.001)
    observed, original, expected, raw = evidence()
    run = context(gateway)
    for index, item in enumerate(object_items(run.original["deployments"])):
        if object_value(item["metadata"])["name"] == "risk-sim":
            object_items(run.original["deployments"])[index].update(original)
    run.retained, run.requested = expected, START.isoformat()
    second = deepcopy(observed)
    status = object_items(
        object_value(object_items(second["pods"])[0]["status"])["containerStatuses"]
    )[0]
    status["restartCount"] = 2
    terminated = object_value(object_value(status["lastState"])["terminated"])
    terminated.update(
        {
            "containerID": "containerd://" + "b" * 64,
            "startedAt": (START + timedelta(seconds=60)).isoformat(),
            "finishedAt": (END + timedelta(seconds=60)).isoformat(),
        }
    )
    records = [
        r.model_copy(update={"timestamp": r.timestamp + timedelta(seconds=60)})
        for r in sequence("retained-v1")
    ]
    second_raw = "\n".join(r.timestamp.isoformat() + " " + r.model_dump_json() for r in records)
    with (
        patch.object(runner, "_prepare", return_value=run),
        patch.object(runner, "_control", side_effect=control_passes),
        patch.object(runner, "_transition"),
        patch.object(runner, "_undo"),
        patch.object(runner, "_settle"),
        patch.object(
            runner,
            "_capture",
            side_effect=[
                (observed, observed, raw),
                (observed, observed, raw),
                (second, second, second_raw),
            ],
        ),
    ):
        receipt = runner.run()
    assert receipt.activated and receipt.cleanup_verified
    assert len([a for a in receipt.artifacts if "oom-lifetime" in a.name]) == 3
