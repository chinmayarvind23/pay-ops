"""Exercise ambiguous writes and restoration without launching fault containers."""

from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from typing import IO, Any
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


@pytest.mark.parametrize("failure", ["none", "changed-container", "restart", "incomplete"])
def test_actual_control_capture_and_rejection(tmp_path: Path, failure: str) -> None:
    """Drive the actual collector/validator path; a race or incomplete log blocks fault entry."""
    gateway = FixtureGateway()
    runner = LeakHarness(
        tmp_path / "config", tmp_path / "evidence", gateway, 2 if failure == "none" else 0.06, 0.001
    )
    observed, original, expected, _ = evidence()
    run = context(gateway)
    for document in object_items(run.original["deployments"]):
        if object_value(document["metadata"])["name"] == "risk-sim":
            document.update(original)
    gateway.current = deepcopy(original)
    run.control, run.requested = expected, START.isoformat()
    pod = object_items(observed["pods"])[0]
    pod["status"] = {
        "containerStatuses": [
            {
                "name": "sandbox",
                "restartCount": 0,
                "ready": True,
                "containerID": "containerd://" + "a" * 64,
                "imageID": "sha256:fixed-image",
                "state": {"running": {"startedAt": START.isoformat()}},
            }
        ]
    }
    after = deepcopy(observed)
    status = object_items(
        object_value(object_items(after["pods"])[0]["status"])["containerStatuses"]
    )[0]
    if failure == "changed-container":
        status["containerID"] = "containerd://" + "b" * 64
    elif failure == "restart":
        status["restartCount"] = 1
    records = sequence("released-v1")
    if failure == "incomplete":
        records = records[:-1]
    raw = "\n".join(r.timestamp.isoformat() + " " + r.model_dump_json() for r in records)
    captures = iter([observed, after] * 100)
    with (
        patch.object(runner, "_prepare", return_value=run),
        patch.object(runner, "_transition"),
        patch.object(runner, "_settle"),
        patch.object(runner, "_fault", side_effect=fault_passes) as fault,
        patch.object(gateway, "snapshot", side_effect=lambda: next(captures)),
        patch.object(gateway, "read_log", return_value={"text": raw}),
    ):
        receipt = runner.run()
    assert receipt.control_verified is (failure == "none")
    assert receipt.activated is (failure == "none")
    assert fault.call_count == int(failure == "none")
    assert receipt.cleanup_verified
    names = [a.name for a in receipt.artifacts]
    assert any("raw-log" in name for name in names)
    assert any("capture-after" in name for name in names)


class AuditFailureHarness(LeakHarness):
    """Fail an exact evidence boundary while leaving the real restoration code intact."""

    fail_name = "journal"

    def _save(self, directory: Path, receipt: ScenarioReceipt, name: str, data: JsonObject) -> None:
        """A simulated disk failure cannot precede recovery of an already applied variant."""
        if name == self.fail_name:
            raise OSError("fixture evidence disk unavailable")
        super()._save(directory, receipt, name, data)


@pytest.mark.parametrize("boundary", ["journal", "transition", "healthy-final"])
def test_audit_failures_restore_or_retain_latch(tmp_path: Path, boundary: str) -> None:
    """No pre-journal write occurs, and final audit failure retains the latch after restoration."""
    gateway = FixtureGateway()
    runner = AuditFailureHarness(tmp_path / "config", tmp_path / "evidence", gateway, 0.01, 0.001)
    runner.fail_name = boundary
    with (
        patch.object(runner, "_prepare", return_value=context(gateway)),
        patch.object(runner, "_control", side_effect=control_passes),
        patch.object(runner, "_fault", side_effect=fault_passes),
        patch.object(runner, "_settle"),
    ):
        receipt = runner.run()
    assert gateway.current["spec"] == gateway.original["spec"]
    assert gateway.writes == (3 if boundary == "healthy-final" else 0)
    assert receipt.cleanup_verified is (boundary != "healthy-final")
    assert runner.block_file.exists() is (boundary == "healthy-final")


def test_baseline_and_final_runtime_verification_execute(tmp_path: Path) -> None:
    """Exercise orchestration around separately tested five-service runtime validators."""
    gateway = FixtureGateway()
    runner = LeakHarness(tmp_path / "config", tmp_path / "evidence", gateway, 0.2, 0.001)
    state = context(gateway).original
    with (
        patch.object(gateway, "verify_scope", return_value={"namespace": "payops-sandbox"}),
        patch.object(gateway, "state", return_value=state),
        patch("payops.scenarios.leak_harness.validate_runtime_baseline") as validate,
        patch(
            "payops.scenarios.leak_harness.protocol_identities", return_value={"peer": "stable"}
        ) as identities,
        patch.object(runner, "_control", side_effect=control_passes),
        patch.object(runner, "_fault", side_effect=fault_passes),
    ):
        receipt = runner.run()
    validate.assert_called_once_with(state)
    assert identities.call_count == 2
    assert receipt.activated and receipt.cleanup_verified and not runner.block_file.exists()


def test_final_receipt_failure_retains_latch_after_restore(tmp_path: Path) -> None:
    """Loss of the final record cannot release a run whose cleanup proof is no longer durable."""
    gateway = FixtureGateway()
    runner = LeakHarness(tmp_path / "config", tmp_path / "evidence", gateway, 0.2, 0.001)
    original_open = Path.open

    def fail_receipt(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO[Any]:
        """Fail only final receipt creation; allow retained snapshots and the latch update."""
        if path.name == "receipt.json":
            raise OSError("fixture receipt disk failure")
        return original_open(path, mode, buffering, encoding, errors, newline)

    with (
        patch.object(runner, "_prepare", return_value=context(gateway)),
        patch.object(runner, "_control", side_effect=control_passes),
        patch.object(runner, "_fault", side_effect=fault_passes),
        patch.object(runner, "_settle"),
        patch.object(Path, "open", autospec=True, side_effect=fail_receipt),
    ):
        receipt = runner.run()
    assert gateway.current["spec"] == gateway.original["spec"] and gateway.writes == 3
    assert not receipt.cleanup_verified and runner.block_file.exists()
    assert "receipt/latch" in str(receipt.cleanup_failure)
