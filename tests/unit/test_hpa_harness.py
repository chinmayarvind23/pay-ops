"""Exercise lifecycle ordering and exact-state cleanup independently of live Kubernetes."""

from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from test_hpa_runtime import runtime

from payops.scenarios.contracts import object_value
from payops.scenarios.hpa_harness import HpaHarness
from payops.scenarios.runner import CleanupUnverified
from payops.scenarios.sampling_gateway import deployment_map


def harness(tmp_path: Path) -> HpaHarness:
    """A fake transport prevents mutations while the actual latch and receipt remain on disk."""
    access = Mock()
    access.mode = "fixture_replay"
    access.create_load.return_value = {}
    return HpaHarness(tmp_path / "config", tmp_path / "evidence", access)


@pytest.mark.parametrize("failed", [None, "prepare", "raise_cap", "completed_load", "replica_work"])
def test_lifecycle_always_recovers(tmp_path: Path, failed: str | None) -> None:
    """Failures at each major phase preserve the receipt and still invoke recovery exactly once."""
    runner = harness(tmp_path)
    names = [
        "prepare",
        "demand",
        "settle",
        "raise_cap",
        "completed_load",
        "replica_work",
        "cleanup",
    ]
    mocks = {
        name: Mock(side_effect=ValueError("injected") if name == failed else None) for name in names
    }
    with patch.multiple(runner, **mocks):
        receipt = runner.run()
    assert bool(receipt.failure) == (failed is not None)
    mocks["cleanup"].assert_called_once()
    assert receipt.cleanup_verified and not runner.block_file.exists()
    assert (runner.directory / "receipt.json").exists()
    if failed is None:
        assert receipt.activated and receipt.control_verified
        assert mocks["demand"].call_count == 3
        mocks["raise_cap"].assert_called_once()


def test_failed_cleanup_keeps_latch(tmp_path: Path) -> None:
    """Later scenarios cannot start after failed restoration."""
    runner = harness(tmp_path)
    with (
        patch.object(runner, "prepare", side_effect=ValueError("fault")),
        patch.object(runner, "cleanup", side_effect=CleanupUnverified("restore failed")),
    ):
        receipt = runner.run()
    assert not receipt.cleanup_verified and runner.block_file.exists()
    assert "restore failed" in str(receipt.cleanup_failure)
    with pytest.raises(CleanupUnverified):
        runner.run()


@pytest.mark.parametrize("foreign", [None, "uid", "spec"])
def test_cleanup_restores_only_known_original(tmp_path: Path, foreign: str | None) -> None:
    """Delete the Job and HPA before restoring replicas, refusing independent operator changes."""
    runner = harness(tmp_path)
    state, _, payments = runtime(1)
    runner.original = state
    runner.enabled = deepcopy(payments)
    runner.enabled["revisionHistoryLimit"] = 9
    runner.directory, runner.receipt = runner._start("SCHED-03")  # pyright: ignore[reportPrivateUsage]
    original = deployment_map(state)["payments-api"]
    current = deepcopy(original)
    current["spec"] = runner.spec(2)
    if foreign == "uid":
        object_value(current["metadata"])["uid"] = "foreign"
    elif foreign == "spec":
        object_value(current["spec"])["revisionHistoryLimit"] = 19
    with (
        patch.object(runner.access, "deployment", return_value=current),
        patch.object(runner.access, "replace_spec") as replace,
        patch.object(runner, "remove") as remove,
        patch.object(runner, "settle"),
        patch.object(runner, "_wait"),
        patch.object(runner, "save"),
    ):
        if foreign:
            with pytest.raises(CleanupUnverified):
                runner.cleanup()
            replace.assert_not_called()
        else:
            runner.cleanup()
            replace.assert_called_once_with("payments-api", current, original["spec"])
        assert [call.args[0] for call in remove.call_args_list] == ["job", "hpa"]
