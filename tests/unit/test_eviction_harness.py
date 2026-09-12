"""Eviction recovery must handle uncertain creates and preserve unrelated kubelet changes."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from payops.scenarios.concurrency_harness import ConcurrencyRun
from payops.scenarios.contracts import JsonObject, ScenarioReceipt
from payops.scenarios.eviction_contract import NAMESPACE
from payops.scenarios.eviction_harness import EvictionHarness
from payops.scenarios.runner import CleanupUnverified

RUN = "a" * 32


def setup(tmp_path: Path) -> tuple[EvictionHarness, ConcurrencyRun, ScenarioReceipt]:
    """Keep the actual recovery machinery while replacing all external cluster transport."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        runner = EvictionHarness(config, tmp_path / "evidence")
    context = ConcurrencyRun(
        {"config": "original", "container_id": "owned"}, {"config": "enabled"}, {}
    )
    receipt = ScenarioReceipt(
        run_id=RUN, case_id="SCHED-04", mode="fixture_replay", implementation_variant="fixture"
    )
    return runner, context, receipt


def namespace(run_id: str = RUN) -> JsonObject:
    """A server-observed namespace has an immutable identity and a unique experiment label."""
    return {
        "metadata": {
            "name": NAMESPACE,
            "uid": "namespace-owned",
            "resourceVersion": "7",
            "labels": {"payops.dev/eviction-run": run_id},
        }
    }


@pytest.mark.parametrize("change", ["original", "enabled", "foreign"])
def test_config_restore_refuses_unjournaled_bytes(tmp_path: Path, change: str) -> None:
    """Recovery can reverse only the exact injected configuration in its original container."""
    runner, context, _ = setup(tmp_path)
    with (
        patch.object(runner.access, "config_text", return_value=change),
        patch.object(runner.access, "replace_config") as write,
    ):
        if change == "foreign":
            with pytest.raises(CleanupUnverified):
                runner._restore_config(context)
        else:
            runner._restore_config(context)
    assert write.call_count == (1 if change == "enabled" else 0)
    if change == "enabled":
        write.assert_called_once_with("enabled", "original", "owned")


@pytest.mark.parametrize("creation", ["acknowledged", "response_lost", "absent", "foreign"])
def test_namespace_cleanup_reconciles_uncertain_create(tmp_path: Path, creation: str) -> None:
    """A lost create response must not release the latch while its namespace is still present."""
    runner, context, receipt = setup(tmp_path)
    document = namespace("b" * 32 if creation == "foreign" else RUN)
    runner.namespace_document = document if creation == "acknowledged" else None
    with (
        patch.object(runner.access, "config_text", return_value="original"),
        patch.object(
            runner.access,
            "namespace_absence",
            return_value={"items": [] if creation == "absent" else [document]},
        ),
        patch.object(runner.access, "remove_namespace") as remove,
        patch.object(runner, "_save"),
        patch.object(runner, "_wait"),
    ):
        if creation == "foreign":
            with pytest.raises(CleanupUnverified):
                runner._restore_original(context, tmp_path, receipt)
        else:
            runner._restore_original(context, tmp_path, receipt)
    assert remove.call_count == (1 if creation in {"acknowledged", "response_lost"} else 0)


@pytest.mark.parametrize("failure", ["config", "audit", "delete"])
def test_cleanup_attempts_both_resources_and_retains_latch(tmp_path: Path, failure: str) -> None:
    """Node and evidence failures cannot suppress namespace cleanup or release the latch."""
    runner, context, _ = setup(tmp_path)
    directory, receipt = runner._start("SCHED-04")
    runner.namespace_document = namespace(receipt.run_id)
    with (
        patch.object(
            runner.access,
            "config_text",
            return_value="foreign" if failure == "config" else "original",
        ),
        patch.object(
            runner.access,
            "remove_namespace",
            side_effect=ValueError("delete conflict") if failure == "delete" else None,
        ) as remove,
        patch.object(
            runner, "_save", side_effect=OSError("full disk") if failure == "audit" else None
        ),
        patch.object(runner, "_wait"),
    ):
        runner._recover(context, directory, receipt)
    remove.assert_called_once()
    assert runner.block_file.exists() and not receipt.cleanup_verified
    assert receipt.cleanup_failure


@pytest.mark.parametrize("exists", [False, True])
def test_prepare_requires_absent_namespace_before_capturing_restore_source(
    tmp_path: Path, exists: bool
) -> None:
    """A pre-existing namespace must be rejected before the harness can claim any resource."""
    runner, _, receipt = setup(tmp_path)
    with (
        patch.object(runner.access, "verify_scope", return_value={"container_id": "owned"}),
        patch.object(
            runner.access,
            "namespace_absence",
            return_value={"items": [namespace()] if exists else []},
        ),
        patch.object(runner.access, "config_text", return_value="original"),
        patch.object(runner, "_save"),
    ):
        if exists:
            with pytest.raises(ValueError, match="already exists"):
                runner._prepare(tmp_path, receipt)
        else:
            assert runner._prepare(tmp_path, receipt).original == {
                "config": "original",
                "container_id": "owned",
            }


@pytest.mark.parametrize("failure", [None, "namespace", "pressure", "recovery", "interrupt"])
def test_experiment_runs_controls_and_recovers_after_failures(
    tmp_path: Path, failure: str | None
) -> None:
    """Actual lifecycle ordering and persisted receipts cover failure before and after injection."""
    runner, context, _ = setup(tmp_path)
    context.original["config"] = "evictionHard: {}\n"
    baseline: JsonObject = {
        "nodes": {
            "items": [
                {
                    "status": {
                        "conditions": [
                            {"type": "Ready", "status": "True"},
                            {"type": "MemoryPressure", "status": "False"},
                        ]
                    }
                }
            ]
        },
        "configz": {"kubeletconfig": {"evictionHard": {}}},
        "stats": {
            "node": {"memory": {"availableBytes": 2_000_000_000, "workingSetBytes": 1_000_000_000}}
        },
    }
    callback = Mock()
    with (
        patch.object(runner, "_prepare", return_value=context),
        patch.object(
            runner.access,
            "namespace",
            return_value=namespace(),
            side_effect=TimeoutError("create uncertain") if failure == "namespace" else None,
        ),
        patch.object(runner.access, "create_victim", return_value={}),
        patch.object(runner.access, "observation", return_value=baseline),
        patch.object(
            runner.access,
            "replace_config",
            side_effect=ValueError("pressure failed")
            if failure == "pressure"
            else KeyboardInterrupt()
            if failure == "interrupt"
            else None,
        ),
        patch.object(
            runner,
            "_restore_config",
            side_effect=ValueError("restore failed") if failure == "recovery" else None,
        ),
        patch.object(runner, "_wait"),
        patch.object(runner, "_restore_original") as restore,
    ):
        if failure == "interrupt":
            with pytest.raises(KeyboardInterrupt):
                runner.run(after_activation=callback)
            receipt = ScenarioReceipt.model_validate_json(
                next(runner.root.glob("*/receipt.json")).read_bytes()
            )
        else:
            receipt = runner.run(after_activation=callback)
    restore.assert_called_once()
    assert receipt.cleanup_verified and not runner.block_file.exists()
    assert bool(receipt.failure) is (failure is not None)
    assert callback.call_count == (1 if failure in {None, "recovery"} else 0)
    if failure is None:
        assert receipt.activated and receipt.control_verified
