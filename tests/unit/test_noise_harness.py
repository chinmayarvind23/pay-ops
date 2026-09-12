"""Noise experiments must recover owned resources without overwriting unrelated changes."""

# Protected lifecycle methods are deliberate fault-injection boundaries.
# pyright: reportPrivateUsage=false

import json
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from test_dependency_scenarios import exercise
from test_noise_scenarios import RUN, fixture
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.concurrency_harness import ConcurrencyRun
from payops.scenarios.contracts import ScenarioReceipt, object_items, object_value
from payops.scenarios.noise_harness import NoiseHarness, noise_running
from payops.scenarios.runner import CleanupUnverified
from payops.scenarios.sampling_gateway import deployment_map


def setup(tmp_path: Path) -> tuple[NoiseHarness, ConcurrencyRun, ScenarioReceipt]:
    """Keep real ownership checks and persistence while substituting only cluster transport."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        runner = NoiseHarness(config, tmp_path / "evidence")
    state = SamplingCluster(Clock()).state()
    off = deepcopy(object_value(deployment_map(state)["processor-adapter"]["spec"]))
    off["replicas"] = 0
    receipt = ScenarioReceipt(
        run_id=RUN, case_id="TELEM-01", mode="fixture_replay", implementation_variant="fixture"
    )
    return runner, ConcurrencyRun(state, off, {}), receipt


@pytest.mark.parametrize("failure", [None, "processor", "inventory_write", "delete"])
def test_cleanup_attempts_independent_resources_despite_failure(
    tmp_path: Path, failure: str | None
) -> None:
    """A failed evidence sink or processor restore must not strand the owned load Job."""
    runner, context, receipt = setup(tmp_path)
    job, _ = fixture()

    def save(*args: object) -> None:
        """Simulate a full evidence disk after the inventory was successfully acquired."""
        if failure == "inventory_write" and args[2] == "cleanup-inventory":
            raise OSError("evidence unavailable")

    with (
        patch.object(
            runner,
            "_restore_processor",
            side_effect=ValueError("processor changed") if failure == "processor" else None,
        ) as restore,
        patch.object(runner, "_save", side_effect=save),
        patch.object(runner, "_wait"),
        patch.object(runner.access, "noise_state", return_value={"jobs": [job], "pods": []}),
        patch.object(
            runner.access,
            "remove_noise",
            side_effect=ValueError("delete conflict") if failure == "delete" else None,
        ) as remove,
        patch.object(runner.access, "state", return_value=context.original),
    ):
        if failure:
            with pytest.raises(CleanupUnverified):
                runner._restore_original(context, tmp_path, receipt)
        else:
            runner._restore_original(context, tmp_path, receipt)
    restore.assert_called_once()
    remove.assert_called_once_with(job, RUN)


@pytest.mark.parametrize("change", ["off", "original", "foreign_uid", "foreign_spec"])
def test_processor_restoration_preserves_concurrent_changes(tmp_path: Path, change: str) -> None:
    """Only the captured original or the exact zero-replica transition can authorize recovery."""
    runner, context, _ = setup(tmp_path)
    original = deployment_map(context.original)["processor-adapter"]
    current = deepcopy(original)
    if change == "off":
        current["spec"] = context.enabled
    elif change == "foreign_uid":
        object_value(current["metadata"])["uid"] = "replacement"
    elif change == "foreign_spec":
        object_value(current["spec"])["replicas"] = 3
    with (
        patch.object(runner.access, "deployment", return_value=current),
        patch.object(runner.access, "replace_spec") as write,
    ):
        if change.startswith("foreign"):
            with pytest.raises(CleanupUnverified):
                runner._restore_processor(context)
        else:
            runner._restore_processor(context)
    assert write.call_count == (1 if change == "off" else 0)
    if change == "off":
        write.assert_called_once_with("processor-adapter", current, original["spec"])


@pytest.mark.parametrize("change", [None, "missing_job", "replacement"])
def test_snapshot_excludes_only_verified_owned_noise_pod(
    tmp_path: Path, change: str | None
) -> None:
    """A distractor cannot hide an unrelated pod or substitute a new CPU worker mid-run."""
    runner, context, receipt = setup(tmp_path)
    job, pod = fixture()
    runner.job = None if change == "missing_job" else job
    runner.noise_uid = "previous" if change == "replacement" else None
    state = deepcopy(context.original)
    state["pods"] = [*object_items(state["pods"]), pod]
    with patch.object(runner.access, "state", return_value=state), patch.object(runner, "_save"):
        if change:
            with pytest.raises(ValueError):
                runner._snapshot(context, False, tmp_path, receipt)
        else:
            assert runner._snapshot(context, False, tmp_path, receipt) == pod
            assert runner.noise_uid == "pod"
    assert noise_running(state, job, RUN)
    assert not noise_running(state, None, RUN)
    assert not noise_running({"pods": []}, job, RUN)


@pytest.mark.parametrize(
    "failure", [None, "prepare", "control", "fault", "recovered", "interrupt", "cleanup"]
)
def test_lifecycle_persists_failure_and_recovers(tmp_path: Path, failure: str | None) -> None:
    """Exercise experiment order, finally recovery, receipt and the retained failure latch."""
    runner, context, _ = setup(tmp_path)
    stages: list[str] = []

    def batch(*args: object) -> None:
        """Fail a selected stage without replacing the surrounding experiment lifecycle."""
        stage = str(args[1])
        stages.append(stage)
        if stage == failure:
            raise ValueError("stage failed")
        if stage == "fault" and failure == "interrupt":
            raise KeyboardInterrupt()

    callback = Mock()
    with (
        patch.object(
            runner,
            "_prepare",
            return_value=context,
            side_effect=ValueError("prepare failed") if failure == "prepare" else None,
        ),
        patch.object(runner, "_batch", side_effect=batch),
        patch.object(runner, "_wait"),
        patch.object(runner, "_restore_processor"),
        patch.object(
            runner,
            "_restore_original",
            side_effect=ValueError("cleanup failed") if failure == "cleanup" else None,
        ) as restore,
        patch.object(runner.access, "create_noise", return_value={}),
        patch.object(runner.access, "replace_spec"),
        patch("payops.scenarios.noise_harness.time.sleep"),
    ):
        if failure == "interrupt":
            with pytest.raises(KeyboardInterrupt):
                runner.run(after_activation=callback)
            path = next(runner.root.glob("*/receipt.json"))
            receipt = ScenarioReceipt.model_validate_json(path.read_bytes())
        else:
            receipt = runner.run(after_activation=callback)
    assert restore.call_count == (0 if failure == "prepare" else 1)
    assert runner.block_file.exists() is (failure == "cleanup")
    assert receipt.cleanup_verified is (failure not in {"prepare", "cleanup"})
    assert callback.call_count == (1 if failure in {None, "recovered", "cleanup"} else 0)
    if failure is None:
        assert stages == ["control", "fault", "recovered"]
        assert receipt.activated and receipt.control_verified and receipt.failure is None


@pytest.mark.parametrize("existing", [False, True])
def test_prepare_preserves_baseline_and_rejects_existing_controller(
    tmp_path: Path, existing: bool
) -> None:
    """The preflight creates only a proposed processor spec and refuses competing controllers."""
    runner, context, receipt = setup(tmp_path)
    with (
        patch.object(runner.access, "verify_scope", return_value={}),
        patch.object(
            runner.access,
            "experiment_resources",
            return_value={"jobs": [{}] if existing else [], "hpas": []},
        ),
        patch.object(runner.access, "state", return_value=context.original),
        patch.object(runner, "_save"),
        patch.object(runner, "_wait"),
    ):
        if existing:
            with pytest.raises(ValueError, match="existing controller"):
                runner._prepare(tmp_path, receipt)
        else:
            prepared = runner._prepare(tmp_path, receipt)
            assert prepared.original == context.original
            assert prepared.enabled == context.enabled
            assert (
                object_value(deployment_map(prepared.original)["processor-adapter"]["spec"])[
                    "replicas"
                ]
                == 1
            )


@pytest.mark.parametrize("stage", ["control", "fault", "recovered"])
def test_batch_checks_actual_driver_receipt_and_rejects_reuse(tmp_path: Path, stage: str) -> None:
    """Fixture HTTP responses exercise the real three-request and persisted-plan validators."""
    runner, context, receipt = setup(tmp_path)
    observed, plan, _ = exercise(tmp_path / "driver", "redis", stage == "fault")
    # The simulated acquisition mode exercises the live-only contract without a cluster call.
    observed = observed.model_copy(update={"mode": "local_kind"})
    output = tmp_path / "traffic" / stage / observed.run_id
    output.mkdir(parents=True)
    (output / "plan.json").write_text(json.dumps(plan))
    _, pod = fixture()
    with (
        patch.object(runner, "_snapshot", side_effect=[ValueError("starting"), pod, pod, pod, pod]),
        patch.object(runner, "_save"),
        patch.object(runner, "_capture_noise") as capture,
        patch(
            "payops.scenarios.noise_harness.TrafficDriver.run",
            new_callable=AsyncMock,
            return_value=observed,
        ),
        patch("payops.scenarios.noise_harness.time.sleep"),
    ):
        runner._batch(context, stage, tmp_path, receipt)
        assert len(context.samples) == 3
        capture.assert_called_once_with(pod, observed, tmp_path, receipt)
        with pytest.raises(ValueError, match="reused a sample"):
            runner._batch(context, stage, tmp_path, receipt)
