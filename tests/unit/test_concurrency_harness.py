"""Failure-boundary tests retain original specs, failed evidence and the cleanup latch."""

# These tests deliberately exercise protected lifecycle failure boundaries.
# pyright: reportPrivateUsage=false

from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest
from test_concurrency_gateway import gateway
from test_concurrency_lifetime import evidence
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.concurrency_harness import ConcurrencyHarness, ConcurrencyRun
from payops.scenarios.contracts import ScenarioReceipt, object_value
from payops.scenarios.runner import CleanupUnverified
from payops.scenarios.sampling_gateway import deployment_map


def setup(tmp_path: Path) -> tuple[ConcurrencyHarness, ConcurrencyRun]:
    """Only external acquisition is substituted; run orchestration and persistence stay real."""
    adapter = gateway(tmp_path)
    harness = ConcurrencyHarness(tmp_path / "config", tmp_path / "evidence", adapter)
    state = SamplingCluster(Clock()).state()
    enabled = deepcopy(object_value(deployment_map(state)["payments-api"]["spec"]))
    enabled["strategy"] = {"type": "Recreate"}
    return harness, ConcurrencyRun(state, enabled, {})


@pytest.mark.parametrize("failure", [None, "control", "parallel", "recovered"])
def test_failed_stage_always_restores_and_persists(tmp_path: Path, failure: str | None) -> None:
    """Every stage exception reaches restoration without reporting an unevaluated control."""
    harness, context = setup(tmp_path)
    stages: list[str] = []

    def stage(*args: object) -> None:
        """Inject a stage failure without substituting the finally path under test."""
        name = str(args[1])
        stages.append(name)
        if name == failure:
            raise RuntimeError("stage failed")

    with (
        patch.object(harness, "_prepare", return_value=context),
        patch.object(harness, "_stage", side_effect=stage),
        patch.object(harness, "_restore_original") as restore,
    ):
        receipt = harness.run()
    restore.assert_called_once()
    assert receipt.cleanup_verified and not harness.block_file.exists()
    assert (receipt.failure is None) is (failure is None)
    assert receipt.control_verified is (failure != "control")
    persisted = ScenarioReceipt.model_validate_json(
        (harness.root / receipt.run_id / "receipt.json").read_text()
    )
    assert persisted == receipt
    assert stages == ["control", "parallel", "recovered"][: len(stages)]


def test_cleanup_error_retains_latch(tmp_path: Path) -> None:
    """Failed cleanup must prevent any next experiment even when stages returned normally."""
    harness, context = setup(tmp_path)
    with (
        patch.object(harness, "_prepare", return_value=context),
        patch.object(harness, "_stage"),
        patch.object(harness, "_restore_original", side_effect=RuntimeError("restore failed")),
    ):
        receipt = harness.run()
    assert not receipt.cleanup_verified and "restore failed" in str(receipt.cleanup_failure)
    assert harness.block_file.exists()
    with pytest.raises(CleanupUnverified):
        harness.run()


@pytest.mark.parametrize("foreign", ["uid", "spec", None])
def test_restore_rejects_foreign_state_and_uses_exact_original(
    tmp_path: Path, foreign: str | None
) -> None:
    """Known enabled specs are reversible; concurrent operator changes cannot be overwritten."""
    harness, context = setup(tmp_path)
    current = deepcopy(deployment_map(context.original)["payments-api"])
    current["spec"] = deepcopy(context.enabled)
    if foreign == "uid":
        object_value(current["metadata"])["uid"] = "foreign"
    elif foreign == "spec":
        object_value(current["spec"])["replicas"] = 99
    receipt = ScenarioReceipt(
        run_id="fixture", case_id="OOM-04", mode="fixture_replay", implementation_variant="fixture"
    )
    with (
        patch.object(harness.access, "deployment", return_value=current),
        patch.object(harness.access, "replace_spec") as write,
        patch.object(harness, "_settle"),
        patch.object(harness, "_wait"),
    ):
        if foreign:
            with pytest.raises(CleanupUnverified):
                harness._restore_original(context, tmp_path, receipt)
            write.assert_not_called()
        else:
            harness._restore_original(context, tmp_path, receipt)
            write.assert_called_once_with(
                "payments-api", current, deployment_map(context.original)["payments-api"]["spec"]
            )


def test_parallel_callback_precedes_restoration(tmp_path: Path) -> None:
    """The incident callback runs after OOM proof while the fault deployment still exists."""
    harness, context = setup(tmp_path)
    _, _, _, identity, window, _ = evidence()
    receipt = ScenarioReceipt(
        run_id="fixture", case_id="OOM-04", mode="fixture_replay", implementation_variant="fixture"
    )
    order: list[str] = []

    def oom(*args: object) -> None:
        """Record verified activation before the callback."""
        order.append("oom")

    def restore(*args: object) -> None:
        """Record restoration after the callback."""
        order.append("restore")

    with (
        patch.object(harness, "_enable", return_value=identity),
        patch.object(harness, "_traffic", return_value=window),
        patch.object(harness, "_oom", side_effect=oom),
        patch.object(harness, "_restore_original", side_effect=restore),
    ):
        harness._stage(context, "parallel", tmp_path, receipt, lambda: order.append("investigate"))
    assert order == ["oom", "investigate", "restore"] and receipt.activated


def test_prepare_and_runtime_checks_use_real_owned_state(tmp_path: Path) -> None:
    """Real runtime validators accept the baseline and pin only the payments worker image."""
    from payops.scenarios.concurrency_specs import RUNTIME_IMAGE_DIGEST
    from payops.scenarios.contracts import object_items
    from payops.scenarios.recipes import container

    harness, _ = setup(tmp_path)
    cluster = SamplingCluster(Clock())
    item = container(object_value(cluster.documents["payments-api"]["spec"]))
    object_value(item["resources"])["requests"] = {"cpu": "50m", "memory": "96Mi"}
    state = cluster.state()
    directory, receipt = harness._start("OOM-04")
    with (
        patch.object(harness.access, "verify_scope", return_value={"fixture": True}),
        patch.object(harness.access, "state", return_value=state),
        patch.object(harness, "_wait"),
    ):
        context = harness._prepare(directory, receipt)
    assert harness._identities(context, state, False) == context.peers
    cluster.documents["payments-api"]["spec"] = context.enabled
    enabled = cluster.state()
    pod = object_items(enabled["pods"])[0]
    status = object_items(object_value(pod["status"])["containerStatuses"])[0]
    status["imageID"] = "image@" + RUNTIME_IMAGE_DIGEST
    assert (
        harness._identities(context, enabled, True)["payments-api"].pod_uid
        == context.peers["payments-api"].pod_uid
    )
    status["imageID"] = "foreign"
    with pytest.raises(ValueError, match="worker image"):
        harness._identities(context, enabled, True)


def test_traffic_reuses_driver_receipt_and_rejects_reused_samples(tmp_path: Path) -> None:
    """Exercise validation and persistence using an actual fixture-transport driver output."""
    import json
    from unittest.mock import AsyncMock

    from test_concurrency_traffic import exercise

    harness, context = setup(tmp_path)
    observed, plan, _ = exercise(tmp_path, False)
    directory, receipt = harness._start("OOM-04")
    output = directory / "traffic" / "control" / observed.run_id
    output.mkdir(parents=True)
    (output / "plan.json").write_text(json.dumps(plan))
    with (
        patch("payops.scenarios.concurrency_harness.TrafficDriver") as driver,
        patch.object(harness.access, "mode", "fixture_replay"),
    ):
        driver.return_value.run = AsyncMock(return_value=observed)
        result = harness._traffic(context, "control", directory, receipt)
        assert len(result.samples) == 8
        with pytest.raises(ValueError, match="reused"):
            harness._traffic(context, "control", directory, receipt)


def test_capture_and_oom_validate_real_fixture_provenance(tmp_path: Path) -> None:
    """Raw records and actual controller-chain validation are joined without mocking the proof."""
    from test_concurrency_evidence import START

    from payops.scenarios.contracts import object_items

    harness, context = setup(tmp_path)
    observed, original, expected, identity, window, raw = evidence()
    documents = object_items(context.original["deployments"])
    documents[0].clear()
    documents[0].update(original)
    context.enabled, context.requested = expected, START.isoformat()
    directory, receipt = harness._start("OOM-04")
    with (
        patch.object(harness.access, "snapshot", return_value=observed),
        patch.object(harness.access, "read_log", return_value={"text": raw}),
    ):
        before, after, captured = harness._capture(context, True, directory, receipt)
        assert before == after == observed and captured == raw
        harness._oom(context, identity, window, directory, receipt)
    assert any(row.name.endswith("oom-proof.json") for row in receipt.artifacts)


def test_serial_stage_requires_complete_records_and_stable_process(tmp_path: Path) -> None:
    """Both sides of current-log acquisition require the same healthy container identity."""
    from test_concurrency_evidence import END, SAMPLES, START, records

    from payops.scenarios.concurrency_traffic import TrafficWindow

    harness, context = setup(tmp_path)
    _, _, _, identity, _, _ = evidence()
    raw = "\n".join(
        row.timestamp.isoformat() + " " + row.model_dump_json() for row in records(False)
    )
    directory, receipt = harness._start("OOM-04")
    with (
        patch.object(harness, "_enable", return_value=identity),
        patch.object(harness, "_traffic", return_value=TrafficWindow(SAMPLES, START, END, 0)),
        patch.object(harness, "_settle", return_value=identity),
        patch.object(harness, "_capture", return_value=({}, {}, raw)),
        patch.object(harness, "_restore_original") as restore,
    ):
        harness._stage(context, "control", directory, receipt)
    restore.assert_called_once()
    assert any(row.name.endswith("control-memory.json") for row in receipt.artifacts)


def test_enable_journals_before_cas_and_rejects_reused_process(tmp_path: Path) -> None:
    """A successful stage start has a post-request owned pod and a unique container identity."""
    from test_concurrency_evidence import START

    from payops.scenarios.contracts import object_items

    harness, context = setup(tmp_path)
    observed, original, expected, identity, _, _ = evidence()
    documents = object_items(context.original["deployments"])
    documents[0].clear()
    documents[0].update(original)
    context.enabled = expected
    directory, receipt = harness._start("OOM-04")

    def write(*args: object) -> None:
        """A mutation is forbidden until its exact transition artifact exists."""
        assert any(row.name.endswith("transition.json") for row in receipt.artifacts)

    with (
        patch.object(harness.access, "deployment", return_value=original),
        patch.object(harness.access, "replace_spec", side_effect=write),
        patch.object(harness.access, "snapshot", return_value=observed),
        patch.object(harness, "_settle", return_value=identity),
        patch("payops.scenarios.concurrency_harness.utc_timestamp", return_value=START.isoformat()),
    ):
        assert harness._enable(context, directory, receipt) == identity
        with pytest.raises(ValueError, match="fresh owned process"):
            harness._enable(context, directory, receipt)


def test_settle_retries_invalid_state_without_accepting_it(tmp_path: Path) -> None:
    """Retained invalid state cannot satisfy the ready-runtime predicate."""
    harness, context = setup(tmp_path)
    directory, receipt = harness._start("OOM-04")
    with (
        patch.object(harness.access, "state", side_effect=[{}, context.original]),
        patch("payops.scenarios.runner.time.sleep"),
    ):
        identity = harness._settle(context, False, directory, receipt)
    assert identity.pod_name.startswith("payments-api-")
    assert len(receipt.artifacts) == 2
