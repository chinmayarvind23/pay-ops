"""Development scoring preserves failures and freezes the evidence used to compute results."""

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from payops.contracts import IncidentReport, RootCauseHypothesis
from payops.evaluation import local
from payops.scenarios.contracts import CaseId, ScenarioReceipt
from payops.tools.collect import Collection


def test_write_once_preserves_original_prediction(tmp_path: Path) -> None:
    """A second scoring attempt must not replace a previously retained prediction."""
    path = tmp_path / "prediction.json"
    local.write_once(path, "first")
    with pytest.raises(FileExistsError):
        local.write_once(path, "second")
    assert path.read_text() == "first"


def test_unverified_cleanup_stops_injection_but_retains_denominator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A contaminated sandbox cannot continue, and the unrun cases cannot disappear."""
    calls: list[CaseId] = []

    class FailedRunner:
        """Only the injection seam is replaced; real manifest and scoring code still execute."""

        def __init__(self, _kubeconfig: Path, _output: Path) -> None:
            """No infrastructure is contacted in a failure-path test."""

        def run(self, case: CaseId, _callback: Callable[[], None]) -> ScenarioReceipt:
            """Simulate a cleanup failure before any valid investigation output exists."""
            calls.append(case)
            return ScenarioReceipt(
                run_id="failed",
                case_id=case,
                mode="fixture_replay",
                implementation_variant="test",
                failure="injection failed",
                cleanup_failure="restore failed",
            )

    monkeypatch.setattr(local, "LocalScenarioRunner", FailedRunner)
    result = local.run_suite(tmp_path / "config", tmp_path, Path("evals/golden/local-initial.json"))
    score = json.loads((result / "score.json").read_text())
    assert len(calls) == 1
    assert score["recall_at_1"] == {"hits": 0, "total": 4, "k": 1}
    assert score["recall_at_3"] == {"hits": 0, "total": 4, "k": 3}
    assert score["predictions"] == {}
    assert (result / "gold.json").is_file()
    assert (result / "source-manifest.json").is_file()


def test_incomplete_gold_rejected_before_creating_run(tmp_path: Path) -> None:
    """Operators cannot inflate development recall by selecting only an easy subset."""
    gold = tmp_path / "gold.json"
    gold.write_text('{"ROLLOUT-01":["STARTUP_FAILURE"]}')
    output = tmp_path / "runs"
    with pytest.raises(ValueError, match="all four"):
        local.run_suite(tmp_path / "config", output, gold)
    assert not output.exists()


def test_gold_duplicate_keys_and_causes_rejected() -> None:
    """Malformed frozen labels are rejected before denominator validation or injection."""
    with pytest.raises(ValueError, match="duplicate gold"):
        local.load_gold(b'{"ROLLOUT-01":["a"],"ROLLOUT-01":["b"]}')
    gold = json.loads(Path("evals/golden/local-initial.json").read_text())
    gold["ROLLOUT-01"] *= 2
    with pytest.raises(ValueError, match="distinct"):
        local.load_gold(json.dumps(gold).encode())


@pytest.mark.parametrize("ranked", [False, True])
def test_investigation_persists_validated_prediction_without_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ranked: bool
) -> None:
    """Report terminal state follows grounded diagnosis and is recoverable from saved bytes."""
    config = tmp_path / "config"
    config.write_text("test")
    output = tmp_path / "output"
    output.mkdir()
    collection = Collection(incident_id="incident", evidence=(), failures=())
    hypotheses = (
        (RootCauseHypothesis(cause_code="STARTUP_FAILURE", confidence=0.5),) if ranked else ()
    )

    def collect(*_args: object) -> Collection:
        """Supply a typed empty evidence bundle without calling external readers."""
        return collection

    def rank(*_args: object) -> tuple[RootCauseHypothesis, ...]:
        """The report builder receives a controlled ranking, independently of scorer labels."""
        return hypotheses

    monkeypatch.setattr(local, "collect_local", collect)
    monkeypatch.setattr(local, "rank_evidence", rank)
    local.investigation_callback(config, output)()
    path = output / "prediction.json"
    report = IncidentReport.model_validate_json(path.read_bytes())
    assert report.terminal_state == ("ESCALATED" if ranked else "EVIDENCE_INSUFFICIENT")
    assert report.duration_seconds >= 0
    assert local.frozen_predictions({"case": path}) == {
        "case": ("STARTUP_FAILURE",) if ranked else ()
    }


def test_completed_runs_are_scored_from_saved_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Saved output supplies rankings; a completed callback flag alone never supplies labels."""

    class CompleteRunner:
        """Exercise real callback binding while replacing only the operator injection layer."""

        def __init__(self, _config: Path, _output: Path) -> None:
            """Avoid external cluster operations in metric-path testing."""

        def run(self, case: CaseId, callback: Callable[[], None]) -> ScenarioReceipt:
            """Every case gets the same diagnosis, so only one gold case should match."""
            callback()
            return ScenarioReceipt(
                run_id=case,
                case_id=case,
                mode="fixture_replay",
                implementation_variant="test",
                activated=True,
                cleanup_verified=True,
                investigation_status="completed",
            )

    def predict(_config: Path, output: Path) -> IncidentReport:
        """The fake diagnosis accepts no case key or scorer labels, matching the real seam."""
        output.mkdir(parents=True)
        report = IncidentReport(
            incident_id="incident",
            mode="fixture_replay",
            duration_seconds=1,
            terminal_state="ESCALATED",
            ranked_root_causes=(RootCauseHypothesis(cause_code="STARTUP_FAILURE", confidence=0.5),),
        )
        local.write_once(output / "prediction.json", report.model_dump_json())
        return report

    monkeypatch.setattr(local, "LocalScenarioRunner", CompleteRunner)
    monkeypatch.setattr(local, "investigate", predict)
    result = local.run_suite(tmp_path / "config", tmp_path, Path("evals/golden/local-initial.json"))
    score = json.loads((result / "score.json").read_text())
    assert score["recall_at_1"] == {"hits": 1, "total": 4, "k": 1}
    assert len(score["prediction_sha256"]) == 4
    assert score["source_manifest_unchanged"] is True
