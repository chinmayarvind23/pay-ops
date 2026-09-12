"""Attribution requires semantic reference membership as well as case-scoped source integrity."""

import json
from pathlib import Path
from shutil import copytree

import pytest
from pydantic import TypeAdapter

from payops.evaluation.labels import EXPECTED, load_labels
from payops.evaluation.replay_attribution import FactReview, score_replay_attribution
from payops.evaluation.replay_facts import load_cases
from payops.orchestrator.model_runtime import ModelObservation
from payops.orchestrator.reasoning import RankedCause, ReasoningDecision

ROOT = Path(__file__).resolve().parents[2] / "evals/replay-v1"
CASES = load_cases((ROOT / "corpus.json").read_bytes())
REVIEWS = TypeAdapter(tuple[FactReview, ...]).validate_python(
    json.loads((ROOT / "attribution-review-v1.json").read_bytes())["reviews"]
)
VOCABULARY = load_labels((ROOT.parent / "golden/release-v2.json").read_bytes()).cause_vocabulary()


def prediction(cause: str, evidence: tuple[str, ...] = ("e1",)) -> ModelObservation:
    """Use a typed model observation with deliberately caller-controlled source links."""
    return ModelObservation(
        status="OK",
        decision=ReasoningDecision(
            decision="finish", summary="Recorded prediction", reads=(),
            hypotheses=(RankedCause(
                cause_code=cause, confidence=0.0, supporting_evidence_ids=evidence,
                refuting_evidence_ids=(), missing_evidence=(),
            ),),
        ),
    )


def outcomes() -> dict[str, ModelObservation]:
    """Failure rows stay explicit so selective omission cannot improve apparent coverage."""
    return {case: ModelObservation(status="ERROR") for case in EXPECTED}


def test_semantics_case_scope_deduplication_and_invalid_ids() -> None:
    """The same e1 in two cases is not interchangeable; duplicate citations cannot pad the score."""
    observed = outcomes()
    observed["DEP-01"] = prediction("PROCESSOR_UNAVAILABLE", ("e1", "e1", "missing"))
    observed["DEP-02"] = prediction("PROCESSOR_UNAVAILABLE")
    observed["DEP-03"] = prediction("unknown-cause")
    result = score_replay_attribution(ROOT, CASES, REVIEWS, observed, VOCABULARY)
    assert (result.correct, result.total, result.invalid) == (1, 4, 2)
    assert result.accuracy == 0.25
    assert result.cases == 24 and result.cases_without_decision == 21
    assert result.cases_with_citations == 3


def test_no_predictions_is_undefined_and_missing_case_is_rejected() -> None:
    """No citations cannot become 100%; omitting a failed row cannot hide that case."""
    observed = outcomes()
    result = score_replay_attribution(ROOT, CASES, REVIEWS, observed, VOCABULARY)
    assert result.accuracy is None and result.total == 0 and result.cases_without_decision == 24
    observed.pop("DEP-01")
    with pytest.raises(ValueError, match="every case"):
        score_replay_attribution(ROOT, CASES, REVIEWS, observed, VOCABULARY)


@pytest.mark.parametrize("change", ["missing", "duplicate", "hash", "cause", "repeated-cause"])
def test_invalid_or_incomplete_review_cannot_score(change: str) -> None:
    """Annotations cannot silently omit difficult sources or bind to a different source/version."""
    reviews = list(REVIEWS)
    if change == "missing":
        reviews.pop()
    elif change == "duplicate":
        reviews.append(reviews[0])
    else:
        changes = {
            "hash": {"sha256": "0" * 64},
            "cause": {"supported_causes": ("unknown",)},
            "repeated-cause": {"supported_causes": ("PROCESSOR_UNAVAILABLE",) * 2},
        }
        reviews[0] = reviews[0].model_copy(update=changes[change])
    with pytest.raises(ValueError):
        score_replay_attribution(ROOT, CASES, tuple(reviews), outcomes(), VOCABULARY)


def test_uncited_source_corruption_still_invalidates_review(tmp_path: Path) -> None:
    """Even a source no prediction cites must retain its original bytes."""
    root = tmp_path / "corpus"
    copytree(ROOT, root)
    (root / CASES[-1].facts[-1].path).write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        score_replay_attribution(root, CASES, REVIEWS, outcomes(), VOCABULARY)


def test_source_case_census_is_fixed() -> None:
    """A partial corpus cannot be graded as the declared 24-case evaluation."""
    with pytest.raises(ValueError, match="24"):
        score_replay_attribution(ROOT, CASES[:-1], REVIEWS, outcomes(), VOCABULARY)
