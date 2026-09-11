"""Metric arithmetic cannot be tuned by dropping failures or repeating citations."""

from datetime import timedelta
from fractions import Fraction
from pathlib import Path

import pytest

from payops.contracts import utc_now
from payops.evaluation.metrics import Attribution, attribution_accuracy, percentile, recall_at_k
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import Observation, normalize


def test_recall_includes_missing_cases_and_exact_gate() -> None:
    """22/24 must pass its exact ratio despite rounded decimal thresholds."""
    gold = {f"case-{index}": ("cause",) for index in range(24)}
    predictions = {f"case-{index}": ("cause",) for index in range(22)}
    score = recall_at_k(gold, predictions, 3)
    assert (score.hits, score.total) == (22, 24)
    assert score.fraction == Fraction(22, 24)
    assert score.meets(22, 24)
    assert not score.meets(23, 24)
    with pytest.raises(ValueError):
        score.meets(1, 0)
    with pytest.raises(ValueError):
        score.meets(25, 24)
    with pytest.raises(ValueError):
        score.meets(-1, 24)


def test_recall_top_k_and_alternative_gold() -> None:
    """The scorer permits adjudicated alternatives without letting duplicate ranks help."""
    gold = {"a": ("cause", "alternative"), "b": ("cause",)}
    predictions = {"a": ("other", "alternative"), "b": ()}
    assert recall_at_k(gold, predictions, 1).hits == 0
    assert recall_at_k(gold, predictions, 2).hits == 1
    with pytest.raises(ValueError):
        recall_at_k(gold, {"a": ("cause", "cause")}, 3)


@pytest.mark.parametrize("invalid", ["empty", "extra", "zero_k", "no_gold"])
def test_recall_invalid_suites_fail(invalid: str) -> None:
    """A malformed suite is not an empty success or a changed denominator."""
    gold = {} if invalid == "empty" else {"a": () if invalid == "no_gold" else ("cause",)}
    predictions = {"unknown": ("cause",)} if invalid == "extra" else {}
    with pytest.raises(ValueError):
        recall_at_k(gold, predictions, 0 if invalid == "zero_k" else 1)


def test_attribution_verifies_relation_content_and_scope(tmp_path: Path) -> None:
    """A correct ID attached to the wrong cause or relation remains incorrect."""
    store = ArtifactStore(tmp_path)
    now = utc_now()
    evidence = normalize(
        Observation(
            source="LOG",
            resource="processor",
            observed_at=now,
            query="logs.recent",
            summary="connection refused",
        ),
        "incident-a",
        now - timedelta(seconds=1),
        now + timedelta(seconds=1),
        store,
    )
    correct = Attribution(
        incident_id="incident-a",
        cause_code="processor_unavailable",
        evidence_id=evidence.evidence_id,
        relation="supports",
    )
    wrong = correct.model_copy(update={"relation": "refutes"})
    missing = correct.model_copy(update={"evidence_id": "missing"})
    score = attribution_accuracy(
        (correct, correct, wrong, missing),
        frozenset({correct}),
        {evidence.evidence_id: evidence},
        store,
    )
    assert (score.correct, score.total, score.invalid) == (1, 3, 1)
    assert score.accuracy == pytest.approx(1 / 3)
    foreign = correct.model_copy(update={"incident_id": "foreign"})
    foreign_score = attribution_accuracy(
        (foreign,), frozenset({foreign}), {evidence.evidence_id: evidence}, store
    )
    assert (foreign_score.correct, foreign_score.invalid) == (0, 1)
    store.path_for(evidence.artifact_sha256).write_text("changed")
    corrupted = attribution_accuracy(
        (correct,), frozenset({correct}), {evidence.evidence_id: evidence}, store
    )
    assert (corrupted.correct, corrupted.invalid) == (0, 1)
    assert attribution_accuracy((), frozenset(), {}, store).accuracy is None


def test_attribution_mapping_cannot_alias_another_id(tmp_path: Path) -> None:
    """Lookup table keys do not override the signed artifact envelope's evidence identity."""
    store = ArtifactStore(tmp_path)
    now = utc_now()
    item = normalize(
        Observation(
            source="LOG",
            resource="processor",
            observed_at=now,
            query="logs.recent",
            summary="unavailable",
        ),
        "incident-a",
        now,
        now,
        store,
    )
    link = Attribution(
        incident_id="incident-a", cause_code="cause", evidence_id="alias", relation="supports"
    )
    score = attribution_accuracy((link,), frozenset({link}), {"alias": item}, store)
    assert (score.correct, score.invalid) == (0, 1)


def test_latency_quantile_requires_real_finite_samples() -> None:
    """Use a declared nearest-rank estimator rather than a fabricated target duration."""
    assert percentile(tuple(range(1, 21)), 0.95) == 19
    assert percentile((4.0,), 0.95) == 4
    assert percentile((1.0, 2.0, 3.0, 4.0), 0.95) == 4.0
    for values, quantile in [((), 0.95), ((float("nan"),), 0.95), ((-1.0,), 0.95), ((1.0,), 0)]:
        with pytest.raises(ValueError):
            percentile(values, quantile)
