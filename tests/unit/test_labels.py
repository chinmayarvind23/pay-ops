"""A telemetry gap cannot become a root-cause success by changing the scored question."""

import json
from pathlib import Path
from typing import Any

import pytest

from payops.evaluation.labels import load_labels, score_causes, score_conditions

ROOT = Path(__file__).resolve().parents[2]


def content() -> bytes:
    """Use the checked-in pre-evaluation labels rather than reconstructing test-only gold."""
    return (ROOT / "evals/golden/release-v2.json").read_bytes()


def test_cause_and_observation_metrics_have_separate_denominators() -> None:
    """Recognizing all four distractors alone earns no incident-cause Recall credit."""
    labels = load_labels(content())
    conditions = {case: tuple(values) for case, values in labels.observation_conditions.items()}
    causes = score_causes(labels, conditions, 3)
    observed = score_conditions(labels, conditions, 1)
    assert (causes.hits, causes.total) == (0, 24)
    assert (observed.hits, observed.total) == (4, 4)
    assert score_causes(labels, {}, 1).total == 24
    assert score_conditions(labels, {}, 1).total == 4
    assert labels.primary_causes["TELEM-03"] == ("PROCESSOR_LATENCY",)
    assert labels.observation_conditions["TELEM-03"] == ("TRACE_SAMPLING_GAP",)


def test_initial_four_case_gold_is_unchanged() -> None:
    """Published development-baseline results keep their historical cause vocabulary."""
    initial = json.loads((ROOT / "evals/golden/local-initial.json").read_bytes())
    labels = load_labels(content())
    assert {case: list(labels.primary_causes[case]) for case in initial} == initial


@pytest.mark.parametrize("variant", ["version", "case", "condition", "empty", "duplicate", "mix"])
def test_changed_census_or_ambiguous_labels_fail(variant: str) -> None:
    """Missing cases, repeated alternatives and substituted conditions fail before scoring."""
    raw: dict[str, Any] = json.loads(content())
    if variant == "version":
        raw["version"] = 1
    elif variant == "case":
        del raw["primary_causes"]["OOM-01"]
    elif variant == "condition":
        del raw["observation_conditions"]["TELEM-03"]
    elif variant == "empty":
        raw["observation_conditions"]["TELEM-03"] = []
    elif variant == "duplicate":
        raw["primary_causes"]["TELEM-03"] *= 2
    else:
        raw["primary_causes"]["TELEM-03"] = ["TRACE_SAMPLING_GAP"]
    with pytest.raises(ValueError):
        load_labels(json.dumps(raw).encode())


@pytest.mark.parametrize("raw", [b"x" * 16385, b'{"version":2,"version":2}', b"[" * 2000])
def test_duplicate_oversized_and_nested_gold_rejected(raw: bytes) -> None:
    """Malformed gold is never normalized into a changed release suite."""
    with pytest.raises(ValueError):
        load_labels(raw)


def test_scorer_revalidates_mutated_contract_and_rejects_unknown_predictions() -> None:
    """Pydantic model_copy or mutable nested mappings cannot silently shrink denominators."""
    labels = load_labels(content())
    with pytest.raises(ValueError):
        score_conditions(labels, {"OOM-01": ("TRACE_SAMPLING_GAP",)}, 1)
    labels.primary_causes.pop("OOM-01")
    with pytest.raises(ValueError):
        score_causes(labels, {}, 1)


def test_removing_condition_cannot_make_it_an_incident_cause() -> None:
    """A paired label rewrite cannot evade separation by removing the overlapping condition."""
    raw: dict[str, Any] = json.loads(content())
    raw["observation_conditions"]["TELEM-03"] = ["DELAYED_METRICS"]
    raw["primary_causes"]["TELEM-03"] = ["TRACE_SAMPLING_GAP"]
    with pytest.raises(ValueError):
        load_labels(json.dumps(raw).encode())
