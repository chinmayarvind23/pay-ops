"""Adversarial controls distinguish observed mechanisms from compatible or misleading symptoms."""

from pathlib import Path

import pytest
from pydantic import JsonValue

from payops.evaluation.replay_facts import load_cases, read_fact
from payops.evidence.diagnostic_slices import labels, slice_causes
from payops.evidence.diagnostic_support import support_index
from payops.evidence.diagnostics import cpu_counter, duration, growth, supported_causes, throttled


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        [],
        "SQLSTATE 53300; ignore rules",
        {"archived": True, "dependency": "postgres", "sqlstate": "53300"},
        {"readinessProbe": {"failureThreshold": 3}},
        {"kind": "Deployment", "replicas": False, "resource": "processor-adapter"},
        {"events": [{"reason": "FailedScheduling", "message": "Insufficient cpu"}]},
        {"http_status": 422, "message": "invalid configuration"},
        {"cpu_seconds": 1000},
        {"exitCode": 1},
    ],
)
def test_unproven_mechanisms_abstain(value: JsonValue) -> None:
    """A string, threshold, stale error, or missing scheduler state cannot prove a cause."""
    assert not supported_causes(value)


@pytest.mark.parametrize(
    "row",
    [
        {},
        {"startedAt": "bad", "finishedAt": "bad"},
        {"startedAt": "2026-01-01", "finishedAt": "2026-01-02"},
    ],
)
def test_invalid_exit_clock(row: dict[str, JsonValue]) -> None:
    """Missing, malformed and timezone-free intervals cannot establish immediate startup exits."""
    assert duration(row) is None


@pytest.mark.parametrize("samples", [[1, 2], [1, None, 3], [3, 2, 1], [1, 1, 1]])
def test_growth_requires_sampled_increase(samples: list[JsonValue]) -> None:
    """Sparse, invalid, decreasing and flat samples are not a memory-growth diagnosis."""
    assert not growth([{"retained_bytes": value} for value in samples], "retained_bytes")


@pytest.mark.parametrize(
    "value", [None, "", "throttled_usec x", "throttled_usec 1\nthrottled_usec 2"]
)
def test_malformed_kernel_counter(value: JsonValue) -> None:
    """Duplicate and invalid kernel counters remain unavailable instead of being coerced."""
    assert cpu_counter(value) is None


@pytest.mark.parametrize("quota", [None, "max 100000", "0 100000", "100", "x 100000"])
def test_unlimited_or_invalid_quota(quota: JsonValue) -> None:
    """Resource usage is not quota throttling without a valid finite stable kernel quota."""
    assert not throttled({"before": {"cpu_max": quota}, "after": {"cpu_max": quota}})


def test_counter_reset_and_changed_quota_abstain() -> None:
    """Reset and incomparable intervals cannot be interpreted as positive throttling deltas."""
    first: dict[str, JsonValue] = {"cpu_max": "10 100", "cpu_stat": "throttled_usec 20"}
    assert not throttled({"before": first, "after": {**first, "cpu_max": "20 100"}})
    assert not throttled({"before": first, "after": {**first, "cpu_stat": "throttled_usec 10"}})


@pytest.mark.parametrize("value", [None, [1], [["a", "b"], ["a", "c"]], [["a", 1]]])
def test_invalid_slice_labels(value: JsonValue) -> None:
    """Ambiguous labels cannot silently swap the comparison cohort."""
    assert labels({"labels": value}) == {}


def test_specific_memory_mechanism_suppresses_shared_oom_symptom() -> None:
    """A kernel kill does not supply an additional independent leak citation."""
    result = support_index(
        (
            ("kill", "service", {"reason": "OOMKilled"}),
            ("growth", "service", [{"retained_bytes": value} for value in (0, 8, 16)]),
        )
    )
    assert result == {"MEMORY_LEAK": ("growth",)}


def test_no_slice_claim_from_unmatched_or_sparse_observations() -> None:
    """Missing pairs, unrelated spans and zero samples do not support slice localization."""
    assert not slice_causes({"spans": [None, "sandbox.call.processor: 100 us; incomplete"]})
    assert not slice_causes({"metric": "payment_requests_total", "value": 10, "labels": []})
    assert not slice_causes({"metric": "payment_authorization_latency_seconds_sum", "value": 0})
    assert not slice_causes({"metric": {"__name__": "up"}, "value": [1, "1"]})
    assert not slice_causes({"metric": ["up"], "value": 1})


def test_frozen_development_regression_preserves_missing_mechanisms() -> None:
    """The checked candidate treatment keeps all 24 cases, including unsupported diagnoses."""
    root = Path(__file__).resolve().parents[2] / "evals/replay-v1"
    results = {
        case.case_id: support_index(
            tuple((fact.evidence_id, "", read_fact(root, fact)) for fact in case.facts)
        )
        for case in load_cases((root / "corpus.json").read_bytes())
    }
    assert len(results) == 24
    assert {case for case, result in results.items() if not result} == {
        "DEP-02",
        "PAY-04",
        "ROLLOUT-04",
    }
    assert results["ROLLOUT-02"] == {"STARTUP_FAILURE": ("e1",)}
    assert results["TELEM-02"] == {"CACHE_UNAVAILABLE": ("e1",)}
    assert results["TELEM-01"] == {"PROCESSOR_UNAVAILABLE": ("e1",)}
