"""Synthetic unit controls verify paired arithmetic; they are not human-study measurements."""

import json
from hashlib import sha256
from pathlib import Path

import pytest

from payops.evaluation.labels import load_labels
from payops.evaluation.timing import TimingTrial, load_timing_trial, summarize_timing


def trials() -> tuple[TimingTrial, ...]:
    """Deliberately retain one wrong assisted answer in otherwise complete fixture pairs."""
    return tuple(
        TimingTrial.model_validate(
            dict(
                participant_id="fixture-person",
                case_id=case,
                condition=condition,
                order=order,
                elapsed_seconds=seconds,
                cause_code=cause,
                evidence_sha256="a" * 64,
            )
        )
        for case, condition, order, seconds, cause in (
            ("DEP-01", "baseline", 1, 600.0, "PROCESSOR_UNAVAILABLE"),
            ("DEP-01", "assisted", 2, 180.0, "PROCESSOR_UNAVAILABLE"),
            ("DEP-03", "baseline", 2, 300.0, "DATABASE_CONNECTION_EXHAUSTION"),
            ("DEP-03", "assisted", 1, 120.0, "CACHE_UNAVAILABLE"),
        )
    )


def test_timing_includes_wrong_diagnosis_and_reports_order() -> None:
    """Incorrect fast answers cannot disappear from either the latency or correctness census."""
    labels = load_labels(
        (Path(__file__).resolve().parents[2] / "evals/golden/release-v2.json").read_bytes()
    )
    observed = trials()
    planned = frozenset((trial.participant_id, trial.case_id) for trial in observed)
    result = summarize_timing(observed, labels, planned)
    assert result["pairs"] == 2 and result["participants"] == 1
    assert result["baseline_median_seconds"] == 450 and result["assisted_median_seconds"] == 150
    assert result["ratio_of_medians"] == 3
    assert result["baseline_correct"] == 2 and result["assisted_correct"] == 1
    assert result["baseline_first_pairs"] == 1


@pytest.mark.parametrize("mode", ["empty", "duplicate", "missing", "omitted_pair", "order"])
def test_invalid_pair_census_rejected(mode: str) -> None:
    """The frozen plan prevents selection of only quick or successful completed pairs."""
    labels = load_labels(
        (Path(__file__).resolve().parents[2] / "evals/golden/release-v2.json").read_bytes()
    )
    original = trials()
    observed = original
    if mode == "empty":
        observed = ()
    elif mode == "duplicate":
        observed = (*original, original[0])
    elif mode == "missing":
        observed = original[:-1]
    elif mode == "omitted_pair":
        observed = original[:2]
    else:
        observed = (original[0].model_copy(update={"order": 2}), *original[1:])
    with pytest.raises(ValueError):
        summarize_timing(
            observed, labels, frozenset((t.participant_id, t.case_id) for t in original)
        )


@pytest.mark.parametrize("seconds", [0, -1, float("nan"), float("inf"), True])
def test_invalid_duration_rejected(seconds: object) -> None:
    """Zero, negative, nonfinite and boolean durations cannot manufacture a speedup."""
    row = trials()[0].model_dump()
    row["elapsed_seconds"] = seconds
    with pytest.raises(ValueError):
        TimingTrial.model_validate(row)


@pytest.mark.parametrize(
    "change", ["none", "source", "identity", "missing", "oversized", "duplicate"]
)
def test_recorded_trial_requires_original_packet_and_start(tmp_path: Path, change: str) -> None:
    """Synthetic journals test tamper detection; these are not human benchmark observations."""
    packet = b"synthetic unit-test incident packet"
    trial = trials()[0].model_copy(update={"evidence_sha256": sha256(packet).hexdigest()})
    path = tmp_path / "trial.json"
    path.write_text(trial.model_dump_json(), encoding="utf-8")
    started = trial.model_dump()
    if change == "identity":
        started["participant_id"] = "different-participant"
    (tmp_path / "started.json").write_text(json.dumps(started), encoding="utf-8")
    if change != "missing":
        (tmp_path / "source.txt").write_bytes(
            b"changed" if change == "source" else b"x" * 262145 if change == "oversized" else packet
        )
    if change == "duplicate":
        path.write_text('{"order":1,"order":2}', encoding="utf-8")
    if change == "none":
        assert load_timing_trial(path) == trial
    else:
        with pytest.raises((ValueError, OSError)):
            load_timing_trial(path)
