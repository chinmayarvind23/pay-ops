"""Reject stale, mixed, fabricated or incomplete allocation sequences before OOM qualification."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from payops.scenarios.leak_evidence import LeakRecord, parse_records, validate_progression
from payops.scenarios.leak_workload import MIB, Mode

START = datetime(2026, 9, 11, tzinfo=UTC)
END = START + timedelta(seconds=34)


def sequence(mode: Mode) -> tuple[LeakRecord, ...]:
    """Use explicit kernel growth distinct from declared retained-byte counters."""
    count = 40 if mode == "released-v1" else 20
    phases = ["started"] + ["allocated"] * count + (["released"] if count == 40 else [])
    records: list[LeakRecord] = []
    for index, phase in enumerate(phases):
        step = index if phase == "allocated" else 0
        record = {
            "event": "synthetic.leak",
            "mode": mode,
            "phase": phase,
            "step": step,
            "retained_bytes": step * 8 * MIB if mode == "retained-v1" else 0,
            "cgroup_current_bytes": (50 + step * 8) * MIB if mode == "retained-v1" else 60 * MIB,
            "cgroup_limit_bytes": 256 * MIB,
            "timestamp": (START + timedelta(seconds=10 + index / 2)).isoformat(),
            "monotonic_ns": 1_000_000_000 + index * 500_000_000,
            "pid": 1,
        }
        records.append(LeakRecord.model_validate_json(json.dumps(record)))
    return tuple(records)


@pytest.mark.parametrize("mode", ["retained-v1", "released-v1"])
def test_timestamped_source_round_trip(mode: Mode) -> None:
    """An intact current sequence survives raw-source parsing and semantic validation."""
    records = sequence(mode)
    raw = "\n".join(
        record.timestamp.isoformat() + " " + record.model_dump_json() for record in records
    )
    parsed = parse_records("unrelated log\n" + raw)
    assert parsed == records
    assert len(validate_progression(parsed, mode, START, END)) == (
        40 if mode == "released-v1" else 20
    )


@pytest.mark.parametrize(
    "change",
    [
        "duplicate",
        "pid",
        "mode",
        "step",
        "bytes",
        "flat",
        "stale",
        "missing",
        "release",
        "clock",
        "boundary",
    ],
)
def test_invalid_retention_sources_reject(change: str) -> None:
    """Correct-looking OOM metadata must not rescue a false allocation progression."""
    records = list(sequence("retained-v1"))
    changes = {
        "pid": {"pid": 2},
        "mode": {"mode": "released-v1"},
        "step": {"step": 19},
        "bytes": {"retained_bytes": 1},
        "flat": {"cgroup_current_bytes": 60 * MIB},
        "stale": {"timestamp": START - timedelta(seconds=5)},
        "release": {"phase": "released", "step": 0, "retained_bytes": 0},
        "clock": {"monotonic_ns": 40_000_000_000},
    }
    if change == "duplicate":
        records.append(records[-1])
    elif change == "missing":
        records.pop(0)
    elif change == "boundary":
        records[0] = records[0].model_copy(update={"step": 1})
    else:
        records[-1] = records[-1].model_copy(update=changes[change])
    with pytest.raises(ValueError):
        validate_progression(tuple(records), "retained-v1", START, END)


def test_release_requires_all_steps_and_terminal_release() -> None:
    """Early cancellation or an incomplete log read cannot qualify the surviving control."""
    records = sequence("released-v1")
    for incomplete in (records[:-1], records[:-2] + records[-1:], records[1:]):
        with pytest.raises(ValueError):
            validate_progression(incomplete, "released-v1", START, END)


@pytest.mark.parametrize(
    "raw",
    [
        '"synthetic.leak"',
        '2026-09-11T00:00:00Z {"event":"synthetic.leak"}',
        "x" * 262145,
        "\n" * 2001,
        "",
    ],
    ids=["untimestamped", "missing-fields", "too-many-bytes", "too-many-lines", "empty"],
)
def test_malformed_or_oversized_log_rejects(raw: str) -> None:
    """Failure to read complete timestamped evidence must fail closed."""
    with pytest.raises(ValueError):
        parse_records(raw)


def test_time_window_and_runtime_clock_disagreement_reject() -> None:
    """Naive/reversed container lifetimes and mismatched runtime timestamps cannot bind logs."""
    records = sequence("released-v1")
    for start, end in ((START.replace(tzinfo=None), END), (END, START)):
        with pytest.raises(ValueError):
            validate_progression(records, "released-v1", start, end)
    raw = "\n".join(START.isoformat() + " " + record.model_dump_json() for record in records)
    with pytest.raises(ValueError):
        parse_records(raw)
