"""Acceptance depends on real allocation overlap and complete controls, not reported peaks alone."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from payops.scenarios.concurrency_evidence import MemoryEvent, parse_events, validate_events

START = datetime(2026, 9, 11, tzinfo=UTC)
END = START + timedelta(seconds=30)
SAMPLES = tuple(f"synthetic-memory-{index}" for index in range(8))
MIB = 1024 * 1024


def event(index: int, phase: str, active: int, elapsed: float, memory: int) -> MemoryEvent:
    """Create explicit recorded values; validators independently reconstruct occupancy."""
    return MemoryEvent.model_validate_json(
        json.dumps(
            {
                "event": "synthetic.concurrency_memory",
                "sample_id": SAMPLES[index],
                "phase": phase,
                "active": active,
                "request_bytes": 32 * MIB,
                "hold_seconds": 1,
                "cgroup_current_bytes": memory * MIB,
                "cgroup_limit_bytes": 256 * MIB,
                "timestamp": (START + timedelta(seconds=elapsed)).isoformat(),
                "monotonic_ns": 1_000_000_000 + int(elapsed * 1_000_000_000),
            }
        )
    )


def records(parallel: bool) -> tuple[MemoryEvent, ...]:
    """Serial controls complete all eight; a terminated parallel capture can end at six overlaps."""
    result: list[MemoryEvent] = []
    for index in range(6 if parallel else 8):
        base = index * (0.1 if parallel else 1.5)
        active = index + 1 if parallel else 1
        result.extend(
            (
                event(index, "admitted", active, base, 32 + (active - 1) * 32),
                event(index, "allocated", active, base + 0.01, 32 + active * 32),
            )
        )
        if not parallel:
            result.append(event(index, "released", 0, base + 1.01, 32))
    return tuple(result)


@pytest.mark.parametrize("parallel", [False, True])
def test_source_round_trip_and_derived_counts(parallel: bool) -> None:
    """Raw parsing preserves the event sequence and derives admission counts from transitions."""
    source = records(parallel)
    raw = "\n".join(r.timestamp.isoformat() + " " + r.model_dump_json() for r in source)
    parsed = parse_events("unrelated log\n" + raw)
    assert parsed == source
    result = validate_events(parsed, SAMPLES, START, END, parallel=parallel)
    assert result.peak_allocated == (6 if parallel else 1)
    assert result.completed == (0 if parallel else 8)
    assert result.admitted == (6 if parallel else 8)


@pytest.mark.parametrize(
    "change",
    [
        "count",
        "order",
        "sample",
        "window",
        "hold",
        "limit",
        "duplicate",
        "missing-admission",
        "missing-release",
    ],
)
def test_invalid_serial_sequences_reject(change: str) -> None:
    """Forged peaks, old samples, missing lifecycle records and changed resource limits all fail."""
    source = list(records(False))
    updates = {
        "count": {"active": 3},
        "order": {"monotonic_ns": 1},
        "sample": {"sample_id": "synthetic-other"},
        "window": {"timestamp": START - timedelta(seconds=5)},
        "hold": {"monotonic_ns": 1_020_000_000},
        "limit": {"cgroup_limit_bytes": 128 * MIB},
    }
    if change == "duplicate":
        source[1] = source[1].model_copy(update={"phase": "admitted"})
    elif change == "missing-admission":
        source.pop(0)
    elif change == "missing-release":
        source.pop()
    else:
        source[2] = source[2].model_copy(update=updates[change])
    with pytest.raises(ValueError):
        validate_events(tuple(source), SAMPLES, START, END, parallel=False)


def test_parallel_requires_allocations_and_kernel_growth() -> None:
    """High declared active counts cannot substitute for allocated memory or measured growth."""
    with pytest.raises(ValueError):
        validate_events(records(True)[:-2], SAMPLES, START, END, parallel=True)
    flat = tuple(r.model_copy(update={"cgroup_current_bytes": 32 * MIB}) for r in records(True))
    with pytest.raises(ValueError):
        validate_events(flat, SAMPLES, START, END, parallel=True)
    with pytest.raises(ValueError):
        validate_events(records(False), SAMPLES, START, END, parallel=True)


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "\n" * 2001,
        "x" * 262144,
        '"synthetic.concurrency_memory"',
        '2026-09-11T00:00:00Z {"event":"synthetic.concurrency_memory"}',
    ],
    ids=["empty", "lines", "bytes", "no-runtime-time", "malformed"],
)
def test_capped_or_missing_sources_fail(raw: str) -> None:
    """Log transport failure or malformed event data cannot become a zero-pressure observation."""
    with pytest.raises(ValueError):
        parse_events(raw)


def test_sample_set_and_clock_bounds_reject() -> None:
    """Duplicate planned IDs and unbounded/naive windows cannot silently alter denominators."""
    for samples in (SAMPLES[:-1], (SAMPLES[0],) * 8):
        with pytest.raises(ValueError):
            validate_events(records(False), samples, START, END, parallel=False)
    for end in (START, START + timedelta(seconds=46), END.replace(tzinfo=None)):
        with pytest.raises(ValueError):
            validate_events(records(False), SAMPLES, START, end, parallel=False)
    with pytest.raises(ValueError):
        validate_events((), SAMPLES, START, END, parallel=False)
    raw = "\n".join(END.isoformat() + " " + r.model_dump_json() for r in records(False))
    with pytest.raises(ValueError):
        parse_events(raw)
