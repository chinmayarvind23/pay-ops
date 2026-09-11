"""Reconstruct actual admission/retention overlap from bounded per-request kernel records."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from payops.sandbox.models import Sample

MIB = 1024 * 1024


class MemoryEvent(BaseModel):
    """Strict event fields cannot turn string counters or arbitrary workloads into measurements."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    event: Literal["synthetic.concurrency_memory"]
    sample_id: str = Field(pattern=r"^synthetic-[a-zA-Z0-9-]{1,64}$")
    phase: Literal["admitted", "allocated", "released"]
    active: int = Field(ge=0, le=8)
    request_bytes: Literal[33554432]
    hold_seconds: Literal[1]
    cgroup_current_bytes: int = Field(ge=0, le=512 * MIB)
    cgroup_limit_bytes: Literal[268435456]
    timestamp: AwareDatetime
    monotonic_ns: int = Field(gt=0)


@dataclass(frozen=True)
class MemoryProgress:
    """Derived counts describe allocation evidence, not HTTP success or kernel termination."""

    admitted: int
    completed: int
    peak_active: int
    peak_allocated: int
    peak_memory_bytes: int


def parse_events(raw: str) -> tuple[MemoryEvent, ...]:
    """Require runtime timestamps and reject capped or malformed candidate-event captures."""
    if len(raw.encode("utf-8")) >= 262144 or len(raw.splitlines()) > 2000:
        raise ValueError("concurrent memory log exceeds capture bounds")
    events: list[MemoryEvent] = []
    for line in raw.splitlines():
        if '"synthetic.concurrency_memory"' not in line:
            continue
        stamp, separator, body = line.partition(" ")
        if not separator:
            raise ValueError("missing runtime timestamp")
        runtime = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        event = MemoryEvent.model_validate_json(body)
        if runtime.tzinfo is None or abs((runtime - event.timestamp).total_seconds()) > 1:
            raise ValueError("runtime and event timestamps disagree")
        events.append(event)
    if not 1 <= len(events) <= 24:
        raise ValueError("missing or excessive concurrency events")
    return tuple(events)


def _advance(event: MemoryEvent, active: dict[str, int | None], completed: set[str]) -> None:
    """Reconstruct occupancy from transitions and check each reported count."""
    sample = event.sample_id
    if event.phase == "admitted":
        if sample in active or sample in completed:
            raise ValueError("duplicate memory admission")
        active[sample] = None
    elif event.phase == "allocated":
        if sample not in active or active[sample] is not None:
            raise ValueError("allocation without unique admission")
        active[sample] = event.monotonic_ns
    else:
        allocated = active.get(sample)
        if allocated is None or event.monotonic_ns - allocated < 1_000_000_000:
            raise ValueError("release lacks a complete allocation hold")
        del active[sample]
        completed.add(sample)
    if event.active != len(active):
        raise ValueError("reported active count disagrees with lifecycle events")


def validate_events(
    events: tuple[MemoryEvent, ...],
    samples: tuple[str, ...],
    started: datetime,
    ended: datetime,
    *,
    parallel: bool,
) -> MemoryProgress:
    """Bind planned sample IDs and the container window before accepting overlap."""
    if len(samples) != 8 or len(set(samples)) != 8:
        raise ValueError("concurrency workload requires eight distinct sample IDs")
    for sample in samples:
        Sample(sample_id=sample)
    if (
        started.tzinfo is None
        or ended.tzinfo is None
        or not 0 < (ended - started).total_seconds() <= 45
    ):
        raise ValueError("invalid concurrency evidence window")
    checked = tuple(MemoryEvent.model_validate_json(row.model_dump_json()) for row in events)
    if not 1 <= len(checked) <= 24:
        raise ValueError("missing or excessive concurrency sequence")
    active: dict[str, int | None] = {}
    completed: set[str] = set()
    peak_active = peak_allocated = last = 0
    for event in checked:
        if (
            event.sample_id not in samples
            or event.monotonic_ns <= last
            or not started - timedelta(seconds=1) <= event.timestamp <= ended + timedelta(seconds=1)
        ):
            raise ValueError("foreign, reordered or out-of-window memory event")
        _advance(event, active, completed)
        last = event.monotonic_ns
        peak_active = max(peak_active, len(active))
        peak_allocated = max(peak_allocated, sum(value is not None for value in active.values()))
    peak = max(event.cgroup_current_bytes for event in checked)
    if parallel:
        if peak_allocated < 6 or peak - checked[0].cgroup_current_bytes < 128 * MIB:
            raise ValueError("insufficient simultaneous allocations or actual kernel growth")
    elif active or completed != set(samples) or peak_active != 1:
        raise ValueError("serial control is incomplete or overlapping")
    return MemoryProgress(
        len(active) + len(completed), len(completed), peak_active, peak_allocated, peak
    )
