"""Validate allocation progression inside one independently identified container lifetime."""

from datetime import datetime, timedelta
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from payops.scenarios.leak_workload import CHUNK_BYTES, MAX_CHUNKS, MIB, Mode


class LeakRecord(BaseModel):
    """Reject malformed counters rather than coercing log text into measured evidence."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    event: Literal["synthetic.leak"]
    mode: Mode
    phase: Literal["started", "allocated", "released"]
    step: int = Field(ge=0, le=MAX_CHUNKS)
    retained_bytes: int = Field(ge=0, le=MAX_CHUNKS * CHUNK_BYTES)
    cgroup_current_bytes: int = Field(ge=0, le=512 * MIB)
    cgroup_limit_bytes: Literal[268435456]
    timestamp: AwareDatetime
    monotonic_ns: int = Field(gt=0)
    pid: int = Field(gt=0)


def parse_records(raw: str) -> tuple[LeakRecord, ...]:
    """Read bounded timestamped kubectl logs; malformed candidate events invalidate the capture."""
    if len(raw.encode("utf-8")) > 262144 or len(raw.splitlines()) > 2000:
        raise ValueError("leak log capture exceeds bounds")
    result: list[LeakRecord] = []
    for line in raw.splitlines():
        if '"synthetic.leak"' not in line:
            continue
        stamp, separator, body = line.partition(" ")
        if not separator:
            raise ValueError("leak record lacks runtime timestamp")
        runtime = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        record = LeakRecord.model_validate_json(body)
        if runtime.tzinfo is None or abs((runtime - record.timestamp).total_seconds()) > 1:
            raise ValueError("leak event and runtime timestamps disagree")
        result.append(record)
    if not 2 <= len(result) <= MAX_CHUNKS + 2:
        raise ValueError("incomplete or repeated leak log sequence")
    return tuple(result)


def validate_progression(
    records: tuple[LeakRecord, ...], mode: Mode, started: datetime, ended: datetime
) -> tuple[LeakRecord, ...]:
    """The caller must bind these logs and times to an owned container, never just a pod label."""
    if started.tzinfo is None or ended.tzinfo is None or not started < ended:
        raise ValueError("invalid container lifetime")
    if not records or records[0].phase != "started":
        raise ValueError("missing allocation start")
    _validate_clock(records, mode, started, ended)
    allocations = tuple(record for record in records if record.phase == "allocated")
    expected_phases = ["started"] + ["allocated"] * len(allocations)
    if mode == "released-v1":
        expected_phases.append("released")
        if len(allocations) != MAX_CHUNKS:
            raise ValueError("release control did not complete the full workload")
    elif not 5 <= len(allocations) < MAX_CHUNKS:
        raise ValueError("retention growth not established before termination")
    if [record.phase for record in records] != expected_phases:
        raise ValueError("unexpected release, repeated start or reordered workload")
    for step, record in enumerate(allocations, 1):
        expected = step * CHUNK_BYTES if mode == "retained-v1" else 0
        if record.step != step or record.retained_bytes != expected:
            raise ValueError("allocation steps or retained bytes disagree with frozen workload")
    if mode == "retained-v1" and (
        allocations[-1].cgroup_current_bytes - allocations[0].cgroup_current_bytes < 64 * MIB
    ):
        raise ValueError("retained allocations lack measured kernel memory growth")
    return allocations


def _validate_clock(
    records: tuple[LeakRecord, ...], mode: Mode, started: datetime, ended: datetime
) -> None:
    """Wall time binds lifetime; monotonic time proves ordering and bounds one worker execution."""
    for record in records:
        if (
            record.mode != mode
            or record.pid != records[0].pid
            or not started - timedelta(seconds=1)
            <= record.timestamp
            <= ended + timedelta(seconds=1)
        ):
            raise ValueError("leak record belongs to another mode or container lifetime")
        if record.phase != "allocated" and (record.step != 0 or record.retained_bytes != 0):
            raise ValueError("invalid boundary record")
    if any(b.monotonic_ns <= a.monotonic_ns for a, b in zip(records, records[1:], strict=False)):
        raise ValueError("leak records are duplicated or reordered")
    if records[-1].monotonic_ns - records[0].monotonic_ns > 35_000_000_000:
        raise ValueError("leak records exceed bounded worker lifetime")
