"""Retain kernel sources around admitted CPU work without exposing a read endpoint."""

import json
from collections.abc import Callable
from pathlib import Path
from time import monotonic, monotonic_ns
from typing import Self

from pydantic import AwareDatetime, model_validator

from payops.contracts import utc_now
from payops.evidence.trace_span import Immutable
from payops.sandbox.models import Sample
from payops.scenarios.cpu_counters import Counter, Raw, counters, quota


class KernelSnapshot(Immutable):
    """Ownership comes from the collector's verified container log, not self-reported IDs."""

    started_at: AwareDatetime
    completed_at: AwareDatetime
    monotonic_started_ns: Counter
    monotonic_completed_ns: Counter
    cpu_stat: Raw
    cpu_max: Raw

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        """Reject slow acquisition and malformed kernel files before publishing a record."""
        if not 0 <= (self.completed_at - self.started_at).total_seconds() <= 2:
            raise ValueError("CPU source acquisition exceeds two seconds")
        if not 0 <= self.monotonic_completed_ns - self.monotonic_started_ns <= 2_000_000_000:
            raise ValueError("CPU monotonic acquisition exceeds two seconds")
        counters(self.cpu_stat)
        quota(self.cpu_max)
        return self


def snapshot() -> KernelSnapshot:
    """Read only two fixed cgroup-v2 files with a hard byte cap, never caller paths."""
    started = utc_now()
    mono_started = monotonic_ns()
    values: dict[str, str] = {}
    for name in ("cpu.stat", "cpu.max"):
        with (Path("/sys/fs/cgroup") / name).open("rb") as source:
            raw = source.read(4097)
        if len(raw) > 4096:
            raise ValueError("CPU kernel file exceeds byte cap")
        values[name] = raw.decode("ascii")
    return KernelSnapshot(
        started_at=started,
        completed_at=utc_now(),
        monotonic_started_ns=mono_started,
        monotonic_completed_ns=monotonic_ns(),
        cpu_stat=values["cpu.stat"],
        cpu_max=values["cpu.max"],
    )


def observed_work(sample_id: str, rounds: int, work: Callable[[int], float]) -> float:
    """Emit one successful work record; failures never masquerade as complete measurements."""
    sample = Sample(sample_id=sample_id)
    before = snapshot()
    started = monotonic()
    consumed = work(rounds)
    elapsed = monotonic() - started
    after = snapshot()
    print(
        json.dumps(
            {
                "event": "synthetic.cpu",
                "sample_id": sample.sample_id,
                "rounds": rounds,
                "before": before.model_dump(mode="json"),
                "after": after.model_dump(mode="json"),
                "wall_seconds": elapsed,
                "thread_cpu_seconds": consumed,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    return consumed
