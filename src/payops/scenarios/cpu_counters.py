"""Derive CPU deltas only from bounded kernel records belonging to one unchanged container."""

import re
from typing import Annotated, Self

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from payops.contracts import Identifier
from payops.evidence.trace_span import Immutable, PodIdentity

Counter = Annotated[int, Field(ge=0, le=2**63 - 1)]
Raw = Annotated[str, StringConstraints(min_length=1, max_length=4096)]
REQUIRED = frozenset(
    {"usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"}
)


def counters(raw: str) -> dict[str, int]:
    """Unknown kernel fields remain valid, but duplicate, missing and malformed fields fail."""
    if len(raw.encode("utf-8")) > 4096 or not 1 <= len(raw.splitlines()) <= 32:
        raise ValueError("CPU counter source exceeds bounds")
    result: dict[str, int] = {}
    for line in raw.splitlines():
        match = re.fullmatch(r"([a-z_]+) ([0-9]{1,19})", line)
        if match is None or match[1] in result or int(match[2]) > 2**63 - 1:
            raise ValueError("CPU counter source is malformed or duplicated")
        result[match[1]] = int(match[2])
    if not REQUIRED <= result.keys():
        raise ValueError("CPU bandwidth counters are missing")
    return result


def quota(raw: str) -> tuple[int, int]:
    """This experiment requires finite cgroup-v2 bandwidth rather than inferring a CPU limit."""
    match = re.fullmatch(r"([0-9]{1,10}) ([0-9]{1,7})\n?", raw)
    if match is None:
        raise ValueError("finite CPU quota and period are required")
    maximum, period = int(match[1]), int(match[2])
    if maximum < 1000 or not 1000 <= period <= 1000000:
        raise ValueError("CPU quota or period outside kernel bounds")
    return maximum, period


class CpuSnapshot(Immutable):
    """The collector must verify this complete Pod/container identity around both file reads."""

    incident_id: Identifier
    identity: PodIdentity
    started_at: AwareDatetime
    completed_at: AwareDatetime
    cpu_stat: Raw
    cpu_max: Raw

    @model_validator(mode="after")
    def bounded(self) -> Self:
        """No stale or malformed snapshot may participate in later arithmetic."""
        if not 0 <= (self.completed_at - self.started_at).total_seconds() <= 2:
            raise ValueError("CPU snapshot acquisition exceeds two seconds")
        counters(self.cpu_stat)
        quota(self.cpu_max)
        return self


class CpuDelta(Immutable):
    """Keep integer kernel units; callers may convert microseconds explicitly for presentation."""

    usage_usec: Counter
    nr_periods: Counter
    nr_throttled: Counter
    throttled_usec: Counter


def delta(before: CpuSnapshot, after: CpuSnapshot) -> CpuDelta:
    """Reject quota drift, changed containers and resets, including extra kernel fields."""
    # Revalidation prevents unchecked model_copy updates from bypassing source constraints.
    before = CpuSnapshot.model_validate_json(before.model_dump_json())
    after = CpuSnapshot.model_validate_json(after.model_dump_json())
    if (
        before.incident_id != after.incident_id
        or before.identity != after.identity
        or before.completed_at > after.started_at
        or not 0 < (after.completed_at - before.started_at).total_seconds() <= 30
        or quota(before.cpu_max) != quota(after.cpu_max)
    ):
        raise ValueError("CPU measurement scope, time or quota changed")
    first, last = counters(before.cpu_stat), counters(after.cpu_stat)
    if first.keys() != last.keys() or any(last[key] < value for key, value in first.items()):
        raise ValueError("CPU counters reset or their fields changed")
    values = {key: last[key] - first[key] for key in CpuDelta.model_fields}
    if values["nr_throttled"] > values["nr_periods"]:
        raise ValueError("throttled period delta exceeds elapsed periods")
    return CpuDelta.model_validate(values)
