"""Conservative diagnostic predicates inspect source fields, never labels or model prose."""

from collections.abc import Iterator
from datetime import datetime

from pydantic import JsonValue

Object = dict[str, JsonValue]


def objects(value: JsonValue) -> Iterator[Object]:
    """Walk bounded source JSON while excluding explicitly archived records and their children."""
    if isinstance(value, dict):
        if value.get("archived") is True:
            return
        yield value
        for child in value.values():
            yield from objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from objects(child)


def number(value: JsonValue) -> float | None:
    """Boolean fields are not measurements; source parsing already rejects nonfinite JSON."""
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def duration(row: Object) -> float | None:
    """A recorded immediate process exit differs from an arbitrary historical error."""
    start, end = row.get("startedAt"), row.get("finishedAt")
    if not isinstance(start, str) or not isinstance(end, str):
        return None
    try:
        first, last = datetime.fromisoformat(start), datetime.fromisoformat(end)
        if first.tzinfo is None or last.tzinfo is None:
            return None
        return (last - first).total_seconds()
    except ValueError:
        return None


def direct(row: Object, resource: str) -> set[str]:
    """Require mechanism fields together, not probe configuration or generic HTTP errors."""
    found: set[str] = set()
    if row.get("dependency") == "postgres" and row.get("sqlstate") == "53300":
        found.add("DATABASE_CONNECTION_EXHAUSTION")
    if row.get("dependency") == "redis" and row.get("outcome") == "unavailable":
        found.add("CACHE_UNAVAILABLE")
    if (
        row.get("kind") == "Deployment"
        and type(row.get("replicas")) is int
        and row["replicas"] == 0
        and row.get("resource", resource) == "processor-adapter"
    ):
        found.add("PROCESSOR_UNAVAILABLE")
    if (row.get("type"), row.get("status"), row.get("reason")) == (
        "ScalingLimited",
        "True",
        "TooManyReplicas",
    ):
        found.add("AUTOSCALING_MAX_REPLICAS")
    if row.get("exitCode") == 1:
        seconds = duration(row)
        if seconds is not None and 0 <= seconds <= 5:
            found.add("STARTUP_FAILURE")
    return found


def events(rows: list[Object]) -> set[str]:
    """A scheduler or readiness claim needs observed state, not a configured threshold."""
    found: set[str] = set()
    pending = any(row.get("phase") == "Pending" for row in rows)
    running = any(running_unready(row) for row in rows)
    pressure = any(
        row.get("type") == "MemoryPressure" and row.get("status") == "True" for row in rows
    )
    for row in rows:
        message = row.get("message")
        if not isinstance(message, str):
            continue
        if pending and row.get("reason") == "FailedScheduling":
            for label, cause in (("cpu", "CPU"), ("memory", "MEMORY")):
                if f"Insufficient {label}" in message:
                    found.add(f"INSUFFICIENT_{cause}_REQUEST_CAPACITY")
        if (
            running
            and row.get("reason") == "Unhealthy"
            and message.startswith("Readiness probe failed:")
        ):
            found.add("READINESS_PROBE_FAILURE")
        if pressure and row.get("reason") == "Evicted" and "memory" in message:
            found.add("NODE_PRESSURE_EVICTION")
    return found


def growth(rows: list[Object], field: str) -> bool:
    """Require three monotonically ordered samples and net growth, not one large allocation."""
    values = [number(row.get(field)) for row in rows if field in row]
    return (
        len(values) >= 3
        and all(value is not None for value in values)
        and all(
            a <= b
            for a, b in zip(values, values[1:], strict=False)
            if a is not None and b is not None
        )
        and values[0] is not None
        and values[-1] is not None
        and values[-1] > values[0]
    )


def memory(rows: list[Object]) -> set[str]:
    """Specific allocation mechanisms outrank the shared OOM termination symptom."""
    found: set[str] = set()
    if growth(rows, "retained_bytes"):
        found.add("MEMORY_LEAK")
    if growth(rows, "active") and growth(rows, "cgroup_current_bytes"):
        found.add("CONCURRENCY_MEMORY_PRESSURE")
    if any(row.get("reason") == "OOMKilled" for row in rows):
        found.add("MEMORY_LIMIT_BELOW_WORKING_SET")
    return found


def cpu_counter(value: JsonValue) -> int | None:
    """Parse the exact kernel counter; high process CPU usage is not proof of throttling."""
    if not isinstance(value, str):
        return None
    pairs = [line.split() for line in value.splitlines()]
    matches = [pair[1] for pair in pairs if len(pair) == 2 and pair[0] == "throttled_usec"]
    return int(matches[0]) if len(matches) == 1 and matches[0].isdigit() else None


def throttled(row: Object) -> bool:
    """Require a stable finite quota and a positive within-interval throttling delta."""
    before, after = row.get("before"), row.get("after")
    if not isinstance(before, dict) or not isinstance(after, dict):
        return False
    quota = before.get("cpu_max")
    if not isinstance(quota, str) or quota != after.get("cpu_max"):
        return False
    parts = quota.split()
    if len(parts) != 2 or not all(part.isdigit() and int(part) > 0 for part in parts):
        return False
    first, last = cpu_counter(before.get("cpu_stat")), cpu_counter(after.get("cpu_stat"))
    return first is not None and last is not None and last > first


def supported_causes(value: JsonValue, resource: str = "") -> frozenset[str]:
    """Return candidate mechanisms with explicit predicates; no confidence or gold lookup exists."""
    rows = list(objects(value))
    found = set[str]().union(*(direct(row, resource) for row in rows))
    found.update(events(rows), memory(rows))
    if any(throttled(row) for row in rows):
        found.add("CPU_THROTTLING")
    return frozenset(found)


def running_unready(row: Object) -> bool:
    """Readiness configuration alone never supplies observed container failure."""
    state = row.get("state")
    return (
        row.get("ready") is False
        and row.get("restartCount") == 0
        and isinstance(state, dict)
        and "running" in state
    )
