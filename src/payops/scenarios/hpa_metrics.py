"""Bind raw resource metrics to unchanged owned payments processes and the active load window."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.memory_provenance import timestamp


@dataclass(frozen=True)
class CpuDemand:
    """Utilization uses the frozen 50m request; HPA's separate sample can differ in timing."""

    pod_names: tuple[str, ...]
    average_cores: Decimal
    average_utilization: Decimal
    earliest_window_start: datetime
    latest_sample: datetime


def cpu_cores(value: object) -> Decimal:
    """Accept bounded Kubernetes decimal CPU quantities, including nanocores from Metrics Server."""
    if not isinstance(value, str) or len(value) > 32:
        raise ValueError("invalid CPU metric quantity")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(n|u|m)?", value)
    if match is None:
        raise ValueError("unsupported CPU metric quantity")
    scale = {"n": Decimal("1e-9"), "u": Decimal("1e-6"), "m": Decimal("1e-3")}
    result = Decimal(match[1]) * scale.get(match[2], Decimal(1))
    if not 0 <= result <= 1:
        raise ValueError("CPU usage exceeds the bounded experiment envelope")
    return result


def _sample(
    row: JsonObject, load_started: datetime, captured: datetime
) -> tuple[Decimal, datetime, datetime]:
    """The whole utilization averaging window must follow load start, not merely its end."""
    ended = timestamp(row.get("timestamp"))
    window = re.fullmatch(r"([0-9]{1,2}(?:\.[0-9]{1,9})?)s", str(row.get("window", "")))
    if ended is None or window is None or not 1 <= Decimal(window[1]) <= 60:
        raise ValueError("invalid resource metric timestamp or window")
    started = ended - timedelta(seconds=float(window[1]))
    if not load_started <= started < ended <= captured or captured - ended > timedelta(seconds=45):
        raise ValueError("CPU metric is stale or overlaps work before the active load")
    containers = object_items(row.get("containers", []))
    if len(containers) != 1 or containers[0].get("name") != "sandbox":
        raise ValueError("CPU metric has an unexpected container set")
    usage = cpu_cores(object_value(containers[0].get("usage", {})).get("cpu"))
    return usage, started, ended


def validate_cpu_metrics(
    raw: bytes,
    before: tuple[PodIdentity, ...],
    after: tuple[PodIdentity, ...],
    load_started: datetime,
    captured: datetime,
) -> CpuDemand:
    """Caller verifies ownership and 50m requests; unchanged identities bind this read."""
    if (
        not 1 <= len(before) <= 2
        or before != after
        or len({p.pod_name for p in before}) != len(before)
        or any(not p.pod_name.startswith("payments-api-") or p.restart_count != 0 for p in before)
        or load_started.tzinfo is None
        or captured.tzinfo is None
        or not load_started < captured
        or len(raw) >= 262144
    ):
        raise ValueError("invalid CPU metric identity, time or byte bounds")
    document = JSON_OBJECT.validate_json(raw)
    if (
        document.get("kind") != "PodMetricsList"
        or document.get("apiVersion") != "metrics.k8s.io/v1beta1"
    ):
        raise ValueError("unexpected resource metrics response")
    rows = object_items(document.get("items", []))
    names = [str(object_value(row.get("metadata", {})).get("name", "")) for row in rows]
    if not 1 <= len(rows) <= 6 or len(set(names)) != len(names):
        raise ValueError("resource metrics response has missing or duplicate pods")
    expected = {p.pod_name for p in before}
    actual = {name for name in names if name.startswith("payments-api-")}
    if actual != expected:
        raise ValueError("resource metric pod set differs from the owned payments processes")
    selected = [row for row, name in zip(rows, names, strict=True) if name in expected]
    if any(object_value(row["metadata"]).get("namespace") != "payops-sandbox" for row in selected):
        raise ValueError("CPU metric came from another namespace")
    values = [_sample(row, load_started, captured) for row in selected]
    average = sum((row[0] for row in values), Decimal(0)) / len(values)
    return CpuDemand(
        tuple(sorted(expected)),
        average,
        average / Decimal("0.05") * 100,
        min(row[1] for row in values),
        max(row[2] for row in values),
    )
