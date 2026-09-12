"""Matched payment and trace contrasts avoid treating every slow request as a root cause."""

import re
from itertools import combinations

from pydantic import JsonValue

from payops.evidence.diagnostics import Object, number, objects


def labels(row: Object) -> dict[str, str]:
    """Malformed or duplicate labels cannot silently change the compared payment slice."""
    raw = row.get("labels")
    if not isinstance(raw, list):
        return {}
    result: dict[str, str] = {}
    for pair in raw:
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or not isinstance(pair[0], str)
            or not isinstance(pair[1], str)
            or pair[0] in result
        ):
            return {}
        result[pair[0]] = pair[1]
    return result


def declines(rows: list[Object]) -> set[str]:
    """Require accepted/declined contrast with every other slice dimension held constant."""
    found: set[str] = set()
    counts = [
        row
        for row in rows
        if row.get("metric") == "payment_requests_total" and (number(row.get("value")) or 0) > 0
    ]
    for left, right in combinations(counts, 2):
        a, b = labels(left), labels(right)
        if set(a) != {"processor", "region", "payment_method", "status"} or set(a) != set(b):
            continue
        if {a["status"], b["status"]} != {"accepted", "declined"}:
            continue
        changed = {key for key in a if key != "status" and a[key] != b[key]}
        if changed == {"processor"}:
            found.add("PROCESSOR_SPECIFIC_DECLINES")
        if changed == {"payment_method"}:
            found.add("METHOD_SPECIFIC_DECLINES")
    return found


def region_latency(rows: list[Object]) -> bool:
    """Compare measured sum/count means for one processor, requiring both sampled regions."""
    values: dict[tuple[str, str], dict[str, float]] = {}
    for row in rows:
        metric, label, value = row.get("metric"), labels(row), number(row.get("value"))
        if (
            not isinstance(metric, str)
            or metric
            not in {
                "payment_authorization_latency_seconds_count",
                "payment_authorization_latency_seconds_sum",
            }
            or set(label) != {"processor", "region"}
            or value is None
            or value <= 0
        ):
            continue
        key = (label["processor"], label["region"])
        assert isinstance(metric, str)
        values.setdefault(key, {})[metric.rsplit("_", 1)[1]] = value
    means = {
        key: value["sum"] / value["count"]
        for key, value in values.items()
        if set(value) == {"sum", "count"} and value["count"] >= 5
    }
    return any(
        a[0] == b[0] and a[1] != b[1] and max(means[a], means[b]) >= 2 * min(means[a], means[b])
        for a, b in combinations(means, 2)
    )


def processor_latency(rows: list[Object]) -> bool:
    """Caller span durations can establish a slow processor despite missing callee telemetry."""
    durations: dict[str, list[int]] = {}
    for row in rows:
        spans = row.get("spans")
        if not isinstance(spans, list):
            continue
        for span in spans:
            if not isinstance(span, str):
                continue
            match = re.fullmatch(
                r"sandbox\.call\.(processor|risk|ledger|webhook): (\d+) us; "
                r"status \w+; bounded sample",
                span,
            )
            if match:
                durations.setdefault(match[1], []).append(int(match[2]))
    processor = durations.pop("processor", [])
    peers = [value for group in durations.values() for value in group]
    return len(processor) >= 2 and len(peers) >= 2 and min(processor) > 3 * max(peers)


def slice_causes(value: JsonValue) -> frozenset[str]:
    """Predicates use raw counters and durations; thresholds are development heuristics."""
    rows = list(objects(value))
    found = declines(rows)
    if region_latency(rows):
        found.add("REGION_SPECIFIC_LATENCY")
    if processor_latency(rows):
        found.add("PROCESSOR_LATENCY")
    return frozenset(found)
