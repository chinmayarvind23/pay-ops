"""Verified two-snapshot payment arithmetic, independent of scenario or traffic harnesses."""

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import product
from typing import Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    model_validator,
)

from payops.contracts import EvidenceItem, Identifier, utc_now
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize

Service = Literal["payments-api", "processor-adapter", "risk-sim", "ledger-sim", "webhook-sim"]
Status = Literal["complete", "missing", "reset", "mixed_epoch"]
type Labels = tuple[tuple[str, str], ...]
type Key = tuple[str, Labels]
type Object = dict[str, JsonValue]
REQUESTS = "payment_requests_total"
COUNT = "payment_authorization_latency_seconds_count"
SUM = "payment_authorization_latency_seconds_sum"
CONFLICTS = "payment_idempotency_conflicts_total"
EPOCH = "sandbox_process_start_time_seconds"
NAMES = (REQUESTS, COUNT, SUM, CONFLICTS, EPOCH, "up")
DERIVED_QUERY = "payment.window.v1"


class Immutable(BaseModel):
    """Nested tuples and frozen models prevent arithmetic inputs changing after validation."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)


class MeasurementInterval(Immutable):
    """A trusted collector supplies incident ownership and a bounded requested interval."""

    incident_id: Identifier
    start: AwareDatetime
    end: AwareDatetime

    @model_validator(mode="after")
    def bounded(self) -> Self:
        """An inverted or excessive interval cannot make stale observations appear relevant."""
        if not 0 < (self.end - self.start).total_seconds() <= 180:
            raise ValueError("measurement interval must be positive and at most 180 seconds")
        return self


class Delta(Immutable):
    """Keep exported dimensions rather than inventing method labels for latency."""

    metric: str
    labels: Labels
    value: float = Field(ge=0)


class PaymentWindow(Immutable):
    """Two raw references form one derived evidence lineage, not independent corroboration."""

    kind: Literal["payment_slice_window"] = "payment_slice_window"
    transformation: Literal["payment-counter-window-v1"] = "payment-counter-window-v1"
    service: Service
    interval: MeasurementInterval
    inputs: tuple[EvidenceItem, EvidenceItem]
    status: Status
    window_start: AwareDatetime | None
    window_end: AwareDatetime | None
    before_epoch: float | None
    after_epoch: float | None
    request_counts: tuple[Delta, ...] = ()
    latency: tuple[Delta, ...] = ()
    conflicts: tuple[Delta, ...] = ()


@dataclass(frozen=True)
class Parsed:
    """Only fully scoped finite raw values enter the arithmetic stage."""

    evaluated_at: float
    watermark: float | None
    epoch: float | None
    available: bool
    values: tuple[tuple[Key, float], ...]


def snapshot_queries(service: Service) -> tuple[str, str]:
    """Closed service selectors bind raw observations to exact allowed query semantics."""
    selected = TypeAdapter[Service](Service).validate_python(service)
    scope = f'job="payops-sandbox",service="{selected}"'
    return (f'{{__name__=~"{"|".join(NAMES)}",{scope}}}', f"timestamp({EPOCH}{{{scope}}})")


def _required() -> set[Key]:
    """All initialized zero series must actually exist before absent values can mean zero."""
    expected: set[Key] = {(CONFLICTS, ())}
    for processor, region, method, status in product(
        ("A", "B"),
        ("us", "eu"),
        ("credit", "debit"),
        ("accepted", "declined", "error"),
    ):
        labels = {
            "processor": processor,
            "region": region,
            "payment_method": method,
            "status": status,
        }
        expected.add((REQUESTS, tuple(sorted(labels.items()))))
    for processor, region, name in product(("A", "B"), ("us", "eu"), (COUNT, SUM)):
        expected.add((name, (("processor", processor), ("region", region))))
    return expected


def _object(value: JsonValue) -> Object:
    """Malformed source structure is an integrity failure rather than an empty observation."""
    if not isinstance(value, dict):
        raise EvidenceIntegrityError("expected source object")
    return value


def _finite(value: JsonValue) -> float:
    """Timestamps must be actual finite nonnegative numbers; booleans are not timestamps."""
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise EvidenceIntegrityError("expected numeric timestamp")
    try:
        result = float(value)
    except OverflowError as exc:
        raise EvidenceIntegrityError("numeric value exceeds finite range") from exc
    if not math.isfinite(result) or result < 0:
        raise EvidenceIntegrityError("expected finite nonnegative value")
    return result


def _value(row: Object, evaluated_at: float) -> float:
    """Match instant-vector timestamps to evaluation time within millisecond rounding."""
    pair = row.get("value")
    if not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[1], str):
        raise EvidenceIntegrityError("expected Prometheus timestamp/string pair")
    if abs(_finite(pair[0]) - evaluated_at) > 0.001:
        raise EvidenceIntegrityError("sample timestamp disagrees with pinned evaluation")
    try:
        result = float(pair[1])
    except ValueError as exc:
        raise EvidenceIntegrityError("invalid counter value") from exc
    return _finite(result)


def _rows(value: JsonValue) -> tuple[Object, ...]:
    """Only complete bounded instant-vector envelopes are eligible for source parsing."""
    response = _object(value)
    if response.get("status") != "success" or response.get("warnings"):
        raise EvidenceIntegrityError("Prometheus query failed or warned")
    data = _object(response.get("data"))
    rows = data.get("result")
    if data.get("resultType") != "vector" or not isinstance(rows, list) or len(rows) > 128:
        raise EvidenceIntegrityError("bounded instant vector required")
    return tuple(_object(row) for row in rows)


def _labels(row: Object, service: Service) -> Object:
    """One static sandbox target prevents unrelated service or instance samples entering a sum."""
    labels = _object(row.get("metric"))
    expected = {
        "job": "payops-sandbox",
        "service": service,
        "instance": f"{service}.payops-sandbox.svc.cluster.local:8080",
    }
    if any(labels.get(key) != value for key, value in expected.items()):
        raise EvidenceIntegrityError("metric target is outside snapshot scope")
    if any(not isinstance(value, str) for value in labels.values()):
        raise EvidenceIntegrityError("metric labels must be strings")
    return labels


def _metric_values(raw: Object, service: Service, at: float) -> dict[Key, float]:
    """Reject unknown labels and duplicate series instead of dropping them during projection."""
    values: dict[Key, float] = {}
    allowed = _required() | {(EPOCH, ()), ("up", ())}
    for row in _rows(raw.get("metrics")):
        labels = _labels(row, service)
        name = str(labels.get("__name__"))
        kept = tuple(
            sorted(
                (key, str(value))
                for key, value in labels.items()
                if key not in {"__name__", "job", "service", "instance"}
            )
        )
        key = (name, kept)
        if key not in allowed or key in values:
            raise EvidenceIntegrityError("unknown or duplicated metric series")
        values[key] = _value(row, at)
    return values


def _watermark(raw: Object, service: Service, at: float) -> float | None:
    """The underlying sample time must not come from the future or an ambiguous target."""
    rows = _rows(raw.get("watermark"))
    if not rows:
        return None
    if len(rows) != 1:
        raise EvidenceIntegrityError("ambiguous scrape watermark")
    labels = _labels(rows[0], service)
    if set(labels) != {"job", "service", "instance"}:
        raise EvidenceIntegrityError("unexpected watermark labels")
    watermark = _value(rows[0], at)
    if watermark > at:
        raise EvidenceIntegrityError("future scrape watermark")
    return watermark


def _parse_payload(payload: Object, service: Service) -> Parsed:
    """Both live readers and retained lineage apply the same source validation rules."""
    if set(payload) != {"evaluated_at", "query", "watermark_query", "metrics", "watermark"}:
        raise EvidenceIntegrityError("unexpected raw snapshot payload fields")
    query, watermark_query = snapshot_queries(service)
    if payload.get("query") != query or payload.get("watermark_query") != watermark_query:
        raise EvidenceIntegrityError("snapshot query provenance disagrees")
    at = _finite(payload.get("evaluated_at"))
    values = _metric_values(payload, service, at)
    mark = _watermark(payload, service, at)
    epoch = values.pop((EPOCH, ()), None)
    available = values.pop(("up", ()), None) == 1
    if epoch is not None and mark is not None and epoch > mark:
        raise EvidenceIntegrityError("process epoch is after its scrape")
    return Parsed(at, mark, epoch, available, tuple(sorted(values.items())))


def snapshot_observation(payload: Object, service: Service) -> Observation:
    """A live snapshot keeps the actual scrape time; an empty watermark stays explicitly missing."""
    parsed = _parse_payload(payload, service)
    if parsed.evaluated_at > utc_now().timestamp():
        raise EvidenceIntegrityError("snapshot timestamp is after collection")
    observed = parsed.watermark if parsed.watermark is not None else parsed.evaluated_at
    return Observation(
        source="PROMETHEUS",
        resource=service,
        observed_at=datetime.fromtimestamp(observed, UTC),
        query=snapshot_queries(service)[0],
        summary=f"Payment snapshot for {service}; target available: {parsed.available}",
        payload=payload,
    )


def _parse(item: EvidenceItem, payload: Object, service: Service) -> Parsed:
    """Bind retained metadata to exact query and source time before interpreting counters."""
    parsed = _parse_payload(payload, service)
    if item.query != snapshot_queries(service)[0]:
        raise EvidenceIntegrityError("snapshot query provenance disagrees")
    if parsed.evaluated_at > item.collected_at.timestamp() or item.observed_at > item.collected_at:
        raise EvidenceIntegrityError("snapshot timestamp is after collection")
    observed = parsed.watermark if parsed.watermark is not None else parsed.evaluated_at
    if abs(item.observed_at.timestamp() - observed) > 0.001:
        raise EvidenceIntegrityError("observation metadata disagrees with source time")
    return parsed


def _coverage(first: Parsed, last: Parsed, interval: MeasurementInterval) -> Status:
    """The requested interval must be covered by recent actual scrapes from one process epoch."""
    start, end = interval.start.timestamp(), interval.end.timestamp()
    if (
        not first.available
        or not last.available
        or first.epoch is None
        or last.epoch is None
        or first.watermark is None
        or last.watermark is None
    ):
        return "missing"
    if first.epoch != last.epoch:
        return "mixed_epoch"
    if (
        not start - 15 <= first.watermark <= first.evaluated_at <= start
        or not end <= last.watermark <= last.evaluated_at <= end + 30
    ):
        return "missing"
    before, after = dict(first.values), dict(last.values)
    if set(before) != _required() or set(after) != _required():
        return "missing"
    return "reset" if any(after[key] < value for key, value in before.items()) else "complete"


def _consistent(delta: dict[Key, float]) -> bool:
    """Match histogram observations to request counts across their shared dimensions."""
    for processor, region in product(("A", "B"), ("us", "eu")):
        labels = (("processor", processor), ("region", region))
        requests = sum(
            value
            for (name, dimensions), value in delta.items()
            if name == REQUESTS
            and dict(dimensions)["processor"] == processor
            and dict(dimensions)["region"] == region
        )
        count, duration = delta[COUNT, labels], delta[SUM, labels]
        if count != requests or (count == 0 and duration != 0):
            return False
    return True


def _date(value: float | None) -> datetime | None:
    """Preserve unavailable scrape times as null instead of inventing a wall-clock observation."""
    return None if value is None else datetime.fromtimestamp(value, UTC)


def _compute(
    before: EvidenceItem, after: EvidenceItem, interval: MeasurementInterval, store: ArtifactStore
) -> PaymentWindow:
    """Verify both references even before rejecting incompatible incident or service ownership."""
    first_payload = _object(store.verify(before).get("payload"))
    last_payload = _object(store.verify(after).get("payload"))
    if (
        before.evidence_id == after.evidence_id
        or before.incident_id != interval.incident_id
        or after.incident_id != interval.incident_id
        or before.resource != after.resource
        or before.source != "PROMETHEUS"
        or after.source != "PROMETHEUS"
    ):
        raise EvidenceIntegrityError("raw snapshot lineage crosses ownership or source scope")
    service = TypeAdapter[Service](Service).validate_python(before.resource)
    first, last = _parse(before, first_payload, service), _parse(after, last_payload, service)
    status = _coverage(first, last, interval)
    delta: dict[Key, float] = {}
    if status == "complete":
        earlier = dict(first.values)
        delta = {key: value - earlier[key] for key, value in last.values}
        if not _consistent(delta):
            status, delta = "missing", {}
    points = tuple(
        Delta(metric=name, labels=labels, value=value)
        for (name, labels), value in sorted(delta.items())
    )
    return PaymentWindow(
        service=service,
        interval=interval,
        inputs=(before, after),
        status=status,
        window_start=_date(first.watermark),
        window_end=_date(last.watermark),
        before_epoch=first.epoch,
        after_epoch=last.epoch,
        request_counts=tuple(point for point in points if point.metric == REQUESTS),
        latency=tuple(point for point in points if point.metric in {COUNT, SUM}),
        conflicts=tuple(point for point in points if point.metric == CONFLICTS),
    )


def derive_payment_window(
    before: EvidenceItem, after: EvidenceItem, interval: MeasurementInterval, store: ArtifactStore
) -> EvidenceItem:
    """Publish recomputed arithmetic and nested references through ordinary evidence redaction."""
    window = _compute(before, after, interval, store)
    observed = window.window_end or after.observed_at
    payload = TypeAdapter[Object](Object).validate_json(window.model_dump_json())
    observation = Observation(
        source="PAYMENT",
        resource=window.service,
        observed_at=observed,
        query=DERIVED_QUERY,
        summary=f"Payment counter window: {window.status}",
        payload=payload,
    )
    return normalize(
        observation,
        interval.incident_id,
        min(before.observed_at, interval.start),
        max(after.collected_at, interval.end),
        store,
    )


def verify_payment_window(item: EvidenceItem, store: ArtifactStore) -> PaymentWindow:
    """Verify nested artifacts and recompute numbers; a valid outer digest alone is insufficient."""
    raw = _object(store.verify(item).get("payload"))
    window = PaymentWindow.model_validate_json(json.dumps(raw, allow_nan=False))
    expected = _compute(*window.inputs, window.interval, store)
    observed = expected.window_end or expected.inputs[1].observed_at
    if (
        item.source != "PAYMENT"
        or item.query != DERIVED_QUERY
        or item.resource != window.service
        or item.incident_id != window.interval.incident_id
        or item.observed_at != observed
        or item.observed_at > item.collected_at
        or window != expected
    ):
        raise EvidenceIntegrityError("derived window disagrees with verified raw lineage")
    return expected
