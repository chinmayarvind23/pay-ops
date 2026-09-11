"""Fixed Package A workloads and epoch-checked Prometheus window observations."""

import asyncio
import hashlib
import json
import math
import time
from collections import Counter
from datetime import UTC, datetime
from itertools import product
from pathlib import Path
from typing import Literal, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from payops.scenarios.contracts import CaseId, JsonObject, object_items, object_value
from payops.scenarios.recipes import PACKAGE_A_CASES
from payops.scenarios.traffic import (
    SliceCount,
    TrafficDriver,
    TrafficReceipt,
    TrafficRole,
    Workload,
)

REQUESTS = "payment_requests_total"
LATENCY_COUNT = "payment_authorization_latency_seconds_count"
LATENCY_SUM = "payment_authorization_latency_seconds_sum"
CONFLICTS = "payment_idempotency_conflicts_total"
EPOCH = "sandbox_process_start_time_seconds"
METRICS = (REQUESTS, LATENCY_COUNT, LATENCY_SUM, CONFLICTS, EPOCH, "up")
type SeriesKey = tuple[str, tuple[tuple[str, str], ...]]
Completeness = Literal["complete", "missing", "reset", "mixed_epoch"]


class SnapshotRef(BaseModel):
    """Immutable references bind derived observations to exact raw query files."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)
    path: str
    sha256: str
    evaluated_at: float = Field(ge=0)
    query: str
    watermark_query: str


class MetricDelta(BaseModel):
    """Only exported labels survive; latency never acquires an invented payment-method label."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    metric: str
    labels: tuple[tuple[str, str], ...]
    value: float


class PaymentWindow(BaseModel):
    """A two-snapshot derived observation is one source, not two independent corroborations."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    kind: Literal["payment_slice_window"] = "payment_slice_window"
    source: Literal["prometheus_counter_delta"] = "prometheus_counter_delta"
    service: str
    window_start: float
    window_end: float
    traffic_start: float
    traffic_end: float
    status: Completeness
    before: SnapshotRef
    after: SnapshotRef
    request_counts: tuple[MetricDelta, ...] = ()
    latency: tuple[MetricDelta, ...] = ()
    conflicts: tuple[MetricDelta, ...] = ()


class MetricSnapshot(BaseModel):
    """Parsed samples retain raw provenance and a real scrape watermark, not query wall time."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    reference: SnapshotRef
    service: str
    epoch: float | None
    watermark: float | None
    available: bool
    points: tuple[MetricDelta, ...]


class PackageAObserver(Protocol):
    """The runner invokes bounded traffic only after the injected Deployment is ready."""

    def collect(self, case_id: CaseId, directory: Path) -> JsonObject:
        """Return retained observation results and a verified runtime activation predicate."""
        ...


def queries(role: TrafficRole) -> tuple[str, str]:
    """A closed role map supplies fixed selectors; callers cannot submit arbitrary PromQL."""
    service = {"payments": "payments-api", "webhook": "webhook-sim"}[role]
    selector = f'job="payops-sandbox",service="{service}"'
    return (f'{{__name__=~"{"|".join(METRICS)}",{selector}}}', f"timestamp({EPOCH}{{{selector}}})")


def workload_for(case_id: CaseId) -> Workload:
    """Each fault has an explicit affected slice and an equally sized unaffected control."""
    if case_id not in PACKAGE_A_CASES:
        raise ValueError("case is outside Package A")
    first = SliceCount(processor="A", region="us", payment_method="credit", count=16)
    second = SliceCount(processor="B", region="us", payment_method="credit", count=16)
    if case_id == "PAY-02":
        second = SliceCount(processor="A", region="eu", payment_method="credit", count=16)
    elif case_id == "PAY-03":
        second = SliceCount(processor="A", region="us", payment_method="debit", count=16)
    return Workload(
        role="webhook" if case_id == "PAY-04" else "payments",
        distribution=(first, second),
        concurrency=4,
    )


def required_series() -> set[SeriesKey]:
    """Preinitialized finite labels let absence mean missing data, including absent zero series."""
    result: set[SeriesKey] = {(CONFLICTS, ())}
    for processor, region, method, status in product(
        ("A", "B"),
        ("us", "eu"),
        ("credit", "debit"),
        ("accepted", "declined", "error"),
    ):
        labels = tuple(
            sorted(
                {
                    "processor": processor,
                    "region": region,
                    "payment_method": method,
                    "status": status,
                }.items()
            )
        )
        result.add((REQUESTS, labels))
    for processor, region, metric in product(
        ("A", "B"), ("us", "eu"), (LATENCY_COUNT, LATENCY_SUM)
    ):
        result.add((metric, (("processor", processor), ("region", region))))
    return result


def _vector(payload: JsonObject) -> list[JsonObject]:
    """Warnings and non-vector responses are incomplete evidence, never successful empty data."""
    data = object_value(payload.get("data", {}))
    if (
        payload.get("status") != "success"
        or payload.get("warnings")
        or data.get("resultType") != "vector"
    ):
        raise ValueError("complete Prometheus vector required")
    result = object_items(data.get("result", []))
    if len(result) > 128:
        raise ValueError("metric response exceeds fixed cardinality bound")
    return result


def _number(point: JsonObject) -> float:
    """Reject NaNs, infinities and malformed vector pairs before counter arithmetic."""
    value = point.get("value")
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("expected timestamp/value pair")
    timestamp = value[0]
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, (int, float))
        or not math.isfinite(timestamp)
        or timestamp < 0
    ):
        raise ValueError("finite nonnegative numeric sample timestamp required")
    result = float(str(value[1]))
    if not math.isfinite(result) or result < 0:
        raise ValueError("nonnegative finite sample required")
    return result


def parse_snapshot(reference: SnapshotRef, role: TrafficRole, raw: JsonObject) -> MetricSnapshot:
    """Separate exact-service availability and process epoch from counter samples."""
    service = {"payments": "payments-api", "webhook": "webhook-sim"}[role]
    points: list[MetricDelta] = []
    special: dict[str, float] = {}
    for row in _vector(object_value(raw["metrics"])):
        labels = object_value(row["metric"])
        name = str(labels.get("__name__"))
        if labels.get("service") != service or name not in METRICS:
            raise ValueError("metric outside fixed service/name scope")
        if name in {EPOCH, "up"}:
            if name in special:
                raise ValueError("multiple target epochs are ambiguous")
            special[name] = _number(row)
        else:
            kept = tuple(
                sorted(
                    (key, str(value))
                    for key, value in labels.items()
                    if key not in {"__name__", "service", "job", "instance"}
                )
            )
            points.append(MetricDelta(metric=name, labels=kept, value=_number(row)))
    marks = _vector(object_value(raw["watermark"]))
    mark = _number(marks[0]) if len(marks) == 1 else None
    if mark is not None and mark > reference.evaluated_at:
        raise ValueError("scrape watermark is after the query evaluation instant")
    if marks and object_value(marks[0]["metric"]).get("service") != service:
        raise ValueError("watermark outside service scope")
    return MetricSnapshot(
        reference=reference,
        service=service,
        epoch=special.get(EPOCH),
        watermark=mark,
        available=special.get("up") == 1,
        points=tuple(points),
    )


def _completeness(
    before: MetricSnapshot, after: MetricSnapshot, traffic_start: float, traffic_end: float
) -> Completeness:
    """Missing data, process changes and actual counter decreases are distinct failures."""
    if (
        not before.available
        or not after.available
        or before.epoch is None
        or after.epoch is None
        or before.watermark is None
        or after.watermark is None
        or before.watermark > traffic_start
        or after.watermark < traffic_end
        or before.reference.evaluated_at > traffic_start
        or after.reference.evaluated_at < traffic_end
    ):
        return "missing"
    if before.service != after.service or before.epoch != after.epoch:
        return "mixed_epoch"
    first = {(item.metric, item.labels): item.value for item in before.points}
    last = {(item.metric, item.labels): item.value for item in after.points}
    if (
        set(first) != required_series()
        or set(last) != required_series()
        or len(first) != len(before.points)
        or len(last) != len(after.points)
    ):
        return "missing"
    return "reset" if any(last[key] < value for key, value in first.items()) else "complete"


def derive_window(
    before: MetricSnapshot, after: MetricSnapshot, traffic: TrafficReceipt
) -> PaymentWindow:
    """Only complete same-epoch snapshots produce numbers; unsupported windows stay empty."""
    start, end = traffic.started_at.timestamp(), traffic.completed_at.timestamp()
    status = _completeness(before, after, start, end)
    delta: tuple[MetricDelta, ...] = ()
    if status == "complete":
        prior = {(point.metric, point.labels): point.value for point in before.points}
        delta = tuple(
            MetricDelta(
                metric=point.metric,
                labels=point.labels,
                value=point.value - prior[point.metric, point.labels],
            )
            for point in after.points
        )
    return PaymentWindow(
        service=before.service,
        window_start=before.reference.evaluated_at,
        window_end=after.reference.evaluated_at,
        traffic_start=start,
        traffic_end=end,
        status=status,
        before=before.reference,
        after=after.reference,
        request_counts=tuple(point for point in delta if point.metric == REQUESTS),
        latency=tuple(
            point for point in delta if point.metric.startswith("payment_authorization_latency_")
        ),
        conflicts=tuple(point for point in delta if point.metric == CONFLICTS),
    )


def _sum(points: tuple[MetricDelta, ...], metric: str, **labels: str) -> float:
    """Aggregate only exact supported labels from an already complete window."""
    return sum(
        point.value
        for point in points
        if point.metric == metric
        and all(dict(point.labels).get(key) == value for key, value in labels.items())
    )


def _mean(window: PaymentWindow, **labels: str) -> float:
    """Window mean latency uses histogram sum/count and makes no p95 claim."""
    count = _sum(window.latency, LATENCY_COUNT, **labels)
    return _sum(window.latency, LATENCY_SUM, **labels) / count if count else 0


def traffic_counts_match(window: PaymentWindow, traffic: TrafficReceipt) -> bool:
    """Exact per-slice request deltas reject unrelated traffic and missing control observations."""
    expected: Counter[tuple[tuple[str, str], ...]] = Counter()
    for item in traffic.attempts:
        sample = item.planned.sample
        status = item.outcome if item.outcome in {"accepted", "declined"} else "error"
        expected[
            tuple(
                sorted(
                    {
                        "processor": sample.processor,
                        "region": sample.region,
                        "payment_method": sample.payment_method,
                        "status": status,
                    }.items()
                )
            )
        ] += 1
    observed = {point.labels: point.value for point in window.request_counts if point.value != 0}
    service = {"payments": "payments-api", "webhook": "webhook-sim"}[traffic.role]
    return observed == dict(expected) and window.service == service


def histogram_counts_match(window: PaymentWindow) -> bool:
    """Missing control latency cannot masquerade as zero latency in a complete window."""
    for processor, region in product(("A", "B"), ("us", "eu")):
        labels = {"processor": processor, "region": region}
        requests = _sum(window.request_counts, REQUESTS, **labels)
        count = _sum(window.latency, LATENCY_COUNT, **labels)
        duration = _sum(window.latency, LATENCY_SUM, **labels)
        if count != requests or (count == 0 and duration != 0):
            return False
    return True


def verify_runtime(case_id: CaseId, window: PaymentWindow, traffic: TrafficReceipt) -> bool:
    """Actual HTTP outcomes corroborate counter deltas and the unaffected control slice."""
    if (
        case_id not in PACKAGE_A_CASES
        or window.status != "complete"
        or traffic.status != "completed"
        or not traffic_counts_match(window, traffic)
        or not histogram_counts_match(window)
    ):
        return False
    if case_id == "PAY-04":
        return (
            traffic.probe_verified is True
            and _sum(window.conflicts, CONFLICTS) == 1
            and _sum(window.request_counts, REQUESTS) == 3
        )
    if len(traffic.attempts) != 32 or _sum(window.request_counts, REQUESTS) != 32:
        return False
    if case_id == "PAY-02":
        return (
            all(attempt.outcome == "accepted" for attempt in traffic.attempts)
            and _mean(window, region="eu") - _mean(window, region="us") >= 0.3
        )
    label = {"payment_method": "debit"} if case_id == "PAY-03" else {"processor": "B"}
    affected_status = "error" if case_id == "DEP-02" else "declined"
    expected_http = 429 if case_id == "DEP-02" else 200
    affected = [
        item
        for item in traffic.attempts
        if all(getattr(item.planned.sample, key) == value for key, value in label.items())
    ]
    return (
        _sum(window.request_counts, REQUESTS, status=affected_status, **label) == 16
        and _sum(window.request_counts, REQUESTS, status="accepted") == 16
        and len(affected) == 16
        and all(item.http_status == expected_http for item in affected)
        and (case_id != "DEP-02" or all((item.latency_seconds or 0) >= 0.3 for item in affected))
    )


class PackageAHarness:
    """Fixed read-only metrics queries surround operator traffic within the runner's latch."""

    def __init__(self, kubeconfig: Path, transport: httpx.BaseTransport | None = None) -> None:
        """Fixture transports are injectable; the live Prometheus origin is fixed loopback."""
        self._config = kubeconfig
        self._transport = transport

    def _query(self, query: str, evaluated_at: float) -> JsonObject:
        """Pin both metric and watermark queries to one evaluation time to avoid scrape races."""
        with httpx.Client(
            base_url="http://127.0.0.1:19090",
            timeout=5,
            trust_env=False,
            follow_redirects=False,
            transport=self._transport,
        ) as client:
            with client.stream(
                "GET",
                "/api/v1/query",
                params={"query": query, "time": evaluated_at, "timeout": "3s", "limit": 128},
                headers={"Accept-Encoding": "identity"},
            ) as response:
                response.raise_for_status()
                content = bytearray()
                for chunk in response.iter_raw(chunk_size=8192):
                    if len(content) + len(chunk) > 131072:
                        raise ValueError("bounded metric response exceeded")
                    content.extend(chunk)
                return TypeAdapter[JsonObject](JsonObject).validate_json(bytes(content))

    def snapshot(self, role: TrafficRole, root: Path, index: int) -> MetricSnapshot:
        """Raw query envelopes are retained before parsing so rejected responses remain visible."""
        query, watermark_query = queries(role)
        evaluated_at = time.time()
        raw = {
            "evaluated_at": evaluated_at,
            "query": query,
            "watermark_query": watermark_query,
            "metrics": self._query(query, evaluated_at),
            "watermark": self._query(watermark_query, evaluated_at),
        }
        content = json.dumps(raw, indent=2, sort_keys=True).encode()
        path = root / f"snapshot-{index:03d}.json"
        with path.open("xb") as handle:
            handle.write(content)
        reference = SnapshotRef(
            path=path.name,
            sha256=hashlib.sha256(content).hexdigest(),
            evaluated_at=evaluated_at,
            query=query,
            watermark_query=watermark_query,
        )
        return parse_snapshot(
            reference, role, TypeAdapter[JsonObject](JsonObject).validate_python(raw)
        )

    def _fresh(self, role: TrafficRole, root: Path, minimum: float) -> MetricSnapshot:
        """Use a twenty-second polling cutoff; in-flight queries retain five-second I/O timeouts."""
        deadline = time.monotonic() + 20
        while True:
            snapshot = self.snapshot(role, root, len(list(root.glob("snapshot-*.json"))))
            if (
                snapshot.available
                and snapshot.watermark is not None
                and snapshot.watermark >= minimum
            ):
                return snapshot
            if time.monotonic() >= deadline:
                raise TimeoutError("post-traffic scrape watermark unavailable")
            time.sleep(0.5)

    def collect(self, case_id: CaseId, directory: Path) -> JsonObject:
        """Retain hashes and failure classes even when collection raises before activation."""
        workload = workload_for(case_id)
        root = directory / "package-a"
        root.mkdir(exist_ok=False)
        try:
            return self._collect_window(case_id, workload, root)
        except BaseException as exc:
            (root / "failure.json").write_text(
                json.dumps({"error_type": type(exc).__name__}), encoding="utf-8"
            )
            raise
        finally:
            files = [
                {
                    "path": str(path.relative_to(directory)),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
                for path in root.rglob("*.json")
                if path.name != "manifest.json"
            ]
            (root / "manifest.json").write_text(json.dumps(files, indent=2), encoding="utf-8")

    def _collect_window(self, case_id: CaseId, workload: Workload, root: Path) -> JsonObject:
        """The operator case chooses traffic; the derived observation contains no case or gold."""
        before = self._fresh(workload.role, root, time.time())
        driver = TrafficDriver(self._config, root / "traffic")
        traffic = asyncio.run(
            driver.run_idempotency_probe() if case_id == "PAY-04" else driver.run(workload)
        )
        after = self._fresh(workload.role, root, traffic.completed_at.timestamp())
        window = derive_window(before, after, traffic)
        verified = verify_runtime(case_id, window, traffic)
        result: JsonObject = {
            "package_a_verified": verified,
            "observed_at": datetime.now(UTC).isoformat(),
            "payment_window": TypeAdapter[JsonObject](JsonObject).validate_json(
                window.model_dump_json()
            ),
            "traffic_run_id": traffic.run_id,
            "traffic_mode": traffic.mode,
        }
        result["files"] = [
            {
                "path": str(path.relative_to(root.parent)),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in root.rglob("*.json")
        ]
        return result
