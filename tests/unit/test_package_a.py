"""Counterfactual metrics and operator workload tests do not claim live incident performance."""

import json
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from pydantic import JsonValue

from payops.sandbox.models import Sample, SimulationResult
from payops.scenarios.contracts import CaseId, JsonObject
from payops.scenarios.package_a import (
    CONFLICTS,
    EPOCH,
    LATENCY_COUNT,
    LATENCY_SUM,
    REQUESTS,
    MetricDelta,
    MetricSnapshot,
    PackageAHarness,
    SnapshotRef,
    derive_window,
    parse_snapshot,
    queries,
    required_series,
    verify_runtime,
    workload_for,
)
from payops.scenarios.traffic import AttemptRecord, PlannedAttempt, TrafficReceipt


def stamp(value: float) -> datetime:
    """Fixed timestamps make freshness and process-epoch counterfactuals deterministic."""
    return datetime.fromtimestamp(value, UTC)


def reference(name: str, at: float) -> SnapshotRef:
    """Use inert file references for pure arithmetic tests; harness tests verify actual hashes."""
    query, watermark = queries("payments")
    return SnapshotRef(
        path=name, sha256="a" * 64, evaluated_at=at, query=query, watermark_query=watermark
    )


def snapshot(at: float = 100, epoch: float = 1) -> MetricSnapshot:
    """All initialized zero series exist, avoiding absence-as-zero shortcuts."""
    return MetricSnapshot(
        reference=reference(f"snapshot-{at}.json", at),
        service="payments-api",
        epoch=epoch,
        watermark=at,
        available=True,
        points=tuple(
            MetricDelta(metric=metric, labels=labels, value=0.0)
            for metric, labels in sorted(required_series())
        ),
    )


def add(snapshot: MetricSnapshot, metric: str, amount: float, **labels: str) -> MetricSnapshot:
    """Change one exact series; tests cannot accidentally create nonexistent combinations."""
    selected = tuple(sorted(labels.items()))
    assert (metric, selected) in required_series()
    points = tuple(
        point.model_copy(update={"value": point.value + amount})
        if (point.metric, point.labels) == (metric, selected)
        else point
        for point in snapshot.points
    )
    return snapshot.model_copy(update={"points": points})


def traffic(case_id: CaseId) -> TrafficReceipt:
    """Build explicit attempted outcomes to test metric agreement independently of the driver."""
    workload = workload_for(case_id)
    attempts: list[AttemptRecord] = []
    for index, item in enumerate(part for part in workload.distribution for _ in range(part.count)):
        affected = index >= 16
        declined = affected and case_id in {"PAY-01", "PAY-03"}
        rejected = affected and case_id == "DEP-02"
        sample = Sample(
            sample_id=f"synthetic-fixture-{index}",
            processor=item.processor,
            region=item.region,
            payment_method=item.payment_method,
        )
        result = (
            None
            if rejected
            else SimulationResult(
                sample_id=sample.sample_id,
                role="payments",
                status="declined" if declined else "accepted",
            )
        )
        attempts.append(
            AttemptRecord(
                planned=PlannedAttempt(
                    index=index,
                    sample=sample,
                    traceparent="00-" + "a" * 32 + "-" + "b" * 16 + "-01",
                ),
                started_at=stamp(110),
                completed_at=stamp(111),
                latency_seconds=0.5,
                outcome="http_error" if rejected else ("declined" if declined else "accepted"),
                http_status=429 if rejected else 200,
                result=result,
            )
        )
    return TrafficReceipt(
        run_id="fixture",
        mode="fixture_replay",
        role="payments",
        started_at=stamp(110),
        completed_at=stamp(120),
        status="completed",
        failure=None,
        attempts=tuple(attempts),
    )


def measured(case_id: CaseId) -> MetricSnapshot:
    """Simulated cumulative exports match explicit counts without pretending to be observations."""
    result = snapshot(130)
    for attempt in traffic(case_id).attempts:
        sample = attempt.planned.sample
        status = "error" if attempt.http_status == 429 else attempt.outcome
        result = add(
            result,
            REQUESTS,
            1,
            processor=sample.processor,
            region=sample.region,
            payment_method=sample.payment_method,
            status=status,
        )
        result = add(result, LATENCY_COUNT, 1, processor=sample.processor, region=sample.region)
        latency = 0.8 if case_id == "PAY-02" and sample.region == "eu" else 0.2
        result = add(result, LATENCY_SUM, latency, processor=sample.processor, region=sample.region)
    return result


@pytest.mark.parametrize("case_id", ["DEP-02", "PAY-01", "PAY-02", "PAY-03"])
def test_observed_counts_and_controls_activate(case_id: CaseId) -> None:
    """Two metric snapshots and matching HTTP outcomes prove the bounded local variant."""
    receipt = traffic(case_id)
    window = derive_window(snapshot(), measured(case_id), receipt)
    assert window.status == "complete" and verify_runtime(case_id, window, receipt)
    assert all("payment_method" not in dict(point.labels) for point in window.latency)
    assert sum(point.value for point in window.request_counts) == 32


@pytest.mark.parametrize(
    "mutation,expected",
    [
        ({"epoch": 2.0}, "mixed_epoch"),
        ({"epoch": None}, "missing"),
        ({"available": False}, "missing"),
        ({"watermark": 119.0}, "missing"),
        ({"watermark": None}, "missing"),
        ({"service": "webhook-sim"}, "mixed_epoch"),
        ({"points": ()}, "missing"),
    ],
)
def test_invalid_windows_never_emit_zero_substitutes(
    mutation: dict[str, Any], expected: str
) -> None:
    """Missing and incompatible epochs remain explicit rather than yielding plausible deltas."""
    after = measured("PAY-01").model_copy(update=mutation)
    window = derive_window(snapshot(), after, traffic("PAY-01"))
    assert window.status == expected and window.request_counts == () and window.latency == ()


def test_reset_duplicate_series_and_reversed_window() -> None:
    """Counter decreases and duplicate identities cannot be hidden by dictionary projection."""
    before = add(snapshot(), CONFLICTS, 2)
    assert derive_window(before, measured("PAY-01"), traffic("PAY-01")).status == "reset"
    after = measured("PAY-01")
    duplicate = after.model_copy(update={"points": (*after.points, after.points[0])})
    assert derive_window(snapshot(), duplicate, traffic("PAY-01")).status == "missing"
    assert derive_window(snapshot(111), after, traffic("PAY-01")).status == "missing"


def test_counterfactual_http_metrics_and_latency() -> None:
    """Missing causal deltas, wrong HTTP status and control contamination prevent activation."""
    receipt = traffic("DEP-02")
    window = derive_window(snapshot(), measured("DEP-02"), receipt)
    wrong_http = receipt.model_copy(
        update={
            "attempts": tuple(
                item.model_copy(update={"http_status": 200}) for item in receipt.attempts
            )
        }
    )
    assert not verify_runtime("DEP-02", window, wrong_http)
    assert not verify_runtime("DEP-02", window.model_copy(update={"status": "missing"}), receipt)
    assert not verify_runtime("DEP-02", window, receipt.model_copy(update={"status": "failed"}))
    assert not verify_runtime("DEP-02", window, receipt.model_copy(update={"attempts": ()}))
    empty = derive_window(snapshot(), snapshot(130), receipt)
    assert not verify_runtime("DEP-02", empty, receipt)
    slow_control = add(measured("PAY-02"), LATENCY_SUM, 20, processor="A", region="us")
    assert not verify_runtime(
        "PAY-02", derive_window(snapshot(), slow_control, traffic("PAY-02")), traffic("PAY-02")
    )


def vector(rows: list[JsonObject]) -> JsonObject:
    """Construct the official instant-vector envelope for parser protocol tests."""
    return {"status": "success", "data": {"resultType": "vector", "result": list[JsonValue](rows)}}


def raw_snapshot() -> JsonObject:
    """Provide all finite counter labels plus explicit availability, epoch and scrape time."""
    rows: list[JsonObject] = []
    for point in snapshot().points:
        labels: JsonObject = {
            "__name__": point.metric,
            "service": "payments-api",
            "job": "payops-sandbox",
            "instance": "fixed",
        }
        labels.update(dict(point.labels))
        rows.append({"metric": labels, "value": [100, str(point.value)]})
    for name in (EPOCH, "up"):
        rows.append({"metric": {"__name__": name, "service": "payments-api"}, "value": [100, "1"]})
    return {
        "metrics": vector(rows),
        "watermark": vector([{"metric": {"service": "payments-api"}, "value": [100, "100"]}]),
    }


def test_parser_accepts_exact_service_and_retains_zero() -> None:
    """Explicit exported zeros survive; they do not originate from missing-series defaults."""
    parsed = parse_snapshot(reference("raw", 100), "payments", raw_snapshot())
    assert parsed.available and parsed.epoch == 1 and parsed.watermark == 100
    assert len(parsed.points) == 33 and all(point.value == 0 for point in parsed.points)
    assert queries("webhook")[0].count('service="webhook-sim"') == 1
    with pytest.raises(ValueError):
        workload_for("DEP-01")


@pytest.mark.parametrize(
    "raw",
    [
        {"metrics": {"status": "error"}, "watermark": vector([])},
        {
            "metrics": vector(
                [{"metric": {"__name__": "up", "service": "other"}, "value": [0, "1"]}]
            ),
            "watermark": vector([]),
        },
        {
            "metrics": vector(
                [{"metric": {"__name__": "up", "service": "payments-api"}, "value": [0, "NaN"]}]
            ),
            "watermark": vector([]),
        },
        {
            "metrics": vector(
                [{"metric": {"__name__": "up", "service": "payments-api"}, "value": [0]}]
            ),
            "watermark": vector([]),
        },
    ],
)
def test_parser_rejects_malformed_and_wrong_scope(raw: JsonObject) -> None:
    """Malformed provider outputs fail before evidence arithmetic."""
    with pytest.raises(ValueError):
        parse_snapshot(reference("bad", 100), "payments", raw)


def test_harness_retains_raw_snapshot_and_fixed_queries(tmp_path: Path) -> None:
    """One pinned evaluation instant binds metrics and watermark without arbitrary URL access."""
    raw = raw_snapshot()
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        """Return fixed fixture vectors according to the two reviewed query forms."""
        calls.append(request)
        key = "watermark" if request.url.params["query"].startswith("timestamp(") else "metrics"
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(raw[key]).encode()))

    harness = PackageAHarness(tmp_path / "config", httpx.MockTransport(handler))
    with patch("payops.scenarios.package_a.time.time", return_value=100.0):
        parsed = harness.snapshot("payments", tmp_path, 0)
    assert parsed.reference.path == "snapshot-000.json"
    assert len(calls) == 2 and calls[0].url.params["time"] == calls[1].url.params["time"]
    assert all(call.url.host == "127.0.0.1" and call.url.path == "/api/v1/query" for call in calls)
    assert (tmp_path / parsed.reference.path).is_file()


def test_webhook_probe_requires_actual_conflict_delta() -> None:
    """Identical replay and changed-payload conflict remain distinct in the metrics window."""
    original = traffic("PAY-01").attempts[0]
    conflict = original.model_copy(
        update={
            "outcome": "http_error",
            "http_status": 409,
            "result": None,
            "idempotency_conflict": True,
        }
    )
    receipt = traffic("PAY-01").model_copy(
        update={
            "role": "webhook",
            "probe_verified": True,
            "attempts": (original, original, conflict),
        }
    )
    after = snapshot(130)
    after = add(
        after, REQUESTS, 2, processor="A", region="us", payment_method="credit", status="accepted"
    )
    after = add(
        after, REQUESTS, 1, processor="A", region="us", payment_method="credit", status="error"
    )
    after = add(after, CONFLICTS, 1)
    after = add(after, LATENCY_COUNT, 3, processor="A", region="us")
    before = snapshot().model_copy(update={"service": "webhook-sim"})
    after = after.model_copy(update={"service": "webhook-sim"})
    window = derive_window(before, after, receipt)
    assert verify_runtime("PAY-04", window, receipt)
    assert not verify_runtime("PAY-04", window.model_copy(update={"conflicts": ()}), receipt)


@pytest.mark.parametrize("case_id", ["PAY-01", "PAY-04"])
def test_collect_owns_driver_and_manifest(tmp_path: Path, case_id: CaseId) -> None:
    """The operator harness records typed window output and hashes even with mocked network I/O."""
    receipt = traffic("PAY-01")
    with (
        patch.object(PackageAHarness, "_fresh", side_effect=[snapshot(), measured("PAY-01")]),
        patch("payops.scenarios.package_a.TrafficDriver") as driver,
    ):
        driver.return_value.run = AsyncMock(return_value=receipt)
        driver.return_value.run_idempotency_probe = AsyncMock(return_value=receipt)
        result = PackageAHarness(tmp_path / "config").collect(case_id, tmp_path)
    assert result["traffic_mode"] == "fixture_replay"
    assert (tmp_path / "package-a/manifest.json").is_file()
    if case_id == "PAY-01":
        assert result["package_a_verified"] is True
    else:
        driver.return_value.run_idempotency_probe.assert_awaited_once()


def test_failed_collection_retains_failure_manifest(tmp_path: Path) -> None:
    """A query failure remains inspectable even when no measurement window can be returned."""
    with patch.object(PackageAHarness, "_fresh", side_effect=TimeoutError()):
        with pytest.raises(TimeoutError):
            PackageAHarness(tmp_path / "config").collect("PAY-01", tmp_path)
    assert (tmp_path / "package-a/failure.json").is_file()
    assert "failure.json" in (tmp_path / "package-a/manifest.json").read_text()


@pytest.mark.parametrize("timed_out", [False, True])
def test_fresh_wait_uses_scrape_watermark(tmp_path: Path, timed_out: bool) -> None:
    """Query wall time cannot substitute for a scrape newer than the completed traffic."""
    moments = [0.0, 21.0] if timed_out else [0.0, 1.0, 2.0]
    raw = raw_snapshot()

    def handler(request: httpx.Request) -> httpx.Response:
        """Return metrics whose scrape timestamp remains earlier than the desired window."""
        key = "watermark" if request.url.params["query"].startswith("timestamp(") else "metrics"
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(raw[key]).encode()))

    harness = PackageAHarness(tmp_path / "config", httpx.MockTransport(handler))
    with (
        patch("payops.scenarios.package_a.time") as clock,
        patch("payops.scenarios.package_a.TrafficDriver") as driver,
    ):
        clock.monotonic.side_effect = moments
        driver.return_value.run = AsyncMock(return_value=traffic("PAY-01"))
        if timed_out:
            with patch("payops.scenarios.package_a.time.time", return_value=101.0):
                with pytest.raises(TimeoutError):
                    harness.collect("PAY-01", tmp_path)
        else:
            with patch("payops.scenarios.package_a.time.time", return_value=99.0):
                with patch.object(
                    harness,
                    "snapshot",
                    side_effect=[snapshot(98), snapshot(100), measured("PAY-01")],
                ):
                    result = harness.collect("PAY-01", tmp_path)
            assert result["package_a_verified"] is True


def test_parser_ambiguity_and_cardinality_bound() -> None:
    """Multiple epochs, overlarge vectors and wrong-service watermarks are rejected."""
    epoch: JsonObject = {
        "metric": {"__name__": EPOCH, "service": "payments-api"},
        "value": [0, "1"],
    }
    for rows in ([epoch, epoch], [epoch] * 129):
        with pytest.raises(ValueError):
            parse_snapshot(
                reference("bad", 100),
                "payments",
                {"metrics": vector(rows), "watermark": vector([])},
            )
    with pytest.raises(ValueError):
        parse_snapshot(
            reference("bad", 100),
            "payments",
            {
                "metrics": vector([]),
                "watermark": vector([{"metric": {"service": "other"}, "value": [0, "1"]}]),
            },
        )


class CountedStream(httpx.SyncByteStream):
    """Track actual consumption to distinguish a streaming cap from post-allocation truncation."""

    def __init__(self) -> None:
        """Counters verify both early termination and response closure."""
        self.chunks = 0
        self.closed = False

    def __iter__(self) -> Generator[bytes]:
        """Offer more content than the cap while recording how much the client actually reads."""
        for _ in range(100):
            self.chunks += 1
            yield b" " * 8192

    def close(self) -> None:
        """HTTP response context exit must close the stream after a cap violation."""
        self.closed = True


def test_bounded_query_response(tmp_path: Path) -> None:
    """The cap stops reading before allocating the complete oversized response."""
    stream = CountedStream()
    harness = PackageAHarness(
        tmp_path / "config", httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
    )
    with pytest.raises(ValueError):
        harness.snapshot("payments", tmp_path, 0)
    assert stream.chunks == 17 and stream.closed


def test_missing_control_histogram_cannot_activate_region_case() -> None:
    """A complete series set with US requests but zero US histogram increments must fail."""
    after = measured("PAY-02")
    points = tuple(
        point.model_copy(update={"value": 0.0})
        if point.metric in {LATENCY_COUNT, LATENCY_SUM} and dict(point.labels).get("region") == "us"
        else point
        for point in after.points
    )
    window = derive_window(
        snapshot(), after.model_copy(update={"points": points}), traffic("PAY-02")
    )
    assert window.status == "complete"
    assert sum(point.value for point in window.request_counts) == 32
    assert not verify_runtime("PAY-02", window, traffic("PAY-02"))


def test_latency_sum_without_observations_is_rejected() -> None:
    """An unused processor-region slice cannot accumulate duration with zero observations."""
    after = add(measured("PAY-02"), LATENCY_SUM, 1, processor="B", region="eu")
    window = derive_window(snapshot(), after, traffic("PAY-02"))
    assert window.status == "complete"
    assert not verify_runtime("PAY-02", window, traffic("PAY-02"))


@pytest.mark.parametrize("timestamp", [float("nan"), float("inf"), -1, "100", True, None])
def test_invalid_sample_timestamp_fails_closed(timestamp: JsonValue) -> None:
    """Counter values cannot lend credibility to malformed provider sample timestamps."""
    raw: JsonObject = {
        "metrics": vector(
            [{"metric": {"__name__": "up", "service": "payments-api"}, "value": [timestamp, "1"]}]
        ),
        "watermark": vector([]),
    }
    with pytest.raises(ValueError, match="timestamp"):
        parse_snapshot(reference("bad", 100), "payments", raw)


def test_future_scrape_watermark_is_rejected() -> None:
    """A future watermark cannot falsely cover traffic which the snapshot has not observed."""
    raw = raw_snapshot()
    raw["watermark"] = vector([{"metric": {"service": "payments-api"}, "value": [100, "101"]}])
    with pytest.raises(ValueError, match="watermark"):
        parse_snapshot(reference("future", 100), "payments", raw)
