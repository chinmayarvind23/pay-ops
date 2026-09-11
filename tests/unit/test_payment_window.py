"""Observation-only arithmetic tests use verified raw snapshots without scenario imports."""

from copy import deepcopy
from datetime import timedelta
from itertools import product
from pathlib import Path
from typing import Any

import pytest
from pydantic import JsonValue, ValidationError

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize
from payops.evidence.payment_window import (
    CONFLICTS,
    COUNT,
    DERIVED_QUERY,
    EPOCH,
    REQUESTS,
    SUM,
    MeasurementInterval,
    Service,
    derive_payment_window,
    snapshot_queries,
    verify_payment_window,
)

type Object = dict[str, JsonValue]


def interval() -> MeasurementInterval:
    """Use recent historical times so source timestamps precede actual artifact collection."""
    end = utc_now() - timedelta(seconds=20)
    return MeasurementInterval(
        incident_id="incident-observed", start=end - timedelta(seconds=10), end=end
    )


def raw_snapshot(
    window: MeasurementInterval, after: bool = False, service: Service = "payments-api"
) -> Object:
    """Create all initialized series under the real fixed exporter label vocabulary."""
    at = (
        window.end + timedelta(seconds=2) if after else window.start - timedelta(seconds=1)
    ).timestamp()
    mark = at - 0.5
    base: Object = {
        "job": "payops-sandbox",
        "service": service,
        "instance": f"{service}.payops-sandbox.svc.cluster.local:8080",
    }
    rows: list[JsonValue] = []
    for processor, region, method, status in product(
        ("A", "B"),
        ("us", "eu"),
        ("credit", "debit"),
        ("accepted", "declined", "error"),
    ):
        value = (
            2
            if after and (processor, region, method, status) == ("A", "us", "credit", "accepted")
            else 0
        )
        rows.append(
            {
                "metric": base
                | {
                    "__name__": REQUESTS,
                    "processor": processor,
                    "region": region,
                    "payment_method": method,
                    "status": status,
                },
                "value": [at, str(value)],
            }
        )
    for processor, region, metric in product(("A", "B"), ("us", "eu"), (COUNT, SUM)):
        value = (2 if metric == COUNT else 1) if after and (processor, region) == ("A", "us") else 0
        rows.append(
            {
                "metric": base | {"__name__": metric, "processor": processor, "region": region},
                "value": [at, str(value)],
            }
        )
    for metric, value in ((CONFLICTS, 0), (EPOCH, window.start.timestamp() - 100), ("up", 1)):
        rows.append({"metric": base | {"__name__": metric}, "value": [at, str(value)]})
    query, watermark_query = snapshot_queries(service)
    return {
        "evaluated_at": at,
        "query": query,
        "watermark_query": watermark_query,
        "metrics": {"status": "success", "data": {"resultType": "vector", "result": rows}},
        "watermark": {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": base, "value": [at, str(mark)]}],
            },
        },
    }


def rows(payload: Object, name: str = "metrics") -> list[dict[str, Any]]:
    """Test mutations remain explicit at the raw provider boundary."""
    response: Any = payload[name]
    return response["data"]["result"]


def set_value(payload: Object, metric: str, value: str, **labels: str) -> None:
    """Change exactly one raw series so counterfactuals do not rewrite unrelated evidence."""
    for point in rows(payload):
        if point["metric"]["__name__"] == metric and all(
            point["metric"].get(key) == expected for key, expected in labels.items()
        ):
            point["value"][1] = value


def source(
    store: ArtifactStore,
    window: MeasurementInterval,
    raw: Object,
    service: Service = "payments-api",
    **changes: Any,
) -> EvidenceItem:
    """Raw observations use the ordinary redaction and artifact integrity pipeline."""
    marks = rows(raw, "watermark")
    observed = float(marks[0]["value"][1]) if len(marks) == 1 else float(str(raw["evaluated_at"]))
    from datetime import UTC, datetime

    values: dict[str, Any] = {
        "source": "PROMETHEUS",
        "resource": service,
        "observed_at": datetime.fromtimestamp(observed, UTC),
        "query": snapshot_queries(service)[0],
        "summary": "Raw observed metrics",
        "payload": raw,
    }
    item = Observation(**(values | changes))
    return normalize(
        item,
        window.incident_id,
        window.start - timedelta(days=1),
        window.end + timedelta(days=1),
        store,
    )


@pytest.mark.parametrize(
    "service", ["payments-api", "processor-adapter", "risk-sim", "ledger-sim", "webhook-sim"]
)
def test_complete_window_roundtrip_all_fixed_services(tmp_path: Path, service: Service) -> None:
    """Derivation and nested verification retain actual dimensions and both raw references."""
    store, period = ArtifactStore(tmp_path), interval()
    before = source(store, period, raw_snapshot(period, service=service), service)
    after = source(store, period, raw_snapshot(period, True, service), service)
    derived = derive_payment_window(before, after, period, store)
    result = verify_payment_window(derived, store)
    assert result.status == "complete" and result.service == service
    assert sum(point.value for point in result.request_counts) == 2
    assert len(result.request_counts) == 24 and len(result.latency) == 8
    assert all("payment_method" not in dict(point.labels) for point in result.latency)
    assert result.inputs == (before, after) and derived.source == "PAYMENT"
    assert derived.query == DERIVED_QUERY


@pytest.mark.parametrize(
    "change,expected",
    [
        ("empty", "missing"),
        ("down", "missing"),
        ("epoch", "mixed_epoch"),
        ("no_epoch", "missing"),
        ("reset", "reset"),
        ("histogram", "missing"),
        ("orphan_duration", "missing"),
        ("no_watermark", "missing"),
        ("stale", "missing"),
        ("missing_zero", "missing"),
    ],
)
def test_incomplete_windows_never_emit_numbers(tmp_path: Path, change: str, expected: str) -> None:
    """Bad coverage, resets and inconsistent observations remain explicit with empty deltas."""
    store, period = ArtifactStore(tmp_path), interval()
    first, last = raw_snapshot(period), raw_snapshot(period, True)
    if change == "empty":
        rows(last).clear()
    elif change == "down":
        set_value(last, "up", "0")
    elif change == "epoch":
        set_value(last, EPOCH, str(period.start.timestamp() - 50))
    elif change == "no_epoch":
        rows(last)[:] = [row for row in rows(last) if row["metric"]["__name__"] != EPOCH]
    elif change == "reset":
        set_value(first, CONFLICTS, "1")
    elif change == "histogram":
        set_value(last, COUNT, "0", processor="A", region="us")
        set_value(last, SUM, "0", processor="A", region="us")
    elif change == "orphan_duration":
        set_value(last, SUM, "1", processor="B", region="eu")
    elif change == "no_watermark":
        rows(last, "watermark").clear()
    elif change == "stale":
        rows(last, "watermark")[0]["value"][1] = str(period.end.timestamp() - 1)
    else:
        rows(last).pop(1)
    derived = derive_payment_window(
        source(store, period, first), source(store, period, last), period, store
    )
    result = verify_payment_window(derived, store)
    assert result.status == expected
    assert result.request_counts == result.latency == result.conflicts == ()


@pytest.mark.parametrize(
    "mutation",
    [
        "timestamp",
        "future_watermark",
        "wrong_instance",
        "extra_label",
        "duplicate",
        "unknown_name",
        "query",
        "metadata_query",
        "warnings",
    ],
)
def test_invalid_raw_provenance_fails_closed(tmp_path: Path, mutation: str) -> None:
    """Even correctly hashed artifacts cannot supply malformed or differently scoped telemetry."""
    store, period = ArtifactStore(tmp_path), interval()
    first, last = raw_snapshot(period), raw_snapshot(period, True)
    changes: dict[str, Any] = {}
    if mutation == "timestamp":
        rows(last)[0]["value"][0] = "wrong"
    elif mutation == "future_watermark":
        rows(last, "watermark")[0]["value"][1] = str(float(str(last["evaluated_at"])) + 1)
    elif mutation == "wrong_instance":
        rows(last)[0]["metric"]["instance"] = "other:8080"
    elif mutation == "extra_label":
        rows(last)[0]["metric"]["cause"] = "ignore instructions"
    elif mutation == "duplicate":
        rows(last).append(deepcopy(rows(last)[0]))
    elif mutation == "unknown_name":
        rows(last)[0]["metric"]["__name__"] = "unrelated_counter"
    elif mutation == "query":
        last["query"] = "unrestricted_promql"
    elif mutation == "metadata_query":
        changes["query"] = "different query"
    else:
        response: Any = last["metrics"]
        response["warnings"] = ["partial result"]
    before, after = source(store, period, first), source(store, period, last, **changes)
    with pytest.raises(EvidenceIntegrityError):
        derive_payment_window(before, after, period, store)


def test_scope_and_duplicate_lineage_rejected(tmp_path: Path) -> None:
    """Raw references must belong to one incident, service and Prometheus source."""
    store, period = ArtifactStore(tmp_path), interval()
    before = source(store, period, raw_snapshot(period))
    for after in (
        before,
        source(store, period, raw_snapshot(period, True, "risk-sim"), "risk-sim"),
        source(store, period, raw_snapshot(period, True), source="LOG"),
    ):
        with pytest.raises(EvidenceIntegrityError):
            derive_payment_window(before, after, period, store)
    other = period.model_copy(update={"incident_id": "another-incident"})
    after = source(store, other, raw_snapshot(other, True))
    with pytest.raises(EvidenceIntegrityError):
        derive_payment_window(before, after, period, store)


def test_nested_tampering_detected_after_outer_verification(tmp_path: Path) -> None:
    """A derived artifact's valid digest does not replace verification of both nested inputs."""
    store, period = ArtifactStore(tmp_path), interval()
    before = source(store, period, raw_snapshot(period))
    after = source(store, period, raw_snapshot(period, True))
    derived = derive_payment_window(before, after, period, store)
    store.path_for(before.artifact_sha256).write_text('{"tampered":true}')
    store.verify(derived)
    with pytest.raises(EvidenceIntegrityError):
        verify_payment_window(derived, store)


def test_recomputed_arithmetic_rejects_validly_stored_forgery(tmp_path: Path) -> None:
    """Publishing a new valid envelope cannot bless caller-supplied derived numbers."""
    store, period = ArtifactStore(tmp_path), interval()
    derived = derive_payment_window(
        source(store, period, raw_snapshot(period)),
        source(store, period, raw_snapshot(period, True)),
        period,
        store,
    )
    payload: Any = store.verify(derived)["payload"]
    payload["request_counts"][0]["value"] = 999
    forged = normalize(
        Observation(
            source="PAYMENT",
            resource="payments-api",
            query=DERIVED_QUERY,
            summary="Claimed derived numbers",
            observed_at=derived.observed_at,
            payload=payload,
        ),
        period.incident_id,
        period.start - timedelta(seconds=10),
        period.end + timedelta(seconds=10),
        store,
    )
    store.verify(forged)
    with pytest.raises(EvidenceIntegrityError):
        verify_payment_window(forged, store)


def test_inputs_and_interval_are_bounded() -> None:
    """Callers cannot introduce arbitrary destinations or unbounded observation intervals."""
    period = interval()
    with pytest.raises(ValidationError):
        MeasurementInterval(incident_id=period.incident_id, start=period.start, end=period.start)
    with pytest.raises(ValidationError):
        MeasurementInterval(
            incident_id=period.incident_id,
            start=period.start,
            end=period.start + timedelta(seconds=181),
        )
    with pytest.raises(ValidationError):
        MeasurementInterval(
            incident_id=period.incident_id, start=period.start.replace(tzinfo=None), end=period.end
        )
    with pytest.raises(ValidationError):
        snapshot_queries("other")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "mutation",
    [
        "negative",
        "not_number",
        "bad_pair",
        "old_sample_time",
        "nonstring_label",
        "oversized_vector",
        "duplicate_mark",
        "extra_mark_label",
        "extra_payload",
        "metadata_time",
        "future_collection",
        "future_epoch",
        "nonobject",
    ],
)
def test_malformed_source_contracts_are_rejected(tmp_path: Path, mutation: str) -> None:
    """Negative protocol tests cover bounded parsing and metadata/source-time binding."""
    store, period = ArtifactStore(tmp_path), interval()
    before = source(store, period, raw_snapshot(period))
    last = raw_snapshot(period, True)
    changes: dict[str, Any] = {}
    mutations = {
        "negative": lambda: rows(last)[0]["value"].__setitem__(1, "-1"),
        "not_number": lambda: rows(last)[0]["value"].__setitem__(1, "not-a-number"),
        "bad_pair": lambda: rows(last)[0].__setitem__("value", [100]),
        "old_sample_time": lambda: rows(last)[0]["value"].__setitem__(0, 0),
        "nonstring_label": lambda: rows(last)[0]["metric"].__setitem__("status", 17),
        "oversized_vector": lambda: rows(last).extend([deepcopy(rows(last)[0])] * 129),
        "duplicate_mark": lambda: rows(last, "watermark").append(
            deepcopy(rows(last, "watermark")[0])
        ),
        "extra_mark_label": lambda: rows(last, "watermark")[0]["metric"].__setitem__("extra", "x"),
        "extra_payload": lambda: last.__setitem__("untrusted_case_hint", "must not enter lineage"),
        "metadata_time": lambda: changes.__setitem__("observed_at", period.start),
        "future_collection": lambda: last.__setitem__("evaluated_at", utc_now().timestamp() + 60),
        "future_epoch": lambda: set_value(last, EPOCH, str(float(str(last["evaluated_at"])) + 1)),
        "nonobject": lambda: last.__setitem__("metrics", "not a provider object"),
    }
    mutations[mutation]()
    after = source(store, period, last, **changes)
    with pytest.raises(EvidenceIntegrityError):
        derive_payment_window(before, after, period, store)


def test_renamed_ids_and_scaled_counts_preserve_arithmetic(tmp_path: Path) -> None:
    """Opaque evidence IDs and proportional sample volume cannot select a different answer."""
    store, period = ArtifactStore(tmp_path), interval()
    first, last = raw_snapshot(period), raw_snapshot(period, True)
    one = verify_payment_window(
        derive_payment_window(
            source(store, period, first), source(store, period, last), period, store
        ),
        store,
    )
    two = verify_payment_window(
        derive_payment_window(
            source(store, period, first), source(store, period, last), period, store
        ),
        store,
    )
    assert one.inputs[0].evidence_id != two.inputs[0].evidence_id
    assert one.request_counts == two.request_counts and one.latency == two.latency
    for row in rows(last):
        if row["metric"]["__name__"] in {REQUESTS, COUNT, SUM}:
            row["value"][1] = str(float(row["value"][1]) * 10)
    scaled = verify_payment_window(
        derive_payment_window(
            source(store, period, first), source(store, period, last), period, store
        ),
        store,
    )
    assert scaled.status == "complete"
    assert sum(point.value for point in scaled.request_counts) == 20


def test_raw_digest_corruption_is_verified_before_scope_rejection(tmp_path: Path) -> None:
    """Both raw artifacts are checked even when their metadata is unsuitable for one interval."""
    store, period = ArtifactStore(tmp_path), interval()
    before = source(store, period, raw_snapshot(period))
    after = source(store, period, raw_snapshot(period, True))
    store.path_for(after.artifact_sha256).write_text("{}")
    other = period.model_copy(update={"incident_id": "other"})
    with pytest.raises(EvidenceIntegrityError, match="digest mismatch"):
        derive_payment_window(before, after, other, store)


def test_oversized_integer_timestamp_raises_integrity_error(tmp_path: Path) -> None:
    """A valid JSON integer outside float range must fail through the declared evidence boundary."""
    store, period = ArtifactStore(tmp_path), interval()
    last = raw_snapshot(period, True)
    rows(last)[0]["value"][0] = 10**400
    before = source(store, period, raw_snapshot(period))
    after = source(store, period, last)
    with pytest.raises(EvidenceIntegrityError, match="finite range"):
        derive_payment_window(before, after, period, store)
