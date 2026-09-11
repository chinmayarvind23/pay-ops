"""Raw-artifact counterfactuals verify sampling acceptance independently of case labels."""

from datetime import timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from test_payment_window import raw_snapshot, set_value, source

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.payment_window import (
    CONFLICTS,
    COUNT,
    REQUESTS,
    SUM,
    MeasurementInterval,
    Service,
    derive_payment_window,
)
from payops.evidence.trace_span import (
    CapturedSpan,
    ParsedLog,
    PodIdentity,
    SourceSpan,
    TraceLog,
    TraceScope,
    publish_trace_log,
)
from payops.sandbox.models import Sample, SimulationResult
from payops.scenarios.sampling_observation import (
    SamplingObservation,
    Stage,
    verify_sampling_counterfactual,
    verify_sampling_stage,
)
from payops.scenarios.traffic import AttemptRecord, PlannedAttempt, TrafficReceipt
from payops.tools.traces import TraceCollection, summarize_graph


def traffic(age_seconds: int = 100) -> TrafficReceipt:
    """Known synthetic attempt parents allow lineage substitutions without a live endpoint."""
    start = utc_now() - timedelta(seconds=age_seconds)
    unique = uuid4().hex
    attempts: list[AttemptRecord] = []
    for index in range(8):
        sample = Sample(
            sample_id=f"synthetic-{unique}-{index}", processor="A" if index < 4 else "B"
        )
        attempts.append(
            AttemptRecord(
                planned=PlannedAttempt(
                    index=index,
                    sample=sample,
                    traceparent=f"00-{unique[:24]}{index + 1:08x}-{index + 10:016x}-01",
                ),
                started_at=start,
                completed_at=start + timedelta(seconds=1),
                latency_seconds=1.0,
                outcome="accepted",
                http_status=200,
                result=SimulationResult(
                    sample_id=sample.sample_id, role="payments", status="accepted"
                ),
            )
        )
    return TrafficReceipt(
        run_id=unique,
        mode="fixture_replay",
        role="payments",
        started_at=start,
        completed_at=start + timedelta(seconds=2),
        status="completed",
        failure=None,
        attempts=tuple(attempts),
    )


def identity(service: str, receipt: TrafficReceipt | None = None) -> PodIdentity:
    """All captures must retain the trusted stage's expected runtime identity."""
    suffix = receipt.run_id if receipt is not None and service == "processor-adapter" else ""
    return PodIdentity(
        pod_name=service + "-abc",
        pod_uid=service + "-uid" + suffix,
        deployment_uid=service + "-deployment",
        replica_set_uid=service + "-rs",
        container_id="containerd://" + service + suffix,
        restart_count=0,
    )


def metrics(
    store: ArtifactStore,
    receipt: TrafficReceipt,
    service: Service,
    mean: float,
    count: int = 4,
    conflict: float = 0,
    incident: str = "sampling",
    histogram_count: int | None = None,
) -> EvidenceItem:
    """Publish genuine two-snapshot arithmetic inputs with eight matching histogram observations."""
    period = MeasurementInterval(
        incident_id=incident, start=receipt.started_at, end=receipt.completed_at
    )
    before, after = raw_snapshot(period, service=service), raw_snapshot(period, True, service)
    for processor in ("A", "B"):
        set_value(
            after,
            REQUESTS,
            str(count),
            processor=processor,
            region="us",
            payment_method="credit",
            status="accepted",
        )
        set_value(
            after,
            COUNT,
            str(count if histogram_count is None else histogram_count),
            processor=processor,
            region="us",
        )
        set_value(after, SUM, str(count * mean), processor=processor, region="us")
    set_value(after, CONFLICTS, str(conflict))
    return derive_payment_window(
        source(store, period, before, service), source(store, period, after, service), period, store
    )


def log(receipt: TrafficReceipt, service: str, suppressed: bool, offset: int) -> TraceLog:
    """Build parsed source records with explicit exporter and request timestamps."""
    start = receipt.started_at
    records: list[CapturedSpan] = []
    for row in receipt.attempts:
        trace_id = "0x" + row.planned.traceparent.split("-")[1]
        parent = "0x" + row.planned.traceparent.split("-")[2]
        server_id, client_id = (
            "0x" + f"{row.planned.index + 100:016x}",
            "0x" + f"{row.planned.index + 200:016x}",
        )
        shapes = [
            ("sandbox.payments", server_id, parent, "SpanKind.SERVER"),
            ("sandbox.call.processor", client_id, server_id, "SpanKind.CLIENT"),
        ]
        if service == "processor-adapter":
            shapes = (
                []
                if suppressed
                else [
                    (
                        "sandbox.processor",
                        "0x" + f"{row.planned.index + 300:016x}",
                        client_id,
                        "SpanKind.SERVER",
                    )
                ]
            )
        for name, span_id, parent_id, kind in shapes:
            span = SourceSpan.model_validate(
                {
                    "name": name,
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "parent_id": parent_id,
                    "kind": kind,
                    "start_time": start,
                    "end_time": start + timedelta(seconds=0.6),
                    "status_code": "UNSET",
                    "service_name": "payops-sandbox-"
                    + ("payments" if service == "payments-api" else "processor"),
                }
            )
            records.append(
                CapturedSpan(
                    span=span,
                    log_start=start + timedelta(seconds=5),
                    log_end=start + timedelta(seconds=5),
                )
            )
    end = receipt.completed_at + timedelta(seconds=offset)
    return TraceLog(
        scope=TraceScope.model_validate(
            {
                "incident_id": "sampling",
                "service": service,
                "start": start - timedelta(seconds=1),
                "end": end,
            }
        ),
        identity=identity(service, receipt),
        captured_start=end,
        captured_end=end + timedelta(seconds=1),
        parsed=ParsedLog(
            spans=tuple(records),
            raw_bytes=1000,
            line_count=200,
            partial_candidates=0,
            malformed_candidates=0,
            excluded_spans=0,
            limit_reached=False,
        ),
    )


def collection(store: ArtifactStore, logs: tuple[TraceLog, TraceLog]) -> TraceCollection:
    """Publish each sanitized LOG through the real artifact and source verifier path."""
    return TraceCollection(
        sources=tuple(publish_trace_log(item, store) for item in logs),
        spans=(),
        graph=summarize_graph(tuple(row.span for item in logs for row in item.parsed.spans)),
        commands_used=10,
        reserved_commands=16,
        services_without_pods=(),
        partial_candidates=0,
        malformed_candidates=0,
        excluded_spans=0,
        capped_sources=0,
    )


def fixture(store: ArtifactStore, stage: Stage = "suppressed") -> SamplingObservation:
    """Keep delay and sampling as separately selectable observation dimensions."""
    age = {"original": 240, "suppressed": 180, "restored_sampling": 120, "final": 60}
    receipt = traffic(age[stage])
    mean = 0.6 if stage in {"suppressed", "restored_sampling"} else 0.01
    return SamplingObservation(
        traffic=receipt,
        metrics=(
            metrics(store, receipt, "payments-api", mean),
            metrics(store, receipt, "processor-adapter", mean),
        ),
        captures=tuple(
            collection(
                store,
                (
                    log(receipt, "payments-api", False, offset),
                    log(receipt, "processor-adapter", stage == "suppressed", offset),
                ),
            )
            for offset in (12, 17)
        ),  # type: ignore[arg-type]
        payments_identity=identity("payments-api"),
        processor_identity=identity("processor-adapter", receipt),
    )


@pytest.mark.parametrize("stage", ["original", "suppressed", "restored_sampling", "final"])
def test_complete_stage_from_verified_sources(tmp_path: Path, stage: Stage) -> None:
    """Actual arithmetic and span lineage support each frozen stage separately."""
    store = ArtifactStore(tmp_path)
    assert verify_sampling_stage(stage, fixture(store, stage), store) == (
        (0.6, 0.6) if stage in {"suppressed", "restored_sampling"} else (0.01, 0.01)
    )


@pytest.mark.parametrize(
    "change",
    [
        "incoming-parent",
        "client-parent",
        "missing-client",
        "failed-span",
        "duplicate-span",
        "processor-present",
        "cap",
        "partial",
        "identity",
        "window",
        "early",
        "duration",
        "wrong-service",
    ],
)
def test_observation_counterfactuals_fail(tmp_path: Path, change: str) -> None:
    """Each required source edge, bound and identity is necessary, independent of summary claims."""
    store, observed = ArtifactStore(tmp_path), None
    observed = fixture(store)
    payment, processor = (
        log(observed.traffic, "payments-api", False, 12),
        log(observed.traffic, "processor-adapter", True, 12),
    )
    rows = list(payment.parsed.spans)
    if change in {"incoming-parent", "client-parent"}:
        ordinal = 0 if change == "incoming-parent" else 1
        rows[ordinal] = rows[ordinal].model_copy(
            update={"span": rows[ordinal].span.model_copy(update={"parent_id": "0x" + "f" * 16})}
        )
    elif change == "missing-client":
        rows.pop(1)
    elif change == "failed-span":
        rows[0] = rows[0].model_copy(
            update={"span": rows[0].span.model_copy(update={"status_code": "ERROR"})}
        )
    elif change == "duplicate-span":
        rows.append(rows[0])
    elif change == "duration":
        rows[1] = rows[1].model_copy(
            update={
                "span": rows[1].span.model_copy(
                    update={"end_time": rows[1].span.start_time + timedelta(seconds=0.1)}
                )
            }
        )
    elif change == "processor-present":
        processor = log(observed.traffic, "processor-adapter", False, 12)
    payment = payment.model_copy(
        update={"parsed": payment.parsed.model_copy(update={"spans": tuple(rows)})}
    )
    if change in {"cap", "partial"}:
        payment = payment.model_copy(
            update={
                "parsed": payment.parsed.model_copy(
                    update={
                        "limit_reached": change == "cap",
                        "partial_candidates": int(change == "partial"),
                    }
                )
            }
        )
    elif change == "identity":
        processor = processor.model_copy(update={"identity": identity("foreign")})
    elif change in {"window", "early"}:
        key = "start" if change == "window" else "end"
        value = (
            payment.scope.start - timedelta(seconds=1)
            if key == "start"
            else payment.scope.end - timedelta(seconds=1)
        )
        payment = payment.model_copy(
            update={"scope": payment.scope.model_copy(update={key: value})}
        )
    pair = (payment, processor) if change != "wrong-service" else (payment, payment)
    altered = observed.model_copy(
        update={"captures": (collection(store, pair), observed.captures[1])}
    )
    with pytest.raises(ValueError):
        verify_sampling_stage("suppressed", altered, store)


@pytest.mark.parametrize(
    "change",
    [
        "http",
        "census",
        "duplicates",
        "flag",
        "bad-parent",
        "not-started",
        "low-delay",
        "wrong-stage",
        "source-missing",
    ],
)
def test_request_and_metric_counterfactuals(tmp_path: Path, change: str) -> None:
    """A correct trace alone cannot replace actual requests and latency arithmetic."""
    store = ArtifactStore(tmp_path)
    observed = fixture(store)
    attempts = list(observed.traffic.attempts)
    changes: dict[str, Any] = {}
    if change == "http":
        attempts[0] = attempts[0].model_copy(update={"http_status": 503})
    elif change == "census":
        attempts.pop()
    elif change == "duplicates":
        attempts[1] = attempts[0]
    elif change in {"flag", "bad-parent"}:
        parent = attempts[0].planned.traceparent[:-2] + "00" if change == "flag" else "bad"
        attempts[0] = attempts[0].model_copy(
            update={"planned": attempts[0].planned.model_copy(update={"traceparent": parent})}
        )
    elif change == "not-started":
        attempts[0] = attempts[0].model_copy(update={"started_at": None})
    elif change == "low-delay":
        changes["metrics"] = (
            observed.metrics[0],
            metrics(store, observed.traffic, "processor-adapter", 0.1),
        )
    elif change == "source-missing":
        capture = observed.captures[0].model_copy(
            update={"services_without_pods": ("processor-adapter",)}
        )
        changes["captures"] = (capture, observed.captures[1])
    changes["traffic"] = observed.traffic.model_copy(update={"attempts": tuple(attempts)})
    with pytest.raises(ValueError):
        verify_sampling_stage(
            cast(Stage, "wrong") if change == "wrong-stage" else "suppressed",
            observed.model_copy(update=changes),
            store,
        )


@pytest.mark.parametrize(
    "variant",
    ["count", "conflict", "incident", "service", "interval", "failed", "trace-reuse", "deadline"],
)
def test_metric_scope_census_and_receipt_limits(tmp_path: Path, variant: str) -> None:
    """Fully rehashed raw sources still fail semantic census and incident checks."""
    store = ArtifactStore(tmp_path)
    observed = fixture(store)
    changes: dict[str, Any] = {}
    if variant in {"count", "conflict", "incident"}:
        replacement = metrics(
            store,
            observed.traffic,
            "processor-adapter",
            0.6,
            count=3 if variant == "count" else 4,
            conflict=1 if variant == "conflict" else 0,
            incident="another" if variant == "incident" else "sampling",
        )
        changes["metrics"] = (observed.metrics[0], replacement)
    elif variant == "service":
        changes["metrics"] = (observed.metrics[0], observed.metrics[0])
    elif variant == "interval":
        shifted = observed.traffic.model_copy(
            update={"completed_at": observed.traffic.completed_at + timedelta(seconds=1)}
        )
        changes["metrics"] = (
            observed.metrics[0],
            metrics(store, shifted, "processor-adapter", 0.6),
        )
    elif variant == "failed":
        changes["traffic"] = observed.traffic.model_copy(update={"status": "failed"})
    elif variant == "deadline":
        changes["traffic"] = observed.traffic.model_copy(
            update={"completed_at": observed.traffic.started_at + timedelta(seconds=31)}
        )
    else:
        attempts = list(observed.traffic.attempts)
        prior = attempts[0].planned.traceparent.split("-")[1]
        parent = f"00-{prior}-aaaaaaaaaaaaaaaa-01"
        attempts[1] = attempts[1].model_copy(
            update={"planned": attempts[1].planned.model_copy(update={"traceparent": parent})}
        )
        changes["traffic"] = observed.traffic.model_copy(update={"attempts": tuple(attempts)})
    with pytest.raises(ValueError):
        verify_sampling_stage("suppressed", observed.model_copy(update=changes), store)


def test_sampling_counterfactual_preserves_latency(tmp_path: Path) -> None:
    """Returning spans alone are insufficient if the incident delay also changed."""
    store = ArtifactStore(tmp_path)
    suppressed, restored = fixture(store), fixture(store, "restored_sampling")
    verify_sampling_counterfactual(suppressed, restored, store)
    altered = restored.model_copy(
        update={
            "metrics": (
                restored.metrics[0],
                metrics(store, restored.traffic, "processor-adapter", 1.1),
            )
        }
    )
    with pytest.raises(ValueError, match="changed the observed latency"):
        verify_sampling_counterfactual(suppressed, altered, store)


@pytest.mark.parametrize(
    "variant", ["request-time", "client-time", "processor-time", "processor-parent"]
)
def test_temporal_and_processor_parent_substitutions(tmp_path: Path, variant: str) -> None:
    """Valid individual spans still need causal timing and exact downstream parent ownership."""
    store = ArtifactStore(tmp_path)
    observed = fixture(store, "restored_sampling")
    payment, processor = (
        log(observed.traffic, "payments-api", False, 12),
        log(observed.traffic, "processor-adapter", False, 12),
    )
    if variant == "request-time":
        attempts = tuple(
            row.model_copy(
                update={
                    "completed_at": row.completed_at + timedelta(seconds=3),
                    "started_at": row.started_at + timedelta(seconds=3),
                }
            )
            for row in observed.traffic.attempts
            if row.started_at is not None
        )
        shifted = observed.traffic.model_copy(
            update={
                "attempts": attempts,
                "completed_at": observed.traffic.completed_at + timedelta(seconds=3),
            }
        )
        observed = observed.model_copy(
            update={
                "traffic": shifted,
                "metrics": (
                    metrics(store, shifted, "payments-api", 0.6),
                    metrics(store, shifted, "processor-adapter", 0.6),
                ),
            }
        )
        payment, processor = (
            log(shifted, "payments-api", False, 12),
            log(shifted, "processor-adapter", False, 12),
        )
    elif variant == "client-time":
        rows = list(payment.parsed.spans)
        rows[1] = rows[1].model_copy(
            update={
                "span": rows[1].span.model_copy(
                    update={"end_time": rows[1].span.end_time + timedelta(seconds=0.1)}
                )
            }
        )
        payment = payment.model_copy(
            update={"parsed": payment.parsed.model_copy(update={"spans": tuple(rows)})}
        )
    else:
        rows = list(processor.parsed.spans)
        change: dict[str, Any] = (
            {"parent_id": "0x" + "f" * 16}
            if variant == "processor-parent"
            else {"end_time": rows[0].span.end_time + timedelta(seconds=0.1)}
        )
        rows[0] = rows[0].model_copy(update={"span": rows[0].span.model_copy(update=change)})
        processor = processor.model_copy(
            update={"parsed": processor.parsed.model_copy(update={"spans": tuple(rows)})}
        )
    altered = observed.model_copy(
        update={"captures": (collection(store, (payment, processor)), observed.captures[1])}
    )
    with pytest.raises(ValueError):
        verify_sampling_stage("restored_sampling", altered, store)


def test_histogram_zero_with_real_request_census_is_missing(tmp_path: Path) -> None:
    """Absent latency observations cannot be accepted as a fast unaffected control slice."""
    store = ArtifactStore(tmp_path)
    observed = fixture(store)
    wrong = metrics(store, observed.traffic, "processor-adapter", 0.6, histogram_count=0)
    with pytest.raises(ValueError, match="incomplete"):
        verify_sampling_stage(
            "suppressed",
            observed.model_copy(update={"metrics": (observed.metrics[0], wrong)}),
            store,
        )
