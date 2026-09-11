"""Operator acceptance combines receipts and verified observations without model-facing gold."""

from collections import Counter
from datetime import timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict

from payops.contracts import EvidenceItem
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.payment_window import COUNT, SUM, PaymentWindow, verify_payment_window
from payops.evidence.trace_span import PodIdentity, SourceSpan, TraceLog, verify_trace_log
from payops.scenarios.sampling_contract import PLAN, sampling_workload
from payops.scenarios.traffic import AttemptRecord, TrafficReceipt
from payops.tools.traces import TraceCollection

Stage = Literal["original", "suppressed", "restored_sampling", "final"]


class SamplingObservation(BaseModel):
    """Retain operator traffic separately from references usable by an independent investigator."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    traffic: TrafficReceipt
    metrics: tuple[EvidenceItem, EvidenceItem]
    captures: tuple[TraceCollection, TraceCollection]
    payments_identity: PodIdentity
    processor_identity: PodIdentity


def _traffic(receipt: TrafficReceipt) -> None:
    """Unique sampled identities and exact census prevent replay or extra probe contamination."""
    expected = sampling_workload()
    if receipt.status != "completed" or receipt.failure or receipt.role != "payments":
        raise ValueError("sampling traffic did not complete")
    census = Counter(
        (row.planned.sample.processor, row.planned.sample.region, row.planned.sample.payment_method)
        for row in receipt.attempts
    )
    if census != Counter(
        {
            (row.processor, row.region, row.payment_method): row.count
            for row in expected.distribution
        }
    ):
        raise ValueError("sampling traffic slice census differs")
    traces = [row.planned.traceparent for row in receipt.attempts]
    if (
        len(set(traces)) != 8
        or len({row.planned.sample.sample_id for row in receipt.attempts}) != 8
    ):
        raise ValueError("sampling requires eight unique requests")
    for row in receipt.attempts:
        _attempt(row, receipt)
    if len({value.split("-")[1] for value in traces}) != 8:
        raise ValueError("sampling requires eight distinct trace IDs")
    if not 0 < (receipt.completed_at - receipt.started_at).total_seconds() <= 30:
        raise ValueError("sampling traffic interval exceeds deadline")


def _attempt(row: AttemptRecord, receipt: TrafficReceipt) -> None:
    """Success and parent binding refer to the exact started request, not a trace-ID summary."""
    parts = row.planned.traceparent.split("-")
    if (
        len(parts) != 4
        or parts[0] != "00"
        or parts[3] != "01"
        or len(parts[1]) != 32
        or len(parts[2]) != 16
        or any(char not in "0123456789abcdef" for char in parts[1] + parts[2])
        or int(parts[1], 16) == 0
        or int(parts[2], 16) == 0
    ):
        raise ValueError("sampling requires a valid sampled incoming parent")
    if (
        row.outcome != "accepted"
        or row.http_status != 200
        or row.result is None
        or row.result.role != "payments"
        or row.result.status != "accepted"
        or row.result.sample_id != row.planned.sample.sample_id
        or row.started_at is None
        or not receipt.started_at <= row.started_at <= row.completed_at <= receipt.completed_at
        or row.planned.step != "sample"
    ):
        raise ValueError("sampling request acceptance or time does not match plan")


def _window(
    item: EvidenceItem, observation: SamplingObservation, store: ArtifactStore
) -> PaymentWindow:
    """Recompute complete metrics from both raw artifacts before using any numeric threshold."""
    window = verify_payment_window(item, store)
    traffic = observation.traffic
    if (
        window.status != "complete"
        or window.interval.start != traffic.started_at
        or window.interval.end != traffic.completed_at
    ):
        raise ValueError("sampling metric interval is incomplete or mismatched")
    expected: dict[tuple[str, ...], int] = {
        ("A", "us", "credit", "accepted"): 4,
        ("B", "us", "credit", "accepted"): 4,
    }
    for delta in window.request_counts:
        labels = dict(delta.labels)
        key = tuple(labels[name] for name in ("processor", "region", "payment_method", "status"))
        if delta.value != expected.get(key, 0):
            raise ValueError("sampling metric request census differs")
    if any(delta.value != 0 for delta in window.conflicts):
        raise ValueError("sampling conflict contamination")
    return window


def _means(window: PaymentWindow) -> tuple[float, float]:
    """Verified histogram census supplies a real nonzero denominator for each selected slice."""
    values = {(delta.metric, delta.labels): delta.value for delta in window.latency}
    result: list[float] = []
    for processor in ("A", "B"):
        labels = (("processor", processor), ("region", "us"))
        count = values.get((COUNT, labels), 0)
        if count != 4:
            raise ValueError("sampling histogram census differs")
        result.append(values[SUM, labels] / count)
    return result[0], result[1]


def _metrics(
    stage: Stage, observed: SamplingObservation, store: ArtifactStore
) -> tuple[float, float]:
    """Payment and processor windows must agree in incident, service and exact request census."""
    windows = [_window(item, observed, store) for item in observed.metrics]
    if {window.service for window in windows} != {"payments-api", "processor-adapter"}:
        raise ValueError("sampling requires both service metric windows")
    if len({window.interval.incident_id for window in windows}) != 1:
        raise ValueError("sampling metrics cross incidents")
    means = _means(next(window for window in windows if window.service == "processor-adapter"))
    low, high = (PLAN.delayed_mean_min_seconds, PLAN.delayed_mean_max_seconds)
    if stage in {"original", "final"}:
        low, high = 0, PLAN.healthy_mean_max_seconds
    if not all(low <= mean <= high for mean in means):
        raise ValueError("processor latency does not match frozen stage threshold")
    return means


def _logs(
    observed: SamplingObservation, capture: TraceCollection, offset: int, store: ArtifactStore
) -> tuple[TraceLog, TraceLog]:
    """No cap flags means a usable bounded sample; it never proves complete global ingestion."""
    if (
        len(capture.sources) != 2
        or capture.services_without_pods
        or capture.partial_candidates
        or capture.malformed_candidates
        or capture.capped_sources
        or capture.reserved_commands != 16
    ):
        raise ValueError("sampling trace source collection is unavailable or capped")
    logs = tuple(verify_trace_log(item, store) for item in capture.sources)
    by_service = {log.scope.service: log for log in logs}
    if set(by_service) != {"payments-api", "processor-adapter"}:
        raise ValueError("sampling trace services differ")
    identities = {
        "payments-api": observed.payments_identity,
        "processor-adapter": observed.processor_identity,
    }
    for log in logs:
        _log_scope(log, observed, offset)
        if log.identity != identities[log.scope.service] or any(
            (
                log.parsed.partial_candidates,
                log.parsed.malformed_candidates,
                log.parsed.limit_reached,
            )
        ):
            raise ValueError("sampling trace identity or parser changed")
    return by_service["payments-api"], by_service["processor-adapter"]


def _log_scope(log: TraceLog, observed: SamplingObservation, offset: int) -> None:
    """Retain actual delayed capture times and align them with this exact traffic interval."""
    scope, traffic = log.scope, observed.traffic
    if (
        scope.incident_id != observed.metrics[0].incident_id
        or scope.start != traffic.started_at - timedelta(seconds=1)
        or scope.end < traffic.completed_at + timedelta(seconds=offset)
        or (scope.end - scope.start).total_seconds() > PLAN.maximum_window_seconds
        or log.captured_start < scope.end
    ):
        raise ValueError("sampling trace window differs from frozen request interval")


def _one(spans: tuple[SourceSpan, ...], name: str, trace_id: str) -> SourceSpan:
    """Exactly one observed named span must serve each known request edge."""
    matches = [span for span in spans if span.trace_id == trace_id and span.name == name]
    if len(matches) != 1 or matches[0].status_code == "ERROR":
        raise ValueError("sampling span missing, duplicated or failed")
    return matches[0]


def _request_spans(
    stage: Stage, row: AttemptRecord, payment: TraceLog, processor: TraceLog
) -> None:
    """Bind inbound parent, caller edge and optional processor edge for the same request."""
    _, trace_id, parent, _ = row.planned.traceparent.split("-")
    pay_spans = tuple(record.span for record in payment.parsed.spans)
    processor_spans = tuple(record.span for record in processor.parsed.spans)
    server = _one(pay_spans, "sandbox.payments", "0x" + trace_id)
    client = _one(pay_spans, "sandbox.call.processor", "0x" + trace_id)
    if server.parent_id != "0x" + parent or client.parent_id != server.span_id:
        raise ValueError("sampling incoming or client parent differs")
    tolerance = timedelta(seconds=PLAN.host_clock_tolerance_seconds)
    if row.started_at is None or not (
        row.started_at - tolerance
        <= server.start_time
        <= server.end_time
        <= row.completed_at + tolerance
    ):
        raise ValueError("sampling server timing differs from actual request")
    if not server.start_time <= client.start_time <= client.end_time <= server.end_time:
        raise ValueError("sampling client timing lies outside its server")
    duration = (client.end_time - client.start_time).total_seconds()
    if stage in {"suppressed", "restored_sampling"} and not (
        PLAN.delayed_mean_min_seconds <= duration <= PLAN.caller_duration_max_seconds
    ):
        raise ValueError("sampling client does not observe processor delay")
    if stage == "suppressed":
        if any(span.trace_id == "0x" + trace_id for span in processor_spans):
            raise ValueError("processor span present while suppression expected")
    else:
        downstream = _one(processor_spans, "sandbox.processor", "0x" + trace_id)
        if downstream.parent_id != client.span_id:
            raise ValueError("processor span parent differs from caller")
        if not client.start_time <= downstream.start_time <= downstream.end_time <= client.end_time:
            raise ValueError("processor timing lies outside the caller span")


def verify_sampling_stage(
    stage: Stage, observed: SamplingObservation, store: ArtifactStore
) -> tuple[float, float]:
    """Combine runtime signals while keeping all source references separately inspectable."""
    if stage not in PLAN.stages:
        raise ValueError("unknown sampling stage")
    _traffic(observed.traffic)
    means = _metrics(stage, observed, store)
    for capture, offset in zip(observed.captures, PLAN.capture_offsets_seconds, strict=True):
        payments, processor = _logs(observed, capture, offset, store)
        for row in observed.traffic.attempts:
            _request_spans(stage, row, payments, processor)
    return means


def verify_sampling_counterfactual(
    suppressed: SamplingObservation, restored: SamplingObservation, store: ArtifactStore
) -> None:
    """Sampling must return while the same bounded delay remains observable in both slices."""
    first = verify_sampling_stage("suppressed", suppressed, store)
    second = verify_sampling_stage("restored_sampling", restored, store)
    _pair_scope(suppressed, restored)
    if any(
        abs(left - right) > PLAN.counterfactual_mean_tolerance_seconds
        for left, right in zip(first, second, strict=True)
    ):
        raise ValueError("sampling counterfactual changed the observed latency")


def _pair_scope(first: SamplingObservation, last: SamplingObservation) -> None:
    """A new startup-config process must explain later disjoint requests in the same incident."""
    if len({item.incident_id for item in (*first.metrics, *last.metrics)}) != 1:
        raise ValueError("sampling counterfactual crosses incidents")
    if (
        first.payments_identity != last.payments_identity
        or first.processor_identity.deployment_uid != last.processor_identity.deployment_uid
        or first.processor_identity.pod_uid == last.processor_identity.pod_uid
        or first.processor_identity.container_id == last.processor_identity.container_id
        or max(item.observed_at for capture in first.captures for item in capture.sources)
        >= last.traffic.started_at
    ):
        raise ValueError("sampling counterfactual lacks a later owned process transition")
    samples = {row.planned.sample.sample_id for row in first.traffic.attempts}
    traces = {row.planned.traceparent.split("-")[1] for row in first.traffic.attempts}
    if samples.intersection(row.planned.sample.sample_id for row in last.traffic.attempts) or (
        traces.intersection(row.planned.traceparent.split("-")[1] for row in last.traffic.attempts)
    ):
        raise ValueError("sampling counterfactual reused prior request identities")
