"""Individually valid observations still need a valid causal counterfactual relationship."""

from datetime import timedelta
from pathlib import Path

import pytest
from test_sampling_observation import collection, fixture, log, metrics

from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import PodIdentity
from payops.sandbox.models import SimulationResult
from payops.scenarios.sampling_observation import (
    SamplingObservation,
    verify_sampling_counterfactual,
    verify_sampling_stage,
)
from payops.scenarios.traffic import AttemptRecord, TrafficReceipt
from payops.tools.traces import TraceCollection


def rebuild(
    store: ArtifactStore,
    previous: SamplingObservation,
    receipt: TrafficReceipt,
    incident: str,
    processor_identity: PodIdentity,
    payments_identity: PodIdentity,
) -> SamplingObservation:
    """Republish raw sources consistently so pair tests cannot pass via broken artifact hashes."""
    captures: list[TraceCollection] = []
    for offset in (12, 17):
        payment = log(receipt, "payments-api", False, offset)
        processor = log(receipt, "processor-adapter", False, offset)
        pair = tuple(
            item.model_copy(
                update={
                    "scope": item.scope.model_copy(update={"incident_id": incident}),
                    "identity": selected,
                }
            )
            for item, selected in ((payment, payments_identity), (processor, processor_identity))
        )
        captures.append(collection(store, (pair[0], pair[1])))
    return previous.model_copy(
        update={
            "traffic": receipt,
            "metrics": (
                metrics(store, receipt, "payments-api", 0.6, incident=incident),
                metrics(store, receipt, "processor-adapter", 0.6, incident=incident),
            ),
            "captures": tuple(captures),
            "processor_identity": processor_identity,
            "payments_identity": payments_identity,
        }
    )


def request_changes(first: TrafficReceipt, last: TrafficReceipt, variant: str) -> TrafficReceipt:
    """Change timing or global identity reuse while preserving each request's local validity."""
    if variant in {"overlap", "reversed", "capture-overlap"}:
        seconds = {"overlap": 1, "reversed": -30, "capture-overlap": 10}[variant]
        start = first.started_at + timedelta(seconds=seconds)
        delta = start - last.started_at
        return last.model_copy(
            update={
                "started_at": start,
                "completed_at": last.completed_at + delta,
                "attempts": tuple(
                    row.model_copy(
                        update={
                            "started_at": row.started_at + delta,
                            "completed_at": row.completed_at + delta,
                        }
                    )
                    for row in last.attempts
                    if row.started_at is not None
                ),
            }
        )
    attempts: list[AttemptRecord] = []
    for old, row in zip(first.attempts, last.attempts, strict=True):
        if variant == "sample-reuse":
            sample = row.planned.sample.model_copy(
                update={"sample_id": old.planned.sample.sample_id}
            )
            planned = row.planned.model_copy(update={"sample": sample})
            result = SimulationResult(
                sample_id=sample.sample_id, role="payments", status="accepted"
            )
            row = row.model_copy(update={"planned": planned, "result": result})
        elif variant == "trace-reuse":
            planned = row.planned.model_copy(update={"traceparent": old.planned.traceparent})
            row = row.model_copy(update={"planned": planned})
        attempts.append(row)
    return last.model_copy(update={"attempts": tuple(attempts)})


@pytest.mark.parametrize(
    "variant",
    [
        "incident",
        "deployment",
        "payments",
        "pod",
        "container",
        "overlap",
        "reversed",
        "capture-overlap",
        "sample-reuse",
        "trace-reuse",
    ],
)
def test_individually_valid_stages_cannot_fake_pair(tmp_path: Path, variant: str) -> None:
    """Require one incident, ordered captures, fresh processor and globally new requests."""
    store = ArtifactStore(tmp_path)
    first, last = fixture(store), fixture(store, "restored_sampling")
    processor, payments = last.processor_identity, last.payments_identity
    if variant == "deployment":
        processor = processor.model_copy(update={"deployment_uid": "foreign-deployment"})
    elif variant == "payments":
        payments = payments.model_copy(update={"pod_uid": "replacement-payments"})
    elif variant == "pod":
        processor = processor.model_copy(update={"pod_uid": first.processor_identity.pod_uid})
    elif variant == "container":
        processor = processor.model_copy(
            update={"container_id": first.processor_identity.container_id}
        )
    receipt = request_changes(first.traffic, last.traffic, variant)
    last = rebuild(
        store,
        last,
        receipt,
        "another" if variant == "incident" else "sampling",
        processor,
        payments,
    )
    assert verify_sampling_stage("suppressed", first, store) == (0.6, 0.6)
    assert verify_sampling_stage("restored_sampling", last, store) == (0.6, 0.6)
    with pytest.raises(ValueError, match="counterfactual"):
        verify_sampling_counterfactual(first, last, store)
