"""Join real payment outcomes to same-request dependency logs and telemetry controls."""

import json
from datetime import datetime, timedelta

from payops.scenarios.concurrency_traffic import payment_outcome, started_request_time
from payops.scenarios.contracts import JsonObject, object_value
from payops.scenarios.traffic import SliceCount, TrafficReceipt, Workload


def dependency_workload() -> Workload:
    """Three serial requests expose repeatable failure without amplifying dependency pressure."""
    return Workload(
        distribution=(SliceCount(processor="A", region="us", payment_method="credit", count=3),),
        concurrency=1,
        request_timeout_seconds=6.0,
        deadline_seconds=30.0,
    )


def dependency_events(raw: str) -> list[JsonObject]:
    """Ignore unrelated log formats but reject malformed JSON for dependency events."""
    result: list[JsonObject] = []
    for line in raw.splitlines():
        _, _, payload = line.partition(" ")
        if '"synthetic.dependency"' in payload or '"synthetic.archived_error"' in payload:
            result.append(object_value(json.loads(payload)))
    return result


def validate_dependency_traffic(
    receipt: TrafficReceipt, plan: JsonObject, raw: str, kind: str, fault: bool
) -> tuple[str, ...]:
    """No timeout or generic error qualifies; every fault needs HTTP 503 and its precise cause."""
    if (
        receipt.status != "completed"
        or receipt.failure
        or receipt.role != "payments"
        or len(receipt.attempts) != 3
        or receipt.probe_verified is not None
        or not 0 <= (receipt.completed_at - receipt.started_at).total_seconds() <= 30
        or plan.get("run_id") != receipt.run_id
        or plan.get("probe") is not False
        or plan.get("workload") != dependency_workload().model_dump(mode="json")
        or plan.get("attempts") != [a.planned.model_dump(mode="json") for a in receipt.attempts]
    ):
        raise ValueError("dependency traffic differs from frozen plan")
    events = dependency_events(raw)
    samples: list[str] = []
    for index, attempt in enumerate(receipt.attempts):
        started = started_request_time(attempt, receipt)
        sample = attempt.planned.sample.sample_id
        if (
            sample != f"synthetic-{receipt.run_id}-{index}"
            or attempt.planned.index != index
            or attempt.idempotency_conflict
            or attempt.planned.step != "sample"
        ):
            raise ValueError("dependency sample identity mismatch")
        success = payment_outcome(attempt)
        if success == fault or (fault and attempt.http_status != 503):
            raise ValueError("dependency HTTP outcome mismatch")
        matching = [
            e
            for e in events
            if e.get("event") == "synthetic.dependency" and e.get("sample_id") == sample
        ]
        if len(matching) != 1:
            raise ValueError("missing or duplicate dependency event")
        validate_dependency_event(
            matching[0],
            kind,
            fault,
            started,
            attempt.completed_at,
            attempt.planned.traceparent.split("-")[1],
        )
        samples.append(sample)
    return tuple(samples)


def validate_dependency_event(
    event: JsonObject, kind: str, fault: bool, started: datetime, ended: datetime, trace: str
) -> None:
    """Original timestamps and trace IDs bind logs to the observed HTTP request window."""
    expected = "connection_exhausted" if kind == "postgres" else "unavailable"
    first = datetime.fromisoformat(str(event["started_at"]))
    last = datetime.fromisoformat(str(event["completed_at"]))
    if (
        event.get("dependency") != kind
        or event.get("outcome") != (expected if fault else "ok")
        or event.get("trace_id") != trace
        or first.tzinfo is None
        or last.tzinfo is None
        or not started - timedelta(seconds=1) <= first <= last <= ended + timedelta(seconds=1)
        or event.get("sqlstate") != ("53300" if fault and kind == "postgres" else None)
    ):
        raise ValueError("dependency event does not prove the observed request outcome")


def validate_archived_error(raw: str, started: datetime) -> None:
    """A labelled synthetic old PostgreSQL error must be distinct from current Redis failures."""
    events = [e for e in dependency_events(raw) if e.get("event") == "synthetic.archived_error"]
    if len(events) != 1:
        raise ValueError("expected one archived distractor")
    event = events[0]
    original = datetime.fromisoformat(str(event["original_event_at"]))
    if (
        event.get("archived") is not True
        or event.get("synthetic") is not True
        or event.get("dependency") != "postgres"
        or original.tzinfo is None
        or not timedelta(hours=23) < started - original < timedelta(hours=25)
    ):
        raise ValueError("archived distractor provenance mismatch")


def validate_delayed_metrics(before: JsonObject, during: JsonObject) -> None:
    """Identical old exposition must coexist with newer observed failures within its TTL."""
    original = datetime.fromisoformat(str(before["snapshot"]))
    acquired = datetime.fromisoformat(str(during["captured_at"]))
    if (
        not before.get("text")
        or before["text"] != during["text"]
        or before["snapshot"] != during["snapshot"]
        or original.tzinfo is None
        or not 0 < (acquired - original).total_seconds() < 60
    ):
        raise ValueError("metrics were not retained inside the documented stale window")
