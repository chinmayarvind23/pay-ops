"""Bind the fixed eight-request contrast to persisted traffic plans and actual outcomes."""

import math
import re
from dataclasses import dataclass
from datetime import datetime

from payops.scenarios.contracts import JsonObject
from payops.scenarios.traffic import AttemptRecord, SliceCount, TrafficReceipt, Workload


@dataclass(frozen=True)
class TrafficWindow:
    """Only observed started requests establish the source window and sample denominator."""

    samples: tuple[str, ...]
    started: datetime
    completed: datetime
    failures: int


def concurrency_workload(*, parallel: bool) -> Workload:
    """Keep count, slice, timeout and deadline fixed; change only concurrency."""
    return Workload(
        role="payments",
        distribution=(SliceCount(processor="A", region="us", payment_method="credit", count=8),),
        concurrency=8 if parallel else 1,
        request_timeout_seconds=5.0,
        deadline_seconds=45.0,
        seed=0,
    )


def payment_outcome(attempt: AttemptRecord) -> bool:
    """A success needs its matching synthetic response; transport failure has no invented status."""
    if attempt.outcome == "accepted":
        result = attempt.result
        if (
            attempt.http_status != 200
            or result is None
            or result.role != "payments"
            or result.status != "accepted"
            or result.sample_id != attempt.planned.sample.sample_id
            or attempt.error_type is not None
        ):
            raise ValueError("invalid accepted payment evidence")
        return True
    if attempt.outcome in {"request_error", "timed_out"}:
        if attempt.http_status is not None or attempt.result is not None or not attempt.error_type:
            raise ValueError("transport error contains fabricated response evidence")
        return False
    if attempt.outcome == "http_error":
        if attempt.http_status not in {500, 502, 503, 504} or attempt.result is not None:
            raise ValueError("HTTP failure is not a payment availability failure")
        return False
    raise ValueError("cancelled, declined or malformed outcomes cannot qualify memory pressure")


def started_request_time(attempt: AttemptRecord, receipt: TrafficReceipt) -> datetime:
    """Reject unstarted requests, naive clocks and nonfinite durations."""
    started, ended, duration = attempt.started_at, attempt.completed_at, attempt.latency_seconds
    if (
        started is None
        or started.tzinfo is None
        or ended.tzinfo is None
        or not receipt.started_at <= started <= ended <= receipt.completed_at
        or duration is None
        or not math.isfinite(duration)
        or not 0 <= duration <= 45
    ):
        raise ValueError("invalid started-request timing")
    return started


def validate_traffic(receipt: TrafficReceipt, plan: JsonObject, *, parallel: bool) -> TrafficWindow:
    """Verify the eight-request plan; HTTP failures still need independent OOM proof."""
    receipt = TrafficReceipt.model_validate_json(receipt.model_dump_json())
    if (
        receipt.role != "payments"
        or receipt.status != "completed"
        or receipt.failure is not None
        or len(receipt.attempts) != 8
        or receipt.probe_verified is not None
        or receipt.started_at.tzinfo is None
        or receipt.completed_at.tzinfo is None
        or not 0 <= (receipt.completed_at - receipt.started_at).total_seconds() <= 45
    ):
        raise ValueError("incomplete or out-of-bounds concurrency workload")
    if (
        plan.get("run_id") != receipt.run_id
        or plan.get("probe") is not False
        or plan.get("workload") != concurrency_workload(parallel=parallel).model_dump(mode="json")
        or plan.get("attempts") != [row.planned.model_dump(mode="json") for row in receipt.attempts]
    ):
        raise ValueError("traffic receipt disagrees with the frozen workload plan")
    samples: list[str] = []
    traces: set[str] = set()
    starts: list[datetime] = []
    failures = 0
    for index, attempt in enumerate(receipt.attempts):
        planned = attempt.planned
        if (
            planned.index != index
            or planned.step != "sample"
            or attempt.idempotency_conflict
            or (planned.sample.processor, planned.sample.region, planned.sample.payment_method)
            != ("A", "us", "credit")
            or re.fullmatch(
                r"00-(?!0{32}-)[0-9a-f]{32}-(?!0{16}-)[0-9a-f]{16}-01", planned.traceparent
            )
            is None
        ):
            raise ValueError("invalid planned concurrency attempt")
        samples.append(planned.sample.sample_id)
        traces.add(planned.traceparent.split("-")[1])
        starts.append(started_request_time(attempt, receipt))
        failures += not payment_outcome(attempt)
    if len(set(samples)) != 8 or len(traces) != 8 or bool(failures) is not parallel:
        raise ValueError("sample uniqueness or control/fault outcomes do not match the experiment")
    return TrafficWindow(tuple(samples), min(starts), receipt.completed_at, failures)
