"""Verify the finite Service-routed load independently of Kubernetes Job success."""

import json
import re
from datetime import datetime, timedelta

from payops.evidence.artifacts import JSON_OBJECT
from payops.scenarios.concurrency_traffic import (
    TrafficWindow,
    payment_outcome,
    started_request_time,
)
from payops.scenarios.contracts import JsonObject, object_value
from payops.scenarios.hpa_job_identity import LoadProcess
from payops.scenarios.hpa_load import load_workload
from payops.scenarios.traffic import TrafficReceipt


def _envelope(raw: bytes) -> tuple[TrafficReceipt, JsonObject]:
    """One bounded structured record preserves the original plan and receipt without log salvage."""
    if not raw or len(raw) >= 262144:
        raise ValueError("load receipt is empty or capped")
    document = JSON_OBJECT.validate_json(raw)
    if set(document) != {"event", "plan", "receipt"} or document["event"] != "synthetic.hpa_load":
        raise ValueError("unexpected load evidence envelope")
    receipt = TrafficReceipt.model_validate_json(json.dumps(document["receipt"]))
    plan = object_value(document["plan"])
    if (
        receipt.mode != "local_kind"
        or receipt.role != "payments"
        or receipt.status != "completed"
        or receipt.failure is not None
        or receipt.probe_verified is not None
        or len(receipt.attempts) != 256
        or plan.get("run_id") != receipt.run_id
        or re.fullmatch(r"[0-9a-f]{32}", receipt.run_id) is None
        or plan.get("launch_interval_seconds") != 0.5
        or plan.get("probe") is not False
        or plan.get("workload") != load_workload().model_dump(mode="json")
        or plan.get("attempts") != [row.planned.model_dump(mode="json") for row in receipt.attempts]
    ):
        raise ValueError("load evidence differs from the frozen local workload")
    return receipt, plan


def validate_load_receipt(raw: bytes, process: LoadProcess, finished: datetime) -> TrafficWindow:
    """Bind every completed attempt to the observed load-process lifetime and unique sample IDs."""
    receipt, _ = _envelope(raw)
    # Kubelet termination timestamps have whole-second precision in this runtime.
    end_bound = finished + timedelta(seconds=1) if finished.microsecond == 0 else finished
    clocks = (process.started, receipt.started_at, receipt.completed_at, finished)
    if (
        any(clock.tzinfo is None for clock in clocks)
        or not process.started <= receipt.started_at <= receipt.completed_at < end_bound
        or not 0 <= (receipt.completed_at - receipt.started_at).total_seconds() <= 180
    ):
        raise ValueError("load receipt falls outside the observed process lifetime")
    starts: list[datetime] = []
    samples: list[str] = []
    traces: set[str] = set()
    failures = 0
    for index, attempt in enumerate(receipt.attempts):
        planned = attempt.planned
        if (
            planned.index != index
            or planned.step != "sample"
            or planned.sample.sample_id != f"synthetic-{receipt.run_id}-{index}"
            or (planned.sample.processor, planned.sample.region, planned.sample.payment_method)
            != ("A", "us", "credit")
            or attempt.idempotency_conflict
            or re.fullmatch(
                r"00-(?!0{32}-)[0-9a-f]{32}-(?!0{16}-)[0-9a-f]{16}-01", planned.traceparent
            )
            is None
        ):
            raise ValueError("load attempt differs from the frozen sample plan")
        starts.append(started_request_time(attempt, receipt))
        samples.append(planned.sample.sample_id)
        traces.add(planned.traceparent.split("-")[1])
        failures += not payment_outcome(attempt)
    if len(traces) != 256:
        raise ValueError("load requests reuse trace identities")
    return TrafficWindow(tuple(samples), min(starts), receipt.completed_at, failures)
