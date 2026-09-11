"""Reuse the real bounded traffic driver and reject inconsistent per-request outcomes."""

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from payops.scenarios.concurrency_traffic import concurrency_workload, validate_traffic
from payops.scenarios.contracts import JsonObject
from payops.scenarios.traffic import TrafficDriver, TrafficReceipt


def exercise(tmp_path: Path, parallel: bool) -> tuple[TrafficReceipt, JsonObject, int]:
    """The fixture transport observes actual driver concurrency without any network access."""
    active = peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        """Yield once so the driver's semaphore, rather than this fixture, determines overlap."""
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        sample = json.loads(request.content)
        try:
            await asyncio.sleep(0.001)
            if parallel and sample["sample_id"].endswith("-0"):
                raise httpx.ReadError("fixture container disconnected", request=request)
            return httpx.Response(
                200,
                json={
                    "sample_id": sample["sample_id"],
                    "role": "payments",
                    "status": "accepted",
                    "synthetic": True,
                },
            )
        finally:
            active -= 1

    driver = TrafficDriver(tmp_path / "config", tmp_path / "traffic", httpx.MockTransport(handler))
    receipt = asyncio.run(driver.run(concurrency_workload(parallel=parallel)))
    plan = json.loads((tmp_path / "traffic" / receipt.run_id / "plan.json").read_text())
    assert active == 0
    return receipt, plan, peak


@pytest.mark.parametrize("parallel", [False, True])
def test_fixed_driver_plan_and_actual_concurrency(tmp_path: Path, parallel: bool) -> None:
    """The TaskGroup driver completes all eight records and drains its tasks."""
    receipt, plan, peak = exercise(tmp_path, parallel)
    result = validate_traffic(receipt, plan, parallel=parallel)
    assert receipt.mode == "fixture_replay" and len(result.samples) == 8
    assert result.failures == int(parallel) and peak == (8 if parallel else 1)
    assert result.started <= result.completed


@pytest.mark.parametrize(
    "change",
    [
        "cancelled",
        "queued",
        "status",
        "role",
        "result",
        "negative",
        "index",
        "trace",
        "conflict",
        "declined",
    ],
)
def test_invalid_attempts_cannot_qualify_serial_control(tmp_path: Path, change: str) -> None:
    """Even a completed batch must preserve per-attempt identity, timing and success semantics."""
    receipt, plan, _ = exercise(tmp_path, False)
    first = receipt.attempts[0]
    if change in {"index", "trace"}:
        first = first.model_copy(
            update={
                "planned": first.planned.model_copy(
                    update={
                        "index" if change == "index" else "traceparent": 9
                        if change == "index"
                        else "invalid",
                    }
                )
            }
        )
    elif change in {"role", "result"}:
        assert first.result is not None
        first = first.model_copy(
            update={
                "result": first.result.model_copy(
                    update={
                        "role" if change == "role" else "sample_id": "risk"
                        if change == "role"
                        else "other",
                    }
                )
            }
        )
    else:
        updates = {
            "cancelled": {"outcome": "cancelled"},
            "queued": {"started_at": None},
            "status": {"http_status": 503},
            "negative": {"latency_seconds": -1},
            "conflict": {"idempotency_conflict": True},
            "declined": {"outcome": "declined"},
        }
        first = first.model_copy(update=updates[change])
    changed = receipt.model_copy(update={"attempts": (first, *receipt.attempts[1:])})
    plan["attempts"] = [row.planned.model_dump(mode="json") for row in changed.attempts]
    with pytest.raises(ValueError):
        validate_traffic(changed, plan, parallel=False)


def test_receipt_plan_disagreement_and_deadline_reject(tmp_path: Path) -> None:
    """Changing concurrency after collecting results cannot relabel a serial run as a treatment."""
    receipt, plan, _ = exercise(tmp_path, False)
    with pytest.raises(ValueError):
        validate_traffic(receipt, plan, parallel=True)
    with pytest.raises(ValueError):
        validate_traffic(
            receipt.model_copy(update={"status": "deadline_exceeded"}), plan, parallel=False
        )
    plan["probe"] = True
    with pytest.raises(ValueError):
        validate_traffic(receipt, plan, parallel=False)


def test_transport_failure_has_no_fabricated_response(tmp_path: Path) -> None:
    """A disconnection record must not simultaneously claim an HTTP success status."""
    receipt, plan, _ = exercise(tmp_path, True)
    first = receipt.attempts[0].model_copy(update={"http_status": 200})
    changed = receipt.model_copy(update={"attempts": (first, *receipt.attempts[1:])})
    with pytest.raises(ValueError):
        validate_traffic(changed, plan, parallel=True)


@pytest.mark.parametrize("status", [503, 409])
def test_http_availability_failures_are_separate_from_conflicts(
    tmp_path: Path, status: int
) -> None:
    """A server availability error can support pressure evidence; idempotency conflict cannot."""
    receipt, plan, _ = exercise(tmp_path, True)
    first = receipt.attempts[0].model_copy(
        update={"outcome": "http_error", "http_status": status, "error_type": None}
    )
    changed = receipt.model_copy(update={"attempts": (first, *receipt.attempts[1:])})
    if status == 503:
        assert validate_traffic(changed, plan, parallel=True).failures == 1
    else:
        with pytest.raises(ValueError):
            validate_traffic(changed, plan, parallel=True)


def test_repeated_trace_identity_rejects(tmp_path: Path) -> None:
    """Independent requests need distinct traces even when response bodies are valid."""
    receipt, plan, _ = exercise(tmp_path, False)
    second = receipt.attempts[1].model_copy(
        update={
            "planned": receipt.attempts[1].planned.model_copy(
                update={"traceparent": receipt.attempts[0].planned.traceparent}
            )
        }
    )
    changed = receipt.model_copy(
        update={"attempts": (receipt.attempts[0], second, *receipt.attempts[2:])}
    )
    plan["attempts"] = [row.planned.model_dump(mode="json") for row in changed.attempts]
    with pytest.raises(ValueError):
        validate_traffic(changed, plan, parallel=False)
