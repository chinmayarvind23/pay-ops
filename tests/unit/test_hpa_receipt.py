"""Completed load must preserve real driver plans and observed request outcomes."""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.hpa_job_identity import LoadProcess
from payops.scenarios.hpa_load import load_workload
from payops.scenarios.hpa_receipt import validate_load_receipt
from payops.scenarios.traffic import TrafficDriver


@pytest.fixture
def evidence(tmp_path: Path) -> tuple[JsonObject, LoadProcess, datetime]:
    """Create a full real-driver fixture; local mode is simulated only inside this unit test."""

    def handler(request: httpx.Request) -> httpx.Response:
        """One availability failure checks that batch success does not erase failed payments."""
        sample = json.loads(request.content)
        if sample["sample_id"].endswith("-0"):
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "sample_id": sample["sample_id"],
                "role": "payments",
                "status": "accepted",
                "synthetic": True,
            },
        )

    driver = TrafficDriver(tmp_path / "unused", tmp_path, httpx.MockTransport(handler))
    receipt = asyncio.run(driver.run(load_workload()))
    plan = json.loads((tmp_path / receipt.run_id / "plan.json").read_text())
    simulated = receipt.model_copy(update={"mode": "local_kind"})
    document: JsonObject = {
        "event": "synthetic.hpa_load",
        "plan": plan,
        "receipt": simulated.model_dump(mode="json"),
    }
    process = LoadProcess(
        "job-pod", "pod-uid", "container", receipt.started_at - timedelta(seconds=1)
    )
    return document, process, receipt.completed_at + timedelta(seconds=1)


def test_complete_batch_retains_failures(
    evidence: tuple[JsonObject, LoadProcess, datetime],
) -> None:
    """All 256 started requests count; the observed 503 remains a failure."""
    document, process, finished = evidence
    result = validate_load_receipt(json.dumps(document).encode(), process, finished)
    assert len(result.samples) == 256 and result.failures == 1


@pytest.mark.parametrize(
    "fault", ["mode", "workload", "trace", "sample", "queued", "result", "envelope"]
)
def test_changed_receipt_rejects(
    evidence: tuple[JsonObject, LoadProcess, datetime], fault: str
) -> None:
    """Matching a forged plan to a changed receipt cannot bypass per-attempt semantics."""
    document, process, finished = evidence
    receipt = object_value(document["receipt"])
    plan = object_value(document["plan"])
    attempts = object_items(receipt["attempts"])
    if fault == "mode":
        receipt["mode"] = "fixture_replay"
    elif fault == "workload":
        object_value(plan["workload"])["concurrency"] = 8
    elif fault == "trace":
        object_value(attempts[1]["planned"])["traceparent"] = object_value(attempts[0]["planned"])[
            "traceparent"
        ]
    elif fault == "sample":
        object_value(object_value(attempts[1]["planned"])["sample"])["sample_id"] = "other"
    elif fault == "queued":
        attempts[1]["started_at"] = None
    elif fault == "result":
        object_value(attempts[1]["result"])["sample_id"] = "other"
    else:
        document["event"] = "other"
    plan["attempts"] = [row["planned"] for row in attempts]
    with pytest.raises(ValueError):
        validate_load_receipt(json.dumps(document).encode(), process, finished)


@pytest.mark.parametrize("fault", ["empty", "capped", "early", "naive"])
def test_bounds_reject(evidence: tuple[JsonObject, LoadProcess, datetime], fault: str) -> None:
    """Truncation and impossible process clocks cannot support qualification."""
    document, process, finished = evidence
    raw = json.dumps(document).encode()
    if fault == "empty":
        raw = b""
    elif fault == "capped":
        raw = b" " * 262144
    elif fault == "early":
        finished = process.started
    else:
        finished = finished.replace(tzinfo=None)
    with pytest.raises(ValueError):
        validate_load_receipt(raw, process, finished)
