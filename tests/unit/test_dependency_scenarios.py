"""Dependency evidence must reject wrong causes, request joins and unsafe cleanup."""

# pyright: reportPrivateUsage=false
import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from test_concurrency_harness import setup
from test_concurrency_specs import baseline

from payops.scenarios.concurrency_harness import ConcurrencyHarness
from payops.scenarios.contracts import (
    CaseId,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
)
from payops.scenarios.dependency_evidence import (
    dependency_workload,
    validate_archived_error,
    validate_delayed_metrics,
    validate_dependency_traffic,
)
from payops.scenarios.dependency_gateway import DependencyGateway
from payops.scenarios.dependency_harness import DependencyHarness
from payops.scenarios.dependency_specs import dependency_spec
from payops.scenarios.recipes import container, fault_spec
from payops.scenarios.runner import CleanupUnverified
from payops.scenarios.traffic import TrafficDriver, TrafficReceipt


def exercise(tmp_path: Path, kind: str, fault: bool) -> tuple[TrafficReceipt, JsonObject, str]:
    """Run the real driver through an HTTP transport while retaining generated request IDs."""
    events: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        """Return controlled dependency outcomes with matching event timestamps and traces."""
        sample = json.loads(request.content)["sample_id"]
        stamp = datetime.now(UTC).isoformat()
        events.append(
            stamp
            + " "
            + json.dumps(
                {
                    "event": "synthetic.dependency",
                    "sample_id": sample,
                    "dependency": kind,
                    "started_at": stamp,
                    "completed_at": stamp,
                    "outcome": ("connection_exhausted" if kind == "postgres" else "unavailable")
                    if fault
                    else "ok",
                    "sqlstate": "53300" if fault and kind == "postgres" else None,
                    "trace_id": request.headers["traceparent"].split("-")[1],
                }
            )
        )
        return httpx.Response(
            503 if fault else 200,
            json={
                "sample_id": sample,
                "role": "payments",
                "status": "accepted",
                "synthetic": True,
            },
        )

    output = tmp_path / "traffic"
    receipt = asyncio.run(
        TrafficDriver(tmp_path / "config", output, httpx.MockTransport(handler)).run(
            dependency_workload()
        )
    )
    plan = json.loads((output / receipt.run_id / "plan.json").read_text())
    return receipt, plan, "\n".join(events)


@pytest.mark.parametrize("kind", ["postgres", "redis"])
@pytest.mark.parametrize("fault", [False, True])
def test_real_driver_outcomes_and_request_joins(tmp_path: Path, kind: str, fault: bool) -> None:
    """Both dependencies need all three matching successes or independently explained failures."""
    receipt, plan, raw = exercise(tmp_path, kind, fault)
    assert len(validate_dependency_traffic(receipt, plan, raw, kind, fault)) == 3


@pytest.mark.parametrize(
    "corruption", ["duplicate", "missing", "cause", "trace", "time", "http", "plan"]
)
def test_corrupted_fault_evidence_rejected(tmp_path: Path, corruption: str) -> None:
    """A generic HTTP failure or misleading log cannot qualify PostgreSQL exhaustion."""
    receipt, plan, raw = exercise(tmp_path, "postgres", True)
    if corruption == "duplicate":
        raw += "\n" + raw.splitlines()[0]
    elif corruption == "missing":
        raw = "\n".join(raw.splitlines()[1:])
    elif corruption == "cause":
        raw = raw.replace("connection_exhausted", "unavailable")
    elif corruption == "trace":
        raw = raw.replace(receipt.attempts[0].planned.traceparent.split("-")[1], "0" * 32)
    elif corruption == "time":
        raw = raw.replace('"started_at": "2026', '"started_at": "2025')
    elif corruption == "http":
        receipt = receipt.model_copy(
            update={
                "attempts": (
                    receipt.attempts[0].model_copy(update={"http_status": 500}),
                    *receipt.attempts[1:],
                )
            }
        )
    else:
        plan["run_id"] = "foreign"
    with pytest.raises(ValueError):
        validate_dependency_traffic(receipt, plan, raw, "postgres", True)


@pytest.mark.parametrize("case", ["DEP-03", "DEP-04", "TELEM-02", "TELEM-04"])
def test_scoped_projection_and_generic_runner_rejection(case: CaseId) -> None:
    """Projection is read-only and leaves resource limits and the restoration source unchanged."""
    original = baseline()
    saved = deepcopy(original)
    spec = dependency_spec(original, case)
    item = container(spec)
    assert original == saved
    assert item["resources"] == container(object_value(original["spec"]))["resources"]
    assert object_items(item["volumeMounts"])[0]["readOnly"] is True
    with pytest.raises(ValueError, match="specialized"):
        fault_spec(case, object_value(original["spec"]))


def test_stale_and_archived_evidence_keep_original_timestamps() -> None:
    """An old sample is accepted only inside the actual retention window and labelled archive."""
    now = datetime.now(UTC)
    before: JsonObject = {"text": "original", "snapshot": now.isoformat()}
    during: JsonObject = {**before, "captured_at": (now + timedelta(seconds=10)).isoformat()}
    validate_delayed_metrics(before, during)
    with pytest.raises(ValueError):
        validate_delayed_metrics(before, {**during, "text": "fresh"})
    archive = (
        now.isoformat()
        + " "
        + json.dumps(
            {
                "event": "synthetic.archived_error",
                "archived": True,
                "synthetic": True,
                "dependency": "postgres",
                "original_event_at": (now - timedelta(days=1)).isoformat(),
            }
        )
    )
    validate_archived_error(archive, now)
    with pytest.raises(ValueError):
        validate_archived_error(archive.replace('"archived": true', '"archived": false'), now)


def test_data_cleanup_failure_still_attempts_payments_restoration(tmp_path: Path) -> None:
    """A failed Redis restoration must not skip independent payments cleanup."""
    _, context = setup(tmp_path)
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        harness = DependencyHarness(tmp_path / "config", tmp_path / "dependency", tmp_path)
    receipt = ScenarioReceipt(
        run_id="fixture", case_id="DEP-04", mode="fixture_replay", implementation_variant="fixture"
    )
    with (
        patch.object(harness, "_restore_data", side_effect=RuntimeError("data failure")),
        patch.object(ConcurrencyHarness, "_restore_original") as restore,
    ):
        with pytest.raises(CleanupUnverified, match="data failure"):
            harness._restore_original(context, tmp_path, receipt)
    restore.assert_called_once()


def test_redis_patch_requires_fixed_resource_and_atomic_tests(tmp_path: Path) -> None:
    """The sole data mutation cannot broaden target or replica bounds."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        gateway = DependencyGateway(config)
    document: JsonObject = {
        "metadata": {
            "name": "redis",
            "namespace": "payops-data",
            "uid": "uid",
            "resourceVersion": "1",
        },
        "spec": {"replicas": 1},
    }
    with patch.object(gateway, "verify_scope"), patch.object(gateway, "data_read") as write:
        gateway.redis_replicas(document, 0)
        patch_body = json.loads(write.call_args.args[0][-1])
        assert [row["op"] for row in patch_body] == ["test", "test", "test", "replace"]
        assert patch_body[-1]["value"] == {"replicas": 0}
        with pytest.raises(ValueError):
            gateway.redis_replicas(document, 2)
