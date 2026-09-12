"""An approved SQL pause must prevent new synthetic attempts across independent workers."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import httpx
import pytest

from payops.policy.contracts import PauseAction
from payops.policy.engine import action_digest
from payops.remediation.managed_traffic import ManagedTrafficDriver
from payops.remediation.traffic_control import TrafficControl, TrafficMode, TrafficService
from payops.scenarios.traffic import SliceCount, Workload


def proposal(uid: str, **changes: object) -> PauseAction:
    """The fixture proposal identifies the durable control, not a Kubernetes Deployment."""
    return PauseAction.model_validate(
        {
            "action_type": "pause_synthetic_traffic",
            "incident_id": "incident",
            "namespace": "payops-sandbox",
            "service": "webhook-sim",
            "resource_uid": uid,
            "expected_version": "0",
            "evidence_ids": ["evidence"],
            "mode": "fixture_replay",
            **changes,
        }
    )


def test_pause_persists_across_workers_and_stale_replay(tmp_path: Path) -> None:
    """Every admission after the pause commit is rejected, including by a reopened process."""
    control = TrafficControl(f"sqlite:///{tmp_path / 'traffic.db'}")
    try:
        uid = control.register("webhook-sim", "fixture_replay")
        assert control.admit(uid, "webhook-sim", "fixture_replay")
        action = proposal(uid)
        assert control.snapshot(action).kind == "Traffic"
        result = control.pause(action, action_digest(action))
        assert result.outcome == "SUCCEEDED" and result.resulting_version == "1"
        reopened = TrafficControl(control.engine)

        def admit_worker(index: int) -> bool:
            """Independent sessions observe the committed pause without cached state."""
            assert index >= 0
            return reopened.admit(uid, "webhook-sim", "fixture_replay")

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(admit_worker, range(12)))
        assert results == [False] * 12
        with pytest.raises(PermissionError, match="PRECONDITION"):
            reopened.pause(action, action_digest(action))
        assert reopened.snapshot(action).replicas == 0
        reopened.close()
    finally:
        control.close()


@pytest.mark.parametrize("change", ["missing", "namespace", "service", "mode", "version", "digest"])
def test_pause_scope_and_preconditions(tmp_path: Path, change: str) -> None:
    """Scope or version mismatch never pauses the legitimate source."""
    control = TrafficControl(f"sqlite:///{tmp_path / 'traffic.db'}")
    try:
        uid = control.register("webhook-sim", "fixture_replay")
        changes: dict[str, object] = {}
        if change != "digest":
            field = {"missing": "resource_uid", "version": "expected_version"}.get(change, change)
            changes[field] = "local_kind" if change == "mode" else "other"
        action = proposal(uid, **changes)
        with pytest.raises(PermissionError):
            control.pause(action, "wrong" if change == "digest" else action_digest(action))
        assert control.admit(uid, "webhook-sim", "fixture_replay")
    finally:
        control.close()


def test_managed_driver_pause_stops_queued_http_attempts(tmp_path: Path) -> None:
    """The first fixture HTTP response pauses the gate before queued attempts are admitted."""
    control = TrafficControl(f"sqlite:///{tmp_path / 'traffic.db'}")
    uid = control.register("webhook-sim", "fixture_replay")
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        """One admitted request completes while pause atomically closes subsequent admissions."""
        nonlocal calls
        calls += 1
        action = proposal(uid)
        control.pause(action, action_digest(action))
        return httpx.Response(
            200,
            json={
                "sample_id": json.loads(request.content)["sample_id"],
                "role": "webhook",
                "status": "accepted",
                "synthetic": True,
            },
        )

    try:
        driver = ManagedTrafficDriver(
            control,
            uid,
            "webhook-sim",
            "fixture_replay",
            tmp_path,
            tmp_path / "runs",
            httpx.MockTransport(handler),
        )
        workload = Workload(
            role="webhook",
            concurrency=1,
            distribution=(
                SliceCount(processor="A", region="us", payment_method="credit", count=6),
            ),
        )
        receipt = asyncio.run(driver.run(workload))
        assert calls == 1 and receipt.status == "completed"
        assert [item.outcome for item in receipt.attempts] == ["accepted"] + ["paused"] * 5
        assert all(
            item.started_at is None and item.latency_seconds is None and item.http_status is None
            for item in receipt.attempts[1:]
        )
        assert driver.plan_metadata()["traffic_control_uid"] == uid
    finally:
        control.close()


def test_control_rejects_unsupported_scope_and_driver_mode(tmp_path: Path) -> None:
    """Invalid host configuration and wrong-mode admissions cannot reach the HTTP driver."""
    control = TrafficControl(f"sqlite:///{tmp_path / 'traffic.db'}")
    try:
        for service, mode in (("ledger-sim", "local_kind"), ("webhook-sim", "cloud_gke")):
            with pytest.raises(PermissionError):
                control.register(cast(TrafficService, service), cast(TrafficMode, mode))
            with pytest.raises(PermissionError):
                control.admit("uid", cast(TrafficService, service), cast(TrafficMode, mode))
        with pytest.raises(PermissionError, match="DRIVER_SCOPE"):
            ManagedTrafficDriver(
                control, "uid", "webhook-sim", "fixture_replay", tmp_path, tmp_path
            )
    finally:
        control.close()


def test_wrong_role_and_database_failure_never_dispatch(tmp_path: Path) -> None:
    """A gate exception fails the batch closed, retaining unstarted attempts in its receipt."""
    control = TrafficControl(f"sqlite:///{tmp_path / 'traffic.db'}")
    uid = control.register("webhook-sim", "fixture_replay")

    def forbidden(request: httpx.Request) -> httpx.Response:
        """No synthetic HTTP request is allowed through a wrong-role control."""
        raise AssertionError(request.url)

    try:
        driver = ManagedTrafficDriver(
            control,
            uid,
            "webhook-sim",
            "fixture_replay",
            tmp_path,
            tmp_path / "runs",
            httpx.MockTransport(forbidden),
        )
        workload = Workload(
            role="payments",
            concurrency=1,
            distribution=(
                SliceCount(processor="A", region="us", payment_method="credit", count=1),
            ),
        )
        with pytest.raises(ExceptionGroup):
            asyncio.run(driver.run(workload))
        receipt = json.loads(next((tmp_path / "runs").glob("*/receipt.json")).read_text())
        assert receipt["status"] == "failed"
        assert receipt["attempts"][0]["started_at"] is None
    finally:
        control.close()
