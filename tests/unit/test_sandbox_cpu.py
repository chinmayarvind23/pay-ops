"""Real bounded CPU work and cancelled callers must preserve service admission and replay."""

import asyncio
from threading import Event
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from payops.sandbox.cpu import CpuWork, hash_work
from payops.sandbox.models import SandboxConfig
from payops.sandbox.service import create_service


@pytest.mark.parametrize("value", [-1, 200001, True, "10"])
def test_cpu_workload_is_deployment_bounded(value: Any) -> None:
    """Neither direct construction nor configuration coercion expands the workload bound."""
    with pytest.raises(ValueError):
        CpuWork(value)
    with pytest.raises(ValueError):
        SandboxConfig(cpu_rounds=value)


def test_actual_hash_work_and_disabled_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exact real hash work is counted; coarse OS CPU clocks may round short work to zero."""
    import hashlib

    original = hashlib.sha256
    calls: list[int] = []

    def counted(data: bytes) -> Any:
        """Delegate to OpenSSL rather than replacing the workload with a fake duration."""
        calls.append(len(data))
        return original(data)

    monkeypatch.setattr("payops.sandbox.cpu.hashlib.sha256", counted)
    assert hash_work(256) >= 0 and calls == [4128] * 256
    worker = CpuWork(0)
    assert worker.pool is None and asyncio.run(worker.run()) == 0
    worker.close()
    with pytest.raises(HTTPException) as error:
        asyncio.run(worker.run())
    assert error.value.status_code == 503


def test_hash_work_checks_wall_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quota starvation cannot turn a fixed work count into unlimited wall time."""
    clock = iter((0.0, 5.0))
    monkeypatch.setattr("payops.sandbox.cpu.monotonic", lambda: next(clock))
    with pytest.raises(HTTPException) as error:
        hash_work(128)
    assert error.value.status_code == 504


def test_cancelled_caller_cannot_admit_second_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    """The actual background thread remains busy after its awaiting request is cancelled."""
    entered, release, finished = Event(), Event(), Event()
    calls: list[int] = []

    def held(rounds: int) -> float:
        """Hold the real worker independently of asyncio caller cancellation."""
        calls.append(rounds)
        entered.set()
        try:
            assert release.wait(3)
            return 0.01
        finally:
            finished.set()

    monkeypatch.setattr("payops.sandbox.cpu.hash_work", held)
    worker = CpuWork(256)

    async def exercise() -> None:
        """Cancellation and shutdown do not create a queued or late second operation."""
        pending = asyncio.create_task(worker.run())
        await asyncio.to_thread(entered.wait, 2)
        assert entered.is_set()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        with pytest.raises(HTTPException) as error:
            await worker.run()
        assert error.value.status_code == 503 and calls == [256]
        worker.close()
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)
        with pytest.raises(HTTPException):
            await worker.run()

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        worker.close()


def test_http_replay_skips_cpu_and_request_cannot_change_rounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CPU work happens after idempotency reservation and is never controlled by HTTP fields."""
    calls: list[int] = []
    original = hash_work

    def counted(rounds: int) -> float:
        """Retain real hashing while counting actual execution rather than response requests."""
        calls.append(rounds)
        return original(rounds)

    monkeypatch.setattr("payops.sandbox.cpu.hash_work", counted)
    with TestClient(create_service("risk", SandboxConfig(cpu_rounds=256))) as client:
        sample = {"sample_id": "synthetic-cpu"}
        assert client.post("/simulate", json=sample).status_code == 200
        assert client.post("/simulate", json=sample).status_code == 200
        assert calls == [256]
        assert client.post("/simulate", json={**sample, "cpu_rounds": 200000}).status_code == 422
        assert calls == [256] and client.get("/health").status_code == 200


def test_worker_failure_releases_slot_and_sample_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed workload remains retryable locally without caching a successful payment."""
    calls: list[int] = []

    def failed_once(rounds: int) -> float:
        """The first worker fails; the same sample then performs actual bounded work."""
        calls.append(rounds)
        if len(calls) == 1:
            raise HTTPException(504, "synthetic CPU work deadline exceeded")
        return hash_work(rounds)

    monkeypatch.setattr("payops.sandbox.cpu.hash_work", failed_once)
    with TestClient(create_service("risk", SandboxConfig(cpu_rounds=256))) as client:
        sample = {"sample_id": "synthetic-retry-cpu"}
        assert client.post("/simulate", json=sample).status_code == 504
        assert client.post("/simulate", json=sample).status_code == 200
        assert calls == [256, 256]


def test_active_cpu_work_is_not_an_unrelated_scenario_baseline() -> None:
    """Other experiments must not silently inherit an active CPU workload as their control."""
    from test_scenarios import document

    from payops.scenarios.contracts import object_items, object_value
    from payops.scenarios.recipes import container, validate_baseline

    deployment = document("payments-api")
    validate_baseline(deployment, "payments-api")
    item = container(object_value(deployment["spec"]))
    env = object_items(item["env"])
    for row in env:
        if row["name"] == "PAYOPS_SANDBOX_CONFIG":
            row["value"] = SandboxConfig(cpu_rounds=256).model_dump_json()
    item["env"] = list(env)
    with pytest.raises(ValueError, match="active CPU"):
        validate_baseline(deployment, "payments-api")
