"""Kernel capture retains bounded raw records and does not fabricate successful work."""

import asyncio
import io
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from payops.contracts import utc_now
from payops.sandbox.cpu import CpuWork
from payops.sandbox.cpu_observation import KernelSnapshot, observed_work, snapshot
from payops.sandbox.models import SandboxConfig
from payops.sandbox.service import create_service

RAW = (
    b"usage_usec 1000\nuser_usec 900\nsystem_usec 100\n"
    b"nr_periods 10\nnr_throttled 2\nthrottled_usec 500\n"
)


def kernel(monkeypatch: pytest.MonkeyPatch, stat: bytes = RAW) -> list[str]:
    """Intercept fixed file opens while exercising real bounded reads and parsing."""
    paths: list[str] = []

    def opened(path: Path, mode: str) -> io.BytesIO:
        """No alternate file or write mode may be requested by the capture implementation."""
        assert mode == "rb" and path.parent == Path("/sys/fs/cgroup")
        assert path.name in {"cpu.stat", "cpu.max"}
        paths.append(path.name)
        return io.BytesIO(stat if path.name == "cpu.stat" else b"50000 100000\n")

    monkeypatch.setattr(Path, "open", opened)
    return paths


def test_http_capture_correlates_once_and_replay_skips_work(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The admitted HTTP request emits one source pair; completed replay emits none."""
    with TestClient(
        create_service("risk", SandboxConfig(cpu_rounds=256, cpu_capture=True))
    ) as client:
        paths = kernel(monkeypatch)
        sample = {"sample_id": "synthetic-captured"}
        assert client.post("/simulate", json=sample).status_code == 200
        assert client.post("/simulate", json=sample).status_code == 200
        assert client.post("/simulate", json={**sample, "cpu_capture": True}).status_code == 422
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(records) == 1
    record = records[0]
    assert record["event"] == "synthetic.cpu" and record["sample_id"] == sample["sample_id"]
    assert record["rounds"] == 256 and record["thread_cpu_seconds"] >= 0
    assert record["wall_seconds"] >= 0
    assert record["before"]["cpu_stat"] == record["after"]["cpu_stat"] == RAW.decode()
    assert paths == ["cpu.stat", "cpu.max"] * 2


@pytest.mark.parametrize("raw", [b"x" * 4097, b"\xff", b"missing 0\n"])
def test_bad_kernel_files_fail(monkeypatch: pytest.MonkeyPatch, raw: bytes) -> None:
    """Oversized, non-ASCII and incomplete kernel sources never become valid measurements."""
    kernel(monkeypatch, raw)
    with pytest.raises(ValueError):
        snapshot()


@pytest.mark.parametrize("seconds", [-1, 2.1])
def test_acquisition_clock_bounds(seconds: float) -> None:
    """Clock reversal or slow reads invalidate the source before later pairing."""
    now = utc_now()
    with pytest.raises(ValueError):
        KernelSnapshot(
            started_at=now,
            completed_at=now + timedelta(seconds=seconds),
            monotonic_started_ns=0,
            monotonic_completed_ns=1,
            cpu_stat=RAW.decode(),
            cpu_max="50000 100000",
        )


@pytest.mark.parametrize("end", [0, 2000000002])
def test_monotonic_acquisition_bounds(end: int) -> None:
    """Elapsed acquisition is checked independently of adjustable UTC timestamps."""
    now = utc_now()
    with pytest.raises(ValueError, match="monotonic acquisition"):
        KernelSnapshot(
            started_at=now,
            completed_at=now,
            monotonic_started_ns=1,
            monotonic_completed_ns=end,
            cpu_stat=RAW.decode(),
            cpu_max="50000 100000",
        )


def test_failed_work_emits_no_completion(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An exception cannot leave a success-shaped record with made-up final counters."""
    paths = kernel(monkeypatch)

    def fail(rounds: int) -> float:
        """Model failure inside admitted work rather than during admission."""
        raise TimeoutError("work failed")

    with pytest.raises(TimeoutError):
        observed_work("synthetic-failure", 256, fail)
    assert paths == ["cpu.stat", "cpu.max"] and capsys.readouterr().out == ""


@pytest.mark.parametrize("rounds,capture", [(0, True), (256, "yes")])
def test_capture_configuration_is_closed(rounds: int, capture: Any) -> None:
    """Only a deployment-owned Boolean with enabled work permits kernel capture."""
    with pytest.raises(ValueError):
        CpuWork(rounds, capture)
    with pytest.raises(ValueError):
        SandboxConfig(cpu_rounds=rounds, cpu_capture=capture)


def test_missing_sample_releases_worker() -> None:
    """Direct API misuse fails before reading files and does not strand admission."""
    worker = CpuWork(256, True)
    try:
        with pytest.raises(ValueError, match="sample identity"):
            asyncio.run(worker.run())
        assert not worker.busy
    finally:
        worker.close()
