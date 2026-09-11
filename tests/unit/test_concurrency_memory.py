"""Admission and release invariants without performing pressure allocations on the host."""

import asyncio
import json
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from payops.sandbox import concurrency_memory as memory


def test_disabled_profile_and_closed_admission() -> None:
    """Ordinary startup must remain independent of Linux cgroups and experimental allocation."""
    with (
        patch.object(memory, "validate_container") as guard,
        patch.object(memory, "allocate") as allocate,
    ):
        work = memory.ConcurrentMemory()
        asyncio.run(work.run("synthetic-disabled"))
        work.close()
        with pytest.raises(HTTPException):
            asyncio.run(work.run("synthetic-closed"))
    guard.assert_not_called()
    allocate.assert_not_called()


def test_concurrent_capacity_and_cancellation_release() -> None:
    """Reject excess work and release only the cancelled request's slot."""

    async def exercise() -> None:
        """An event replaces the fixed wait so overlap is deterministic without real memory use."""
        release = asyncio.Event()

        async def hold(seconds: float) -> None:
            """Preserve the declared duration argument while controlling completion in this test."""
            assert seconds == 1
            await release.wait()

        with (
            patch.object(memory, "validate_container"),
            patch.object(memory, "allocate", side_effect=lambda: bytearray(1)) as allocate,
            patch.object(memory.ConcurrentMemory, "_record"),
            patch.object(memory.asyncio, "sleep", side_effect=hold),
        ):
            work = memory.ConcurrentMemory(True)
            tasks = [asyncio.create_task(work.run(f"synthetic-{i}")) for i in range(8)]
            await asyncio.wait(tasks, timeout=0.01)
            assert work.active == 8 and allocate.call_count == 8
            with pytest.raises(HTTPException) as error:
                await work.run("synthetic-excess")
            assert error.value.status_code == 503 and allocate.call_count == 8
            tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[0]
            assert work.active == 7
            work.close()
            release.set()
            await asyncio.gather(*tasks[1:])
            assert work.active == 0

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["allocation", "capture", "deadline"])
def test_failed_request_releases_admission(failure: str) -> None:
    """Exceptions from allocation, logging or elapsed allocation budget cannot leak capacity."""
    with (
        patch.object(memory, "validate_container"),
        patch.object(
            memory,
            "allocate",
            side_effect=MemoryError if failure == "allocation" else None,
            return_value=bytearray(1),
        ),
        patch.object(
            memory.ConcurrentMemory,
            "_record",
            side_effect=OSError if failure == "capture" else None,
        ),
        patch.object(
            memory,
            "monotonic",
            side_effect=[0, 3] if failure == "deadline" else None,
            return_value=0,
        ),
    ):
        work = memory.ConcurrentMemory(True)
        with pytest.raises((MemoryError, OSError, HTTPException)):
            asyncio.run(work.run("synthetic-failure"))
        assert work.active == 0


def test_wrong_container_rejects_before_allocation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wrong role, platform or cgroup limit never reaches the resident-memory workload."""
    monkeypatch.setenv("PAYOPS_SANDBOX_ROLE", "risk")
    with (
        patch.object(memory.sys, "platform", "linux"),
        patch.object(memory, "memory_value", return_value=256 * memory.MIB),
    ):
        with pytest.raises(ValueError):
            memory.ConcurrentMemory(True)
    with pytest.raises(ValueError):
        memory.ConcurrentMemory(cast(bool, 1))


def test_actual_kernel_records_and_normal_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Read bounded files and verify records reflect real admission state at each phase."""
    (tmp_path / "memory.max").write_text(str(256 * memory.MIB))
    (tmp_path / "memory.current").write_text(str(60 * memory.MIB))
    monkeypatch.setenv("PAYOPS_SANDBOX_ROLE", "payments")
    with (
        patch.object(memory, "Path", return_value=tmp_path),
        patch.object(memory.sys, "platform", "linux"),
        patch.object(memory, "allocate", return_value=bytearray(1)),
        patch.object(memory.asyncio, "sleep"),
    ):
        work = memory.ConcurrentMemory(True)
        asyncio.run(work.run("synthetic-record"))
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [r["phase"] for r in records] == ["admitted", "allocated", "released"]
    assert [r["active"] for r in records] == [1, 1, 0]
    assert all(
        r["request_bytes"] == 32 * memory.MIB and r["cgroup_current_bytes"] == 60 * memory.MIB
        for r in records
    )
    assert all(r["monotonic_ns"] > 0 and r["timestamp"] for r in records)
    assert work.active == 0


@pytest.mark.parametrize("raw", [b"max", b"-1", b"1" * 65, b""])
def test_malformed_kernel_accounting_rejects(tmp_path: Path, raw: bytes) -> None:
    """Unlimited or unreadable kernel limits cannot be replaced with guessed defaults."""
    (tmp_path / "memory.max").write_bytes(raw)
    with patch.object(memory, "Path", return_value=tmp_path), pytest.raises(ValueError):
        memory.memory_value("memory.max")


def test_real_page_touch_uses_fixed_allocation() -> None:
    """A small real allocation confirms resident page touching without host pressure."""
    with patch.object(memory, "REQUEST_BYTES", 8192):
        block = memory.allocate()
    assert len(block) == 8192 and block[0] == block[4096] == 1
