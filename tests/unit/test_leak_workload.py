"""Bounded worker tests never allocate the live experiment's memory on the host."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from payops.scenarios import leak_workload as leak


@pytest.mark.parametrize("mode", ["retained-v1", "released-v1"])
def test_retention_is_the_only_control_difference(mode: leak.Mode) -> None:
    """Both modes allocate forty chunks at the same rate; only retained bytes diverge."""
    stop = MagicMock()
    stop.wait.return_value = False
    stop.is_set.return_value = False
    with (
        patch.object(leak, "validate_container", return_value=mode),
        patch.object(leak, "kernel_value", return_value=256 * leak.MIB),
        patch.object(leak, "touched_chunk", side_effect=lambda: bytearray(1)) as allocate,
        patch.object(leak, "record") as record,
        patch.object(leak, "monotonic", return_value=0),
    ):
        leak.allocate_leak(stop)
    assert allocate.call_count == 40
    allocations = [call.args for call in record.call_args_list if call.args[1] == "allocated"]
    assert [args[3] for args in allocations] == [
        step * leak.CHUNK_BYTES if mode == "retained-v1" else 0 for step in range(1, 41)
    ]
    assert record.call_args.args == (mode, "released", 0, 0)
    assert [call.args[0] for call in stop.wait.call_args_list] == [10] + [0.5] * 40


@pytest.mark.parametrize("reason", ["grace", "cancel", "deadline", "limit", "allocation"])
def test_worker_stops_and_releases_on_all_boundaries(reason: str) -> None:
    """Cancellation, expiration and changed cgroup bounds cannot leave a background allocator."""
    stop = MagicMock()
    stop.wait.return_value = reason == "grace"
    stop.is_set.return_value = reason == "cancel"
    with (
        patch.object(leak, "validate_container", return_value="retained-v1"),
        patch.object(leak, "kernel_value", return_value=0 if reason == "limit" else 256 * leak.MIB),
        patch.object(leak, "touched_chunk", side_effect=MemoryError) as allocate,
        patch.object(leak, "record") as record,
        patch.object(
            leak, "monotonic", side_effect=[0, 36] if reason == "deadline" else None, return_value=0
        ),
    ):
        if reason in {"limit", "allocation"}:
            with pytest.raises((ValueError, MemoryError)):
                leak.allocate_leak(stop)
        else:
            leak.allocate_leak(stop)
    assert allocate.call_count == int(reason == "allocation")
    if reason == "grace":
        record.assert_not_called()
    else:
        assert record.call_args.args == ("retained-v1", "released", 0, 0)


@pytest.mark.parametrize(
    "platform,role,mode,maximum",
    [
        ("win32", "risk", "retained-v1", 256),
        ("linux", "payments", "retained-v1", 256),
        ("linux", "risk", "unknown", 256),
        ("linux", "risk", "retained-v1", 128),
    ],
)
def test_container_guard_rejects_unreviewed_execution(
    platform: str, role: str, mode: str, maximum: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the fixed risk role, explicit mode and 256Mi kernel cap authorize allocation."""
    monkeypatch.setenv("PAYOPS_SANDBOX_ROLE", role)
    monkeypatch.setenv("PAYOPS_SYNTHETIC_LEAK", mode)
    with (
        patch.object(leak.sys, "platform", platform),
        patch.object(leak, "kernel_value", return_value=maximum * leak.MIB),
    ):
        with pytest.raises(ValueError):
            leak.validate_container()


@pytest.mark.parametrize("raw", [b"max", b"-1", b"1" * 65, b""])
def test_kernel_values_require_bounded_numeric_files(tmp_path: Path, raw: bytes) -> None:
    """Unlimited and malformed kernel files cannot authorize allocations."""
    (tmp_path / "memory.max").write_bytes(raw)
    with patch.object(leak, "CGROUP", tmp_path), pytest.raises(ValueError):
        leak.kernel_value("memory.max")


@pytest.mark.parametrize("mode", ["retained-v1", "released-v1"])
def test_real_file_guard_and_structured_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode: leak.Mode,
) -> None:
    """Records use kernel values and current clocks, with startup modes validated independently."""
    (tmp_path / "memory.max").write_text(str(256 * leak.MIB))
    (tmp_path / "memory.current").write_text(str(60 * leak.MIB))
    monkeypatch.setenv("PAYOPS_SANDBOX_ROLE", "risk")
    monkeypatch.setenv("PAYOPS_SYNTHETIC_LEAK", mode)
    with patch.object(leak.sys, "platform", "linux"), patch.object(leak, "CGROUP", tmp_path):
        assert leak.validate_container() == mode
        leak.record(mode, "started", 0, 0)
    record = json.loads(capsys.readouterr().out)
    assert record["cgroup_current_bytes"] == 60 * leak.MIB
    assert record["cgroup_limit_bytes"] == 256 * leak.MIB
    assert record["monotonic_ns"] > 0 and record["pid"] > 0 and record["timestamp"]


def test_chunk_touches_each_page() -> None:
    """A small real allocation verifies resident page touching without performing the fault."""
    with patch.object(leak, "CHUNK_BYTES", 8192):
        chunk = leak.touched_chunk()
    assert len(chunk) == 8192 and chunk[0] == chunk[4096] == 1
