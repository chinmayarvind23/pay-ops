"""Startup-only retained-allocation experiment with a fixed-rate release control."""

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from time import monotonic, monotonic_ns
from typing import Literal

Mode = Literal["retained-v1", "released-v1"]
MIB = 1024 * 1024
CHUNK_BYTES = 8 * MIB
MAX_CHUNKS = 40
INTERVAL_SECONDS = 0.5
GRACE_SECONDS = 10
LIFETIME_SECONDS = 35
CGROUP = Path("/sys/fs/cgroup")


def kernel_value(name: Literal["memory.current", "memory.max"]) -> int:
    """Only bounded numeric kernel files can establish the fixed container envelope."""
    with (CGROUP / name).open("rb") as stream:
        raw = stream.read(65)
    if len(raw) > 64 or not raw.strip().isdigit():
        raise ValueError("missing or unbounded cgroup memory accounting")
    return int(raw)


def validate_container() -> Mode:
    """Reject host execution, other services and limits outside the reviewed 256Mi experiment."""
    mode = os.environ.get("PAYOPS_SYNTHETIC_LEAK")
    if (
        sys.platform != "linux"
        or os.environ.get("PAYOPS_SANDBOX_ROLE") != "risk"
        or mode not in {"retained-v1", "released-v1"}
        or kernel_value("memory.max") != 256 * MIB
    ):
        raise ValueError("leak worker requires the explicit bounded synthetic risk container")
    return "retained-v1" if mode == "retained-v1" else "released-v1"


def touched_chunk() -> bytearray:
    """Fault every page into resident memory; virtual reservation alone is not the experiment."""
    chunk = bytearray(CHUNK_BYTES)
    for offset in range(0, CHUNK_BYTES, 4096):
        chunk[offset] = 1
    return chunk


def record(mode: Mode, phase: str, step: int, retained: int) -> None:
    """Expose real accounting and process-local timing without claiming an OOM from app logs."""
    print(
        json.dumps(
            {
                "event": "synthetic.leak",
                "mode": mode,
                "phase": phase,
                "step": step,
                "retained_bytes": retained,
                "cgroup_current_bytes": kernel_value("memory.current"),
                "cgroup_limit_bytes": kernel_value("memory.max"),
                "timestamp": datetime.now(UTC).isoformat(),
                "monotonic_ns": monotonic_ns(),
                "pid": os.getpid(),
            }
        ),
        flush=True,
    )


def allocate_leak(stop: Event) -> None:
    """Retain at most 320Mi for one bounded lifetime; the control releases each identical chunk."""
    mode = validate_container()
    deadline = monotonic() + LIFETIME_SECONDS
    if stop.wait(GRACE_SECONDS):
        return
    blocks: list[bytearray] = []
    try:
        record(mode, "started", 0, 0)
        for step in range(1, MAX_CHUNKS + 1):
            if stop.is_set() or monotonic() >= deadline:
                return
            if kernel_value("memory.max") != 256 * MIB:
                raise ValueError("container memory envelope changed during workload")
            block = touched_chunk()
            if mode == "retained-v1":
                blocks.append(block)
            del block
            record(mode, "allocated", step, len(blocks) * CHUNK_BYTES)
            if stop.wait(min(INTERVAL_SECONDS, max(0, deadline - monotonic()))):
                return
    finally:
        blocks.clear()
        record(mode, "released", 0, 0)
