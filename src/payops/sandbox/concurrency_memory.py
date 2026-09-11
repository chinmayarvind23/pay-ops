"""Fixed per-request resident memory makes concurrency the experimental variable."""

import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, monotonic_ns
from typing import Literal

from fastapi import HTTPException

from payops.sandbox.models import Sample

MIB = 1024 * 1024
REQUEST_BYTES = 32 * MIB
MAX_ACTIVE = 8
HOLD_SECONDS = 1


def memory_value(name: Literal["memory.current", "memory.max"]) -> int:
    """Read bounded numeric kernel accounting, never values supplied by an HTTP request."""
    with (Path("/sys/fs/cgroup") / name).open("rb") as stream:
        raw = stream.read(65)
    if len(raw) > 64 or not raw.strip().isdigit():
        raise ValueError("invalid cgroup memory accounting")
    return int(raw)


def validate_container() -> None:
    """The deployment-only workload is restricted to a 256Mi synthetic payments container."""
    if (
        sys.platform != "linux"
        or os.environ.get("PAYOPS_SANDBOX_ROLE") != "payments"
        or memory_value("memory.max") != 256 * MIB
    ):
        raise ValueError("concurrent memory work requires the bounded payments container")


def allocate() -> bytearray:
    """Touch each page so request overlap increases resident rather than only virtual memory."""
    block = bytearray(REQUEST_BYTES)
    for offset in range(0, REQUEST_BYTES, 4096):
        block[offset] = 1
    return block


class ConcurrentMemory:
    """One ASGI event loop owns admission; no background tasks or unbounded queue are created."""

    def __init__(self, enabled: bool = False) -> None:
        """Disabled service startup never reads host cgroups or allocates experimental memory."""
        if type(enabled) is not bool:
            raise ValueError("memory profile must be an explicit boolean")
        if enabled:
            validate_container()
        self.enabled, self.closed, self.active = enabled, False, 0

    def _record(self, sample_id: str, phase: str) -> None:
        """Keep sample-linked admission and kernel memory evidence outside metric label space."""
        print(
            json.dumps(
                {
                    "event": "synthetic.concurrency_memory",
                    "sample_id": sample_id,
                    "phase": phase,
                    "active": self.active,
                    "request_bytes": REQUEST_BYTES,
                    "hold_seconds": HOLD_SECONDS,
                    "cgroup_current_bytes": memory_value("memory.current"),
                    "cgroup_limit_bytes": memory_value("memory.max"),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "monotonic_ns": monotonic_ns(),
                }
            ),
            flush=True,
        )

    async def run(self, sample_id: str) -> None:
        """Hold one fixed allocation, then release it even on cancellation or capture failure."""
        if self.closed:
            raise HTTPException(503, "synthetic memory admission closed")
        if not self.enabled:
            return
        Sample(sample_id=sample_id)
        if self.active >= MAX_ACTIVE:
            raise HTTPException(503, "synthetic memory admission full")
        validate_container()
        # No await separates the capacity check and increment on the owning ASGI loop.
        self.active += 1
        block: bytearray | None = None
        started = monotonic()
        try:
            self._record(sample_id, "admitted")
            block = allocate()
            self._record(sample_id, "allocated")
            if monotonic() - started >= 2:
                raise HTTPException(504, "synthetic memory allocation deadline exceeded")
            await asyncio.sleep(HOLD_SECONDS)
        finally:
            del block
            self.active -= 1
            self._record(sample_id, "released")

    def close(self) -> None:
        """Close admission while active requests retain their allocations until release."""
        self.closed = True
