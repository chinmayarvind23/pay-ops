"""Fixed startup-only synthetic memory work inside a tightly limited Linux container."""

import json
import os
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event, Thread

import uvicorn
from fastapi import FastAPI

from payops.sandbox.entrypoint import create_app as sandbox_app

MIB = 1024 * 1024
CHUNK_BYTES = 8 * MIB
TARGET_BYTES = 128 * MIB
GRACE_SECONDS = 10
HOLD_SECONDS = 20


def container_memory() -> tuple[int, int]:
    """Read actual cgroup-v2 accounting, never an environment-supplied memory measurement."""
    root = Path("/sys/fs/cgroup")
    return int((root / "memory.current").read_text()), int((root / "memory.max").read_text())


def validate_container() -> None:
    """The fixed worker cannot allocate on an ordinary host or an unbounded container."""
    if (
        sys.platform != "linux"
        or os.environ.get("PAYOPS_SANDBOX_ROLE") != "payments"
        or os.environ.get("PAYOPS_SYNTHETIC_MEMORY_WORKLOAD") != "bounded-v1"
    ):
        raise ValueError("memory worker requires explicit synthetic payments container startup")
    _, maximum = container_memory()
    if maximum not in {128 * MIB, 256 * MIB}:
        raise ValueError("memory worker requires the reviewed 128Mi or 256Mi cgroup limit")


def record_phase(phase: str, allocated: int) -> None:
    """Structured runtime records report touched allocation and actual kernel accounting."""
    current, maximum = container_memory()
    print(
        json.dumps(
            {
                "event": "synthetic.memory",
                "phase": phase,
                "allocated_bytes": allocated,
                "cgroup_current_bytes": current,
                "cgroup_limit_bytes": maximum,
                "hold_seconds": HOLD_SECONDS,
            }
        ),
        flush=True,
    )


def touched_chunk() -> bytearray:
    """Touch every page so the workload consumes resident memory instead of just virtual space."""
    block = bytearray(CHUNK_BYTES)
    for offset in range(0, CHUNK_BYTES, 4096):
        block[offset] = 1
    return block


def allocate_workload(stop: Event) -> None:
    """Allocate at most 128MiB, hold briefly, then release without an unbounded repeat loop."""
    if stop.wait(GRACE_SECONDS):
        return
    blocks: list[bytearray] = []
    try:
        record_phase("started", 0)
        for _ in range(TARGET_BYTES // CHUNK_BYTES):
            if stop.is_set():
                return
            blocks.append(touched_chunk())
            record_phase("allocated", len(blocks) * CHUNK_BYTES)
            if stop.wait(0.1):
                return
        record_phase("holding", TARGET_BYTES)
        stop.wait(HOLD_SECONDS)
    finally:
        blocks.clear()
        record_phase("released", 0)


def create_app() -> FastAPI:
    """Keep the normal payments application and attach only the fixed startup memory worker."""
    validate_container()
    app = sandbox_app()
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
        """Worker cancellation and bounded join accompany normal server teardown."""
        stop = Event()
        worker = Thread(
            target=allocate_workload, args=(stop,), daemon=True, name="synthetic-memory-workload"
        )
        async with original_lifespan(application):
            worker.start()
            try:
                yield
            finally:
                stop.set()
                worker.join(timeout=5)
                if worker.is_alive():
                    raise RuntimeError("synthetic memory worker did not stop")

    app.router.lifespan_context = lifespan
    return app


def main() -> None:
    """The image starts a fixed local application, never a caller-provided command."""
    uvicorn.run(create_app(), host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
