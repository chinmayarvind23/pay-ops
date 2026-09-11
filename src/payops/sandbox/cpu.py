"""Fixed real CPU work supports quota experiments without blocking the ASGI event loop."""

import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import monotonic, thread_time

from fastapi import HTTPException


def hash_work(rounds: int) -> float:
    """Fixed rounds bound allocation; a cooperative wall deadline bounds throttled work."""
    started, cpu_started = monotonic(), thread_time()
    value, block = bytes(32), bytes(4096)
    for index in range(rounds):
        if index % 128 == 0 and monotonic() - started >= 5:
            raise HTTPException(504, "synthetic CPU work deadline exceeded")
        value = hashlib.sha256(block + value).digest()
    return thread_time() - cpu_started


class CpuWork:
    """One admitted worker per service; cancellation cannot free a still-running slot."""

    def __init__(self, rounds: int) -> None:
        """Zero disables this optional deployment control without creating a worker pool."""
        if type(rounds) is not int or not 0 <= rounds <= 200000:
            raise ValueError("CPU rounds outside bounded deployment profile")
        self.rounds = rounds
        self.lock = Lock()
        self.busy, self.closed = False, False
        self.pool = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="payops-cpu") if rounds else None
        )

    def _execute(self) -> float:
        """The actual worker owns admission until it exits, including late completion."""
        try:
            return hash_work(self.rounds)
        finally:
            with self.lock:
                self.busy = False

    async def run(self) -> float:
        """Reject excess concurrent work immediately; never enqueue synthetic CPU pressure."""
        with self.lock:
            if self.closed or self.busy:
                raise HTTPException(503, "synthetic CPU worker unavailable")
            if self.pool is None:
                return 0.0
            self.busy = True
            try:
                future = self.pool.submit(self._execute)
            except BaseException:
                self.busy = False
                raise
        # Shield preserves admission even if cancellation arrives before the worker starts.
        return await asyncio.shield(asyncio.wrap_future(future))

    def close(self) -> None:
        """Stop admission; already admitted bounded work retains its worker until completion."""
        with self.lock:
            self.closed = True
            if self.pool is not None:
                self.pool.shutdown(wait=False, cancel_futures=False)
