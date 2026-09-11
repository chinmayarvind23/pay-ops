"""Process-local fault and idempotency state for an isolated synthetic workload."""

import asyncio
import hashlib
from threading import Lock

from fastapi import HTTPException

from payops.sandbox.models import FaultConfig, Sample, SimulationResult


class SampleStore:
    """Bounded reservations prevent concurrent duplicate execution without eviction."""

    def __init__(self, capacity: int) -> None:
        """A full store fails closed; deleting old keys would silently weaken replay safety."""
        self.capacity = capacity
        self._entries: dict[str, tuple[str, SimulationResult | None]] = {}
        self._lock = Lock()

    def reserve(self, sample: Sample) -> SimulationResult | None:
        """Identical completed requests replay; pending or changed requests conflict."""
        fingerprint = hashlib.sha256(sample.model_dump_json().encode()).hexdigest()
        with self._lock:
            prior = self._entries.get(sample.sample_id)
            if prior is not None:
                if prior[0] != fingerprint or prior[1] is None:
                    raise HTTPException(409, "synthetic idempotency conflict")
                return prior[1]
            if len(self._entries) >= self.capacity:
                raise HTTPException(503, "synthetic sample store at capacity")
            self._entries[sample.sample_id] = (fingerprint, None)
        return None

    def complete(self, sample_id: str, result: SimulationResult) -> None:
        """Publish the result only after the complete synthetic service path succeeds."""
        with self._lock:
            fingerprint, _ = self._entries[sample_id]
            self._entries[sample_id] = (fingerprint, result)

    def abandon(self, sample_id: str) -> None:
        """Failed attempts may retry because downstream services never move funds."""
        with self._lock:
            self._entries.pop(sample_id, None)


class FaultState:
    """The trusted scenario harness swaps a validated immutable fault configuration."""

    def __init__(self, config: FaultConfig | None = None) -> None:
        """A healthy default permits real HTTP-path verification before fault injection."""
        self.config = config or FaultConfig()

    async def apply(self, sample: Sample) -> bool:
        """Seeded sample hashing is stable across run order and process scheduling."""
        config = self.config
        if not config.matches(sample):
            return False
        if config.unavailable:
            raise HTTPException(503, "synthetic dependency unavailable")
        if config.delay_ms:
            await asyncio.sleep(config.delay_ms / 1000)
        bucket = int.from_bytes(
            hashlib.sha256(f"{config.seed}:{sample.sample_id}".encode()).digest()[:8]
        )
        if config.rate_limit_every and bucket % config.rate_limit_every == 0:
            raise HTTPException(429, "synthetic rate limit")
        return bool(config.decline_every and bucket % config.decline_every == 0)
