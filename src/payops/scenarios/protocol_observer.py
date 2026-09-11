"""Acquire one real fixed-destination wire probe and its bounded diagnostic evidence."""

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import uuid4

import httpx

from payops.contracts import utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import ROLES, PodIdentity, TraceScope
from payops.sandbox.models import Sample
from payops.scenarios.contracts import JsonObject
from payops.scenarios.protocol_contract import PLAN, ProtocolStage
from payops.scenarios.protocol_gateway import ProtocolGateway
from payops.scenarios.protocol_observation import ProtocolObservation, ProtocolProbe
from payops.scenarios.traffic import scoped_forward, wait_forward_ready
from payops.tools.traces import TraceCollection, TraceRead


class ProtocolObserver(Protocol):
    """Only the operator invokes the concrete traffic/evidence adapter."""

    def collect(
        self,
        stage: ProtocolStage,
        incident: str,
        identities: dict[str, PodIdentity],
        directory: Path,
        store: ArtifactStore,
    ) -> ProtocolObservation:
        """Return the actual probe and source artifacts under fixed runtime identities."""
        ...


class ProtocolLogReader(Protocol):
    """Trusted runtime identity selects one owned risk pod rather than an arbitrary log target."""

    def risk_log(self, identity: PodIdentity, start: datetime) -> JsonObject:
        """Retain bounded access-log bytes and the scope used to acquire them."""
        ...


class ProtocolTraceReader(Protocol):
    """The concrete adapter always supplies all five fixed roles within existing trace limits."""

    def collect(self, scopes: tuple[TraceScope, ...], store: ArtifactStore) -> TraceCollection:
        """Read and verify actual console sources, independent of operator case labels."""
        ...


class RuntimeProtocolObserver:
    """Every stage owns its payment forward and preserves partial progress on failure."""

    def __init__(
        self,
        kubeconfig: Path,
        log_reader: ProtocolLogReader | None = None,
        trace_reader: ProtocolTraceReader | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Fixture transport selection changes the receipt mode and never exposes a URL input."""
        self.kubeconfig = kubeconfig
        self.logs = log_reader or ProtocolGateway(kubeconfig)
        self.traces = trace_reader or TraceRead(kubeconfig)
        self.transport, self.clock, self.sleep = transport, clock, sleep

    @staticmethod
    def _save(path: Path, content: str) -> None:
        """Exclusive phase files preserve failed attempts without replacing earlier evidence."""
        with path.open("x", encoding="utf-8") as stream:
            stream.write(content)

    async def _http(self, origin: str, sample: Sample, traceparent: str) -> ProtocolProbe:
        """Stream at most 8192 response bytes and keep actual status/body separate from judgment."""
        async with httpx.AsyncClient(
            base_url=origin,
            transport=self.transport,
            timeout=5,
            trust_env=False,
            follow_redirects=False,
        ) as client:
            await wait_forward_ready(client, "payments")
            started = self.clock()
            async with asyncio.timeout(5):
                async with client.stream(
                    "POST",
                    "/simulate",
                    json=sample.model_dump(),
                    headers={"traceparent": traceparent, "Accept-Encoding": "identity"},
                ) as response:
                    if response.headers.get("Content-Encoding", "identity") != "identity":
                        raise ValueError("compressed protocol probe response denied")
                    body = bytearray()
                    async for chunk in response.aiter_raw(chunk_size=4096):
                        if len(body) + len(chunk) > 8192:
                            raise ValueError("protocol probe response exceeds byte cap")
                        body.extend(chunk)
            return ProtocolProbe(
                mode="fixture_replay" if self.transport is not None else "local_kind",
                sample=sample,
                traceparent=traceparent,
                started_at=started,
                completed_at=self.clock(),
                status=response.status_code,
                body=body.decode("utf-8"),
            )

    def _probe(self, sample: Sample, traceparent: str) -> ProtocolProbe:
        """The fixed payments-only forward closes before any source acquisition begins."""
        if self.transport is not None:
            return asyncio.run(self._http("http://127.0.0.1:18082", sample, traceparent))
        with scoped_forward(self.kubeconfig, "payments") as origin:
            return asyncio.run(self._http(origin, sample, traceparent))

    def wait_export(self, completed_at: datetime) -> datetime:
        """Recheck the actual wall clock; one requested sleep does not prove the export offset."""
        target = completed_at + timedelta(seconds=PLAN.capture_offset_seconds)
        for _ in range(8):
            now = self.clock()
            remaining = (target - now).total_seconds()
            if remaining <= 0:
                return now
            if remaining > PLAN.capture_offset_seconds:
                raise ValueError("protocol export clock moved backwards")
            self.sleep(max(0.001, remaining))
        raise TimeoutError("protocol export clock did not reach capture offset")

    def collect(
        self,
        stage: ProtocolStage,
        incident: str,
        identities: dict[str, PodIdentity],
        directory: Path,
        store: ArtifactStore,
    ) -> ProtocolObservation:
        """One probe plus one complete five-service capture supplies each declared stage."""
        if stage not in PLAN.stages or set(identities) != set(ROLES):
            raise ValueError("invalid protocol observation scope")
        phase = directory / stage
        phase.mkdir(exist_ok=False)
        sample = Sample(sample_id="synthetic-" + uuid4().hex)
        traceparent = "00-" + uuid4().hex + "-" + uuid4().hex[:16] + "-01"
        self._save(phase / "plan.json", sample.model_dump_json())
        self._save(phase / "traceparent.txt", traceparent)
        probe = self._probe(sample, traceparent)
        self._save(phase / "probe.json", probe.model_dump_json(indent=2))
        access = (
            self.logs.risk_log(identities["risk-sim"], probe.started_at - timedelta(seconds=1))
            if stage == "mismatch"
            else None
        )
        if access is not None:
            from payops.evidence.artifacts import JSON_OBJECT

            self._save(phase / "risk-access.json", JSON_OBJECT.dump_json(access).decode())
        end = self.wait_export(probe.completed_at)
        start = probe.started_at - timedelta(seconds=1)
        if not 0 < (end - start).total_seconds() <= PLAN.maximum_window_seconds:
            raise ValueError("protocol capture window exceeds bound")
        scopes = tuple(
            TraceScope(incident_id=incident, service=service, start=start, end=end)
            for service in ROLES
        )
        capture = self.traces.collect(scopes, store)
        self._save(phase / "capture.json", capture.model_dump_json(indent=2))
        return ProtocolObservation(
            incident_id=incident,
            probe=probe,
            identities=identities,
            capture=capture,
            risk_access=access,
        )
