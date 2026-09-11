"""Measure the frozen sampling experiment using independent metric and trace adapters."""

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.evidence.payment_window import MeasurementInterval, Service, derive_payment_window
from payops.evidence.trace_span import PodIdentity, TraceScope
from payops.scenarios.sampling_contract import PLAN, sampling_workload
from payops.scenarios.sampling_observation import SamplingObservation, Stage
from payops.scenarios.traffic import TrafficDriver, TrafficReceipt, Workload
from payops.tools.payment import PaymentRead
from payops.tools.traces import TraceCollection, TraceRead


class SamplingObserver(Protocol):
    """A trusted adapter returns operator measurements; this is not a model tool."""

    def collect(
        self,
        stage: Stage,
        incident: str,
        identities: tuple[PodIdentity, PodIdentity],
        ready_at: datetime,
        directory: Path,
        store: ArtifactStore,
    ) -> SamplingObservation:
        """Capture fixed requests and bounded evidence under the trusted runtime identity."""
        ...


class MetricReader(Protocol):
    """The observer only selects the two fixed service names, never caller-provided PromQL."""

    def snapshot(self, service: Service) -> Observation:
        """One snapshot charges two bounded backend queries without hidden retries."""
        ...


class TraceReader(Protocol):
    """Trace collection retains its own command, byte and time budget."""

    def collect(self, scopes: tuple[TraceScope, ...], store: ArtifactStore) -> TraceCollection:
        """Collect the two fixed service windows under identity and output bounds."""
        ...


class TrafficSource(Protocol):
    """Fixtures substitute transport behavior without changing the production destination scope."""

    async def run(self, workload: Workload) -> TrafficReceipt:
        """Execute one exact frozen batch and retain interrupted attempts."""
        ...


class RuntimeSamplingObserver:
    """Four snapshots and two trace collections reserve forty backend operations per stage."""

    def __init__(
        self,
        kubeconfig: Path,
        metrics: MetricReader | None = None,
        traces: TraceReader | None = None,
        driver: Callable[[Path], TrafficSource] | None = None,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Trusted readers and clocks preserve fixed local runtime destinations."""
        self.metrics = metrics or PaymentRead()
        self.traces = traces or TraceRead(kubeconfig)
        self.driver: Callable[[Path], TrafficSource] = driver or (
            lambda directory: TrafficDriver(kubeconfig, directory)
        )
        self.clock, self.sleep = clock, sleep

    def _until(self, target: datetime) -> None:
        """Wait only the remaining fixed allowance; actual capture times remain explicit."""
        remaining = (target - self.clock()).total_seconds()
        if remaining > 0:
            self.sleep(remaining)

    def _snapshots(self, incident: str, store: ArtifactStore) -> tuple[EvidenceItem, EvidenceItem]:
        """Normalize both raw snapshots independently so derived windows retain exact lineage."""
        items: list[EvidenceItem] = []
        for service in ("payments-api", "processor-adapter"):
            observed = self.metrics.snapshot(service)
            items.append(normalize(observed, incident, observed.observed_at, self.clock(), store))
        return items[0], items[1]

    @staticmethod
    def _save(directory: Path, name: str, content: str) -> None:
        """Exclusive phase files retain partial progress without rewriting prior observations."""
        with (directory / name).open("x", encoding="utf-8") as stream:
            stream.write(content)

    def _capture(
        self, traffic: TrafficReceipt, incident: str, store: ArtifactStore
    ) -> TraceCollection:
        """The timer is a lower bound for two local samples, never a completeness certificate."""
        end = self.clock()
        start = traffic.started_at - timedelta(seconds=1)
        if not 0 < (end - start).total_seconds() <= PLAN.maximum_window_seconds:
            raise ValueError("sampling capture interval exceeds frozen bound")
        scopes = tuple(
            TraceScope(incident_id=incident, service=service, start=start, end=end)
            for service in ("payments-api", "processor-adapter")
        )
        return self.traces.collect(scopes, store)

    def collect(
        self,
        stage: Stage,
        incident: str,
        identities: tuple[PodIdentity, PodIdentity],
        ready_at: datetime,
        directory: Path,
        store: ArtifactStore,
    ) -> SamplingObservation:
        """Fresh scrape gates exclude earlier probes from the exact traffic census."""
        if stage not in PLAN.stages or ready_at.tzinfo is None or ready_at > self.clock():
            raise ValueError("invalid sampling stage or readiness time")
        phase = directory / stage
        phase.mkdir(exist_ok=False)
        self._until(ready_at + timedelta(seconds=6))
        before = self._snapshots(incident, store)
        self._save(
            phase,
            "before.json",
            JSON_OBJECT.dump_json(
                {"sources": [item.model_dump(mode="json") for item in before]}
            ).decode(),
        )
        if any(item.observed_at < ready_at for item in before):
            raise ValueError("sampling before snapshot predates ready process or prior traffic")
        traffic = asyncio.run(self.driver(phase / "traffic").run(sampling_workload()))
        self._save(phase, "traffic.json", traffic.model_dump_json(indent=2))
        self._until(traffic.completed_at + timedelta(seconds=6))
        after = self._snapshots(incident, store)
        self._save(
            phase,
            "after.json",
            JSON_OBJECT.dump_json(
                {"sources": [item.model_dump(mode="json") for item in after]}
            ).decode(),
        )
        interval = MeasurementInterval(
            incident_id=incident, start=traffic.started_at, end=traffic.completed_at
        )
        windows = tuple(
            derive_payment_window(first, last, interval, store)
            for first, last in zip(before, after, strict=True)
        )
        captures: list[TraceCollection] = []
        for index, offset in enumerate(PLAN.capture_offsets_seconds):
            self._until(traffic.completed_at + timedelta(seconds=offset))
            capture = self._capture(traffic, incident, store)
            self._save(phase, f"capture-{index}.json", capture.model_dump_json(indent=2))
            captures.append(capture)
        return SamplingObservation(
            traffic=traffic,
            metrics=(windows[0], windows[1]),
            captures=(captures[0], captures[1]),
            payments_identity=identities[0],
            processor_identity=identities[1],
        )
