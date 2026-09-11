"""A fixed read plan adds two payment windows without consulting scenario or scoring inputs."""

import os
import subprocess
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import httpx

from payops.contracts import Contract, EvidenceItem, Incident, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.evidence.payment_window import MeasurementInterval, Service, derive_payment_window
from payops.tools.collect import Collection, CollectionFailure
from payops.tools.kubernetes import SERVICES, KubernetesRead
from payops.tools.payment import PaymentRead


class WindowPlan(Contract):
    """Reservations count fixed commands/queries; kubectl may use multiple wire HTTP requests."""

    incident_id: str
    services: tuple[Service, Service]
    logical_operations: Literal[20] = 20
    backend_reads: Literal[34] = 34


def window_plan(incident: Incident) -> WindowPlan:
    """Select a fixed pair from alert service scope, never from title, case ID or gold cause."""
    if incident.request.namespace != "payops-sandbox":
        raise ValueError("unsupported payment window scope")
    service = incident.request.service
    if service in {"payments-api", "processor-adapter"}:
        pair: tuple[Service, Service] = ("payments-api", "processor-adapter")
    elif service == "webhook-sim":
        pair = ("webhook-sim", "payments-api")
    else:
        raise ValueError("unsupported payment window service")
    return WindowPlan(incident_id=incident.incident_id, services=pair)


def retain_once(path: Path, value: Contract | MeasurementInterval) -> None:
    """Exclusive fsynced records prevent accidental redispatch after an ambiguous prior run."""
    with path.open("xb") as stream:
        stream.write(value.model_dump_json().encode())
        stream.flush()
        os.fsync(stream.fileno())


class WindowCollector:
    """One invocation reserves its complete read plan before contacting either backend."""

    def __init__(self, kubernetes: KubernetesRead, payment: PaymentRead) -> None:
        """Trusted host wiring supplies scoped adapters; no credentials enter incident state."""
        self.kubernetes, self.payment = kubernetes, payment

    def __call__(self, incident: Incident, output: Path) -> Collection:
        """Keep partial observations and failures without retries or changed service pairs."""
        plan = window_plan(incident)
        output.mkdir(parents=True, exist_ok=True)
        retain_once(output / "window-plan.json", plan)
        batch = WindowBatch(plan, output)
        before = self._snapshots(batch)
        start = utc_now()
        batch.record_start(start)
        self._workload_reads(batch)
        end = utc_now()
        try:
            interval = MeasurementInterval(incident_id=incident.incident_id, start=start, end=end)
        except ValueError:
            batch.failures.append(
                CollectionFailure(
                    tool="payment_window",
                    resource=incident.request.service,
                    error_type="MeasurementIntervalExceeded",
                )
            )
        else:
            retain_once(output / "window-interval.json", interval)
            # One attempt follows the five-second scrape interval; stale data stays missing.
            time.sleep(5.2)
            after = self._snapshots(batch)
            batch.derive(before, after, interval)
        result = Collection(
            incident_id=plan.incident_id,
            evidence=tuple(batch.evidence),
            failures=tuple(batch.failures),
        )
        retain_once(output / "collection.json", result)
        return result

    def _snapshots(self, batch: "WindowBatch") -> dict[str, EvidenceItem]:
        """Each logical snapshot reserves two queries even if its first query fails."""
        result: dict[str, EvidenceItem] = {}
        for service in batch.plan.services:
            items = batch.capture(
                "payment_snapshot",
                service,
                lambda service=service: (self.payment.snapshot(service),),
            )
            if items:
                result[service] = items[0]
        return result

    def _workload_reads(self, batch: "WindowBatch") -> None:
        """Fifteen Kubernetes operations cost25 commands; targets add one Prometheus query."""
        for service in sorted(SERVICES):
            batch.capture(
                "kubernetes", service, lambda service=service: self.kubernetes.collect(service)
            )
            batch.capture(
                "events", service, lambda service=service: self.kubernetes.events(service)
            )
            batch.capture("logs", service, lambda service=service: (self.kubernetes.logs(service),))
        batch.capture("prometheus", "targets", lambda: (self.payment.query("targets"),))


class WindowBatch:
    """Retain successful artifacts as failures accumulate, so one unavailable source is explicit."""

    def __init__(self, plan: WindowPlan, output: Path) -> None:
        """All evidence remains incident-owned in the operator-selected output directory."""
        self.plan, self.output = plan, output
        self.store = ArtifactStore(output / "artifacts")
        self.start = utc_now() - timedelta(minutes=5)
        self.evidence: list[EvidenceItem] = []
        self.failures: list[CollectionFailure] = []

    def record_start(self, start: datetime) -> None:
        """Persist measurement start before workload reads without predicting its actual end."""
        with (self.output / "window-start.txt").open("x", encoding="utf-8") as stream:
            stream.write(str(start))
            stream.flush()
            os.fsync(stream.fileno())

    def capture(
        self, tool: str, resource: str, call: Callable[[], tuple[Observation, ...]]
    ) -> tuple[EvidenceItem, ...]:
        """Classifications expose no provider exception strings; failed reads are never retried."""
        items: list[EvidenceItem] = []
        try:
            for observation in call():
                item = self.keep(observation, tool, resource)
                if item is not None:
                    items.append(item)
        except (ValueError, OSError, subprocess.SubprocessError, httpx.HTTPError) as error:
            self.failures.append(
                CollectionFailure(tool=tool, resource=resource, error_type=type(error).__name__)
            )
        return tuple(items)

    def keep(self, observation: Observation, tool: str, resource: str) -> EvidenceItem | None:
        """A stale event must not discard later current events from the same source response."""
        try:
            item = normalize(
                observation,
                self.plan.incident_id,
                self.start,
                utc_now() + timedelta(seconds=1),
                self.store,
            )
            self.store.verify(item)
        except (ValueError, OSError) as error:
            self.failures.append(
                CollectionFailure(tool=tool, resource=resource, error_type=type(error).__name__)
            )
            return None
        self.evidence.append(item)
        return item

    def derive(
        self,
        before: dict[str, EvidenceItem],
        after: dict[str, EvidenceItem],
        interval: MeasurementInterval,
    ) -> None:
        """Derivation performs only local artifact reads and retains either a window or failure."""
        for service in self.plan.services:
            try:
                item = derive_payment_window(before[service], after[service], interval, self.store)
                self.evidence.append(item)
            except (KeyError, ValueError, OSError) as error:
                self.failures.append(
                    CollectionFailure(
                        tool="payment_window", resource=service, error_type=type(error).__name__
                    )
                )
