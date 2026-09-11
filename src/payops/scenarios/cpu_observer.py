"""Acquire complete payment paths and retain CPU sources for each fixed experiment stage."""

from pathlib import Path
from typing import Protocol

from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import Immutable, PodIdentity, verify_trace_log
from payops.scenarios.contracts import JsonObject
from payops.scenarios.cpu_contract import PLAN, CpuStage
from payops.scenarios.cpu_sources import CpuGateway, CpuRecord, select_record
from payops.scenarios.protocol_observation import ProtocolObservation, verify_protocol_observation
from payops.scenarios.protocol_observer import RuntimeProtocolObserver


class CpuObservation(Immutable):
    """Full path observations and raw CPU logs remain available for independent replay."""

    paths: tuple[ProtocolObservation, ...]
    logs: tuple[str, ...]


class CpuObserver(Protocol):
    """The operator binds acquisition to its captured originals and current full specs."""

    def collect(
        self,
        stage: CpuStage,
        incident: str,
        identities: dict[str, PodIdentity],
        directory: Path,
        store: ArtifactStore,
        original: JsonObject,
        payments: JsonObject,
        risk: JsonObject,
    ) -> CpuObservation:
        """Collect three fresh requests without exposing destinations or mutation controls."""
        ...


def verify_local_interval(
    record: CpuRecord, path: ProtocolObservation, store: ArtifactStore
) -> None:
    """Container-local span clocks bind CPU work before peer calls without host clock skew."""
    spans = [
        item.span
        for source in path.capture.sources
        for item in verify_trace_log(source, store).parsed.spans
        if item.span.trace_id == "0x" + path.probe.traceparent.split("-")[1]
    ]
    server = next(span for span in spans if span.name == "sandbox.payments")
    calls = [span.start_time for span in spans if span.name.startswith("sandbox.call.")]
    if (
        not server.start_time
        <= record.before.started_at
        <= record.after.completed_at
        <= min(calls)
        <= server.end_time
    ):
        raise ValueError("CPU interval lies outside its local pre-dependency span")


def verify_observation(
    stage: CpuStage, observed: CpuObservation, store: ArtifactStore
) -> tuple[CpuRecord, ...]:
    """Every CPU source must accompany one verified successful five-service request."""
    if len(observed.paths) != PLAN.samples_per_stage or len(observed.logs) != 3:
        raise ValueError("CPU stage requires three complete request sources")
    records: list[CpuRecord] = []
    for path, raw in zip(observed.paths, observed.logs, strict=True):
        verify_protocol_observation("original", path, store)
        if stage in {"original", "final"}:
            if raw:
                raise ValueError("disabled CPU stage unexpectedly supplied kernel work")
        else:
            probe = path.probe
            record = select_record(raw.encode(), probe.sample, probe.started_at, probe.completed_at)
            verify_local_interval(record, path, store)
            records.append(record)
    return tuple(records)


class RuntimeCpuObserver:
    """Reuse bounded full-path traffic acquisition and add the owned CPU log reader."""

    def __init__(self, kubeconfig: Path, gateway: CpuGateway) -> None:
        """Only explicit local configuration reaches either concrete adapter."""
        self.paths = RuntimeProtocolObserver(kubeconfig)
        self.gateway = gateway

    def collect(
        self,
        stage: CpuStage,
        incident: str,
        identities: dict[str, PodIdentity],
        directory: Path,
        store: ArtifactStore,
        original: JsonObject,
        payments: JsonObject,
        risk: JsonObject,
    ) -> CpuObservation:
        """Each request owns a separate directory so failed captures survive without overwrite."""
        phase = directory / stage
        phase.mkdir()
        paths: list[ProtocolObservation] = []
        logs: list[str] = []
        for index in range(PLAN.samples_per_stage):
            request = phase / str(index)
            request.mkdir()
            path = self.paths.collect("original", incident, identities, request, store)
            raw = (
                b""
                if stage in {"original", "final"}
                else self.gateway.cpu_log(
                    identities["payments-api"], original, payments, risk, path.probe.started_at
                )
            )
            with (request / "cpu.log").open("xb") as stream:
                stream.write(raw)
            paths.append(path)
            logs.append(raw.decode("utf-8"))
        return CpuObservation(paths=tuple(paths), logs=tuple(logs))
