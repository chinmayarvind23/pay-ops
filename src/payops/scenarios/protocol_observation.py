"""Verify actual wire results and known trace paths without accepting status prose as proof."""

import json
import re
from datetime import timedelta
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, StringConstraints

from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import ROLES, CapturedSpan, Immutable, PodIdentity, verify_trace_log
from payops.sandbox.models import Sample, SimulationResult
from payops.scenarios.contracts import JsonObject, object_value
from payops.scenarios.memory_provenance import timestamp
from payops.scenarios.protocol_contract import PLAN, ProtocolStage
from payops.tools.traces import TraceCollection, summarize_graph


class ProtocolProbe(Immutable):
    """Only the fixed payments endpoint can produce this operator HTTP observation."""

    mode: Literal["local_kind", "fixture_replay"]
    sample: Sample
    traceparent: Annotated[str, StringConstraints(pattern=r"^00-[0-9a-f]{32}-[0-9a-f]{16}-01$")]
    started_at: AwareDatetime
    completed_at: AwareDatetime
    status: int = Field(ge=100, le=599)
    body: Annotated[str, StringConstraints(max_length=8192)]


class ProtocolObservation(Immutable):
    """Direct sources stay in artifacts; the operator additionally records its known HTTP probe."""

    incident_id: str
    probe: ProtocolProbe
    identities: dict[str, PodIdentity]
    capture: TraceCollection
    risk_access: JsonObject | None = None


def _http(stage: ProtocolStage, probe: ProtocolProbe) -> None:
    """A failed HTTP status is retained, but only the exact reviewed behavior qualifies."""
    if (
        not 0
        <= (probe.completed_at - probe.started_at).total_seconds()
        <= PLAN.request_timeout_seconds
    ):
        raise ValueError("protocol HTTP attempt timing is invalid")
    if (probe.sample.processor, probe.sample.region, probe.sample.payment_method) != (
        "A",
        "us",
        "credit",
    ):
        raise ValueError("protocol probe changed its frozen input slice")
    if stage == "mismatch":
        if probe.status != 502 or json.loads(probe.body) != {
            "detail": "synthetic risk returned 422"
        }:
            raise ValueError("protocol mismatch lacks actual upstream schema rejection")
    else:
        result = SimulationResult.model_validate_json(probe.body)
        if (
            probe.status != 200
            or result.role != "payments"
            or result.status != "accepted"
            or result.sample_id != probe.sample.sample_id
        ):
            raise ValueError("protocol control did not accept the fresh full payment")


def _access(observed: ProtocolObservation) -> None:
    """An owned access record corroborates time/status; it has no request-ID field."""
    raw = observed.risk_access
    if (
        raw is None
        or PodIdentity.model_validate(object_value(raw["identity"]))
        != observed.identities["risk-sim"]
    ):
        raise ValueError("protocol risk access identity differs")
    text = raw.get("text")
    if (
        not isinstance(text, str)
        or len(text.encode()) >= PLAN.risk_log_bytes
        or len(text.splitlines()) >= 200
    ):
        raise ValueError("protocol risk access source is missing or capped")
    start, end = observed.probe.started_at, observed.probe.completed_at
    since, captured = timestamp(raw.get("since")), timestamp(raw.get("captured_at"))
    if (
        since != start - timedelta(seconds=1)
        or captured is None
        or not end <= captured <= end + timedelta(seconds=12)
    ):
        raise ValueError("protocol risk access capture timing differs")
    statuses: list[int] = []
    for line in text.splitlines():
        match = re.fullmatch(
            r'(\S+) INFO:\s+[^\s]+ - "POST /simulate HTTP/1\.[01]" (\d{3}) [A-Za-z ]+', line
        )
        if match is None:
            continue
        occurred = timestamp(match[1])
        if occurred is not None and start - timedelta(seconds=1) <= occurred <= end + timedelta(
            seconds=1
        ):
            statuses.append(int(match[2]))
    if statuses != [422]:
        raise ValueError("protocol mismatch lacks one owned temporal risk 422 record")


def _records(observed: ProtocolObservation, store: ArtifactStore) -> list[CapturedSpan]:
    """Read verified source logs directly, pin all processes, and reject acquisition gaps."""
    capture, probe = observed.capture, observed.probe
    if (
        len(capture.sources) != 5
        or capture.services_without_pods
        or any(
            (
                capture.partial_candidates,
                capture.malformed_candidates,
                capture.capped_sources,
            )
        )
    ):
        raise ValueError("protocol trace capture is unavailable or capped")
    logs = [verify_trace_log(source, store) for source in capture.sources]
    if set(observed.identities) != set(ROLES) or {log.scope.service for log in logs} != set(ROLES):
        raise ValueError("protocol full-path source census differs")
    records: list[CapturedSpan] = []
    for log in logs:
        if (
            log.scope.incident_id != observed.incident_id
            or log.identity != observed.identities[log.scope.service]
            or log.scope.start != probe.started_at - timedelta(seconds=1)
            or not probe.completed_at + timedelta(seconds=12) <= log.scope.end
            or (log.scope.end - log.scope.start).total_seconds() > PLAN.maximum_window_seconds
            or log.parsed.partial_candidates
            or log.parsed.malformed_candidates
            or log.parsed.limit_reached
        ):
            raise ValueError("protocol trace source identity, timing or completeness differs")
        records.extend(log.parsed.spans)
    summarize_graph(tuple(record.span for record in records))
    return records


def _path(stage: ProtocolStage, probe: ProtocolProbe, records: list[CapturedSpan]) -> None:
    """Each known parent edge must be real; matching v2 proves every peer executes again."""
    trace_id, parent = ("0x" + part for part in probe.traceparent.split("-")[1:3])
    spans = [record.span for record in records if record.span.trace_id == trace_id]
    expected = {"sandbox.payments", "sandbox.call.risk"}
    if stage != "mismatch":
        expected |= {"sandbox." + role for role in ("risk", "processor", "ledger", "webhook")}
        expected |= {"sandbox.call." + role for role in ("processor", "ledger", "webhook")}
    by_name = {span.name: span for span in spans}
    if set(by_name) != expected or len(spans) != len(expected):
        raise ValueError("protocol known request path is missing, duplicated or unexpected")
    server = by_name["sandbox.payments"]
    if server.parent_id != parent or not (
        probe.started_at - timedelta(seconds=1)
        <= server.start_time
        <= server.end_time
        <= probe.completed_at + timedelta(seconds=1)
    ):
        raise ValueError("protocol incoming parent or actual request time differs")
    for role in ("risk",) if stage == "mismatch" else ("risk", "processor", "ledger", "webhook"):
        client = by_name["sandbox.call." + role]
        if (
            client.parent_id != server.span_id
            or not server.start_time <= client.start_time <= client.end_time <= server.end_time
        ):
            raise ValueError("protocol caller edge differs")
        if stage != "mismatch":
            peer = by_name["sandbox." + role]
            if (
                peer.parent_id != client.span_id
                or not client.start_time <= peer.start_time <= peer.end_time <= client.end_time
            ):
                raise ValueError("protocol peer edge differs")
    if (
        any(span.status_code != "ERROR" for span in spans)
        if stage == "mismatch"
        else any(span.status_code == "ERROR" for span in spans)
    ):
        raise ValueError("protocol span status disagrees with the HTTP outcome")


def verify_protocol_observation(
    stage: ProtocolStage, observed: ProtocolObservation, store: ArtifactStore
) -> None:
    """Acceptance combines bounded actual HTTP, runtime-correlated logs and the known trace path."""
    if stage not in PLAN.stages:
        raise ValueError("unknown protocol stage")
    _http(stage, observed.probe)
    if stage == "mismatch":
        _access(observed)
    _path(stage, observed.probe, _records(observed, store))
