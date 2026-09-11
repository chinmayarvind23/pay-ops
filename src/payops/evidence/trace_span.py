"""Bounded console-span parsing and direct LOG-to-TRACE evidence provenance."""

import json
import math
import re
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator

from payops.contracts import EvidenceItem, Identifier
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize

Service = Literal["payments-api", "risk-sim", "processor-adapter", "ledger-sim", "webhook-sim"]
ROLES: dict[Service, str] = {
    "payments-api": "payments",
    "risk-sim": "risk",
    "processor-adapter": "processor",
    "ledger-sim": "ledger",
    "webhook-sim": "webhook",
}
MAX_LOG_BYTES = 131072
MAX_LOG_LINES = 2000
MAX_SPANS = 128
LOG_QUERY = "trace.console-log.v1"
TRACE_QUERY = "trace.console-span.v1"
TraceId = Annotated[str, StringConstraints(pattern=r"^0x[0-9a-f]{32}$")]
SpanId = Annotated[str, StringConstraints(pattern=r"^0x[0-9a-f]{16}$")]


class Immutable(BaseModel):
    """Strict immutable source records prevent coercion and later projection drift."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)


class TraceScope(Immutable):
    """Bind temporal correlation to trusted incident ownership."""

    incident_id: Identifier
    namespace: Literal["payops-sandbox"] = "payops-sandbox"
    service: Service
    start: AwareDatetime
    end: AwareDatetime

    @model_validator(mode="after")
    def bounded(self) -> Self:
        """A positive ten-minute ceiling limits both admission and source query work."""
        if not 0 < (self.end - self.start).total_seconds() <= 600:
            raise ValueError("trace interval must be positive and at most ten minutes")
        return self


class PodIdentity(Immutable):
    """The reader checks this complete identity before and after an exact current-container read."""

    pod_name: Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9.-]{0,126}$")]
    pod_uid: Identifier
    deployment_uid: Identifier
    replica_set_uid: Identifier
    container: Literal["sandbox"] = "sandbox"
    container_id: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    restart_count: int = Field(ge=0, le=1000000)


class SourceSpan(Immutable):
    """Only observed exporter fields survive; duration is deliberately absent from source data."""

    name: Annotated[str, StringConstraints(min_length=1, max_length=80)]
    trace_id: TraceId
    span_id: SpanId
    parent_id: SpanId | None
    kind: Literal["SpanKind.CLIENT", "SpanKind.SERVER"]
    start_time: AwareDatetime
    end_time: AwareDatetime
    status_code: Literal["UNSET", "OK", "ERROR"]
    service_name: Annotated[str, StringConstraints(min_length=1, max_length=80)]

    @model_validator(mode="after")
    def valid(self) -> Self:
        """Reject impossible identities and inverted source times before any graph edge exists."""
        if (
            int(self.trace_id, 16) == 0
            or int(self.span_id, 16) == 0
            or (self.parent_id is not None and int(self.parent_id, 16) == 0)
            or self.parent_id == self.span_id
            or self.start_time > self.end_time
        ):
            raise ValueError("invalid span identity or time")
        return self

    def check_service(self, service: Service) -> None:
        """Span name and kind must agree with the exact emitting deployment role."""
        expected = {f"sandbox.{ROLES[service]}": "SpanKind.SERVER"}
        if service == "payments-api":
            expected.update(
                {
                    f"sandbox.call.{role}": "SpanKind.CLIENT"
                    for role in ("risk", "processor", "ledger", "webhook")
                }
            )
        if (
            self.service_name != f"payops-sandbox-{ROLES[service]}"
            or expected.get(self.name) != self.kind
        ):
            raise EvidenceIntegrityError("span disagrees with emitting service")


class CapturedSpan(Immutable):
    """Span time and log emission time remain separate despite batched export delays."""

    span: SourceSpan
    log_start: AwareDatetime
    log_end: AwareDatetime

    @model_validator(mode="after")
    def ordered(self) -> Self:
        """Within one source object, log prefixes must retain a nondecreasing capture interval."""
        if not self.span.end_time <= self.log_start <= self.log_end:
            raise ValueError("inverted log emission interval")
        return self


class ParsedLog(Immutable):
    """Every result is a bounded sample; counters never certify absence of dropped source spans."""

    spans: tuple[CapturedSpan, ...] = Field(max_length=MAX_SPANS)
    raw_bytes: int = Field(ge=0, le=MAX_LOG_BYTES)
    line_count: int = Field(ge=0, le=MAX_LOG_LINES)
    partial_candidates: int = Field(ge=0)
    malformed_candidates: int = Field(ge=0)
    excluded_spans: int = Field(ge=0)
    limit_reached: bool
    sampling: Literal["bounded_sample"] = "bounded_sample"


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Duplicate JSON keys cannot replace previously supplied source identity or timestamps."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate span JSON key")
        result[key] = value
    return result


def _constant(value: str) -> None:
    """Reject nonfinite JSON even in attributes that the projection would otherwise drop."""
    raise ValueError("nonfinite span JSON")


def _float(value: str) -> float:
    """JSON exponent overflow must not hide in a field excluded from the final projection."""
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite span JSON number")
    return result


def _project(text: str) -> SourceSpan:
    """Parse a bounded complete object and retain exact documented exporter fields only."""
    obj = JSON_OBJECT.validate_python(
        json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant, parse_float=_float)
    )
    if "parent_id" not in obj:
        raise ValueError("span parent field is missing")
    context, status, resource = obj.get("context"), obj.get("status"), obj.get("resource")
    if (
        not isinstance(context, dict)
        or not isinstance(status, dict)
        or not isinstance(resource, dict)
    ):
        raise ValueError("span nested metadata is missing")
    attributes = resource.get("attributes")
    if not isinstance(attributes, dict):
        raise ValueError("span resource attributes are missing")
    return SourceSpan.model_validate_json(
        json.dumps(
            {
                "name": obj.get("name"),
                "trace_id": context.get("trace_id"),
                "span_id": context.get("span_id"),
                "parent_id": obj.get("parent_id"),
                "kind": obj.get("kind"),
                "start_time": obj.get("start_time"),
                "end_time": obj.get("end_time"),
                "status_code": status.get("status_code"),
                "service_name": attributes.get("service.name"),
            }
        )
    )


def _depth(text: str) -> None:
    """One linear scan bounds JSON nesting before the recursive decoder sees untrusted bytes."""
    depth, quoted, escaped = 0, False, False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
            if depth > 8:
                raise ValueError("span JSON exceeds depth budget")
        elif char in "]}":
            depth -= 1


def _log_lines(raw: bytes) -> tuple[tuple[datetime, str], ...]:
    """Validate bounded UTF-8 and each physical Kubernetes timestamp before object framing."""
    if len(raw) > MAX_LOG_BYTES:
        raise ValueError("trace log exceeds byte budget")
    lines = raw.decode("utf-8", errors="strict").splitlines()
    if len(lines) > MAX_LOG_LINES:
        raise ValueError("trace log exceeds line budget")
    result: list[tuple[datetime, str]] = []
    for raw_line in lines:
        prefix, separator, line = raw_line.partition(" ")
        if (
            not separator
            or re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?Z", prefix) is None
        ):
            raise ValueError("timestamped Kubernetes log line required")
        result.append((datetime.fromisoformat(prefix.replace("Z", "+00:00")), line))
    return tuple(result)


def parse_console_log(raw: bytes, scope: TraceScope) -> ParsedLog:
    """Frame multiline spans while retaining malformed and clipped candidate counts."""
    lines = _log_lines(raw)
    spans: list[CapturedSpan] = []
    candidate: list[str] = []
    started: datetime | None = None
    previous: datetime | None = None
    invalid_timestamps = False
    partial = malformed = excluded = size = 0
    for emitted, line in lines:
        if line == "{":
            partial += bool(candidate)
            candidate, started, size = [line], emitted, 1
            previous, invalid_timestamps = emitted, False
            continue
        if not candidate:
            partial += line.lstrip().startswith(('"', "}", "]"))
            continue
        candidate.append(line)
        assert previous is not None
        invalid_timestamps |= emitted < previous
        previous = emitted
        size += len(line.encode()) + 1
        if size > 8192:
            raise ValueError("span object exceeds byte budget")
        if line != "}":
            continue
        text = "\n".join(candidate)
        try:
            if invalid_timestamps:
                raise ValueError("unordered physical log timestamps")
            _depth(text)
            span = _project(text)
            span.check_service(scope.service)
            assert started is not None
            captured = CapturedSpan(span=span, log_start=started, log_end=emitted)
            if scope.start <= span.start_time <= span.end_time <= scope.end:
                spans.append(captured)
                if len(spans) > MAX_SPANS:
                    raise OverflowError("trace span count exceeds budget")
            else:
                excluded += 1
        except ValueError:
            malformed += 1
        candidate, started, size = [], None, 0
    return ParsedLog(
        spans=tuple(spans),
        raw_bytes=len(raw),
        line_count=len(lines),
        partial_candidates=partial + bool(candidate),
        malformed_candidates=malformed,
        excluded_spans=excluded,
        limit_reached=len(raw) == MAX_LOG_BYTES or len(lines) == MAX_LOG_LINES,
    )


class TraceLog(Immutable):
    """One sanitized LOG source records an identity-pinned read and accepted source projections."""

    transformation: Literal["trace-console-log-v1"] = "trace-console-log-v1"
    scope: TraceScope
    identity: PodIdentity
    captured_start: AwareDatetime
    captured_end: AwareDatetime
    parsed: ParsedLog

    @model_validator(mode="after")
    def consistent(self) -> Self:
        """Retained source records must remain scoped and cannot acquire new event timestamps."""
        if self.captured_start > self.captured_end or self.scope.end > self.captured_end:
            raise ValueError("invalid trace capture interval")
        for record in self.parsed.spans:
            record.span.check_service(self.scope.service)
            if not (
                self.scope.start <= record.span.start_time <= record.span.end_time <= self.scope.end
                and record.span.end_time <= record.log_start <= record.log_end <= self.captured_end
            ):
                raise ValueError("span lies outside retained capture scope")
        return self


class TraceSpan(Immutable):
    """Derived trace evidence is diagnostic, with one direct source and a recomputable duration."""

    transformation: Literal["trace-console-span-v1"] = "trace-console-span-v1"
    diagnostic_only: Literal[True] = True
    scope: TraceScope
    source: EvidenceItem
    ordinal: int = Field(ge=0, lt=MAX_SPANS)
    trace_id: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{32}$")]
    span_id: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{16}$")]
    parent_id: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{16}$")] | None
    duration_microseconds: int = Field(ge=0)
    record: CapturedSpan


def publish_trace_log(capture: TraceLog, store: ArtifactStore) -> EvidenceItem:
    """Persist only sanitized span fields; arbitrary attributes and exception text are excluded."""
    observation = Observation(
        source="LOG",
        resource=capture.scope.service,
        observed_at=capture.captured_end,
        query=LOG_QUERY,
        summary=f"Bounded console span sample: {len(capture.parsed.spans)} records",
        payload=JSON_OBJECT.validate_json(capture.model_dump_json()),
    )
    return normalize(
        observation, capture.scope.incident_id, capture.captured_start, capture.captured_end, store
    )


def _source(item: EvidenceItem, store: ArtifactStore) -> TraceLog:
    """Exactly one direct LOG record is allowed; nested TRACE sources are never traversed."""
    raw = store.verify(item).get("payload")
    capture = TraceLog.model_validate_json(json.dumps(raw, allow_nan=False))
    if (
        item.source != "LOG"
        or item.query != LOG_QUERY
        or item.incident_id != capture.scope.incident_id
        or item.resource != capture.scope.service
        or item.observed_at != capture.captured_end
        or item.observed_at > item.collected_at
        or item.summary != f"Bounded console span sample: {len(capture.parsed.spans)} records"
    ):
        raise EvidenceIntegrityError("trace LOG source metadata disagrees")
    return capture


def verify_trace_log(item: EvidenceItem, store: ArtifactStore) -> TraceLog:
    """Expose diagnostic source validation to context and checkpoint consumers."""
    return _source(item, store)


def _derive(source: EvidenceItem, ordinal: int, store: ArtifactStore) -> TraceSpan:
    """The selected source ordinal determines every derived field independently of outer bytes."""
    capture = _source(source, store)
    if isinstance(ordinal, bool) or not 0 <= ordinal < len(capture.parsed.spans):
        raise EvidenceIntegrityError("trace source ordinal is unavailable")
    record = capture.parsed.spans[ordinal]
    span = record.span
    delta = span.end_time - span.start_time
    return TraceSpan(
        scope=capture.scope,
        source=source,
        ordinal=ordinal,
        record=record,
        trace_id=span.trace_id[2:],
        span_id=span.span_id[2:],
        parent_id=span.parent_id[2:] if span.parent_id is not None else None,
        duration_microseconds=(delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds,
    )


def derive_trace_span(source: EvidenceItem, ordinal: int, store: ArtifactStore) -> EvidenceItem:
    """Keep actual span end as observation time; collecting old logs cannot refresh it."""
    derived = _derive(source, ordinal, store)
    observation = Observation(
        source="TRACE",
        resource=derived.scope.service,
        observed_at=derived.record.span.end_time,
        query=TRACE_QUERY,
        summary=f"{derived.record.span.name}: {derived.duration_microseconds} us; "
        f"status {derived.record.span.status_code}; bounded sample",
        payload=JSON_OBJECT.validate_json(derived.model_dump_json()),
    )
    return normalize(
        observation, derived.scope.incident_id, derived.scope.start, derived.scope.end, store
    )


def verify_trace_span(item: EvidenceItem, store: ArtifactStore) -> TraceSpan:
    """Reverify the complete direct source and recompute identity, timestamps and duration."""
    derived = TraceSpan.model_validate_json(
        json.dumps(store.verify(item).get("payload"), allow_nan=False)
    )
    expected = _derive(derived.source, derived.ordinal, store)
    if (
        derived != expected
        or item.source != "TRACE"
        or item.query != TRACE_QUERY
        or item.incident_id != expected.scope.incident_id
        or item.resource != expected.scope.service
        or item.observed_at != expected.record.span.end_time
        or item.observed_at > item.collected_at
        or expected.source.collected_at > item.collected_at
        or item.summary
        != (
            f"{expected.record.span.name}: {expected.duration_microseconds} us; "
            f"status {expected.record.span.status_code}; bounded sample"
        )
    ):
        raise EvidenceIntegrityError("derived trace disagrees with direct LOG lineage")
    return expected
