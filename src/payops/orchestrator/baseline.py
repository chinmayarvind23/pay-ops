"""Limited deterministic diagnosis from verified runtime evidence, without scenario labels."""

import re
from dataclasses import dataclass
from datetime import datetime

from pydantic import JsonValue

from payops.contracts import EvidenceItem, RootCauseHypothesis
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError

MAX_AGE_SECONDS = 120
CONFIG_HEADER = re.compile(
    r"^pydantic_core\._pydantic_core\.ValidationError: "
    r"[1-9][0-9]* validation errors? for SandboxConfig$"
)
CONFIG_DETAIL = re.compile(
    r"^\s*Value error, destination must be an approved synthetic service origin \[type=value_error,"
)
ACCESS_FAILURE = re.compile(
    r'^INFO:\s+\S+:[0-9]+ - "POST /simulate HTTP/1\.[01]" 503(?: Service Unavailable)?$'
)


@dataclass(frozen=True)
class Signal:
    """The payload is used only after digest, metadata and incident checks succeed."""

    item: EvidenceItem
    payload: dict[str, JsonValue]


def _object(value: JsonValue) -> dict[str, JsonValue]:
    """Malformed optional signal structure contributes no diagnosis rather than a healthy zero."""
    return value if isinstance(value, dict) else {}


def _objects(value: JsonValue) -> tuple[dict[str, JsonValue], ...]:
    """Only actual object entries can satisfy structured Kubernetes predicates."""
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _verified(evidence: tuple[EvidenceItem, ...], store: ArtifactStore) -> tuple[Signal, ...]:
    """Verify even stale items so freshness filtering cannot conceal corrupt input."""
    if len({item.incident_id for item in evidence}) > 1:
        raise EvidenceIntegrityError("baseline evidence crosses incidents")
    if len({item.evidence_id for item in evidence}) != len(evidence):
        raise EvidenceIntegrityError("baseline evidence IDs are duplicated")
    latest = max((item.collected_at for item in evidence), default=None)
    signals: list[Signal] = []
    for item in evidence:
        payload = store.verify(item).get("payload")
        if not isinstance(payload, dict):
            raise EvidenceIntegrityError("baseline artifact payload is not an object")
        age = (latest - item.observed_at).total_seconds() if latest is not None else 0
        if 0 <= age <= MAX_AGE_SECONDS and item.observed_at <= item.collected_at:
            signals.append(Signal(item, payload))
    return tuple(signals)


def _payment_pod(signal: Signal) -> bool:
    """Collector-validated pod names retain service ownership without needing fault labels."""
    return (
        signal.item.source == "KUBERNETES"
        and signal.payload.get("kind") == "Pod"
        and re.fullmatch(r"payments-api-[a-z0-9]+-[a-z0-9]+", signal.item.resource) is not None
    )


def _crashed(signal: Signal) -> bool:
    """Repeated non-OOM process termination supports the baseline's limited startup category."""
    if not _payment_pod(signal):
        return False
    statuses = _objects(_object(signal.payload.get("status")).get("containerStatuses"))
    for status in statuses:
        terminated = _object(_object(status.get("lastState")).get("terminated"))
        code, restarts = terminated.get("exitCode"), status.get("restartCount")
        if (
            type(code) is int
            and code != 0
            and type(restarts) is int
            and restarts > 0
            and terminated.get("reason") != "OOMKilled"
            and status.get("ready") is not True
        ):
            return True
    return False


def _running_unready(signal: Signal) -> bool:
    """A running but unavailable process differs from a startup crash or unscheduled pod."""
    if not _payment_pod(signal):
        return False
    statuses = _objects(_object(signal.payload.get("status")).get("containerStatuses"))
    return any(
        "running" in _object(status.get("state")) and status.get("ready") is False
        for status in statuses
    )


def _degraded(signal: Signal) -> bool:
    """Current deployment unavailability prevents old restarts alone from proving a cause."""
    if signal.item.source != "DEPLOYMENT" or signal.item.resource != "payments-api":
        return False
    desired = signal.payload.get("replicas")
    status = _object(signal.payload.get("status"))
    available = status.get("availableReplicas")
    conditions = _objects(status.get("conditions"))
    unavailable = any(
        item.get("type") == "Available" and item.get("status") == "False" for item in conditions
    )
    return (
        signal.payload.get("kind") == "Deployment"
        and type(desired) is int
        and desired > 0
        and ((type(available) is int and available < desired) or unavailable)
    )


def _timestamp(value: str) -> datetime | None:
    """Only timezone-aware source timestamps can establish freshness in replayed evidence."""
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return result if result.tzinfo is not None else None


def _log_lines(signal: Signal) -> tuple[str, ...]:
    """Evaluate timestamped raw lines; summaries and undated prose cannot create support."""
    if signal.item.source != "LOG" or signal.item.resource != "payments-api":
        return ()
    raw = signal.payload.get("lines")
    if not isinstance(raw, str):
        return ()
    lines: list[str] = []
    for line in raw.splitlines():
        stamp, separator, content = line.partition(" ")
        observed = _timestamp(stamp) if separator else None
        if observed is not None:
            age = (signal.item.collected_at - observed).total_seconds()
            if 0 <= age <= MAX_AGE_SECONDS:
                lines.append(content)
    return tuple(lines)


def _invalid_configuration(signal: Signal) -> bool:
    """Recognize an actual schema exception and adjacent diagnostic, not arbitrary mentions."""
    lines = _log_lines(signal)
    return any(
        CONFIG_HEADER.fullmatch(line)
        and any(CONFIG_DETAIL.match(detail) for detail in lines[index + 1 : index + 4])
        for index, line in enumerate(lines)
    )


def _readiness_event(signal: Signal, pod_uids: set[str]) -> bool:
    """Join the current pod UID and event time before accepting a readiness-404 event."""
    if signal.item.source != "KUBERNETES" or signal.item.resource != "payments-api":
        return False
    involved = _object(signal.payload.get("involvedObject"))
    series = _object(signal.payload.get("series"))
    event_time = (
        series.get("lastObservedTime")
        or signal.payload.get("lastTimestamp")
        or signal.payload.get("eventTime")
    )
    stamp = _timestamp(event_time) if isinstance(event_time, str) else None
    return (
        str(involved.get("uid")) in pod_uids
        and involved.get("namespace") == "payops-sandbox"
        and signal.payload.get("reason") == "Unhealthy"
        and signal.payload.get("type") == "Warning"
        and signal.payload.get("message")
        == "Readiness probe failed: HTTP probe failed with statuscode: 404"
        and stamp is not None
        and abs((signal.item.observed_at - stamp).total_seconds()) <= 1
    )


def _processor_absent(signal: Signal) -> bool:
    """An explicit zero is meaningful; omitted counters or replicas remain missing evidence."""
    return (
        signal.item.source == "DEPLOYMENT"
        and signal.item.resource == "processor-adapter"
        and signal.payload.get("kind") == "Deployment"
        and type(signal.payload.get("replicas")) is int
        and signal.payload.get("replicas") == 0
    )


def _no_processor_pods(signal: Signal) -> bool:
    """An empty current pod query independently corroborates the zero-replica configuration."""
    return (
        signal.item.source == "KUBERNETES"
        and signal.item.resource == "processor-adapter"
        and signal.item.query == "pods.count"
        and type(signal.payload.get("pod_count")) is int
        and signal.payload.get("pod_count") == 0
    )


def _hypothesis(code: str, score: float, signals: tuple[Signal, ...]) -> RootCauseHypothesis:
    """Confidence is an uncalibrated rule-ranking score, never a measured probability."""
    return RootCauseHypothesis(
        cause_code=code,
        confidence=score,
        supporting_evidence_ids=tuple(dict.fromkeys(signal.item.evidence_id for signal in signals)),
    )


def rank_evidence(
    evidence: tuple[EvidenceItem, ...], store: ArtifactStore
) -> tuple[RootCauseHypothesis, ...]:
    """Correlate a limited set of runtime predicates and abstain when corroboration is absent."""
    signals = _verified(evidence, store)
    degraded = tuple(signal for signal in signals if _degraded(signal))
    crashes = tuple(signal for signal in signals if _crashed(signal))
    config = tuple(signal for signal in signals if _invalid_configuration(signal))
    unready = tuple(signal for signal in signals if _running_unready(signal))
    pod_uids = {
        str(signal.payload["resource_uid"])
        for signal in unready
        if isinstance(signal.payload.get("resource_uid"), str)
    }
    probes = tuple(signal for signal in signals if _readiness_event(signal, pod_uids))
    absent = tuple(signal for signal in signals if _processor_absent(signal))
    empty = tuple(signal for signal in signals if _no_processor_pods(signal))
    errors = tuple(
        signal
        for signal in signals
        if any(ACCESS_FAILURE.fullmatch(line) for line in _log_lines(signal))
    )
    ranked: list[RootCauseHypothesis] = []
    if degraded and crashes and config:
        ranked.append(_hypothesis("INVALID_CONFIGURATION", 0.95, degraded + crashes + config))
    if degraded and unready and probes:
        ranked.append(_hypothesis("READINESS_PROBE_FAILURE", 0.90, degraded + unready + probes))
    if absent and empty and errors:
        ranked.append(_hypothesis("PROCESSOR_UNAVAILABLE", 0.85, absent + empty + errors))
    if degraded and crashes:
        ranked.append(_hypothesis("STARTUP_FAILURE", 0.75, degraded + crashes))
    return tuple(ranked)
