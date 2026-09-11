"""Validated public contracts shared by tools, API and evaluation."""

from datetime import UTC, datetime
from typing import Annotated, Literal, Self
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator

Identifier = Annotated[str, StringConstraints(min_length=1, max_length=128, pattern=r"^[\w.-]+$")]
Summary = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=4000)]
Source = Literal[
    "KUBERNETES", "PROMETHEUS", "LOG", "TRACE", "DEPLOYMENT", "PAYMENT", "RUNBOOK", "MEMORY"
]
TerminalState = Literal[
    "ESCALATED",
    "RESOLVED",
    "RESOLVED_WITHOUT_ACTION",
    "SECURITY_BLOCK",
    "EVIDENCE_INSUFFICIENT",
    "BUDGET_EXHAUSTED",
    "DEPENDENCY_UNAVAILABLE",
    "ACTION_FAILED",
    "UNRECOVERABLE",
]


def utc_now() -> datetime:
    """Use aware wall-clock time for correlation, never for latency measurement."""
    return datetime.now(UTC)


def new_id() -> str:
    """Opaque IDs avoid embedding customer information in audit references."""
    return str(uuid4())


class Contract(BaseModel):
    """Reject unknown fields and accidental mutation at trust boundaries."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class IncidentCreate(Contract):
    """An alert carries bounded context but no executable instructions."""

    title: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
    namespace: Identifier = "payops-sandbox"
    service: Identifier = "payments-api"
    severity: Literal["warning", "critical"] = "warning"


class EvidenceItem(Contract):
    """Normalized context retains exact source lineage for later attribution."""

    evidence_id: Identifier
    incident_id: Identifier
    source: Source
    observed_at: AwareDatetime
    collected_at: AwareDatetime
    query: Summary
    resource: Identifier
    artifact_uri: Summary
    artifact_sha256: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    summary: Summary
    untrusted_text: Literal[True] = True


class RootCauseHypothesis(Contract):
    """Confidence is a ranking score until independent calibration is measured."""

    cause_code: Identifier
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False)
    supporting_evidence_ids: tuple[Identifier, ...] = ()
    refuting_evidence_ids: tuple[Identifier, ...] = ()
    missing_evidence: tuple[Summary, ...] = ()


class IncidentReport(Contract):
    """A report cannot cite absent evidence or another incident's artifacts."""

    incident_id: Identifier
    evidence: tuple[EvidenceItem, ...] = ()
    ranked_root_causes: tuple[RootCauseHypothesis, ...] = ()
    terminal_state: TerminalState
    mode: Literal["mock", "fixture_replay", "local_kind", "cloud_gke"]
    duration_seconds: float = Field(ge=0, allow_inf_nan=False)
    trace_id: str = Field(default_factory=new_id)

    @model_validator(mode="after")
    def validate_citations(self) -> Self:
        """Grounding validation is deterministic and independent of model prose."""
        ids = {item.evidence_id for item in self.evidence}
        if len(ids) != len(self.evidence):
            raise ValueError("duplicate evidence IDs")
        if any(item.incident_id != self.incident_id for item in self.evidence):
            raise ValueError("cross-incident evidence")
        causes = [hypothesis.cause_code for hypothesis in self.ranked_root_causes]
        if len(causes) != len(set(causes)):
            raise ValueError("duplicate root causes")
        for hypothesis in self.ranked_root_causes:
            supports, refutes = (
                set(hypothesis.supporting_evidence_ids),
                set(hypothesis.refuting_evidence_ids),
            )
            if not (supports | refutes) <= ids:
                raise ValueError("unresolved evidence citation")
            if supports & refutes:
                raise ValueError("evidence cannot support and refute the same cause")
        return self


class Incident(Contract):
    """The durable incident is the authority; caches only derive from it."""

    incident_id: Identifier = Field(default_factory=new_id)
    request: IncidentCreate
    created_at: AwareDatetime = Field(default_factory=utc_now)
    report: IncidentReport | None = None

    @model_validator(mode="after")
    def validate_report_scope(self) -> Self:
        """Persisted nested reports must retain the parent incident's identity."""
        if self.report is not None and self.report.incident_id != self.incident_id:
            raise ValueError("report belongs to a different incident")
        return self
