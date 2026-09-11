"""Full durable investigation state; operational authority never comes from checkpoint text."""

from datetime import timedelta
from typing import Literal, TypedDict

from pydantic import AwareDatetime, Field

from payops.contracts import (
    Contract,
    EvidenceItem,
    Incident,
    IncidentReport,
    RootCauseHypothesis,
    TerminalState,
    utc_now,
)
from payops.tools.collect import CollectionFailure

Phase = Literal["RECEIVED", "TRIAGED", "READS_RESERVED", "EVIDENCE_COLLECTED", "RANKED", "FINISHED"]


class InvestigationBudget(Contract):
    """Attempt allowances and the node-start cutoff survive restart without replenishment."""

    max_steps: int = Field(default=4, ge=0, le=16)
    max_tool_calls: int = Field(default=20, ge=0, le=40)
    node_start_deadline: AwareDatetime = Field(
        default_factory=lambda: utc_now() + timedelta(minutes=10)
    )


class StepRecord(Contract):
    """Wall time correlates steps; monotonic elapsed time measures completed work only."""

    node: str
    observed_at: AwareDatetime
    duration_seconds: float = Field(ge=0, allow_inf_nan=False)


class InvestigationState(Contract):
    """Every field is serialized, validated on load and scoped to the original incident."""

    incident: Incident
    budget: InvestigationBudget
    mode: Literal["local_kind", "fixture_replay"] = "local_kind"
    phase: Phase = "RECEIVED"
    started_at: AwareDatetime = Field(default_factory=utc_now)
    steps_used: int = Field(default=0, ge=0, le=16)
    tool_calls_reserved: int = Field(default=0, ge=0, le=40)
    evidence: tuple[EvidenceItem, ...] = ()
    failures: tuple[CollectionFailure, ...] = ()
    hypotheses: tuple[RootCauseHypothesis, ...] = ()
    terminal: TerminalState | None = None
    steps: tuple[StepRecord, ...] = ()
    report: IncidentReport | None = None


class Envelope(TypedDict):
    """JSON-only checkpoint values avoid reconstructing arbitrary application objects."""

    state_json: str


def pack(state: InvestigationState) -> Envelope:
    """Use strict contract JSON at every graph boundary, including successful node output."""
    return {"state_json": state.model_dump_json()}


def unpack(value: Envelope) -> InvestigationState:
    """Stored state is revalidated before influencing a transition or report."""
    return InvestigationState.model_validate_json(value["state_json"])
