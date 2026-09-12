"""Disjoint local generation shapes encode rules that Pydantic validators alone cannot expose."""

from typing import Annotated, Literal

from pydantic import Field, JsonValue, TypeAdapter

from payops.contracts import Contract
from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.payment_window import Service
from payops.orchestrator.reasoning import RankedCause

type LocalBrief = Annotated[str, Field(min_length=1, max_length=80)]


class LocalCause(RankedCause):
    """One checked citation is enough for a compact local diagnosis; refutation is not inferred."""

    supporting_evidence_ids: tuple[str, ...] = Field(min_length=1, max_length=1)
    refuting_evidence_ids: tuple[str, ...] = Field(max_length=0)


class FixedRead(Contract):
    """Fixed operational readers never accept model-generated query text."""

    tool: Literal["workload_status", "pod_events", "recent_logs", "payment_snapshot"]
    service: Service
    query: None


class SearchRead(Contract):
    """Only scoped retrieval accepts bounded plain search text."""

    tool: Literal["runbook_search", "incident_search"]
    service: Service
    query: Annotated[str, Field(min_length=1, max_length=240)]


type LocalRead = FixedRead | SearchRead


class ReadDecision(Contract):
    """A read decision cannot simultaneously publish a diagnosis."""

    decision: Literal["read"]
    summary: LocalBrief
    reads: tuple[LocalRead, ...] = Field(min_length=1, max_length=2)
    hypotheses: tuple[RankedCause, ...] = Field(max_length=0)


class FinishDecision(Contract):
    """A compact local finish publishes one checked mechanism or abstains."""

    decision: Literal["finish"]
    summary: LocalBrief
    reads: tuple[LocalRead, ...] = Field(max_length=0)
    hypotheses: tuple[LocalCause, ...] = Field(max_length=1)


class RefuseDecision(Contract):
    """Refusal is terminal and grants neither reads nor a diagnosis."""

    decision: Literal["refuse"]
    summary: LocalBrief
    reads: tuple[LocalRead, ...] = Field(max_length=0)
    hypotheses: tuple[RankedCause, ...] = Field(max_length=0)


type LocalDecision = ReadDecision | FinishDecision | RefuseDecision
LOCAL_DECISION: TypeAdapter[LocalDecision] = TypeAdapter(LocalDecision)
FINAL_DECISION: TypeAdapter[FinishDecision | RefuseDecision] = TypeAdapter(
    FinishDecision | RefuseDecision
)


def _inline(value: JsonValue, definitions: dict[str, JsonValue]) -> JsonValue:
    """Inline generated local definitions to avoid the pinned converter's nested-ref gap."""
    if isinstance(value, list):
        return [_inline(item, definitions) for item in value]
    if not isinstance(value, dict):
        return value
    reference = value.get("$ref")
    if isinstance(reference, str):
        if not reference.startswith("#/$defs/") or len(value) != 1:
            raise ValueError("unsupported local schema reference")
        return _inline(definitions[reference.removeprefix("#/$defs/")], definitions)
    return {key: _inline(item, definitions) for key, item in value.items() if key != "$defs"}


def generation_schema(*, final_only: bool = False) -> dict[str, JsonValue]:
    """Constrain generation shape; Python still validates uniqueness, bounds and citations."""
    adapter = FINAL_DECISION if final_only else LOCAL_DECISION
    schema = JSON_OBJECT.validate_python(adapter.json_schema())
    definitions = JSON_OBJECT.validate_python(schema["$defs"])
    return JSON_OBJECT.validate_python(_inline(schema, definitions))


def final_prompt(content: str) -> bool:
    """Only the host's top-level terminal-turn marker narrows generation; source text cannot."""
    try:
        data = JSON_OBJECT.validate_json(content)
    except (ValueError, RecursionError):
        return False
    return data.get("allowed_decisions") == ["finish", "refuse"]
