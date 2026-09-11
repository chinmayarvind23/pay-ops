"""Strict model decisions and provider usage remain separate from operational authority."""

import json
from typing import Annotated, Literal, Self

from langchain_core.messages import AIMessage
from pydantic import Field, JsonValue, TypeAdapter, model_validator

from payops.contracts import Contract, Identifier, RootCauseHypothesis
from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.payment_window import Service

ToolName = Literal[
    "workload_status",
    "pod_events",
    "recent_logs",
    "payment_snapshot",
    "runbook_search",
    "incident_search",
]
Brief = Annotated[str, Field(min_length=1, max_length=600)]
Count = Annotated[int, Field(strict=True, ge=0, le=10_000_000)]


class ReadRequest(Contract):
    """The model selects a closed read and service; backend scope and credentials are absent."""

    tool: ToolName
    service: Service
    query: Annotated[str, Field(min_length=1, max_length=240)] | None

    @model_validator(mode="after")
    def query_scope(self) -> Self:
        """Only retrieval accepts search text; other tools have fixed queries."""
        if (self.tool in {"runbook_search", "incident_search"}) != (self.query is not None):
            raise ValueError("search text must match the selected read tool")
        return self


class RankedCause(Contract):
    """A reported cause needs actual support IDs; confidence remains an uncalibrated score."""

    cause_code: Identifier
    confidence: float = Field(strict=True, ge=0, le=1, allow_inf_nan=False)
    supporting_evidence_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=8)
    refuting_evidence_ids: tuple[Identifier, ...] = Field(max_length=8)
    missing_evidence: tuple[Brief, ...] = Field(max_length=4)


class ReasoningDecision(Contract):
    """A model can request evidence, finish or refuse, but cannot approve or execute an action."""

    decision: Literal["read", "finish", "refuse"]
    summary: Brief
    reads: tuple[ReadRequest, ...] = Field(max_length=2)
    hypotheses: tuple[RankedCause, ...] = Field(max_length=3)

    @model_validator(mode="after")
    def coherent(self) -> Self:
        """Conflicting instructions and duplicate reads cannot conceal extra work in one turn."""
        if (self.decision == "read") != bool(self.reads):
            raise ValueError("read decision must contain a bounded read request")
        if self.decision != "finish" and self.hypotheses:
            raise ValueError("only a finish decision may publish hypotheses")
        if len({read.model_dump_json() for read in self.reads}) != len(self.reads):
            raise ValueError("duplicate read requests")
        if len({cause.cause_code for cause in self.hypotheses}) != len(self.hypotheses):
            raise ValueError("duplicate ranked causes")
        return self


def unique_keys(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    """A duplicate JSON key cannot replace an earlier decision or scoped tool parameter."""
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate model output key")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    """JSON nonfinite extensions are not part of the structured model output contract."""
    raise ValueError("nonfinite model JSON")


def parse_decision(
    content: str, evidence_ids: frozenset[str], cause_codes: frozenset[str]
) -> ReasoningDecision:
    """Bound output bytes and resolve all citations against the exact submitted context IDs."""
    if len(content.encode("utf-8")) > 16_384:
        raise ValueError("model output exceeds byte budget")
    try:
        value = json.loads(content, object_pairs_hook=unique_keys, parse_constant=reject_constant)
    except RecursionError:
        raise ValueError("model output nesting exceeds parser bounds") from None
    result = ReasoningDecision.model_validate(value)
    for cause in result.hypotheses:
        supports, refutes = set(cause.supporting_evidence_ids), set(cause.refuting_evidence_ids)
        if cause.cause_code not in cause_codes or not (supports | refutes) <= evidence_ids:
            raise ValueError("model output contains an unresolved cause or citation")
        if (
            supports & refutes
            or len(supports) != len(cause.supporting_evidence_ids)
            or len(refutes) != len(cause.refuting_evidence_ids)
        ):
            raise ValueError("model output contains conflicting or duplicate citations")
    return result


def hypotheses(decision: ReasoningDecision) -> tuple[RootCauseHypothesis, ...]:
    """Only validated finish decisions translate to existing public report contracts."""
    if decision.decision != "finish":
        raise ValueError("decision is not final")
    return tuple(
        RootCauseHypothesis.model_validate(cause.model_dump()) for cause in decision.hypotheses
    )


class ProviderUsage(Contract):
    """Counts are provider-reported totals; cached input is a subset, never additional input."""

    input_tokens: Count
    output_tokens: Count
    total_tokens: Count
    cached_input_tokens: Count

    @model_validator(mode="after")
    def census(self) -> Self:
        """Inconsistent totals cannot enter cost or token-budget accounting."""
        if (
            self.input_tokens + self.output_tokens != self.total_tokens
            or self.cached_input_tokens > self.input_tokens
        ):
            raise ValueError("provider token counts disagree")
        return self


def provider_usage(message: AIMessage) -> ProviderUsage | None:
    """Validate projected metadata; adapters must also validate raw usage before SDK coercion."""
    if message.usage_metadata is None:
        return None
    raw = JSON_OBJECT.validate_python(message.usage_metadata)
    if set(raw) - {
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "input_token_details",
        "output_token_details",
    }:
        raise ValueError("unsupported provider usage field")
    details = TypeAdapter(dict[str, Count]).validate_python(raw.get("input_token_details", {}))
    output_details = TypeAdapter(dict[str, Count]).validate_python(
        raw.get("output_token_details", {})
    )
    # This first accounting path supports ordinary text plus cache reads only.
    if any(value != 0 for key, value in details.items() if key != "cache_read") or any(
        value != 0 for key, value in output_details.items() if key != "reasoning"
    ):
        raise ValueError("unsupported provider token category")
    usage = ProviderUsage.model_validate(
        {
            "input_tokens": raw["input_tokens"],
            "output_tokens": raw["output_tokens"],
            "total_tokens": raw["total_tokens"],
            "cached_input_tokens": details.get("cache_read", 0),
        }
    )
    if output_details.get("reasoning", 0) > usage.output_tokens:
        raise ValueError("provider reasoning token count exceeds total output")
    return usage


class TextPrice(Contract):
    """Trusted configuration supplies verified per-token nanodollar rates, never model output."""

    input_nano_usd: Count
    cached_input_nano_usd: Count
    output_nano_usd: Count

    def cost_nano_usd(self, usage: ProviderUsage | None) -> int | None:
        """Integer accounting avoids rounding until presentation; missing usage has unknown cost."""
        if usage is None:
            return None
        return (
            (usage.input_tokens - usage.cached_input_tokens) * self.input_nano_usd
            + usage.cached_input_tokens * self.cached_input_nano_usd
            + usage.output_tokens * self.output_nano_usd
        )
