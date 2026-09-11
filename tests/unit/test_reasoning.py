"""Model-provided JSON cannot extend tool authority, invent support IDs or supply token prices."""

import json
from copy import deepcopy
from typing import Any

import pytest
from langchain_core.messages import AIMessage

from payops.orchestrator.reasoning import (
    ProviderUsage,
    ReasoningDecision,
    TextPrice,
    hypotheses,
    parse_decision,
    provider_usage,
)


def finished() -> dict[str, Any]:
    """Use an explicit fixture vocabulary and one submitted evidence ID."""
    return {
        "decision": "finish",
        "summary": "Observed support",
        "reads": [],
        "hypotheses": [
            {
                "cause_code": "dependency_unavailable",
                "confidence": 0.7,
                "supporting_evidence_ids": ["e1"],
                "refuting_evidence_ids": [],
                "missing_evidence": [],
            }
        ],
    }


def parse(value: dict[str, Any]) -> ReasoningDecision:
    """The host supplies known context IDs and cause vocabulary separately from model output."""
    return parse_decision(
        json.dumps(value), frozenset({"e1", "e2"}), frozenset({"dependency_unavailable"})
    )


@pytest.mark.parametrize("decision", ["finish", "refuse"])
def test_abstention_and_refusal_need_no_invented_cause(decision: str) -> None:
    """Insufficient evidence and refusal can end reasoning without inventing a supported cause."""
    result = parse(
        {"decision": decision, "summary": "Insufficient evidence", "reads": [], "hypotheses": []}
    )
    assert result.hypotheses == ()


def test_unknown_top_level_usage_is_rejected() -> None:
    """Unrecognized billable categories cannot disappear during metadata projection."""
    message = AIMessage.model_construct(
        content="",
        usage_metadata={
            "input_tokens": 100,
            "output_tokens": 20,
            "total_tokens": 120,
            "billable_other_tokens": 99,
        },
    )
    with pytest.raises(ValueError, match="unsupported provider usage field"):
        provider_usage(message)


def test_valid_final_has_only_submitted_citations() -> None:
    """Validated model ranking maps to public contracts without granting execution rights."""
    decision = parse(finished())
    assert hypotheses(decision)[0].supporting_evidence_ids == ("e1",)
    assert not hasattr(decision, "approval")


@pytest.mark.parametrize(
    "field,value",
    [
        ("cause_code", "invented"),
        ("confidence", True),
        ("confidence", "0.7"),
        ("supporting_evidence_ids", []),
        ("supporting_evidence_ids", ["e1", "e1"]),
        ("supporting_evidence_ids", ["foreign"]),
        ("refuting_evidence_ids", ["e1"]),
        ("refuting_evidence_ids", ["e2", "e2"]),
    ],
)
def test_invalid_cause_or_citation_denied(field: str, value: object) -> None:
    """Correctly shaped prose cannot rescue malformed or unresolved supporting evidence."""
    result = finished()
    result["hypotheses"][0][field] = value
    with pytest.raises(ValueError):
        parse(result)


@pytest.mark.parametrize(
    "tool,query",
    [
        ("recent_logs", None),
        ("workload_status", None),
        ("pod_events", None),
        ("payment_snapshot", None),
        ("runbook_search", "timeout"),
        ("incident_search", "timeout"),
    ],
)
def test_closed_read_vocabulary(tool: str, query: str | None) -> None:
    """Each tool accepts only its own required query shape."""
    result = {
        "decision": "read",
        "summary": "Need source evidence",
        "hypotheses": [],
        "reads": [{"tool": tool, "service": "payments-api", "query": query}],
    }
    decision = parse(result)
    assert decision.reads[0].tool == tool
    with pytest.raises(ValueError, match="not final"):
        hypotheses(decision)


@pytest.mark.parametrize(
    "change",
    [
        {"tool": "run_shell"},
        {"service": "foreign"},
        {"namespace": "production"},
        {"query": "arbitrary query"},
        {"approved": True},
    ],
)
def test_unknown_capability_or_scope_fields_denied(change: dict[str, object]) -> None:
    """Model requests cannot carry credentials, scope overrides or mutation verbs."""
    result = {
        "decision": "read",
        "summary": "Read",
        "hypotheses": [],
        "reads": [{"tool": "recent_logs", "service": "payments-api", "query": None, **change}],
    }
    with pytest.raises(ValueError):
        parse(result)


@pytest.mark.parametrize(
    "case",
    [
        "empty_read",
        "finish_read",
        "refuse_rank",
        "duplicate_read",
        "duplicate_rank",
        "missing_search",
    ],
)
def test_contradictory_decisions_denied(case: str) -> None:
    """One accepted turn has one action kind and no hidden duplicate dispatches."""
    result = finished()
    read = {"tool": "recent_logs", "service": "payments-api", "query": None}
    if case == "empty_read":
        result.update(decision="read", hypotheses=[])
    elif case == "finish_read":
        result["reads"] = [read]
    elif case == "refuse_rank":
        result["decision"] = "refuse"
    elif case == "duplicate_read":
        result.update(decision="read", hypotheses=[], reads=[read, deepcopy(read)])
    elif case == "duplicate_rank":
        result["hypotheses"] *= 2
    else:
        result.update(decision="read", hypotheses=[], reads=[{**read, "tool": "runbook_search"}])
    with pytest.raises(ValueError):
        parse(result)


@pytest.mark.parametrize(
    "content",
    [
        '{"decision":"finish","decision":"read"}',
        '{"confidence":NaN}',
        "{",
        "[]",
        " " * 16385,
        "[" * 2000 + "]" * 2000,
    ],
    ids=["duplicate", "nonfinite", "malformed", "array", "oversized", "deep"],
)
def test_model_output_byte_and_json_bounds(content: str) -> None:
    """Parsing fails without accepting duplicate keys or unbounded recursion."""
    with pytest.raises(ValueError):
        parse_decision(content, frozenset(), frozenset())


def test_usage_counts_cached_input_once_and_preserves_unknown() -> None:
    """Fixture rates verify integer arithmetic; these values are not a real provider price quote."""
    message = AIMessage(
        content="",
        usage_metadata={
            "input_tokens": 100,
            "output_tokens": 20,
            "total_tokens": 120,
            "input_token_details": {"cache_read": 30},
            "output_token_details": {"reasoning": 5},
        },
    )
    usage = provider_usage(message)
    assert usage is not None and usage.total_tokens == 120
    price = TextPrice(input_nano_usd=250, cached_input_nano_usd=25, output_nano_usd=2000)
    assert price.cost_nano_usd(usage) == 58_250
    assert provider_usage(AIMessage(content="")) is None
    assert price.cost_nano_usd(None) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"total_tokens": 1},
        {"cached_input_tokens": 101},
        {"input_tokens": True},
        {"output_tokens": -1},
        {"input_tokens": "100"},
    ],
)
def test_inconsistent_usage_is_not_billable_data(changes: dict[str, object]) -> None:
    """Malformed usage cannot masquerade as a cheaper model call."""
    with pytest.raises(ValueError):
        ProviderUsage.model_validate(
            {
                "input_tokens": 100,
                "output_tokens": 20,
                "total_tokens": 120,
                "cached_input_tokens": 0,
                **changes,
            }
        )


@pytest.mark.parametrize(
    "details",
    [
        {"input_token_details": {"audio": 1}},
        {"output_token_details": {"audio": 1}},
        {"output_token_details": {"reasoning": 21}},
        {"input_token_details": {"cache_read": -1}},
    ],
)
def test_unsupported_provider_categories_require_separate_pricing(
    details: dict[str, object],
) -> None:
    """Text-only pricing cannot silently bill audio or cache-write categories as ordinary tokens."""
    message = AIMessage.model_construct(
        content="",
        usage_metadata={
            "input_tokens": 100,
            "output_tokens": 20,
            "total_tokens": 120,
            **details,
        },
    )
    with pytest.raises(ValueError):
        provider_usage(message)
