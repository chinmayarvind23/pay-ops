"""The published generation schema rejects known invalid decisions before semantic parsing."""

import json
from typing import Any, Protocol, cast

import pytest
from jsonschema import Draft202012Validator

from payops.orchestrator.local_schema import LOCAL_DECISION, final_prompt, generation_schema
from payops.orchestrator.reasoning import ReasoningDecision


class Validator(Protocol):
    """Use jsonschema's supported public call without its deprecated untyped overload."""

    def is_valid(self, instance: object) -> bool:
        """Check one instance against the schema already bound to the validator."""
        ...


def decision(kind: str, tool: str | None = None, query: str | None = None) -> dict[str, Any]:
    """Use real wire fields and no model/provider fixture to test both validation layers."""
    return {
        "decision": kind,
        "summary": "Need scoped evidence",
        "reads": []
        if tool is None
        else [{"tool": tool, "service": "payments-api", "query": query}],
        "hypotheses": [],
    }


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        (decision("read", "workload_status"), True),
        (decision("read", "pod_events"), True),
        (decision("read", "recent_logs"), True),
        (decision("read", "payment_snapshot"), True),
        (decision("read", "runbook_search", "processor latency"), True),
        (decision("read", "incident_search", "cache unavailable"), True),
        (decision("finish"), True),
        (decision("refuse"), True),
        (decision("read", "workload_status", "kubernetes.status-snapshot"), False),
        (decision("read", "runbook_search"), False),
        (decision("read", "incident_search", ""), False),
        (decision("read"), False),
        (decision("finish", "pod_events"), False),
        (decision("refuse", "pod_events"), False),
        (decision("read", "execute_shell"), False),
        ({**decision("finish"), "approve": True}, False),
    ],
)
def test_schema_and_typed_contract_agree(value: dict[str, Any], valid: bool) -> None:
    """JSON Schema and typed local contracts agree on every positive and adversarial vector."""
    schema = generation_schema()
    Draft202012Validator.check_schema(schema)
    assert cast(Validator, Draft202012Validator(schema)).is_valid(value) is valid
    typed_schema = LOCAL_DECISION.json_schema()
    assert cast(Validator, Draft202012Validator(typed_schema)).is_valid(value) is valid
    if valid:
        parsed = LOCAL_DECISION.validate_python(value)
        canonical = ReasoningDecision.model_validate(parsed.model_dump())
        assert canonical.model_dump(mode="json") == value
    else:
        with pytest.raises(ValueError):
            LOCAL_DECISION.validate_python(value)


def test_schema_has_no_references_or_shared_mutable_state() -> None:
    """A caller cannot mutate the next request's schema or leave nested references for llama.cpp."""
    first = generation_schema()
    assert '"$ref"' not in json.dumps(first) and '"$defs"' not in json.dumps(first)
    first.clear()
    assert generation_schema()


def test_terminal_generation_excludes_reads() -> None:
    """A last-turn schema can finish or refuse but cannot request another backend operation."""
    schema = generation_schema(final_only=True)
    Draft202012Validator.check_schema(schema)
    validator = cast(Validator, Draft202012Validator(schema))
    assert validator.is_valid(decision("finish"))
    assert validator.is_valid(decision("refuse"))
    assert not validator.is_valid(decision("read", "workload_status"))


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('{"allowed_decisions":["finish","refuse"]}', True),
        ('{"context":{"allowed_decisions":["finish","refuse"]}}', False),
        ('{"allowed_decisions":["read","finish","refuse"]}', False),
        ("Facts", False),
        ("[]", False),
    ],
)
def test_only_host_terminal_marker_narrows_generation(text: str, expected: bool) -> None:
    """Untrusted nested source data cannot masquerade as the host's terminal-turn marker."""
    assert final_prompt(text) is expected
