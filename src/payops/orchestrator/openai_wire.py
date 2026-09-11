"""Bounded raw Responses JSON is validated before any LangChain message conversion."""

import json
from math import isfinite

from pydantic import JsonValue

from payops.orchestrator.reasoning import ProviderUsage, ReasoningDecision, unique_keys

MODEL = "gpt-5.4-mini-2026-03-17"
REFUSAL = ReasoningDecision(
    decision="refuse", summary="Provider declined this request.", reads=(), hypotheses=()
).model_dump_json()


def _finite(value: str) -> float:
    """Overflow in otherwise valid JSON floats is also nonfinite input."""
    number = float(value)
    if not isfinite(number):
        raise ValueError("nonfinite provider JSON")
    return number


def _constant(value: str) -> None:
    """Reject NaN and Infinity JSON extensions in any retained or ignored field."""
    raise ValueError("nonfinite provider JSON")


def _depth(value: JsonValue, level: int = 0) -> None:
    """The byte cap bounds total work; nesting has a separate small maximum."""
    if level > 24:
        raise ValueError("provider JSON nesting exceeds bound")
    if isinstance(value, dict):
        for child in value.values():
            _depth(child, level + 1)
    elif isinstance(value, list):
        for child in value:
            _depth(child, level + 1)


def object_value(value: JsonValue) -> dict[str, JsonValue]:
    """Object checks do not coerce arrays, numeric strings or boolean counters."""
    if not isinstance(value, dict):
        raise ValueError("provider object required")
    return value


def decode(raw: bytes, limit: int) -> dict[str, JsonValue]:
    """Decode strict UTF-8 under byte, duplicate-key, finite-number and nesting bounds."""
    if len(raw) > limit:
        raise ValueError("provider response exceeds byte bound")
    try:
        value: JsonValue = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=unique_keys,
            parse_constant=_constant,
            parse_float=_finite,
        )
        _depth(value)
        return object_value(value)
    except (RecursionError, UnicodeError):
        raise ValueError("invalid provider JSON") from None


def count(value: JsonValue) -> int:
    """Billable counters are exact nonnegative JSON integers, never SDK-coerced values."""
    if type(value) is not int or not 0 <= value <= 10_000_000:
        raise ValueError("invalid provider counter")
    return value


def server_count(value: dict[str, JsonValue], ceiling: int) -> int:
    """The count endpoint cannot authorize a generation outside its reserved input ceiling."""
    if set(value) != {"object", "input_tokens"} or value["object"] != "response.input_tokens":
        raise ValueError("invalid provider count envelope")
    result = count(value["input_tokens"])
    if result > ceiling:
        raise ValueError("provider count exceeds input reservation")
    return result


def _details(value: JsonValue, supported: str) -> int:
    """Unknown positive detail categories cannot disappear from billing projections."""
    details = object_value(value)
    counters = {key: count(number) for key, number in details.items()}
    if any(number != 0 for key, number in counters.items() if key != supported):
        raise ValueError("unsupported provider billing category")
    return counters.get(supported, 0)


def raw_usage(value: JsonValue, measured: int, output_limit: int) -> ProviderUsage | None:
    """Missing usage remains unknown; all present counters are checked before projection."""
    if value is None:
        return None
    raw = object_value(value)
    if set(raw) != {
        "input_tokens", "output_tokens", "total_tokens",
        "input_tokens_details", "output_tokens_details",
    }:
        raise ValueError("invalid provider usage fields")
    cached = _details(raw["input_tokens_details"], "cached_tokens")
    reasoning = _details(raw["output_tokens_details"], "reasoning_tokens")
    usage = ProviderUsage(
        input_tokens=count(raw["input_tokens"]),
        output_tokens=count(raw["output_tokens"]),
        total_tokens=count(raw["total_tokens"]),
        cached_input_tokens=cached,
    )
    if (
        usage.input_tokens != measured
        or usage.output_tokens > output_limit
        or reasoning > usage.output_tokens
    ):
        raise ValueError("provider usage disagrees with request bounds")
    return usage


def response_text(value: dict[str, JsonValue]) -> tuple[str, bool]:
    """Only one completed assistant text or explicit refusal can enter the decision parser."""
    if (
        value.get("object") != "response"
        or value.get("model") != MODEL
        or value.get("service_tier") != "default"
        or value.get("status") != "completed"
        or value.get("error") is not None
        or value.get("incomplete_details") is not None
    ):
        raise ValueError("provider response is not a completed bound model result")
    output = value.get("output")
    if not isinstance(output, list) or len(output) != 1:
        raise ValueError("ambiguous provider output")
    message = object_value(output[0])
    if (
        message.get("type") != "message"
        or message.get("role") != "assistant"
        or message.get("status") != "completed"
    ):
        raise ValueError("unsupported provider output item")
    content = message.get("content")
    if not isinstance(content, list) or len(content) != 1:
        raise ValueError("ambiguous provider content")
    block = object_value(content[0])
    refusal = block.get("type") == "refusal"
    text = block.get("refusal" if refusal else "text")
    if (
        block.get("type") not in {"refusal", "output_text"}
        or not isinstance(text, str)
        or not text
        or len(text.encode("utf-8")) > 16384
    ):
        raise ValueError("unsupported or oversized provider text")
    return (REFUSAL if refusal else text), refusal
