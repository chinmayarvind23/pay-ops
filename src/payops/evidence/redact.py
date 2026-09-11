"""Conservative common-secret redaction before persistence or model submission."""

import re

from pydantic import JsonValue

SENSITIVE_KEY = re.compile(r"password|secret|token|authorization|cookie|api.?key|credential", re.I)
BEARER = re.compile(r"(?i)\bBearer\s+\S+")
ASSIGNMENT = re.compile(
    r"""(?i)\b(password|secret|token|api[_-]?key)\s*[=:]\s*(?:"[^"]*"|'[^']*'|[^\s,;]+)"""
)


def redact_text(text: str) -> str:
    """Retain incident meaning without copying common credential formats into context."""
    return ASSIGNMENT.sub(r"\1=[REDACTED]", BEARER.sub("Bearer [REDACTED]", text))


def redact(value: JsonValue, depth: int = 0) -> JsonValue:
    """Bound recursive telemetry input; this is not a universal personal-data detector."""
    if depth > 16:
        raise ValueError("telemetry nesting exceeds budget")
    if isinstance(value, dict):
        name = value.get("name")
        sensitive_name = isinstance(name, str) and SENSITIVE_KEY.search(name) is not None
        return {
            key: "[REDACTED]"
            if SENSITIVE_KEY.search(key) or (sensitive_name and key in {"value", "valueFrom"})
            else redact(item, depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item, depth + 1) for item in value]
    return redact_text(value) if isinstance(value, str) else value
