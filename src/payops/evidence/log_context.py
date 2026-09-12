"""Parse structured operational log records without interpreting text as instructions."""

import json

from pydantic import JsonValue

from payops.evidence.artifacts import JSON_OBJECT


def log_context(payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Preserve every line as a parsed record or raw text, including its outer timestamp."""
    lines = payload.get("lines")
    if not isinstance(lines, str):
        return payload
    records: list[JsonValue] = []
    unparsed: list[JsonValue] = []
    for line in lines.splitlines():
        timestamp, _, content = line.partition(" ")
        candidate = line if line.startswith("{") else content
        try:
            parsed = JSON_OBJECT.validate_python(
                json.loads(
                    candidate, object_pairs_hook=unique_fields, parse_constant=invalid_constant
                )
            )
        except (ValueError, RecursionError):
            unparsed.append(line)
            continue
        records.append(
            {"record": parsed, "log_timestamp": None if line.startswith("{") else timestamp}
        )
    return {
        **{key: value for key, value in payload.items() if key != "lines"},
        "records": records,
        "unparsed_lines": unparsed,
        "projection": "structured_log_lines_v1",
    }


def unique_fields(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    """Duplicate source fields remain raw text rather than becoming authoritative predicates."""
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate structured log field")
        result[key] = value
    return result


def invalid_constant(value: str) -> None:
    """Nonfinite log extensions cannot supply diagnostic measurements."""
    raise ValueError("nonfinite structured log value")
