"""Source-specific support checks are distinct from citation existence and model confidence."""

from pydantic import JsonValue

from payops.evidence.diagnostic_http import http_causes
from payops.evidence.diagnostic_slices import slice_causes
from payops.evidence.diagnostics import supported_causes


def support_index(
    observations: tuple[tuple[str, str, JsonValue], ...],
) -> dict[str, tuple[str, ...]]:
    """Specific memory mechanisms suppress shared OOM symptoms across included observations."""
    result: dict[str, list[str]] = {}
    for identifier, resource, value in observations:
        for cause in sorted(
            supported_causes(value, resource) | slice_causes(value) | http_causes(value)
        ):
            result.setdefault(cause, []).append(identifier)
    if {"MEMORY_LEAK", "CONCURRENCY_MEMORY_PRESSURE"} & result.keys():
        result.pop("MEMORY_LIMIT_BELOW_WORKING_SET", None)
    return {cause: tuple(dict.fromkeys(ids)) for cause, ids in sorted(result.items())}
