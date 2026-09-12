"""Observed HTTP outcomes localize failures without reading injector settings or case labels."""

from pydantic import JsonValue

from payops.evidence.diagnostics import number, objects


def http_causes(value: JsonValue) -> frozenset[str]:
    """Require endpoint role and matched traffic slices, not an isolated generic error code."""
    found: set[str] = set()
    for envelope in objects(value):
        attempts = envelope.get("attempts")
        if not isinstance(attempts, list):
            continue
        rows = [row for row in attempts if isinstance(row, dict)]
        if envelope.get("role") == "webhook" and any(
            row.get("http_status") == 409 and row.get("idempotency_conflict") is True
            for row in rows
        ):
            found.add("WEBHOOK_IDEMPOTENCY_CONFLICT")
        if envelope.get("role") != "payments":
            continue
        for rejected in rows:
            if (
                rejected.get("http_status") != 429
                or (number(rejected.get("latency_seconds")) or 0) <= 0
            ):
                continue
            if any(matched_control(rejected, accepted) for accepted in rows):
                found.add("PROCESSOR_LATENCY_RATE_LIMIT")
    return frozenset(found)


def matched_control(rejected: dict[str, JsonValue], accepted: dict[str, JsonValue]) -> bool:
    """A successful other-processor control must hold region and payment method constant."""
    if accepted.get("http_status") != 200:
        return False
    keys = ("processor", "region", "payment_method")
    if not all(
        isinstance(row.get(key), str) and row[key] for row in (rejected, accepted) for key in keys
    ):
        return False
    return (
        rejected["processor"] != accepted["processor"]
        and rejected["region"] == accepted["region"]
        and rejected["payment_method"] == accepted["payment_method"]
    )
