"""HTTP evidence must localize a mechanism and retain matched-control requirements."""

import pytest
from pydantic import JsonValue

from payops.evidence.diagnostic_http import http_causes


def test_processor_rate_limit_requires_matching_other_processor_control() -> None:
    """HTTP 429 alone cannot distinguish API-wide throttling from one processor slice."""
    failed: dict[str, JsonValue] = {
        "processor": "B",
        "region": "us",
        "payment_method": "credit",
        "http_status": 429,
        "latency_seconds": 0.8,
    }
    success = {**failed, "processor": "A", "http_status": 200}
    assert http_causes({"role": "payments", "attempts": [failed, success]}) == {
        "PROCESSOR_LATENCY_RATE_LIMIT"
    }
    for changed in (
        {"processor": "B"},
        {"region": "eu"},
        {"payment_method": "debit"},
        {"http_status": 503},
        {"processor": None},
    ):
        assert not http_causes({"role": "payments", "attempts": [failed, {**success, **changed}]})
    assert not http_causes({"role": "payments", "attempts": [failed]})
    assert not http_causes(
        {"role": "payments", "attempts": [{**failed, "latency_seconds": 0}, success]}
    )


@pytest.mark.parametrize(
    "role,status,conflict,expected",
    [
        ("webhook", 409, True, True),
        ("payments", 409, True, False),
        ("webhook", 503, True, False),
        ("webhook", 409, False, False),
    ],
)
def test_webhook_conflict_requires_role_and_specific_response(
    role: str, status: int, conflict: bool, expected: bool
) -> None:
    """Generic conflict counters and unrelated HTTP errors cannot identify webhook idempotency."""
    result = http_causes(
        {
            "role": role,
            "attempts": [
                None,
                {
                    "http_status": status,
                    "idempotency_conflict": conflict,
                },
            ],
        }
    )
    assert bool(result) is expected
