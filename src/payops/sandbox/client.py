"""HTTP-only peer calls preserve real service boundaries in both runtime and tests."""

import httpx
from fastapi import HTTPException
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind
from pydantic import ValidationError

from payops.sandbox.models import Role, Sample, SandboxConfig, SimulationResult


async def call_peer(
    role: Role,
    sample: Sample,
    config: SandboxConfig,
    transport: httpx.AsyncBaseTransport | None,
) -> SimulationResult:
    """No redirects, ambient proxies or request-selected URLs may cross the boundary."""
    tracer = trace.get_tracer("payops.sandbox")
    with tracer.start_as_current_span(f"sandbox.call.{role}", kind=SpanKind.CLIENT):
        headers: dict[str, str] = {}
        propagate.inject(headers)
        try:
            async with httpx.AsyncClient(
                transport=transport,
                timeout=config.timeout_seconds,
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.post(
                    config.destination(role).rstrip("/") + "/simulate",
                    json=sample.model_dump(),
                    headers=headers,
                )
        except httpx.TimeoutException as exc:
            raise HTTPException(504, f"synthetic {role} timeout") from exc
        except httpx.RequestError as exc:
            raise HTTPException(503, f"synthetic {role} unreachable") from exc
        return validate_peer(response, role, sample.sample_id)


def validate_peer(response: httpx.Response, role: Role, sample_id: str) -> SimulationResult:
    """Reject malformed, redirected and mismatched replies without exposing raw bodies."""
    if response.status_code != 200:
        status = response.status_code if response.status_code in {409, 429, 503, 504} else 502
        raise HTTPException(status, f"synthetic {role} returned {response.status_code}")
    try:
        result = SimulationResult.model_validate_json(response.content)
    except ValidationError as exc:
        raise HTTPException(502, f"synthetic {role} invalid response") from exc
    if result.sample_id != sample_id or result.role != role:
        raise HTTPException(502, f"synthetic {role} mismatched response")
    return result
