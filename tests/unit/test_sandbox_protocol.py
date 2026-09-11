"""Real serialized ASGI requests distinguish wire incompatibility from a fake outage."""

import json
from typing import cast

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import JsonValue, ValidationError

from payops.sandbox.models import RiskProtocol, Role, Sample, SandboxConfig
from payops.sandbox.service import create_service


class ProtocolTransport(httpx.AsyncBaseTransport):
    """Record actual request bodies and peer statuses without opening network sockets."""

    def __init__(self, apps: dict[str, FastAPI]) -> None:
        """Each peer keeps its own replay state and versioned decoder."""
        self.apps = apps
        self.calls: list[tuple[str, JsonValue, int]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Pass the original HTTP request through the peer's actual FastAPI validation."""
        response = await httpx.ASGITransport(app=self.apps[request.url.host]).handle_async_request(
            request
        )
        self.calls.append((request.url.host, json.loads(request.content), response.status_code))
        return response


@pytest.mark.parametrize("caller", ["v1", "v2"])
@pytest.mark.parametrize("decoder", ["v1", "v2"])
def test_real_protocol_matching_and_mismatch(caller: RiskProtocol, decoder: RiskProtocol) -> None:
    """Changing only the risk wire contract yields 422 at risk and 502 at payments."""
    roles: tuple[Role, ...] = ("risk", "processor", "ledger", "webhook")
    apps = {
        f"{role}.svc.cluster.local": create_service(
            role, SandboxConfig(risk_protocol=decoder if role == "risk" else "v1")
        )
        for role in roles
    }
    transport = ProtocolTransport(apps)
    config = SandboxConfig(
        risk_url="http://risk.svc.cluster.local",
        processor_url="http://processor.svc.cluster.local",
        ledger_url="http://ledger.svc.cluster.local",
        webhook_url="http://webhook.svc.cluster.local",
        risk_protocol=caller,
    )
    sample = Sample(sample_id="synthetic-fixed-protocol")
    with TestClient(create_service("payments", config, transport)) as client:
        response = client.post("/simulate", json=sample.model_dump())
        assert response.status_code == (200 if caller == decoder else 502)
        assert transport.calls[0][2] == (200 if caller == decoder else 422)
        expected: JsonValue = sample.model_dump()
        if caller == "v2":
            expected = {"protocol": "payops-risk-v2", "sample": expected}
        assert transport.calls[0][1] == expected
        if caller == decoder:
            assert response.json()["status"] == "accepted"
            assert len(transport.calls) == 4
            assert all(body == sample.model_dump() for _, body, _ in transport.calls[1:])
            assert client.post("/simulate", json=sample.model_dump()).json() == response.json()
            assert len(transport.calls) == 4
        else:
            assert response.json() == {"detail": "synthetic risk returned 422"}
            assert len(transport.calls) == 1
            assert 'status="error"' in client.get("/metrics").text


@pytest.mark.parametrize("role", ["payments", "processor", "ledger", "webhook"])
def test_v2_envelope_is_only_a_risk_contract(role: Role) -> None:
    """Caller configuration cannot broaden other service request schemas."""
    body = {"protocol": "payops-risk-v2", "sample": {"sample_id": "synthetic-role"}}
    with TestClient(create_service(role, SandboxConfig(risk_protocol="v2"))) as client:
        assert client.post("/simulate", json=body).status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        {"protocol": "v3", "sample": {"sample_id": "synthetic-invalid"}},
        {"protocol": "payops-risk-v2", "sample": {"sample_id": "synthetic-invalid", "amount": 1}},
        {"protocol": "payops-risk-v2", "sample": {"sample_id": "synthetic-invalid"}, "action": "x"},
        {"protocol": "payops-risk-v2", "sample": {"sample_id": "customer-real"}},
        {"sample_id": "synthetic-old-protocol"},
    ],
)
def test_risk_v2_requires_complete_closed_envelope(body: dict[str, JsonValue]) -> None:
    """Reject financial fields, unknown envelope keys and incompatible flat requests."""
    with TestClient(create_service("risk", SandboxConfig(risk_protocol="v2"))) as client:
        assert client.post("/simulate", json=body).status_code == 422
        valid = {"protocol": "payops-risk-v2", "sample": {"sample_id": "synthetic-valid"}}
        assert client.post("/simulate", json=valid).status_code == 200


def test_unknown_deployment_protocol_rejected() -> None:
    """Only two reviewed wire versions can be selected by deployment configuration."""
    with pytest.raises(ValidationError):
        SandboxConfig(risk_protocol=cast(RiskProtocol, "latest"))
