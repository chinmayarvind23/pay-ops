"""Exercise synthetic HTTP boundaries without reaching any external network."""

import asyncio
from collections.abc import Iterator
from typing import cast

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import ProxyTracerProvider
from pydantic import ValidationError

from payops.sandbox import FaultConfig, Sample, SandboxConfig, create_service
from payops.sandbox.entrypoint import create_app
from payops.sandbox.models import Role, SimulationResult
from payops.sandbox.runtime import FaultState, SampleStore
from payops.sandbox.tracing import configure_tracing


class ServiceTransport(httpx.AsyncBaseTransport):
    """Route real serialized HTTP requests into peer ASGI applications only."""

    def __init__(self, apps: dict[str, FastAPI]) -> None:
        """Record calls and propagated headers for trajectory and tracing assertions."""
        self.apps = apps
        self.calls: list[str] = []
        self.traceparents: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """An unexpected host fails immediately instead of escaping onto the network."""
        self.calls.append(request.url.host)
        self.traceparents.append(request.headers.get("traceparent", ""))
        return await httpx.ASGITransport(app=self.apps[request.url.host]).handle_async_request(
            request
        )


def network(
    processor_fault: FaultConfig | None = None,
) -> tuple[TestClient, ServiceTransport, dict[str, FastAPI]]:
    """Five separate ASGI apps retain distinct state while sharing an in-memory wire."""
    roles: tuple[Role, ...] = ("risk", "processor", "ledger", "webhook")
    apps = {
        f"{role}.payments-sandbox.svc.cluster.local": create_service(
            role, faults=FaultState(processor_fault if role == "processor" else None)
        )
        for role in roles
    }
    transport = ServiceTransport(apps)
    config = SandboxConfig(
        risk_url="http://risk.payments-sandbox.svc.cluster.local",
        processor_url="http://processor.payments-sandbox.svc.cluster.local",
        ledger_url="http://ledger.payments-sandbox.svc.cluster.local",
        webhook_url="http://webhook.payments-sandbox.svc.cluster.local",
    )
    return TestClient(create_service("payments", config, transport)), transport, apps


@pytest.fixture
def exported_spans(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemorySpanExporter]:
    """Use an isolated tracer provider without changing the process global singleton."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer", provider.get_tracer)
    yield exporter
    provider.shutdown()


def test_happy_http_path_trace_and_replay(exported_spans: InMemorySpanExporter) -> None:
    """A completed sample crosses all peers once and preserves one distributed trace."""
    client, transport, _ = network()
    sample = Sample(sample_id="synthetic-happy")
    with client:
        response = client.post("/simulate", json=sample.model_dump())
        assert (
            response.json()
            == SimulationResult(
                sample_id=sample.sample_id, role="payments", status="accepted"
            ).model_dump()
        )
        assert client.post("/simulate", json=sample.model_dump()).json() == response.json()
        assert [host.split(".")[0] for host in transport.calls] == [
            "risk",
            "processor",
            "ledger",
            "webhook",
        ]
        assert all(transport.traceparents)
        assert len({header.split("-")[1] for header in transport.traceparents}) == 1
        spans = exported_spans.get_finished_spans()
        assert {span.name for span in spans} >= {"sandbox.payments", "sandbox.webhook"}
        metrics = client.get("/metrics").text
        assert "synthetic-happy" not in metrics
        assert 'payment_method="credit"' in metrics
        assert (
            'payment_authorization_latency_seconds_count{processor="A",region="us"} 2.0' in metrics
        )
        assert client.get("/health").json()["synthetic"] is True


@pytest.mark.parametrize(
    "fault,status", [(FaultConfig(unavailable=True), 503), (FaultConfig(rate_limit_every=1), 429)]
)
def test_dependency_error_stops_downstream(fault: FaultConfig, status: int) -> None:
    """Downstream ledger/webhook calls cannot occur after a processor failure."""
    client, transport, _ = network(fault)
    with client:
        response = client.post("/simulate", json=Sample(sample_id="synthetic-failed").model_dump())
        assert response.status_code == status
        assert [host.split(".")[0] for host in transport.calls] == ["risk", "processor"]
        assert 'status="error"' in client.get("/metrics").text


def test_decline_isolated_to_payment_slice() -> None:
    """An unaffected control processor distinguishes the selected fault from global failure."""
    client, transport, _ = network(FaultConfig(decline_every=1, processor="B"))
    with client:
        control = client.post("/simulate", json=Sample(sample_id="synthetic-a").model_dump())
        declined = client.post(
            "/simulate", json=Sample(sample_id="synthetic-b", processor="B").model_dump()
        )
        assert control.json()["status"] == "accepted"
        assert declined.json()["status"] == "declined"
        assert len(transport.calls) == 6
        assert (
            'payment_declines_total{processor="B",reason="synthetic_fault"} 1.0'
            in client.get("/metrics").text
        )


def test_conflicts_capacity_and_no_financial_inputs() -> None:
    """Bounded replay state never silently evicts a sample or accepts financial fields."""
    with TestClient(create_service("webhook", SandboxConfig(idempotency_capacity=1))) as client:
        sample = Sample(sample_id="synthetic-one")
        assert client.post("/simulate", json=sample.model_dump()).status_code == 200
        assert client.post("/simulate", json=sample.model_dump()).status_code == 200
        changed = sample.model_copy(update={"processor": "B"})
        assert client.post("/simulate", json=changed.model_dump()).status_code == 409
        assert (
            client.post(
                "/simulate", json=Sample(sample_id="synthetic-two").model_dump()
            ).status_code
            == 503
        )
        assert (
            client.post("/simulate", json={**sample.model_dump(), "amount": 50}).status_code == 422
        )
        assert "payment_idempotency_conflicts_total 1.0" in client.get("/metrics").text
        assert client.post("/fault", json={"unavailable": True}).status_code == 404


@pytest.mark.parametrize(
    "origin",
    [
        "https://bank.example",
        "http://169.254.169.254",
        "http://user:pass@localhost",
        "http://localhost/path",
        "http://localhost/?url=bank",
        "http://localhost/#fragment",
        "file:///tmp/data",
        "http://fake.svc.cluster.local.attacker.test",
        "http://localhost\\@evil.test",
        "http://localhost:0",
        "http://localhost:bad",
    ],
)
def test_unapproved_origins_rejected(origin: str) -> None:
    """Origins cannot hide credentials, metadata endpoints, path routing or suffix spoofing."""
    with pytest.raises(ValidationError):
        SandboxConfig(processor_url=origin)


@pytest.mark.parametrize(
    "origin",
    [
        "http://localhost:8080",
        "http://127.0.0.1",
        "http://[::1]:8080",
        "http://processor.payments-sandbox.svc.cluster.local",
    ],
)
def test_local_origins_allowed(origin: str) -> None:
    """Local development and Kubernetes service DNS are intentionally supported."""
    assert SandboxConfig(processor_url=origin).processor_url == origin


def test_explicit_external_synthetic_origin() -> None:
    """External simulators require an exact HTTPS hostname configured by the deployer."""
    config = SandboxConfig(
        processor_url="https://sim.example", explicit_synthetic_hosts=("sim.example",)
    )
    assert config.processor_url == "https://sim.example"
    with pytest.raises(ValidationError):
        SandboxConfig(processor_url="http://sim.example", explicit_synthetic_hosts=("sim.example",))


@pytest.mark.parametrize(
    "failure,expected",
    [("timeout", 504), ("connection", 503), ("redirect", 502), ("invalid", 502), ("mismatch", 502)],
)
def test_peer_failures_are_bounded(failure: str, expected: int) -> None:
    """Client errors stay meaningful and never reflect an untrusted raw peer response."""

    def respond(request: httpx.Request) -> httpx.Response:
        """Inject each transport boundary failure without opening sockets."""
        if failure == "timeout":
            raise httpx.ReadTimeout("private upstream body", request=request)
        if failure == "connection":
            raise httpx.ConnectError("private upstream body", request=request)
        if failure == "redirect":
            return httpx.Response(302, headers={"location": "https://bank.example"})
        if failure == "mismatch":
            return httpx.Response(
                200,
                json=SimulationResult(
                    sample_id="synthetic-other", role="risk", status="accepted"
                ).model_dump(),
            )
        return httpx.Response(200, text="private upstream body")

    with TestClient(create_service("payments", transport=httpx.MockTransport(respond))) as client:
        response = client.post("/simulate", json=Sample(sample_id="synthetic-error").model_dump())
        assert response.status_code == expected
        assert "private upstream body" not in response.text


def test_fault_reset_and_seed_reproducibility() -> None:
    """Order does not change a seeded fault, and resetting trusted state restores health."""
    state = FaultState(FaultConfig(decline_every=2, delay_ms=1, region="eu", seed=7))
    samples = [Sample(sample_id=f"synthetic-{number}", region="eu") for number in range(8)]
    first = [asyncio.run(state.apply(sample)) for sample in samples]
    second = [asyncio.run(state.apply(sample)) for sample in reversed(samples)]
    assert first == list(reversed(second))
    assert not asyncio.run(state.apply(Sample(sample_id="synthetic-control")))
    state.config = FaultConfig()
    assert not any(asyncio.run(state.apply(sample)) for sample in samples)


def test_pending_reservation_and_retry() -> None:
    """Concurrent duplicates conflict, while abandoned attempts can be retried safely."""
    store = SampleStore(1)
    sample = Sample(sample_id="synthetic-pending")
    assert store.reserve(sample) is None
    with pytest.raises(HTTPException) as conflict:
        store.reserve(sample)
    assert conflict.value.status_code == 409
    store.abandon(sample.sample_id)
    assert store.reserve(sample) is None


def test_startup_fault_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """A Kubernetes env revision can enable a bounded fault without an HTTP admin route."""
    monkeypatch.setenv("PAYOPS_SANDBOX_ROLE", "processor")
    monkeypatch.setenv("PAYOPS_SANDBOX_FAULT", '{"unavailable":true}')
    provider = TracerProvider()
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    with TestClient(create_app()) as client:
        assert (
            client.post(
                "/simulate", json=Sample(sample_id="synthetic-startup").model_dump()
            ).status_code
            == 503
        )
    monkeypatch.setenv("PAYOPS_SANDBOX_ROLE", "shell")
    with pytest.raises(ValidationError):
        create_app()
    provider.shutdown()


def test_existing_trace_provider_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    """Embedding the service must preserve the caller's collector/exporter configuration."""
    provider = TracerProvider()
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    configure_tracing("processor")
    assert trace.get_tracer_provider() is provider
    provider.shutdown()


def test_standalone_trace_provider_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Standalone processes gain an exporter without replacing an already-owned SDK."""
    captured: list[trace.TracerProvider] = []
    monkeypatch.setattr(trace, "get_tracer_provider", ProxyTracerProvider)
    monkeypatch.setattr(trace, "set_tracer_provider", captured.append)
    configure_tracing("processor")
    assert len(captured) == 1
    provider = captured[0]
    assert isinstance(provider, TracerProvider)
    assert provider.resource.attributes["service.name"] == "payops-sandbox-processor"
    provider.shutdown()


def test_unknown_role_fails_runtime_boundary() -> None:
    """Python callers receive the same closed role boundary as deployment configuration."""
    with pytest.raises(ValueError, match="unknown synthetic service role"):
        create_service(cast(Role, "shell"))
