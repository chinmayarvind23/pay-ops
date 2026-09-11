"""Five isolated application roles model dependencies without financial operations."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from time import perf_counter

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from payops.sandbox.client import call_peer
from payops.sandbox.cpu import CpuWork
from payops.sandbox.models import RiskSampleV2, Role, Sample, SandboxConfig, SimulationResult
from payops.sandbox.runtime import FaultState, SampleStore
from payops.sandbox.telemetry import SandboxMetrics


def decode_sample(payload: Sample | RiskSampleV2, role: Role, config: SandboxConfig) -> Sample:
    """The deployed risk decoder accepts one wire version; other roles retain v1."""
    if role == "risk" and config.risk_protocol == "v2":
        if isinstance(payload, RiskSampleV2):
            return payload.sample
    elif isinstance(payload, Sample):
        return payload
    raise HTTPException(422, "synthetic request protocol mismatch")


async def payment_path(
    sample: Sample, config: SandboxConfig, transport: httpx.AsyncBaseTransport | None
) -> SimulationResult:
    """Ledger verifies a synthetic receipt; it has no balances or mutation commands."""
    for role in ("risk", "processor", "ledger", "webhook"):
        result = await call_peer(role, sample, config, transport)
        if result.status == "declined":
            return SimulationResult(sample_id=sample.sample_id, role="payments", status="declined")
    return SimulationResult(sample_id=sample.sample_id, role="payments", status="accepted")


async def execute_sample(
    role: Role,
    sample: Sample,
    config: SandboxConfig,
    faults: FaultState,
    transport: httpx.AsyncBaseTransport | None,
    cpu: CpuWork,
) -> SimulationResult:
    """Apply trusted fault state before executing the role's bounded simulation."""
    declined = await faults.apply(sample)
    if not declined and config.cpu_rounds:
        consumed = await cpu.run(sample.sample_id)
        trace.get_current_span().set_attribute("sandbox.cpu.rounds", config.cpu_rounds)
        trace.get_current_span().set_attribute("sandbox.cpu.thread_seconds", consumed)
    if role == "payments" and not declined:
        return await payment_path(sample, config, transport)
    return SimulationResult(
        sample_id=sample.sample_id, role=role, status="declined" if declined else "accepted"
    )


async def process_sample(
    role: Role,
    sample: Sample,
    config: SandboxConfig,
    faults: FaultState,
    store: SampleStore,
    transport: httpx.AsyncBaseTransport | None,
    cpu: CpuWork,
) -> SimulationResult:
    """Reservations protect each role independently; failure frees only its local slot."""
    cached = store.reserve(sample)
    if cached is not None:
        return cached
    try:
        result = await execute_sample(role, sample, config, faults, transport, cpu)
        store.complete(sample.sample_id, result)
        return result
    except BaseException:
        store.abandon(sample.sample_id)
        raise


def create_service(
    role: Role,
    config: SandboxConfig | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    faults: FaultState | None = None,
) -> FastAPI:
    """Construct one role; scenario controls stay in trusted Python state, never HTTP."""
    if role not in {"payments", "risk", "processor", "ledger", "webhook"}:
        raise ValueError("unknown synthetic service role")
    settings = config or SandboxConfig()
    fault_state = faults or FaultState()
    store = SampleStore(settings.idempotency_capacity)
    metrics = SandboxMetrics()
    cpu = CpuWork(settings.cpu_rounds, settings.cpu_capture)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        """Service shutdown closes admission while bounded in-flight CPU work finishes."""
        try:
            yield
        finally:
            cpu.close()

    app = FastAPI(title=f"PayOps synthetic {role}", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str | bool]:
        """Liveness intentionally does not promise that dependencies or samples succeed."""
        return {"status": "ok", "role": role, "synthetic": True}

    @app.get("/metrics")
    async def metric_export() -> Response:
        """Export only this process's bounded synthetic metric registry."""
        return Response(
            generate_latest(metrics.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    @app.post("/simulate", response_model=SimulationResult)
    async def simulate(payload: Sample | RiskSampleV2, request: Request) -> SimulationResult:
        """Use propagated HTTP trace context without putting sample IDs in metrics."""
        sample = decode_sample(payload, role, settings)
        started = perf_counter()
        status = "error"
        tracer = trace.get_tracer("payops.sandbox")
        with tracer.start_as_current_span(
            f"sandbox.{role}",
            context=propagate.extract(dict(request.headers)),
            kind=SpanKind.SERVER,
        ):
            try:
                result = await process_sample(
                    role, sample, settings, fault_state, store, transport, cpu
                )
                status = result.status
                return result
            except HTTPException as exc:
                if exc.status_code == 409:
                    metrics.conflicts.inc()
                raise
            finally:
                metrics.observe(sample, status, perf_counter() - started)

    return app
