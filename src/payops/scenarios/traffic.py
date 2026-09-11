"""Bounded operator-only synthetic traffic with fixed local destinations and receipts."""

import asyncio
import json
import random
import shutil
import socket
import subprocess
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Literal, Self
from uuid import uuid4

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from payops.sandbox.models import Method, Processor, Region, Sample, SimulationResult
from payops.scenarios.kubectl import KubectlGateway

TrafficRole = Literal["payments", "webhook"]
Outcome = Literal[
    "accepted",
    "declined",
    "http_error",
    "invalid_response",
    "request_error",
    "timed_out",
    "cancelled",
    "failed",
]


class SliceCount(BaseModel):
    """Explicit slice counts avoid pretending random draws have an exact distribution."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    processor: Processor
    region: Region
    payment_method: Method
    count: int = Field(ge=1, le=512)


class Workload(BaseModel):
    """Trusted workloads contain closed roles and bounded counts, never destination URLs."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    role: TrafficRole = "payments"
    distribution: tuple[SliceCount, ...] = Field(min_length=1, max_length=8)
    concurrency: int = Field(default=4, ge=1, le=16)
    request_timeout_seconds: float = Field(default=5.0, gt=0, le=10)
    deadline_seconds: float = Field(default=60.0, gt=0, le=180)
    seed: int = Field(default=0, ge=0, le=2**32 - 1)

    @model_validator(mode="after")
    def bounded_distribution(self) -> Self:
        """Reject duplicate slice declarations and totals exceeding the batch bound."""
        keys = {(item.processor, item.region, item.payment_method) for item in self.distribution}
        if (
            len(keys) != len(self.distribution)
            or sum(item.count for item in self.distribution) > 512
        ):
            raise ValueError("distribution must contain unique slices and at most 512 samples")
        return self


class PlannedAttempt(BaseModel):
    """Persist exact sample IDs before traffic so seeded fault decisions remain auditable."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    index: int
    sample: Sample
    traceparent: str
    step: Literal["sample", "original", "identical_duplicate", "conflict"] = "sample"


class AttemptRecord(BaseModel):
    """Queue cancellation is distinct from a started request and an observed HTTP status."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    planned: PlannedAttempt
    started_at: datetime | None
    completed_at: datetime
    latency_seconds: float | None
    outcome: Outcome
    http_status: int | None = None
    error_type: str | None = None
    result: SimulationResult | None = None
    idempotency_conflict: bool = False


class TrafficReceipt(BaseModel):
    """Batch completion describes execution; unsuccessful HTTP attempts remain explicit."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    run_id: str
    mode: Literal["local_kind", "fixture_replay"]
    role: TrafficRole
    started_at: datetime
    completed_at: datetime
    status: Literal["completed", "deadline_exceeded", "cancelled", "failed"]
    failure: str | None
    attempts: tuple[AttemptRecord, ...]
    probe_verified: bool | None = None


def _now() -> datetime:
    """UTC timestamps align attempts with collector windows without clock arithmetic."""
    return datetime.now(UTC)


def _planned(index: int, sample: Sample, step: str = "sample") -> PlannedAttempt:
    """Each attempt gets fresh W3C context, including deliberate sample-ID replays."""
    return PlannedAttempt.model_validate(
        {
            "index": index,
            "sample": sample,
            "step": step,
            "traceparent": f"00-{uuid4().hex}-{uuid4().hex[:16]}-01",
        }
    )


def _plan(workload: Workload, run_id: str) -> tuple[PlannedAttempt, ...]:
    """The seed determines slice ordering; recorded IDs determine sandbox hash outcomes."""
    slices = [item for item in workload.distribution for _ in range(item.count)]
    random.Random(workload.seed).shuffle(slices)
    return tuple(
        _planned(
            index,
            Sample(
                sample_id=f"synthetic-{run_id}-{index}",
                processor=item.processor,
                region=item.region,
                payment_method=item.payment_method,
            ),
        )
        for index, item in enumerate(slices)
    )


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    """Only the owned forward is terminated; a stuck graceful shutdown gets a bounded kill."""
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


@contextmanager
def scoped_forward(kubeconfig: Path, role: TrafficRole) -> Generator[str]:
    """Pin argv to the dedicated cluster and a closed service map without invoking a shell."""
    targets = {"payments": ("payments-api", 18082), "webhook": ("webhook-sim", 18083)}
    service, port = targets[role]
    executable = shutil.which("kubectl")
    if executable is None:
        raise ValueError("kubectl is required")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    process = subprocess.Popen(
        (
            executable,
            "--kubeconfig",
            str(kubeconfig.resolve()),
            "--context",
            "kind-payops-dev",
            "--namespace",
            "payops-sandbox",
            "--request-timeout=10s",
            "port-forward",
            f"service/{service}",
            f"{port}:8080",
            "--address",
            "127.0.0.1",
        ),
        shell=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        _stop_process(process)


async def wait_forward_ready(client: httpx.AsyncClient, role: TrafficRole) -> None:
    """Liveness establishes the fixed forward without adding synthetic payment attempts."""
    async with asyncio.timeout(12):
        while True:
            try:
                response = await client.get("/health")
                if response.status_code == 200 and response.json() == {
                    "status": "ok",
                    "role": role,
                    "synthetic": True,
                }:
                    return
            except (httpx.RequestError, ValueError):
                pass
            await asyncio.sleep(0.1)


async def _response(
    client: httpx.AsyncClient,
    planned: PlannedAttempt,
    role: TrafficRole,
) -> tuple[Outcome, int, SimulationResult | None, bool]:
    """Bound response bytes and validate identities; redirects never become new targets."""
    async with client.stream(
        "POST",
        "/simulate",
        json=planned.sample.model_dump(),
        headers={"traceparent": planned.traceparent},
    ) as response:
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > 8192:
                return "invalid_response", response.status_code, None, False
        return _classify(bytes(body), response.status_code, planned.sample, role)


def _classify(
    body: bytes,
    status: int,
    sample: Sample,
    role: TrafficRole,
) -> tuple[Outcome, int, SimulationResult | None, bool]:
    """Only validated synthetic success or the exact expected conflict detail gets semantics."""
    if status != 200:
        conflict = False
        if status == 409:
            try:
                conflict = json.loads(body) == {"detail": "synthetic idempotency conflict"}
            except (ValueError, UnicodeError):
                pass
        return "http_error", status, None, conflict
    try:
        decoded = TypeAdapter(dict[str, JsonValue]).validate_json(body)
        if decoded.get("synthetic") is not True:
            return "invalid_response", status, None, False
        result = SimulationResult.model_validate_json(body)
    except (ValidationError, ValueError, UnicodeError):
        return "invalid_response", status, None, False
    if result.sample_id != sample.sample_id or result.role != role:
        return "invalid_response", status, None, False
    return result.status, status, result, False


async def _attempt(
    client: httpx.AsyncClient,
    planned: PlannedAttempt,
    workload: Workload,
    semaphore: asyncio.Semaphore,
    records: dict[int, AttemptRecord],
) -> None:
    """A cancelled queued attempt retains no invented start time, HTTP status or latency."""
    started: datetime | None = None
    clock: float | None = None
    outcome: Outcome = "cancelled"
    status: int | None = None
    result: SimulationResult | None = None
    conflict = False
    error: str | None = None
    try:
        async with semaphore:
            started, clock = _now(), perf_counter()
            async with asyncio.timeout(workload.request_timeout_seconds):
                outcome, status, result, conflict = await _response(client, planned, workload.role)
    except (TimeoutError, httpx.TimeoutException):
        outcome, error = "timed_out", "TimeoutError"
    except httpx.RequestError as exc:
        outcome, error = "request_error", type(exc).__name__
    except Exception as exc:
        outcome, error = "failed", type(exc).__name__
        raise
    finally:
        records[planned.index] = AttemptRecord(
            planned=planned,
            started_at=started,
            completed_at=_now(),
            latency_seconds=None if clock is None else perf_counter() - clock,
            outcome=outcome,
            http_status=status,
            error_type=error,
            result=result,
            idempotency_conflict=conflict,
        )


def _probe_verified(records: tuple[AttemptRecord, ...]) -> bool:
    """Idempotency requires both equivalent successes and the changed-payload conflict."""
    first, duplicate, conflict = records
    return (
        first.http_status == duplicate.http_status == 200
        and first.result is not None
        and first.result == duplicate.result
        and conflict.http_status == 409
        and conflict.idempotency_conflict
    )


class TrafficDriver:
    """An operator harness owns the forward; injected transports are explicitly fixture replay."""

    def __init__(
        self, kubeconfig: Path, output: Path, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        """Paths identify trusted operator resources, not model-selected network destinations."""
        self._kubeconfig = kubeconfig.resolve()
        self._output = output.resolve()
        self._transport = transport

    @asynccontextmanager
    async def _client(self, workload: Workload) -> AsyncGenerator[httpx.AsyncClient]:
        """Always close the HTTP client before the owned port-forward process is stopped."""
        if self._transport is not None:
            async with httpx.AsyncClient(
                base_url="http://127.0.0.1:18082",
                transport=self._transport,
                timeout=workload.request_timeout_seconds,
                trust_env=False,
                follow_redirects=False,
            ) as client:
                yield client
            return
        await asyncio.to_thread(KubectlGateway(self._kubeconfig).verify_scope)
        with scoped_forward(self._kubeconfig, workload.role) as origin:
            async with httpx.AsyncClient(
                base_url=origin,
                timeout=workload.request_timeout_seconds,
                trust_env=False,
                follow_redirects=False,
            ) as client:
                await wait_forward_ready(client, workload.role)
                yield client

    async def run(self, workload: Workload) -> TrafficReceipt:
        """Record exact explicit slice workloads before starting any network activity."""
        run_id = uuid4().hex
        return await self._execute(workload, run_id, _plan(workload, run_id), False)

    async def run_idempotency_probe(self, role: TrafficRole = "webhook") -> TrafficReceipt:
        """Replay an identical body then change its processor while preserving the sample ID."""
        workload = Workload(
            role=role,
            distribution=(
                SliceCount(processor="A", region="us", payment_method="credit", count=2),
                SliceCount(processor="B", region="us", payment_method="credit", count=1),
            ),
            concurrency=1,
        )
        run_id = uuid4().hex
        sample = Sample(sample_id=f"synthetic-{run_id}-probe")
        changed = Sample(sample_id=sample.sample_id, processor="B")
        plan = (
            _planned(0, sample, "original"),
            _planned(1, sample, "identical_duplicate"),
            _planned(2, changed, "conflict"),
        )
        return await self._execute(workload, run_id, plan, True)

    async def _dispatch(
        self,
        workload: Workload,
        plan: tuple[PlannedAttempt, ...],
        records: dict[int, AttemptRecord],
        probe: bool,
    ) -> None:
        """TaskGroup cancellation waits for every worker before client or forward cleanup."""
        semaphore = asyncio.Semaphore(workload.concurrency)
        async with self._client(workload) as client:
            if probe:
                for item in plan:
                    await _attempt(client, item, workload, semaphore, records)
            else:
                async with asyncio.TaskGroup() as tasks:
                    for item in plan:
                        tasks.create_task(_attempt(client, item, workload, semaphore, records))

    async def _execute(
        self, workload: Workload, run_id: str, plan: tuple[PlannedAttempt, ...], probe: bool
    ) -> TrafficReceipt:
        """Persist interrupted receipts after cleanup, then propagate external cancellation."""
        root = self._output / run_id
        root.mkdir(parents=True, exist_ok=False)
        started = _now()
        (root / "plan.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "workload": workload.model_dump(mode="json"),
                    "probe": probe,
                    "attempts": [item.model_dump(mode="json") for item in plan],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        records: dict[int, AttemptRecord] = {}
        status: Literal["completed", "deadline_exceeded", "cancelled", "failed"] = "completed"
        failure: str | None = None
        deadline = asyncio.timeout(workload.deadline_seconds)
        try:
            async with deadline:
                await self._dispatch(workload, plan, records, probe)
        except TimeoutError:
            status = "deadline_exceeded" if deadline.expired() else "failed"
            failure = "TimeoutError"
            if not deadline.expired():
                raise
        except BaseException as exc:
            status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
            failure = type(exc).__name__
            raise
        finally:
            attempts = tuple(
                records.get(item.index)
                or AttemptRecord(
                    planned=item,
                    started_at=None,
                    completed_at=_now(),
                    latency_seconds=None,
                    outcome="cancelled",
                )
                for item in plan
            )
            receipt = TrafficReceipt(
                run_id=run_id,
                mode="fixture_replay" if self._transport else "local_kind",
                role=workload.role,
                started_at=started,
                completed_at=_now(),
                status=status,
                failure=failure,
                attempts=attempts,
                probe_verified=_probe_verified(attempts) if probe else None,
            )
            (root / "receipt.json").write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        return receipt
