"""Closed read dispatch with no queued work, bounded result waits and explicit budget ownership."""

from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass
from hashlib import sha256
from threading import BoundedSemaphore, Lock
from time import monotonic
from types import MappingProxyType
from typing import Literal

from opentelemetry import trace
from opentelemetry.context import Context, get_current
from pydantic import Field, JsonValue

from payops.contracts import Contract, EvidenceItem
from payops.evidence.artifacts import JSON_OBJECT
from payops.orchestrator.reasoning import ReadRequest, ToolName


@dataclass(frozen=True)
class ReadSpec:
    """Costs count underlying commands or queries, not network packets or SDK-internal calls."""

    description: str
    backend_reads: int
    deadline_seconds: float


CATALOG: Mapping[ToolName, ReadSpec] = MappingProxyType(
    {
        "workload_status": ReadSpec("Read workload rollout and current labeled pod status.", 2, 26),
        "pod_events": ReadSpec("Read events bound to current pod identities.", 2, 26),
        "recent_logs": ReadSpec(
            "Read bounded recent application logs as untrusted evidence.", 1, 14
        ),
        "payment_snapshot": ReadSpec(
            "Read counters and scrape watermark at one fixed instant.", 2, 14
        ),
        "runbook_search": ReadSpec(
            "Search scoped runbooks; guidance has no action authority.", 1, 8
        ),
        "incident_search": ReadSpec(
            "Search scoped prior incidents with original source times.", 1, 8
        ),
    }
)
ReadHandler = Callable[[ReadRequest], tuple[EvidenceItem, ...]]
Authorize = Callable[[], bool]
Reserve = Callable[[tuple[ReadRequest, ...], int], bool]
Verify = Callable[[EvidenceItem], None]
Status = Literal["OK", "DENIED", "ERROR", "TIMEOUT", "BUSY", "BUDGET_EXHAUSTED"]


def tool_catalog() -> dict[str, JsonValue]:
    """Export provider-neutral catalog metadata beside the exact shared argument schema."""
    return JSON_OBJECT.validate_python(
        {
            "arguments_schema": ReadRequest.model_json_schema(),
            "tools": [
                {
                    "name": name,
                    "description": spec.description,
                    "backend_reads": spec.backend_reads,
                    "deadline_seconds": spec.deadline_seconds,
                }
                for name, spec in CATALOG.items()
            ],
        }
    )


class ReadResult(Contract):
    """Errors are explicit observations; raw provider exceptions and credentials are excluded."""

    request: ReadRequest
    status: Status
    evidence: tuple[EvidenceItem, ...] = Field(default=(), max_length=64)


@dataclass(frozen=True)
class ReadCompletion:
    """Record finish time so a slow sibling cannot invalidate an earlier completed result."""

    result: ReadResult
    completed_at: float


class ReadRegistry:
    """Trusted callers bind readers, identity refresh, verification and durable reservation."""

    def __init__(
        self,
        handlers: Mapping[ToolName, ReadHandler],
        authorize: Authorize,
        reserve: Reserve,
        verify: Verify,
        incident_id: str,
        *,
        deadline_ceiling: float = 30,
    ) -> None:
        """A ceiling shortens deadlines; unavailable readers must return explicit errors."""
        if set(handlers) != set(CATALOG) or not 0 < deadline_ceiling <= 30:
            raise ValueError("registry requires exact catalog and bounded deadline")
        self._handlers = dict(handlers)
        self._authorize, self._reserve, self._verify = authorize, reserve, verify
        self._incident_id, self._ceiling = incident_id, deadline_ceiling
        self._pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="payops-read")
        self._slots = BoundedSemaphore(2)
        self._lock, self._closed = Lock(), False

    def dispatch(self, requests: tuple[ReadRequest, ...]) -> tuple[ReadResult, ...]:
        """Validate the entire batch and durably charge it before any handler is submitted."""
        validated = tuple(ReadRequest.model_validate(item.model_dump()) for item in requests)
        if not 1 <= len(validated) <= 2 or len({x.model_dump_json() for x in validated}) != len(
            validated
        ):
            raise ValueError("read batch must contain one or two distinct requests")
        with self._lock:
            if self._closed:
                raise RuntimeError("registry closed")
            cost = sum(CATALOG[item.tool].backend_reads for item in validated)
            if not self._reserve(validated, cost):
                return tuple(ReadResult(request=x, status="BUDGET_EXHAUSTED") for x in validated)
            pending = tuple(self._submit(item) for item in validated)
        results = tuple(self._result(item, future, deadline) for item, future, deadline in pending)
        return self._publish(results)

    def _submit(
        self, request: ReadRequest
    ) -> tuple[ReadRequest, Future[ReadCompletion] | None, float]:
        """Timed-out work retains its slot until completion, preventing an unbounded task queue."""
        deadline = monotonic() + min(CATALOG[request.tool].deadline_seconds, self._ceiling)
        if not self._slots.acquire(blocking=False):
            return request, None, deadline
        try:
            return request, self._pool.submit(self._read, request, get_current()), deadline
        except BaseException:
            self._slots.release()
            raise

    def _read(self, request: ReadRequest, parent: Context) -> ReadCompletion:
        """Refresh authority on both sides of I/O and verify complete output before publication."""
        tracer = trace.get_tracer("payops.tools.registry")
        try:
            with tracer.start_as_current_span("tool_call", context=parent) as span:
                span.set_attribute("payops.tool", request.tool)
                span.set_attribute("payops.service", request.service)
                span.set_attribute(
                    "payops.query_sha256", sha256((request.query or "").encode()).hexdigest()
                )
                result = self._authorized_read(request)
                span.set_attribute("payops.status", result.status)
                span.set_attribute("payops.evidence_ids", [x.evidence_id for x in result.evidence])
                return ReadCompletion(result, monotonic())
        finally:
            self._slots.release()

    def _authorized_read(self, request: ReadRequest) -> ReadResult:
        """Late revocation or corrupt evidence discards the whole result, including prefixes."""
        try:
            if not self._authorize():
                return ReadResult(request=request, status="DENIED")
            evidence = self._handlers[request.tool](request)
            if len(evidence) > 64 or len({x.evidence_id for x in evidence}) != len(evidence):
                raise ValueError("invalid evidence count")
            for item in evidence:
                if item.incident_id != self._incident_id or item.resource != request.service:
                    raise ValueError("foreign incident or service evidence")
                self._verify(item)
            if not self._authorize():
                return ReadResult(request=request, status="DENIED")
            return ReadResult(request=request, status="OK", evidence=evidence)
        except PermissionError:
            return ReadResult(request=request, status="DENIED")
        except Exception:
            return ReadResult(request=request, status="ERROR")

    def _result(
        self, request: ReadRequest, future: Future[ReadCompletion] | None, deadline: float
    ) -> ReadResult:
        """Limit result acceptance; threads cannot cancel in-flight transport operations."""
        if future is None:
            return ReadResult(request=request, status="BUSY")
        try:
            result = future.result(timeout=max(0, deadline - monotonic()))
            if result.completed_at > deadline:
                return ReadResult(request=request, status="TIMEOUT")
            return result.result
        except TimeoutError:
            return ReadResult(request=request, status="TIMEOUT")
        except Exception:
            return ReadResult(request=request, status="ERROR")

    def _refresh(self) -> tuple[Status, float]:
        """Publication identity refresh owns its slot until completion, even after timeout."""
        try:
            try:
                status: Status = "OK" if self._authorize() else "DENIED"
            except PermissionError:
                status = "DENIED"
            except Exception:
                status = "ERROR"
            return status, monotonic()
        finally:
            self._slots.release()

    def _publication_status(self) -> Status:
        """A separate bounded identity check covers time spent waiting for sibling reads."""
        with self._lock:
            if self._closed or not self._slots.acquire(blocking=False):
                return "BUSY"
            deadline = monotonic() + min(8, self._ceiling)
            try:
                future = self._pool.submit(self._refresh)
            except BaseException:
                self._slots.release()
                raise
        try:
            status, completed = future.result(timeout=max(0, deadline - monotonic()))
            return status if completed <= deadline else "TIMEOUT"
        except TimeoutError:
            return "TIMEOUT"

    def _publish(self, results: tuple[ReadResult, ...]) -> tuple[ReadResult, ...]:
        """No successful sibling survives a revoked or unavailable final publication grant."""
        if not any(item.status == "OK" for item in results):
            return results
        status = self._publication_status()
        return tuple(
            ReadResult(request=item.request, status=status)
            if item.status == "OK" and status != "OK"
            else item
            for item in results
        )

    def close(self) -> None:
        """Stop new dispatch without waiting for transport-bounded in-flight reads to finish."""
        with self._lock:
            self._closed = True
            self._pool.shutdown(wait=False)
