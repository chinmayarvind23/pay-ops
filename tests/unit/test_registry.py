"""Real threads exercise bounded dispatch, durable-charge ordering and authority after I/O."""

from collections.abc import Iterator
from concurrent.futures import Future
from datetime import timedelta
from pathlib import Path
from threading import Barrier, Event
from time import monotonic
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Tracer

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.orchestrator.reasoning import ReadRequest, ToolName
from payops.tools.registry import (
    CATALOG,
    ReadCompletion,
    ReadRegistry,
    ReadResult,
    tool_catalog,
)


def request(tool: ToolName = "recent_logs") -> ReadRequest:
    """Use one allowed service and the tool's fixed search requirement."""
    return ReadRequest(
        tool=tool, service="payments-api", query="timeout" if tool.endswith("search") else None
    )


class Harness:
    """Retain observable charge, authorization and verification order around actual artifacts."""

    def __init__(self, root: Path) -> None:
        """Every test owns its artifact store and dispatch log."""
        self.store = ArtifactStore(root)
        now = utc_now()
        self.item = normalize(
            Observation(
                source="LOG",
                resource="payments-api",
                observed_at=now,
                query="logs",
                summary="fixture",
                payload={},
            ),
            "incident",
            now - timedelta(seconds=1),
            now + timedelta(seconds=1),
            self.store,
        )
        self.events: list[str] = []
        self.allow = True
        self.charge = True
        self.output: tuple[EvidenceItem, ...] = (self.item,)
        self.registries: list[ReadRegistry] = []

    def authorize(self) -> bool:
        """Model a fresh grant lookup without allowing model-provided role data."""
        self.events.append("authorize")
        return self.allow

    def reserve(self, requests: tuple[ReadRequest, ...], cost: int) -> bool:
        """The production owner must durably persist this charge before returning true."""
        self.events.append(f"reserve:{len(requests)}:{cost}")
        return self.charge

    def read(self, read: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Return retained fixture evidence without a network dependency."""
        self.events.append(read.tool)
        return self.output

    def verify(self, item: EvidenceItem) -> None:
        """Check actual bytes and metadata rather than accepting a fixture boolean."""
        self.events.append("verify")
        self.store.verify(item)

    def registry(self, **changes: Any) -> ReadRegistry:
        """Named test overrides exercise failure boundaries in trusted host collaborators."""
        args: dict[str, Any] = dict(
            handlers={name: self.read for name in CATALOG},
            authorize=self.authorize,
            reserve=self.reserve,
            verify=self.verify,
            incident_id="incident",
        )
        result = ReadRegistry(**{**args, **changes})
        self.registries.append(result)
        return result


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Harness]:
    """Close every pool even when an assertion fails."""
    value = Harness(tmp_path)
    yield value
    for registry in value.registries:
        registry.close()
        # Tests release their bounded fixture work before tearing down its artifact directory.
        registry._pool.shutdown(wait=True)  # pyright: ignore[reportPrivateUsage]


def test_catalog_matches_validated_requests_and_backend_costs(harness: Harness) -> None:
    """All six advertised tools use the same closed schema and independent host cost table."""
    catalog = tool_catalog()
    assert len(catalog["tools"]) == 6  # type: ignore[arg-type]
    schema = catalog["arguments_schema"]
    assert isinstance(schema, dict) and schema["additionalProperties"] is False
    for tool, spec in CATALOG.items():
        result = harness.registry().dispatch((request(tool),))[0]
        assert result.status == "OK" and result.evidence == (harness.item,)
        assert harness.events[-6:] == [
            f"reserve:1:{spec.backend_reads}",
            "authorize",
            tool,
            "verify",
            "authorize",
            "authorize",
        ]


@pytest.mark.parametrize(
    "batch",
    [
        (),
        (request(),) * 2,
        (request(),) * 3,
        (ReadRequest.model_construct(tool="run_shell", service="payments-api", query=None),),
    ],
)
def test_invalid_batch_never_charges_or_reads(
    harness: Harness, batch: tuple[ReadRequest, ...]
) -> None:
    """Even a validation-bypassing constructed instance is revalidated before reservation."""
    with pytest.raises(ValueError):
        harness.registry().dispatch(batch)
    assert harness.events == []


@pytest.mark.parametrize("setting,status", [("allow", "DENIED"), ("charge", "BUDGET_EXHAUSTED")])
def test_authority_and_budget_denials_precede_reads(
    harness: Harness, setting: str, status: str
) -> None:
    """Exhausted budget or revoked grants cannot reach an operational handler."""
    setattr(harness, setting, False)
    assert harness.registry().dispatch((request(),))[0].status == status
    assert "recent_logs" not in harness.events


def test_reservation_storage_failure_never_dispatches(harness: Harness) -> None:
    """An unavailable journal is not permission to execute an uncharged read."""

    def reserve(requests: tuple[ReadRequest, ...], cost: int) -> bool:
        """Simulate a failed durable write before ownership transfers to the dispatcher."""
        raise OSError("disk unavailable")

    with pytest.raises(OSError):
        harness.registry(reserve=reserve).dispatch((request(),))
    assert harness.events == []


@pytest.mark.parametrize("failure", [PermissionError, RuntimeError])
def test_handler_errors_are_typed_without_raw_text(
    harness: Harness, failure: type[Exception]
) -> None:
    """Secret-bearing backend errors cannot become model feedback or successful empty results."""

    def read(read: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Raise provider-shaped text that must never be exposed."""
        raise failure("credential-value-must-not-escape")

    result = harness.registry(handlers={name: read for name in CATALOG}).dispatch((request(),))[0]
    assert result.status == ("DENIED" if failure is PermissionError else "ERROR")
    assert result.evidence == () and "credential-value" not in result.model_dump_json()


@pytest.mark.parametrize("case", ["foreign", "service", "duplicate", "oversize", "corrupt"])
def test_invalid_evidence_discards_complete_result(harness: Harness, case: str) -> None:
    """A verified prefix cannot hide a later foreign, repeated or corrupted item."""
    if case == "foreign":
        harness.output = (harness.item.model_copy(update={"incident_id": "foreign"}),)
    elif case == "service":
        now = utc_now()
        harness.output = (
            normalize(
                Observation(
                    source="LOG",
                    resource="risk-sim",
                    observed_at=now,
                    query="logs",
                    summary="Valid other service",
                ),
                "incident",
                now - timedelta(seconds=1),
                now + timedelta(seconds=1),
                harness.store,
            ),
        )
    elif case in {"duplicate", "oversize"}:
        harness.output = (harness.item,) * (65 if case == "oversize" else 2)
    else:
        harness.store.path_for(harness.item.artifact_sha256).write_bytes(b"corrupt")
    result = harness.registry().dispatch((request(),))[0]
    assert result.status == "ERROR" and result.evidence == ()


def test_grant_is_refreshed_after_artifact_verification(harness: Harness) -> None:
    """Identity expiration while verifying evidence must prevent publication."""

    def verify(item: EvidenceItem) -> None:
        """Revoke the identity after actual byte verification."""
        harness.verify(item)
        harness.allow = False

    result = harness.registry(verify=verify).dispatch((request(),))[0]
    assert result.status == "DENIED" and result.evidence == ()


def test_two_independent_reads_run_concurrently(harness: Harness) -> None:
    """A rendezvous only succeeds if the second read starts before the first completes."""
    barrier = Barrier(2)

    def read(read: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Bound a broken serial implementation instead of hanging the test process."""
        barrier.wait(timeout=2)
        return harness.output

    results = harness.registry(handlers={name: read for name in CATALOG}).dispatch(
        (request(), request("workload_status"))
    )
    assert [x.status for x in results] == ["OK", "OK"]
    assert harness.events[0] == "reserve:2:3"


def test_timed_out_work_keeps_slots_and_late_results_are_discarded(harness: Harness) -> None:
    """Repeated timeouts cannot enqueue more backend operations while two reads remain active."""
    release = Event()
    started = Barrier(2)

    def read(read: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Hold both workers independently of dispatcher timeout, with a bounded emergency exit."""
        started.wait(timeout=2)
        assert release.wait(3)
        return harness.output

    registry = harness.registry(handlers={name: read for name in CATALOG}, deadline_ceiling=0.1)
    try:
        result = registry.dispatch((request(), request("workload_status")))
        assert [x.status for x in result] == ["TIMEOUT", "TIMEOUT"]
        assert registry.dispatch((request(),))[0].status == "BUSY"
        assert all(x.evidence == () for x in result)
        registry.close()
        with pytest.raises(RuntimeError, match="closed"):
            registry.dispatch((request(),))
    finally:
        release.set()


@pytest.mark.parametrize("finished_offset,status", [(-2, "OK"), (1, "TIMEOUT")])
def test_completion_timestamp_controls_late_consumption(
    harness: Harness, finished_offset: float, status: str
) -> None:
    """A sibling wait may delay consumption but cannot change this call's actual completion time."""
    registry = harness.registry()
    deadline = monotonic() - 1
    future: Future[ReadCompletion] = Future()
    future.set_result(
        ReadCompletion(ReadResult(request=request(), status="OK"), deadline + finished_offset)
    )
    assert registry._result(request(), future, deadline).status == status  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("ceiling", [0, -1, 31, float("nan")])
def test_invalid_deadline_rejected(harness: Harness, ceiling: float) -> None:
    """Trusted configuration cannot widen or remove the maximum result deadline."""
    with pytest.raises(ValueError):
        harness.registry(deadline_ceiling=ceiling)


def test_missing_catalog_binding_rejected(harness: Harness) -> None:
    """Catalog and dispatcher cannot silently drift into different tool sets."""
    with pytest.raises(ValueError):
        harness.registry(handlers={})


def test_worker_spans_keep_parent_and_exclude_query_text(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Thread dispatch must preserve the investigation trace without exporting raw search text."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test-registry")

    def bound_tracer(name: str) -> Tracer:
        """Bind only this test's exporter without installing a global provider."""
        return tracer

    monkeypatch.setattr("payops.tools.registry.trace.get_tracer", bound_tracer)
    with tracer.start_as_current_span("invoke_agent") as parent:
        result = harness.registry().dispatch((request("runbook_search"),))[0]
        assert result.status == "OK"
        parent_context = parent.get_span_context()
    spans = exporter.get_finished_spans()
    tool = next(span for span in spans if span.name == "tool_call")
    assert tool.parent is not None and tool.parent.span_id == parent_context.span_id
    assert tool.context is not None and tool.context.trace_id == parent_context.trace_id
    assert tool.attributes is not None
    assert tool.attributes["payops.status"] == "OK"
    assert "timeout" not in str(dict(tool.attributes))
    provider.shutdown()


def test_fast_sibling_cannot_publish_after_later_revocation(harness: Harness) -> None:
    """A slow first read may revoke authority after a fast second read has been authorized."""
    fast_verified = Event()

    def read(read: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Hold the first result until the second reaches its end-of-worker grant lookup."""
        if read.tool == "recent_logs":
            assert fast_verified.wait(2)
            harness.allow = False
        return harness.output

    def authorize() -> bool:
        """Signal after a verified fast result and preserve the captured grant for that check."""
        allowed = harness.allow
        if "verify" in harness.events:
            fast_verified.set()
        return allowed

    results = harness.registry(
        handlers={name: read for name in CATALOG}, authorize=authorize
    ).dispatch((request(), request("workload_status")))
    assert not harness.allow
    assert all(x.status == "DENIED" and x.evidence == () for x in results)


@pytest.mark.parametrize("failure", [PermissionError, RuntimeError])
def test_publication_identity_error_discards_evidence(
    harness: Harness, failure: type[Exception]
) -> None:
    """An identity backend outage during final publication cannot expose retained OK evidence."""
    calls = 0

    def authorize() -> bool:
        """Fail only the third lookup, after worker validation has already completed."""
        nonlocal calls
        calls += 1
        if calls == 3:
            raise failure("identity backend secret")
        return True

    result = harness.registry(authorize=authorize).dispatch((request(),))[0]
    assert result.status == ("DENIED" if failure is PermissionError else "ERROR")
    assert result.evidence == ()


def test_publication_timeout_discards_evidence_and_keeps_slot(harness: Harness) -> None:
    """A blocked final grant cannot delay publication indefinitely or release its active slot."""
    release = Event()
    calls = 0

    def authorize() -> bool:
        """Keep the final lookup active until the test has inspected the timed-out outcome."""
        nonlocal calls
        calls += 1
        if calls == 3:
            assert release.wait(3)
        return True

    registry = harness.registry(authorize=authorize, deadline_ceiling=0.1)
    try:
        result = registry.dispatch((request(),))[0]
        assert result.status == "TIMEOUT" and result.evidence == ()
        assert registry._slots.acquire(blocking=False)  # pyright: ignore[reportPrivateUsage]
        try:
            assert not registry._slots.acquire(blocking=False)  # pyright: ignore[reportPrivateUsage]
            pending = (ReadResult(request=request(), status="OK", evidence=harness.output),)
            blocked = registry._publish(pending)[0]  # pyright: ignore[reportPrivateUsage]
            assert blocked.status == "BUSY" and blocked.evidence == ()
        finally:
            registry._slots.release()  # pyright: ignore[reportPrivateUsage]
    finally:
        release.set()


def test_interrupted_publication_clock_releases_acquired_slot(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Publication admission also returns capacity if interrupted before worker submission."""
    registry = harness.registry()

    def clock() -> float:
        """Simulate an interrupt after ownership transfers to the publication gate."""
        raise KeyboardInterrupt

    monkeypatch.setattr("payops.tools.registry.monotonic", clock)
    with pytest.raises(KeyboardInterrupt):
        registry._publication_status()  # pyright: ignore[reportPrivateUsage]
    assert registry._slots.acquire(blocking=False)  # pyright: ignore[reportPrivateUsage]
    assert registry._slots.acquire(blocking=False)  # pyright: ignore[reportPrivateUsage]
    registry._slots.release()  # pyright: ignore[reportPrivateUsage]
    registry._slots.release()  # pyright: ignore[reportPrivateUsage]


def test_tracer_lookup_failure_releases_worker_slots(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tracing setup failures return both worker slots before a subsequent valid batch."""
    registry = harness.registry()
    requests = (request("recent_logs"), request("workload_status"))

    def unavailable(name: str) -> Tracer:
        """Fail before span creation, without exposing private backend details to results."""
        raise RuntimeError("private tracing backend failure")

    with monkeypatch.context() as scope:
        scope.setattr("payops.tools.registry.trace.get_tracer", unavailable)
        failed = registry.dispatch(requests)
    assert [result.status for result in failed] == ["ERROR", "ERROR"]
    assert all(not result.evidence for result in failed)
    assert harness.events == ["reserve:2:3"]
    assert registry._slots.acquire(blocking=False)  # pyright: ignore[reportPrivateUsage]
    assert registry._slots.acquire(blocking=False)  # pyright: ignore[reportPrivateUsage]
    registry._slots.release()  # pyright: ignore[reportPrivateUsage]
    registry._slots.release()  # pyright: ignore[reportPrivateUsage]
    assert [result.status for result in registry.dispatch(requests)] == ["OK", "OK"]
