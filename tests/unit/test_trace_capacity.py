"""Exercise the enlarged acquisition cap with real exporter bytes and OS pipe boundaries."""

import io
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace import SpanKind
from test_trace_read import Backend, reader, scope

from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import (
    MAX_LOG_BYTES,
    MAX_SPANS,
    ROLES,
    TraceScope,
    parse_console_log,
    verify_trace_log,
)
from payops.tools.traces import MAX_SOURCE_BYTES, bounded_read, drain_pipe


def exporter_workload() -> tuple[bytes, TraceScope]:
    """Export the real eight-payment/four-child shape without changing the global provider."""
    output = io.StringIO()
    resource = Resource(
        {
            "service.name": "payops-sandbox-payments",
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.44.0",
            "service.instance.id": "00000000-0000-4000-8000-000000000001",
        }
    )
    provider = TracerProvider(resource=resource, sampler=ALWAYS_ON, shutdown_on_exit=False)
    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter(out=output)))
    tracer = provider.get_tracer("payops.sandbox")
    start = datetime.now(UTC) - timedelta(seconds=1)
    try:
        for _ in range(8):
            with tracer.start_as_current_span(
                "sandbox.payments", context=Context(), kind=SpanKind.SERVER
            ):
                for role in ("risk", "processor", "ledger", "webhook"):
                    with tracer.start_as_current_span("sandbox.call." + role, kind=SpanKind.CLIENT):
                        pass
    finally:
        provider.shutdown()
    # Exporter nanoseconds round to microseconds; the synthetic emission prefix must be later.
    end = datetime.now(UTC) + timedelta(milliseconds=1)
    prefix = end.isoformat(timespec="microseconds").replace("+00:00", "123Z")
    raw = "".join(f"{prefix} {line}\n" for line in output.getvalue().splitlines()).encode()
    return raw, TraceScope(
        incident_id="capacity-control", service="payments-api", start=start, end=end
    )


def test_actual_exporter_forty_span_workload_fits_reviewed_cap() -> None:
    """Actual pretty JSON plus Kubernetes line prefixes reproduces the old capacity failure."""
    raw, request = exporter_workload()
    assert 65536 < len(raw) < MAX_LOG_BYTES == 131072
    parsed = parse_console_log(raw, request)
    assert len(parsed.spans) == 40 and len({row.span.trace_id for row in parsed.spans}) == 8
    assert (
        not parsed.limit_reached
        and not parsed.partial_candidates
        and not parsed.malformed_candidates
    )
    assert MAX_SPANS == 128


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_real_child_pipe_enforces_new_byte_boundary(
    monkeypatch: pytest.MonkeyPatch, delta: int
) -> None:
    """Cap-minus-one and cap bytes return intact; one extra byte fails and reaps the child."""
    original = subprocess.Popen
    children: list[subprocess.Popen[bytes]] = []

    def tracked(args: tuple[str, ...], **kwargs: Any) -> subprocess.Popen[bytes]:
        """Keep the actual handle to prove the bounded read closes its owned process."""
        child = cast("subprocess.Popen[bytes]", original(args, **kwargs))
        children.append(child)
        return child

    monkeypatch.setattr("payops.tools.traces.subprocess.Popen", tracked)
    command = (
        sys.executable,
        "-c",
        f"import sys;sys.stdout.buffer.write(b'x'*{MAX_LOG_BYTES + delta})",
    )
    if delta > 0:
        with pytest.raises(ValueError, match="subprocess failed"):
            bounded_read(command, MAX_LOG_BYTES, 3)
    else:
        assert len(bounded_read(command, MAX_LOG_BYTES, 3)) == MAX_LOG_BYTES + delta
    assert len(children) == 1 and children[0].poll() is not None


def test_drain_retains_at_most_cap_even_with_excess_input() -> None:
    """The next 4096-byte chunk trips overflow without extending the retained buffer."""
    output, exceeded = bytearray(), threading.Event()
    drain_pipe(io.BytesIO(b"x" * (MAX_LOG_BYTES + 4096)), output, MAX_LOG_BYTES, exceeded)
    assert exceeded.is_set() and len(output) == MAX_LOG_BYTES


@pytest.mark.parametrize("reduced_budget", [False, True])
def test_ten_source_collection_preserves_aggregate_bound_and_cap_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reduced_budget: bool
) -> None:
    """Ten individually capped logs remain bounded and visibly incomplete across all services."""

    class Fleet(Backend):
        """Return a fixed-size timestamped access line from each of ten owned pods."""

        active_service = "payments-api"

        def __call__(self, args: tuple[str, ...], maximum: int, timeout: float) -> bytes:
            """Only the existing closed reader creates the service and log commands."""
            if args[8:10] == ("get", "deployment"):
                self.active_service = args[10]
            result = super().__call__(args, maximum, timeout)
            if args[8] == "logs":
                assert maximum == MAX_LOG_BYTES
                prefix = b"2024-01-01T00:00:12Z "
                return prefix + b"x" * (maximum - len(prefix))
            return result.replace(b"payments-api", self.active_service.encode())

    assert MAX_SOURCE_BYTES == 10 * 131072
    if reduced_budget:
        monkeypatch.setattr("payops.tools.traces.MAX_SOURCE_BYTES", MAX_SOURCE_BYTES - 1)
    backend, store = Fleet(2), ArtifactStore(tmp_path / "artifacts")
    scopes = tuple(scope().model_copy(update={"service": service}) for service in ROLES)
    if reduced_budget:
        with pytest.raises(ValueError, match="aggregate trace bytes"):
            reader(tmp_path, monkeypatch, backend).collect(scopes, store)
        assert not list(store.root.glob("*.json"))
    else:
        captured = reader(tmp_path, monkeypatch, backend).collect(scopes, store)
        assert captured.commands_used == 40 and captured.capped_sources == 10
        assert (
            sum(verify_trace_log(item, store).parsed.raw_bytes for item in captured.sources)
            == MAX_SOURCE_BYTES
        )
        assert not captured.spans and captured.graph.sampling == "bounded_sample"
