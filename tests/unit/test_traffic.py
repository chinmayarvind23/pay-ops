"""Bounded workload, counterfactual protocol and cancellation tests use no live cluster."""

import asyncio
import json
import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pydantic import ValidationError

from payops.sandbox.service import create_service
from payops.scenarios.traffic import (
    SliceCount,
    TrafficDriver,
    TrafficReceipt,
    Workload,
    scoped_forward,
    wait_forward_ready,
)


def workload(count: int = 8, **changes: Any) -> Workload:
    """Construct a typed bounded default while allowing negative contract tests."""
    values: dict[str, Any] = {
        "role": "webhook",
        "distribution": (
            SliceCount(processor="A", region="us", payment_method="credit", count=count),
        ),
    }
    return Workload(**(values | changes))


def stored_receipt(root: Path) -> TrafficReceipt:
    """Read the durable receipt after an exception prevents a normal return value."""
    return TrafficReceipt.model_validate_json(next(root.glob("*/receipt.json")).read_bytes())


@pytest.mark.parametrize(
    "changes",
    [
        {"concurrency": 17},
        {"concurrency": 0},
        {"deadline_seconds": 181.0},
        {"request_timeout_seconds": 11.0},
        {"seed": -1},
        {"role": "ledger"},
        {"url": "https://example.com"},
        {"distribution": ()},
    ],
)
def test_workload_rejects_unbounded_or_unscoped(changes: dict[str, Any]) -> None:
    """No caller can turn a workload into arbitrary destinations or unbounded work."""
    with pytest.raises(ValidationError):
        workload(**changes)


def test_distribution_rejects_duplicates_and_total() -> None:
    """Per-slice valid counts cannot bypass the aggregate bound."""
    first = SliceCount(processor="A", region="us", payment_method="credit", count=300)
    second = SliceCount(processor="B", region="us", payment_method="credit", count=300)
    for distribution in ((first, first), (first, second)):
        with pytest.raises(ValidationError):
            workload(distribution=distribution)


def test_explicit_distribution_concurrency_and_traces(tmp_path: Path) -> None:
    """Actual overlapping requests stay bounded and preserve exact slices and W3C IDs."""
    active = maximum = 0
    headers: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        """Yield while tracking overlap so concurrency is exercised rather than assumed."""
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        headers.append(request.headers["traceparent"])
        assert request.url.host == "127.0.0.1" and request.url.path == "/simulate"
        await asyncio.sleep(0.001)
        active -= 1
        body = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "sample_id": body["sample_id"],
                "role": "webhook",
                "status": "accepted",
                "synthetic": True,
            },
        )

    specification = workload(
        distribution=(
            SliceCount(processor="A", region="us", payment_method="credit", count=256),
            SliceCount(processor="B", region="eu", payment_method="debit", count=256),
        ),
        concurrency=16,
    )
    receipt = asyncio.run(
        TrafficDriver(tmp_path / "config", tmp_path, httpx.MockTransport(handler)).run(
            specification
        )
    )
    assert receipt.status == "completed" and receipt.mode == "fixture_replay"
    assert maximum == 16 and active == 0
    assert len({item.planned.sample.sample_id for item in receipt.attempts}) == 512
    assert Counter(item.planned.sample.processor for item in receipt.attempts) == {
        "A": 256,
        "B": 256,
    }
    assert len(set(headers)) == 512
    assert all(re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-01", header) for header in headers)
    assert all(
        item.started_at and item.http_status == 200 and item.latency_seconds is not None
        for item in receipt.attempts
    )
    assert stored_receipt(tmp_path) == receipt
    assert next(tmp_path.glob("*/plan.json")).is_file()


def test_duplicate_and_conflict_are_distinct(tmp_path: Path) -> None:
    """Real synthetic webhook storage accepts exact replay and rejects changed payload."""
    driver = TrafficDriver(
        tmp_path / "config", tmp_path, httpx.ASGITransport(app=create_service("webhook"))
    )
    receipt = asyncio.run(driver.run_idempotency_probe())
    assert receipt.probe_verified is True
    assert [item.http_status for item in receipt.attempts] == [200, 200, 409]
    assert len({item.planned.sample.sample_id for item in receipt.attempts}) == 1
    assert len({item.planned.traceparent for item in receipt.attempts}) == 3
    assert receipt.attempts[0].planned.sample == receipt.attempts[1].planned.sample
    assert receipt.attempts[2].planned.sample.processor == "B"


@pytest.mark.parametrize(
    "code,body,outcome",
    [
        (503, b"unavailable", "http_error"),
        (302, b"redirect", "http_error"),
        (409, b"invalid json", "http_error"),
        (200, b"not JSON", "invalid_response"),
        (200, b"x" * 8193, "invalid_response"),
        (200, b'{"sample_id":"wrong","role":"webhook","status":"accepted"}', "invalid_response"),
    ],
)
def test_http_results_remain_observations(
    tmp_path: Path, code: int, body: bytes, outcome: str
) -> None:
    """Unsuccessful and malformed responses retain their real status without invented success."""
    driver = TrafficDriver(
        tmp_path / "config",
        tmp_path,
        httpx.MockTransport(lambda _: httpx.Response(code, content=body)),
    )
    result = asyncio.run(driver.run(workload(1))).attempts[0]
    assert result.outcome == outcome and result.http_status == code
    assert result.result is None


class SlowTransport(httpx.AsyncBaseTransport):
    """Track worker and client lifetimes across timeout and external cancellation."""

    def __init__(self) -> None:
        """Counters let tests prove teardown instead of only inspecting a status string."""
        self.active = 0
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Pending work must unwind its finally block before the driver returns."""
        self.active += 1
        try:
            await asyncio.sleep(10)
            return httpx.Response(503)
        finally:
            self.active -= 1

    async def aclose(self) -> None:
        """Client shutdown is visible independently from worker cancellation."""
        self.closed = True


def test_deadline_records_started_and_queued_attempts(tmp_path: Path) -> None:
    """A batch deadline cancels and awaits in-flight workers, preserving all planned IDs."""
    transport = SlowTransport()
    receipt = asyncio.run(
        TrafficDriver(tmp_path / "config", tmp_path, transport).run(
            workload(concurrency=2, deadline_seconds=0.03)
        )
    )
    assert receipt.status == "deadline_exceeded" and transport.closed and transport.active == 0
    assert len(receipt.attempts) == 8
    assert sum(item.started_at is not None for item in receipt.attempts) == 2
    assert all(
        item.outcome == "cancelled" and item.http_status is None for item in receipt.attempts
    )


def test_external_cancellation_persists_receipt(tmp_path: Path) -> None:
    """Caller cancellation propagates only after client closure and complete attempt accounting."""
    transport = SlowTransport()

    async def cancel() -> None:
        """Cancel after workers start to test cancellation inside network I/O."""
        task = asyncio.create_task(
            TrafficDriver(tmp_path / "config", tmp_path, transport).run(workload(concurrency=1))
        )
        while transport.active == 0:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel())
    receipt = stored_receipt(tmp_path)
    assert receipt.status == "cancelled" and len(receipt.attempts) == 8
    assert transport.active == 0 and transport.closed


def test_request_timeout_does_not_abort_remaining_attempts(tmp_path: Path) -> None:
    """Each attempt has a shorter independent deadline and the batch can still finish."""
    receipt = asyncio.run(
        TrafficDriver(tmp_path / "config", tmp_path, SlowTransport()).run(
            workload(2, concurrency=1, request_timeout_seconds=0.01)
        )
    )
    assert receipt.status == "completed"
    assert all(item.outcome == "timed_out" for item in receipt.attempts)


def test_transport_failure_retains_error_class_only(tmp_path: Path) -> None:
    """Raw exception messages cannot leak arbitrary peer content into evidence receipts."""

    def handler(request: httpx.Request) -> httpx.Response:
        """A connection failure has no observed HTTP status."""
        raise httpx.ConnectError("do not retain this raw message", request=request)

    receipt = asyncio.run(
        TrafficDriver(tmp_path / "config", tmp_path, httpx.MockTransport(handler)).run(workload(1))
    )
    assert receipt.attempts[0].error_type == "ConnectError"
    assert receipt.attempts[0].http_status is None
    assert "raw message" not in receipt.model_dump_json()


def test_unexpected_failure_still_has_receipt(tmp_path: Path) -> None:
    """Non-HTTP implementation failures propagate and leave a failed batch receipt."""

    def handler(_: httpx.Request) -> httpx.Response:
        """Inject an exception outside the recoverable HTTP error taxonomy."""
        raise RuntimeError("fixture failure")

    with pytest.raises(ExceptionGroup):
        asyncio.run(
            TrafficDriver(tmp_path / "config", tmp_path, httpx.MockTransport(handler)).run(
                workload(1)
            )
        )
    assert stored_receipt(tmp_path).status == "failed"


def test_fixed_forward_cleanup_and_scope(tmp_path: Path) -> None:
    """Structured process arguments pin the target, and exceptions still terminate the child."""
    process = MagicMock()
    with (
        patch("payops.scenarios.traffic.shutil.which", return_value="kubectl.exe"),
        patch("payops.scenarios.traffic.socket.socket"),
        patch("payops.scenarios.traffic.subprocess.Popen", return_value=process) as launch,
    ):
        with pytest.raises(RuntimeError), scoped_forward(tmp_path / "config", "webhook") as origin:
            assert origin == "http://127.0.0.1:18083"
            raise RuntimeError("interrupt")
    argv = launch.call_args.args[0]
    assert "kind-payops-dev" in argv and "payops-sandbox" in argv
    assert "service/webhook-sim" in argv and launch.call_args.kwargs["shell"] is False
    process.terminate.assert_called_once()
    process.wait.assert_called_once_with(timeout=5)


def test_health_requires_matching_synthetic_role() -> None:
    """Forward readiness checks are fixed GETs and do not create hidden sample attempts."""
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        """Return a wrong role first to prove identity is checked before yielding the client."""
        nonlocal calls
        calls += 1
        assert request.method == "GET" and request.url.path == "/health"
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "synthetic": True,
                "role": "payments" if calls == 1 else "webhook",
            },
        )

    async def ready() -> None:
        """Use a fixture client to exercise the real readiness loop."""
        async with httpx.AsyncClient(
            base_url="http://127.0.0.1:18083", transport=httpx.MockTransport(handler)
        ) as client:
            await wait_forward_ready(client, "webhook")

    asyncio.run(ready())
    assert calls == 2


def test_forward_kills_unresponsive_owned_process(tmp_path: Path) -> None:
    """A timeout during graceful shutdown cannot leave the child forward running silently."""
    process = MagicMock()
    process.wait.side_effect = [subprocess.TimeoutExpired("kubectl", 5), 0]
    with (
        patch("payops.scenarios.traffic.shutil.which", return_value="kubectl.exe"),
        patch("payops.scenarios.traffic.socket.socket"),
        patch("payops.scenarios.traffic.subprocess.Popen", return_value=process),
    ):
        with scoped_forward(tmp_path / "config", "payments"):
            pass
    process.kill.assert_called_once()
    assert process.wait.call_count == 2


def test_forward_rejects_missing_binary(tmp_path: Path) -> None:
    """Missing operator prerequisites cannot fall back to another execution surface."""
    with patch("payops.scenarios.traffic.shutil.which", return_value=None):
        with pytest.raises(ValueError), scoped_forward(tmp_path / "config", "payments"):
            pytest.fail("forward must not start")


def test_scoped_client_lifetime_without_live_process(tmp_path: Path) -> None:
    """The non-fixture route verifies scope and closes its client before forward teardown."""
    client = httpx.AsyncClient(
        base_url="http://127.0.0.1:18083",
        transport=httpx.ASGITransport(app=create_service("webhook")),
    )
    forward = MagicMock()
    forward.return_value.__enter__.return_value = "http://127.0.0.1:18083"
    with (
        patch("payops.scenarios.traffic.KubectlGateway") as gateway,
        patch("payops.scenarios.traffic.scoped_forward", forward),
        patch("payops.scenarios.traffic.httpx.AsyncClient", return_value=client),
    ):
        receipt = asyncio.run(TrafficDriver(tmp_path / "config", tmp_path).run(workload(1)))
    gateway.return_value.verify_scope.assert_called_once()
    forward.return_value.__exit__.assert_called_once()
    assert client.is_closed and receipt.attempts[0].http_status == 200


def test_scope_failure_never_starts_forward(tmp_path: Path) -> None:
    """Failing the dedicated-cluster checks preserves an empty-start receipt and sends no HTTP."""
    with (
        patch("payops.scenarios.traffic.KubectlGateway") as gateway,
        patch("payops.scenarios.traffic.scoped_forward") as forward,
    ):
        gateway.return_value.verify_scope.side_effect = ValueError("wrong cluster")
        with pytest.raises(ValueError):
            asyncio.run(TrafficDriver(tmp_path / "config", tmp_path).run(workload(2)))
    forward.assert_not_called()
    receipt = stored_receipt(tmp_path)
    assert receipt.status == "failed"
    assert all(item.started_at is None for item in receipt.attempts)


def test_health_retries_connection_and_malformed_json() -> None:
    """Startup races and malformed liveness responses do not establish readiness."""
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        """Exercise both recoverable liveness parsing and network errors."""
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("not listening", request=request)
        if calls == 2:
            return httpx.Response(200, content=b"not json")
        return httpx.Response(200, json={"status": "ok", "synthetic": True, "role": "webhook"})

    async def ready() -> None:
        """Keep fixture client ownership explicit during the readiness test."""
        async with httpx.AsyncClient(
            base_url="http://127.0.0.1:18083", transport=httpx.MockTransport(handler)
        ) as client:
            await wait_forward_ready(client, "webhook")

    asyncio.run(ready())
    assert calls == 3


@pytest.mark.parametrize("kind", ["missing_synthetic", "wrong_role", "malformed_schema"])
def test_success_identity_is_strict(tmp_path: Path, kind: str) -> None:
    """A successful status alone cannot stand in for a valid response from the requested role."""

    def handler(request: httpx.Request) -> httpx.Response:
        """Mutate one response fact at a time while preserving other plausible fields."""
        body = {
            "sample_id": json.loads(request.content)["sample_id"],
            "role": "webhook",
            "synthetic": True,
            "status": "accepted",
        }
        if kind == "missing_synthetic":
            del body["synthetic"]
        elif kind == "wrong_role":
            body["role"] = "payments"
        else:
            body["status"] = "unrecognized"
        return httpx.Response(200, json=body)

    receipt = asyncio.run(
        TrafficDriver(tmp_path / "config", tmp_path, httpx.MockTransport(handler)).run(workload(1))
    )
    assert receipt.attempts[0].outcome == "invalid_response"


def test_probe_does_not_accept_arbitrary_409(tmp_path: Path) -> None:
    """Conflict status without the actual storage conflict contract cannot verify idempotency."""
    driver = TrafficDriver(
        tmp_path / "config",
        tmp_path,
        httpx.MockTransport(lambda _: httpx.Response(409, json={"detail": "other"})),
    )
    assert asyncio.run(driver.run_idempotency_probe()).probe_verified is False


def test_setup_timeout_is_not_reported_as_batch_deadline(tmp_path: Path) -> None:
    """An independent operator gateway timeout is a failed setup, not an exhausted workload."""
    with patch("payops.scenarios.traffic.KubectlGateway") as gateway:
        gateway.return_value.verify_scope.side_effect = TimeoutError()
        with pytest.raises(TimeoutError):
            asyncio.run(TrafficDriver(tmp_path / "config", tmp_path).run(workload(1)))
    assert stored_receipt(tmp_path).status == "failed"
