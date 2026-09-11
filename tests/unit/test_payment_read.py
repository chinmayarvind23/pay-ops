"""Pinned snapshot reads enforce provider bounds before payment arithmetic sees evidence."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import httpx
import pytest
from test_payment_window import interval, raw_snapshot, source

from payops.contracts import utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.payment_window import (
    Service,
    derive_payment_window,
    snapshot_observation,
    snapshot_queries,
)
from payops.tools.payment import PaymentRead


class Stream(httpx.SyncByteStream):
    """Exercise the live raw-stream contract rather than an already consumed mock response."""

    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        """Keep explicit transport chunk boundaries for size and elapsed-time tests."""
        self.chunks = chunks

    def __iter__(self) -> Iterator[bytes]:
        """Yield raw bytes without automatic decompression or buffering."""
        yield from self.chunks


def test_snapshot_pins_two_reads_and_preserves_scrape_time() -> None:
    """Both requests select the same instant; source time comes from the actual watermark."""
    payload = raw_snapshot(interval())
    calls: list[httpx.Request] = []
    at = datetime.fromtimestamp(cast(float, payload["evaluated_at"]), UTC)

    def respond(request: httpx.Request) -> httpx.Response:
        """Return the corresponding fixed provider response without networking."""
        calls.append(request)
        key = "metrics" if len(calls) == 1 else "watermark"
        return httpx.Response(200, stream=Stream((json.dumps(payload[key]).encode(),)))

    result = PaymentRead(transport=httpx.MockTransport(respond)).snapshot("payments-api", at)
    assert len(calls) == 2
    assert [call.url.params["query"] for call in calls] == list(snapshot_queries("payments-api"))
    assert all(float(call.url.params["time"]) == at.timestamp() for call in calls)
    assert result.observed_at.timestamp() == at.timestamp() - 0.5
    assert result.payload == payload


@pytest.mark.parametrize(
    "origin", ["https://example.com", "http://localhost", "http://127.0.0.1/x"]
)
def test_origin_is_operator_loopback_only(origin: str) -> None:
    """Provider configuration cannot turn the local reader into a network proxy."""
    with pytest.raises(ValueError):
        PaymentRead(origin)


@pytest.mark.parametrize("service", ["foreign", 'payments-api"}'])
def test_scope_rejected_before_network(service: str) -> None:
    """Invalid selectors fail before any transport dispatch."""

    def respond(request: httpx.Request) -> httpx.Response:
        """Any request would violate the pre-dispatch boundary."""
        pytest.fail("invalid service reached transport")

    with pytest.raises(ValueError):
        PaymentRead(transport=httpx.MockTransport(respond)).snapshot(cast(Service, service))


@pytest.mark.parametrize(
    "body",
    [b"x" * 131073, b"{", b'{"status":"success","status":"error"}'],
    ids=["oversized", "malformed", "duplicate"],
)
def test_invalid_response_denied(body: bytes) -> None:
    """Oversized, malformed and ambiguous JSON never becomes a normalized snapshot."""
    reader = PaymentRead(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Stream((body,))))
    )
    with pytest.raises(ValueError):
        reader.snapshot("payments-api")


def test_redirect_is_not_followed() -> None:
    """A trusted endpoint cannot redirect the reader to a different destination."""
    reader = PaymentRead(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(302, headers={"Location": "http://example.com"})
        )
    )
    with pytest.raises(httpx.HTTPStatusError):
        reader.snapshot("payments-api")


@pytest.mark.parametrize("at", [datetime(2026, 1, 1), utc_now() + timedelta(days=1)])
def test_invalid_time_does_not_dispatch(at: datetime) -> None:
    """Naive or future instants cannot become source provenance."""
    with pytest.raises(ValueError, match="nonfuture aware"):
        PaymentRead().snapshot("payments-api", at)


def test_empty_watermark_remains_missing() -> None:
    """A failed target has no scrape timestamp; its query instant is explicitly retained."""
    payload = raw_snapshot(interval())
    payload["watermark"] = {"status": "success", "data": {"resultType": "vector", "result": []}}
    observation = snapshot_observation(payload, "payments-api")
    assert observation.observed_at.timestamp() == payload["evaluated_at"]


def test_retained_metadata_query_cannot_disagree(tmp_path: Path) -> None:
    """A correctly hashed metadata query still must match its raw snapshot selector."""
    store, period = ArtifactStore(tmp_path), interval()
    before = source(store, period, raw_snapshot(period), query="wrong selector")
    after = source(store, period, raw_snapshot(period, True))
    with pytest.raises(ValueError, match="query provenance"):
        derive_payment_window(before, after, period, store)


def test_live_payload_future_time_denied() -> None:
    """Even direct observation construction rechecks future source timestamps."""
    window = interval().model_copy(
        update={
            "start": utc_now() + timedelta(days=1),
            "end": utc_now() + timedelta(days=1, seconds=10),
        }
    )
    with pytest.raises(ValueError, match="after collection"):
        snapshot_observation(raw_snapshot(window), "payments-api")


def test_compression_rejected_before_decoding() -> None:
    """The server cannot override identity encoding to expand memory before the byte guard."""
    reader = PaymentRead(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200, headers={"Content-Encoding": "gzip"}, stream=Stream((b"invalid gzip",))
            )
        )
    )
    with pytest.raises(ValueError, match="compressed"):
        reader.snapshot("payments-api")


@pytest.mark.parametrize("chunks,times", [((b"{", b"}"), (0, 4, 6)), ((), (0, 6))])
def test_elapsed_budget_checks_each_chunk_and_completion(
    monkeypatch: pytest.MonkeyPatch, chunks: tuple[bytes, ...], times: tuple[int, ...]
) -> None:
    """A trickle or delayed empty body consumes the cooperative elapsed allowance."""
    clock = iter(times)
    monkeypatch.setattr("payops.tools.payment.monotonic", lambda: next(clock))
    reader = PaymentRead(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Stream(chunks)))
    )
    with pytest.raises(ValueError, match="elapsed budget"):
        reader.snapshot("payments-api")
