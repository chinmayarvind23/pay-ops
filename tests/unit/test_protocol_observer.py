"""Exercise actual HTTP streaming and capture ordering before allowing live protocol traffic."""

import json
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest
from test_protocol_observation import observed
from test_sampling_harness import Clock

from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import PodIdentity, TraceScope
from payops.sandbox.models import Sample
from payops.scenarios.contracts import JsonObject
from payops.scenarios.protocol_contract import ProtocolStage
from payops.scenarios.protocol_gateway import ProtocolGateway
from payops.scenarios.protocol_observer import RuntimeProtocolObserver
from payops.tools.traces import TraceCollection


class Body(httpx.AsyncByteStream):
    """Keep mock response bytes unread so the production streaming limit actually runs."""

    def __init__(self, content: bytes) -> None:
        """Retain exact wire bytes, including deliberately oversized bodies."""
        self.content = content

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Yield the body through HTTPX's actual raw streaming iterator."""
        yield self.content


@pytest.mark.parametrize("stage", ["original", "mismatch"])
def test_http_capture_order_and_persisted_probe(tmp_path: Path, stage: ProtocolStage) -> None:
    """The acquisition adapter records actual status, sample, headers and source window."""
    baseline, store = observed(tmp_path)
    clock = Clock()
    requests: list[httpx.Request] = []
    order: list[str] = []

    def http(request: httpx.Request) -> httpx.Response:
        """Only health and the fixed synthetic POST are reachable through this transport."""
        requests.append(request)
        assert request.url.host == "127.0.0.1"
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok", "role": "payments", "synthetic": True})
        assert request.method == "POST" and request.url.path == "/simulate"
        sample = Sample.model_validate_json(request.content)
        assert request.headers["accept-encoding"] == "identity"
        clock.value += timedelta(seconds=0.1)
        order.append("probe")
        body = (
            {"detail": "synthetic risk returned 422"}
            if stage == "mismatch"
            else {"role": "payments", "status": "accepted", "sample_id": sample.sample_id}
        )
        return httpx.Response(
            502 if stage == "mismatch" else 200, stream=Body(json.dumps(body).encode())
        )

    expected_store = store

    class Sources:
        """Scope assertions test acquisition; semantic trace acceptance has separate controls."""

        def risk_log(self, identity: PodIdentity, start: datetime) -> JsonObject:
            """The mismatch log is acquired before waiting for trace export."""
            assert identity == baseline.identities["risk-sim"]
            assert start == clock.now() - timedelta(seconds=1.1)
            order.append("access")
            return {"text": "retained raw source"}

        def collect(self, scopes: tuple[TraceScope, ...], store: ArtifactStore) -> TraceCollection:
            """All five fixed services share the request window and original artifact store."""
            assert store is expected_store and len(scopes) == 5
            assert {scope.service for scope in scopes} == set(baseline.identities)
            assert all(scope.incident_id == "wire-test" for scope in scopes)
            assert all(scope.end == clock.now() for scope in scopes)
            assert all((scope.end - scope.start).total_seconds() == 13.1 for scope in scopes)
            order.append("capture")
            return baseline.capture

    def sleep(seconds: float) -> None:
        """Advance the export wait without a wall-clock delay."""
        assert seconds == 12
        order.append("wait")
        clock.value += timedelta(seconds=seconds)

    sources = Sources()
    observer = RuntimeProtocolObserver(
        tmp_path / "kubeconfig", sources, sources, httpx.MockTransport(http), clock.now, sleep
    )
    result = observer.collect(stage, "wire-test", baseline.identities, tmp_path, store)
    assert result.probe.mode == "fixture_replay"
    assert result.probe.status == (502 if stage == "mismatch" else 200)
    assert len(requests) == 2
    assert result.probe.traceparent == requests[-1].headers["traceparent"]
    assert result.probe.sample == Sample.model_validate_json(requests[-1].content)
    assert order == (
        ["probe", "access", "wait", "capture"]
        if stage == "mismatch"
        else ["probe", "wait", "capture"]
    )
    saved = json.loads((tmp_path / stage / "probe.json").read_bytes())
    assert saved == result.probe.model_dump(mode="json")


@pytest.mark.parametrize("fault", ["oversized", "compressed"])
def test_http_rejects_unbounded_or_encoded_response(tmp_path: Path, fault: str) -> None:
    """No trace collection or successful probe publication follows invalid wire bytes."""
    baseline, store = observed(tmp_path)

    def http(request: httpx.Request) -> httpx.Response:
        """A valid readiness response isolates the failing synthetic POST body."""
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok", "role": "payments", "synthetic": True})
        return httpx.Response(
            200,
            headers={"content-encoding": "gzip"} if fault == "compressed" else {},
            stream=Body(b"x" * 8193),
        )

    class NoSources:
        """Any diagnostic read after a rejected HTTP body is a test failure."""

        def risk_log(self, identity: PodIdentity, start: datetime) -> JsonObject:
            """Failed probes cannot acquire a misleading corroborating source."""
            pytest.fail("unexpected access log read")

        def collect(self, scopes: tuple[TraceScope, ...], store: ArtifactStore) -> TraceCollection:
            """Failed probes stop before trace collection."""
            pytest.fail("unexpected trace collection")

    observer = RuntimeProtocolObserver(
        tmp_path / "kubeconfig",
        log_reader=NoSources(),
        trace_reader=NoSources(),
        transport=httpx.MockTransport(http),
    )
    with pytest.raises(ValueError, match="compressed|byte cap"):
        observer.collect("original", "wire-test", baseline.identities, tmp_path, store)
    assert not (tmp_path / "original" / "probe.json").exists()


def test_gateway_limits_risk_log_and_write_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The concrete gateway rejects unrelated writes and capped or foreign log reads."""
    baseline, _ = observed(tmp_path)
    config = tmp_path / "kubeconfig"
    config.write_text("fixture", encoding="utf-8")

    def executable(name: str) -> str:
        """Only construct argv; the bounded subprocess call is replaced below."""
        return "fixture-kubectl"

    monkeypatch.setattr("payops.scenarios.kubectl.shutil.which", executable)
    gateway = ProtocolGateway(config)
    body = b"retained"

    def read(args: tuple[str, ...], limit: int, timeout: float) -> bytes:
        """Assert fixed namespace, pod, stream limits and deadline at the actual subprocess seam."""
        assert "payops-sandbox" in args and baseline.identities["risk-sim"].pod_name in args
        assert "--tail=200" in args and "--limit-bytes=16384" in args
        assert limit == 16384 and timeout == 12
        return body

    monkeypatch.setattr("payops.scenarios.protocol_gateway.bounded_read", read)
    identity, start = baseline.identities["risk-sim"], baseline.probe.started_at
    assert gateway.risk_log(identity, start)["text"] == "retained"
    body = b"x" * 16384
    with pytest.raises(ValueError, match="capped"):
        gateway.risk_log(identity, start)
    with pytest.raises(ValueError, match="scope"):
        gateway.risk_log(baseline.identities["payments-api"], start)
    for target in ("payments-api", "risk-sim"):
        gateway.validate_target(target)
    with pytest.raises(ValueError, match="allowlist"):
        gateway.validate_target("processor-adapter")
