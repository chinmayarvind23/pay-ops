"""Read adapters cannot turn model choices into shell commands or broad resource access."""

import json
from pathlib import Path

import httpx
import pytest

from payops.contracts import utc_now
from payops.evidence.normalize import Observation
from payops.tools.collect import collect_local
from payops.tools.kubernetes import KubernetesRead
from payops.tools.prometheus import PrometheusRead


def test_kubernetes_projection_and_closed_commands(tmp_path: Path) -> None:
    """The model sees configuration names, while inline environment values stay excluded."""
    config = tmp_path / "kubeconfig"
    config.write_text("test")
    calls: list[tuple[str, ...]] = []

    def invoke(args: tuple[str, ...]) -> str:
        """Return API-shaped snapshots without contacting a Kubernetes cluster."""
        calls.append(args)
        if "pods" in args:
            return '{"items":[]}'
        return json.dumps(
            {
                "metadata": {"name": "payments-api", "namespace": "payops-sandbox"},
                "spec": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "sandbox",
                                    "env": [{"name": "TOKEN", "value": "secret-marker"}],
                                }
                            ]
                        }
                    },
                },
                "status": {},
            }
        )

    reader = KubernetesRead(config, invoke)
    observations = reader.collect("payments-api")
    assert len(observations) == 2
    assert "secret-marker" not in str(observations)
    assert "TOKEN" in str(observations)
    assert all("--context" in call and "kind-payops-dev" in call for call in calls)
    with pytest.raises(ValueError):
        reader.collect("secrets")
    with pytest.raises(ValueError):
        reader.logs("payments-api;delete")
    assert len(calls) == 2


def test_kubernetes_rejects_cross_namespace(tmp_path: Path) -> None:
    """A server response is untrusted even when the requested scope was fixed."""
    config = tmp_path / "kubeconfig"
    config.write_text("test")

    def invoke(_args: tuple[str, ...]) -> str:
        """Simulate a misrouted or compromised response from another namespace."""
        return '{"metadata":{"name":"payments-api","namespace":"foreign"},"items":[]}'

    with pytest.raises(ValueError, match="namespace"):
        KubernetesRead(config, invoke).collect("payments-api")


@pytest.mark.parametrize("bad", ["none", "namespace", "naive"])
def test_event_current_uid_scope_and_series_time(tmp_path: Path, bad: str) -> None:
    """Repeated events use their latest observed time and cannot borrow an old pod's identity."""
    config = tmp_path / "kubeconfig"
    config.write_text("test")
    current = utc_now().isoformat()
    event = {
        "metadata": {"namespace": "payops-sandbox"},
        "involvedObject": {"uid": "current-pod", "namespace": "payops-sandbox"},
        "message": "Readiness probe failed 404",
        "eventTime": "2020-01-01T00:00:00Z",
        "series": {"lastObservedTime": current},
    }
    if bad == "namespace":
        event["metadata"] = {"namespace": "foreign"}
    if bad == "naive":
        event["series"] = {"lastObservedTime": "2026-09-11T00:00:00"}

    def invoke(args: tuple[str, ...]) -> str:
        """Return one current and one unrelated pod event to test the UID join."""
        if "pods" in args:
            return json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "uid": "current-pod",
                                "namespace": "payops-sandbox",
                                "labels": {"app.kubernetes.io/name": "payments-api"},
                            }
                        }
                    ]
                }
            )
        return json.dumps({"items": [event, {**event, "involvedObject": {"uid": "old-pod"}}]})

    reader = KubernetesRead(config, invoke)
    if bad != "none":
        with pytest.raises(ValueError):
            reader.events("payments-api")
    else:
        observations = reader.events("payments-api")
        assert len(observations) == 1
        assert observations[0].observed_at.isoformat() == current


def test_prometheus_query_is_fixed_and_payment_count_is_not_multiplied() -> None:
    """Count ingress payment requests once, excluding each downstream service's counter."""
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Capture the real URL encoding and return a valid empty instant vector."""
        calls.append(str(request.url))
        assert 'service="payments-api"' in request.url.params["query"]
        return httpx.Response(
            200, json={"status": "success", "data": {"resultType": "vector", "result": []}}
        )

    reader = PrometheusRead(transport=httpx.MockTransport(respond))
    assert reader.query("requests").source == "PROMETHEUS"
    with pytest.raises(ValueError):
        reader.query("arbitrary_promql")
    assert len(calls) == 1


@pytest.mark.parametrize(
    "origin",
    [
        "https://bank.example",
        "http://169.254.169.254",
        "http://user:pass@127.0.0.1",
        "http://127.0.0.1/query",
    ],
)
def test_prometheus_does_not_proxy_arbitrary_origins(origin: str) -> None:
    """Endpoint selection is a local deployment setting, not a generic HTTP tool."""
    with pytest.raises(ValueError):
        PrometheusRead(origin)


@pytest.mark.parametrize(
    "payload",
    [
        {"status": "error"},
        {"status": "success"},
        {"status": "success", "data": {"resultType": "vector", "result": None}},
    ],
)
def test_prometheus_malformed_success_is_not_healthy(payload: dict[str, object]) -> None:
    """Response schema failures remain explicit missing telemetry."""

    def respond(_request: httpx.Request) -> httpx.Response:
        """Inject a malformed backend response without a network connection."""
        return httpx.Response(200, json=payload)

    with pytest.raises(ValueError):
        PrometheusRead(transport=httpx.MockTransport(respond)).query("requests")


@pytest.mark.parametrize(
    "value,service", [("NaN", "payments-api"), ("1", "foreign"), ("broken", "payments-api")]
)
def test_prometheus_sample_scope_and_value(value: str, service: str) -> None:
    """A success envelope cannot make nonnumeric or cross-service data valid evidence."""

    def respond(_request: httpx.Request) -> httpx.Response:
        """Return one adversarial scalar under otherwise correct vector metadata."""
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": {"service": service}, "value": [1.0, value]}],
                },
            },
        )

    with pytest.raises(ValueError):
        PrometheusRead(transport=httpx.MockTransport(respond)).query("requests")


def test_collection_retains_failures_and_continues(tmp_path: Path) -> None:
    """Unavailable logs do not erase independently collected deployment and metric evidence."""

    class FakeKubernetes(KubernetesRead):
        """A test-only source has no kubectl transport or credentials."""

        def __init__(self) -> None:
            """Construct no external connections for the partial-failure test."""

        def collect(self, service: str) -> tuple[Observation, ...]:
            """Provide one current status for each fixed service."""
            return (
                Observation(
                    source="KUBERNETES",
                    resource=service,
                    observed_at=utc_now(),
                    query="status",
                    summary="Running",
                ),
            )

        def events(self, service: str) -> tuple[Observation, ...]:
            """No current events is a valid empty observation set."""
            return ()

        def logs(self, service: str) -> Observation:
            """A simulated permission failure must appear in the result."""
            raise ValueError("sensitive backend details")

    class FakePrometheus(PrometheusRead):
        """Metric fixtures isolate orchestration failure semantics."""

        def query(self, signal: str) -> Observation:
            """Return one current typed observation without making network calls."""
            return Observation(
                source="PROMETHEUS",
                resource="payments-api",
                observed_at=utc_now(),
                query=signal,
                summary="Synthetic metric",
            )

    result = collect_local(FakeKubernetes(), FakePrometheus(), tmp_path)
    assert len(result.evidence) == 10
    assert len(result.failures) == 5
    assert "sensitive backend details" not in (tmp_path / "collection.json").read_text()
