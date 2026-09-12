"""Dependency observations retain source bytes and cannot broaden their resource scope."""

from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from payops.scenarios.contracts import JsonObject
from payops.scenarios.dependency_gateway import DependencyGateway, data_deployments

MODULE = "payops.scenarios.dependency_gateway"


def gateway(tmp_path: Path) -> DependencyGateway:
    """Explicit fixture configuration prevents accidental use of a global Kubernetes context."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        return DependencyGateway(config)


def test_fixed_data_scope_and_complete_inventory(tmp_path: Path) -> None:
    """The data namespace is explicit and every response remains bounded before JSON parsing."""
    adapter = gateway(tmp_path)
    args = adapter.postgres_forward_argv()
    assert args[args.index("--namespace") + 1] == "payops-data"
    assert args[-4:] == ("service/postgres", "35532:5432", "--address", "127.0.0.1")
    with patch(MODULE + ".bounded_read", return_value=b'{"items": []}') as read:
        state = adapter.data_state()
        assert state["deployments"] == state["pods"] == {"items": []}
        assert read.call_count == 2
        assert read.call_args.args[0][6] == "payops-data"
        assert adapter.redis() == {"items": []}
    with patch(MODULE + ".bounded_read", return_value=b"x" * 262144):
        with pytest.raises(ValueError, match="exceeds bound"):
            adapter.redis()


def test_session_query_rejects_injection_before_transport(tmp_path: Path) -> None:
    """Only a validated run identity can enter the fixed role-count query."""
    adapter = gateway(tmp_path)
    run = "a" * 32
    with patch.object(adapter, "data_read", return_value='{"limit":4,"total":0}') as read:
        assert adapter.postgres_sessions(run) == {"limit": 4, "total": 0}
        query = read.call_args.args[0][-1]
        assert "FROM pg_stat_activity" in query
        assert "WHERE usename='payops_synthetic'" in query
        assert "payops-scenario-" + run in query
        with pytest.raises(ValueError):
            adapter.postgres_sessions("'; DROP TABLE payments; --")
        assert read.call_count == 1


@pytest.mark.parametrize(
    "names",
    [
        ("postgres", "redis", "elasticsearch"),
        ("postgres", "redis"),
        ("postgres", "redis", "foreign"),
    ],
)
def test_data_inventory_requires_exact_service_set(names: tuple[str, ...]) -> None:
    """Missing or unrelated deployments cannot silently enter an outage experiment."""
    state: JsonObject = {"deployments": {"items": [{"metadata": {"name": name}} for name in names]}}
    if set(names) == {"postgres", "redis", "elasticsearch"}:
        assert set(data_deployments(state)) == set(names)
    else:
        with pytest.raises(ValueError, match="unexpected data service set"):
            data_deployments(state)


@pytest.mark.parametrize("failure", [None, "retry", "timeout", "oversize", "http"])
def test_metrics_stream_preserves_snapshot_and_closes_transport(
    tmp_path: Path, failure: str | None
) -> None:
    """Only connection startup retries; HTTP errors and oversized observations remain failures."""
    adapter = gateway(tmp_path)
    attempts = 0

    def serve(request: httpx.Request) -> httpx.Response:
        """Return real HTTPX streaming responses without starting a live port forward."""
        nonlocal attempts
        attempts += 1
        assert request.method == "GET" and request.url.path == "/metrics"
        if failure == "timeout" or (failure == "retry" and attempts == 1):
            raise httpx.ConnectError("not ready", request=request)
        return httpx.Response(
            503 if failure == "http" else 200,
            content=b"x" * 131073 if failure == "oversize" else b"payments_total 3\n",
            headers={"x-payops-metrics-snapshot": "original-snapshot"},
        )

    client = httpx.Client(transport=httpx.MockTransport(serve))
    with (
        patch.object(adapter, "_forward", return_value=nullcontext("http://fixture")),
        patch(MODULE + ".httpx.Client", return_value=client),
        patch(
            MODULE + ".time.monotonic",
            side_effect=[0, 0, 13] if failure == "timeout" else [0, 0, 1],
        ),
        patch(MODULE + ".time.sleep"),
    ):
        if failure in {"timeout", "oversize", "http"}:
            with pytest.raises((TimeoutError, ValueError, httpx.HTTPStatusError)):
                adapter.metrics()
        else:
            result = adapter.metrics()
            assert result["text"] == "payments_total 3\n"
            assert result["snapshot"] == "original-snapshot"
    assert client.is_closed
    assert attempts == (2 if failure == "retry" else 1)
