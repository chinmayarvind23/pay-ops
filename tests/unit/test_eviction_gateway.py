"""Isolated-cluster transport checks preserve ownership and release failed health forwards."""

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from test_eviction_harness import RUN, namespace

from payops.scenarios.contracts import object_value
from payops.scenarios.eviction_contract import NODE
from payops.scenarios.eviction_gateway import EvictionGateway

MODULE = "payops.scenarios.eviction_gateway"


def gateway(tmp_path: Path) -> EvictionGateway:
    """The fixture uses an explicit kubeconfig and never starts a real subprocess."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        return EvictionGateway(config)


@pytest.mark.parametrize("foreign", [None, "node", "docker"])
def test_scope_requires_both_node_and_container_ownership(
    tmp_path: Path, foreign: str | None
) -> None:
    """A matching node name alone cannot authorize editing an unrelated Docker container."""
    adapter = gateway(tmp_path)
    nodes = {"items": [{"metadata": {"name": "foreign" if foreign == "node" else NODE}}]}
    container = [
        {
            "Id": "owned",
            "Config": {
                "Labels": {
                    "io.x-k8s.kind.cluster": "foreign" if foreign == "docker" else "payops-eviction"
                }
            },
        }
    ]
    with (
        patch.object(adapter, "_json", return_value=nodes),
        patch(MODULE + ".bounded_read", return_value=json.dumps(container).encode()),
    ):
        if foreign:
            with pytest.raises(ValueError, match="mismatch"):
                adapter.verify_scope()
        else:
            assert adapter.verify_scope()["container_id"] == "owned"


@pytest.mark.parametrize("invalid", ["identity", "bytes", "oversize"])
def test_config_transition_rejects_before_writing(tmp_path: Path, invalid: str) -> None:
    """Foreign identity, foreign bytes and oversized configuration cannot reach tee or restart."""
    adapter = gateway(tmp_path)
    with (
        patch.object(
            adapter,
            "verify_scope",
            return_value={"container_id": "foreign" if invalid == "identity" else "owned"},
        ),
        patch.object(
            adapter, "config_text", return_value="foreign" if invalid == "bytes" else "original"
        ),
        patch(MODULE + ".subprocess.run") as write,
        pytest.raises(ValueError),
    ):
        adapter.replace_config("original", "x" * 32769 if invalid == "oversize" else "new", "owned")
    write.assert_not_called()


def test_api_write_preserves_create_only_and_conditional_delete(tmp_path: Path) -> None:
    """Actual serialized payloads retain fixed resource names and server concurrency tokens."""
    adapter = gateway(tmp_path)
    document = namespace()
    with (
        patch.object(adapter, "verify_scope"),
        patch.object(adapter, "_json", return_value=document),
        patch(
            MODULE + ".subprocess.run", return_value=subprocess.CompletedProcess([], 0, "{}", "")
        ) as write,
    ):
        adapter.namespace(RUN)
        assert json.loads(write.call_args.kwargs["input"])["kind"] == "Namespace"
        assert "kind-payops-eviction" in write.call_args.args[0]
        adapter.create_victim(RUN, True)
        body = json.loads(write.call_args.kwargs["input"])
        assert body["metadata"]["name"] == "victim-recovered"
        assert body["spec"]["automountServiceAccountToken"] is False
        adapter.remove_namespace(document, RUN)
        assert json.loads(write.call_args.kwargs["input"])["preconditions"] == {
            "uid": "namespace-owned",
            "resourceVersion": "7",
        }
        object_value(document["metadata"])["labels"] = {}
        with pytest.raises(ValueError, match="ownership changed"):
            adapter.remove_namespace(document, RUN)
        assert write.call_count == 3
    with (
        patch.object(adapter, "verify_scope"),
        patch(
            MODULE + ".subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, "x" * 262145, ""),
        ),
    ):
        with pytest.raises(ValueError, match="response capped"):
            adapter.namespace(RUN)


def test_observation_keeps_partial_failures_explicit_and_enforces_bounds(tmp_path: Path) -> None:
    """Restarting kubelet reads remain failures rather than empty healthy observations."""
    adapter = gateway(tmp_path)
    with (
        patch.object(adapter, "_json", return_value={"items": []}),
        patch(MODULE + ".bounded_read", return_value=b"{}"),
    ):
        assert set(adapter.observation()) == {"nodes", "pods", "events", "stats", "configz"}
        assert adapter.namespace_absence() == adapter.pod() == adapter.pod(True) == {"items": []}
        assert adapter.config_text() == "{}"
    with patch.object(adapter, "_json", side_effect=subprocess.TimeoutExpired("kubectl", 10)):
        assert adapter.observation() == {"acquisition_error": "TimeoutExpired"}
    with (
        patch.object(adapter, "_json", return_value={}),
        patch(MODULE + ".bounded_read", return_value=b"x" * 262144),
    ):
        with pytest.raises(ValueError, match="observation capped"):
            adapter.observation()


@pytest.mark.parametrize("failure", [None, "timeout", "bad_json", "kill"])
def test_health_probe_always_releases_owned_forward(tmp_path: Path, failure: str | None) -> None:
    """Success, malformed response and exhausted startup all close HTTP and process resources."""
    adapter = gateway(tmp_path)

    def serve(request: httpx.Request) -> httpx.Response:
        """Exercise the real HTTP client against a bounded fixture transport."""
        if failure == "timeout":
            raise httpx.ConnectError("starting", request=request)
        return httpx.Response(
            200, content=b"invalid" if failure == "bad_json" else b'{"status":"accepted"}'
        )

    client = httpx.Client(transport=httpx.MockTransport(serve))
    process = MagicMock()
    process.stderr.read.return_value = b"forward diagnostic"
    if failure == "kill":
        process.wait.side_effect = [subprocess.TimeoutExpired("kubectl", 5), 0]
    with (
        patch(MODULE + ".socket.socket"),
        patch(MODULE + ".subprocess.Popen", return_value=process),
        patch(MODULE + ".httpx.Client", return_value=client),
        patch(MODULE + ".time.monotonic", side_effect=[0, 0, 11]),
        patch(MODULE + ".time.sleep"),
        patch(MODULE + ".bounded_read", return_value=b"pod logs"),
        patch.object(adapter, "pod", return_value={}),
    ):
        if failure == "bad_json":
            with pytest.raises(ValueError):
                adapter.healthy_victim()
        else:
            result = adapter.healthy_victim()
            if failure == "timeout":
                assert result["acquisition_error"] == "victim HTTP forward unavailable"
                assert (
                    result["forward_error"] == "forward diagnostic" and result["logs"] == "pod logs"
                )
            else:
                assert result == {"status": 200, "body": {"status": "accepted"}}
    assert client.is_closed
    process.terminate.assert_called_once()
    assert process.kill.call_count == (1 if failure == "kill" else 0)
