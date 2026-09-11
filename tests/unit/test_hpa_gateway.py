"""Create-only resources and conditional deletes preserve concurrent operator changes."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from payops.scenarios.contracts import JsonObject, object_value
from payops.scenarios.hpa_contract import HPA_NAME, hpa_spec
from payops.scenarios.hpa_gateway import HpaGateway, owned_metadata, resource_path

RUN = "a" * 32


def document() -> JsonObject:
    """An API-returned HPA carries both run ownership and server concurrency tokens."""
    return {
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": HPA_NAME,
            "namespace": "payops-sandbox",
            "uid": "owned",
            "resourceVersion": "42",
            "labels": {"payops.dev/hpa-run": RUN},
        },
        "spec": hpa_spec(1),
    }


def gateway(tmp_path: Path) -> HpaGateway:
    """Use explicit fixture configuration and intercept transport in each test."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        return HpaGateway(config)


@pytest.mark.parametrize("field", ["uid", "namespace", "name", "resourceVersion", "labels"])
def test_foreign_metadata_rejects(field: str) -> None:
    """Neither a name match nor an arbitrary resource version can authorize cleanup."""
    observed = document()
    object_value(observed["metadata"])[field] = {} if field == "labels" else ""
    with pytest.raises(ValueError):
        owned_metadata("hpa", observed, RUN)


def test_paths_are_closed() -> None:
    """A supplied path or malformed run identity cannot reach a raw API endpoint."""
    assert resource_path("job", RUN).endswith("/jobs/hpa-load-" + RUN)
    with pytest.raises(ValueError):
        resource_path("hpa", "../other")


def test_writes_send_literal_json_and_atomic_preconditions(tmp_path: Path) -> None:
    """The server receives UID/version tests in its actual DeleteOptions body."""
    adapter = gateway(tmp_path)
    with (
        patch.object(adapter, "verify_scope"),
        patch(
            "payops.scenarios.hpa_gateway.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, "{}", ""),
        ) as invoke,
    ):
        adapter.remove_owned("hpa", document(), RUN)
        args = invoke.call_args.args[0]
        body = json.loads(invoke.call_args.kwargs["input"])
        assert args[-5:] == ("delete", "--raw", resource_path("hpa", RUN), "-f", "-")
        assert body["preconditions"] == {"uid": "owned", "resourceVersion": "42"}
        assert body["propagationPolicy"] == "Foreground"
        assert invoke.call_args.kwargs["shell"] is False
        adapter.create_hpa(RUN)
        assert invoke.call_args.args[0][-5:] == ("create", "-f", "-", "-o", "json")
        adapter.create_load(RUN)
        assert json.loads(invoke.call_args.kwargs["input"])["kind"] == "Job"
        adapter.set_cap(document(), RUN, 2)
        changed = json.loads(invoke.call_args.kwargs["input"])
        assert changed["spec"] == hpa_spec(2) and changed["metadata"]["resourceVersion"] == "42"


def test_unknown_cap_and_capped_reads_reject(tmp_path: Path) -> None:
    """An unrecognized configuration and truncated response cannot become accepted state."""
    adapter = gateway(tmp_path)
    observed = document()
    object_value(observed["spec"])["minReplicas"] = 9
    with pytest.raises(ValueError):
        adapter.set_cap(observed, RUN, 2)
    with patch("payops.scenarios.hpa_gateway.bounded_read", return_value=b"{}"):
        assert adapter.read_resource("hpa", RUN) == {}
    with patch("payops.scenarios.hpa_gateway.bounded_read", return_value=b"x" * 262144):
        with pytest.raises(ValueError):
            adapter.read_resource("job", RUN)


def test_mutation_response_cap_rejects(tmp_path: Path) -> None:
    """Oversized server output remains an error even after the server may have accepted a write."""
    adapter = gateway(tmp_path)
    with (
        patch.object(adapter, "verify_scope"),
        patch(
            "payops.scenarios.hpa_gateway.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, "x" * 262144, ""),
        ),
    ):
        with pytest.raises(ValueError, match="response is capped"):
            adapter.create_hpa(RUN)
