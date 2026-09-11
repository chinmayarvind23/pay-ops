"""Fixed argv, byte caps and exact counterfactual specs for the risk retention experiment."""

import json
import sys
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import JsonValue
from test_memory import replace_field
from test_scheduler import SchedulerFixture

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.leak_gateway import LOG_BYTES, LeakGateway
from payops.scenarios.leak_specs import IMAGE, leak_specs
from payops.scenarios.recipes import container


def baseline() -> JsonObject:
    """Derive the normal risk role from the shared healthy deployment fixture."""
    return json.loads(
        json.dumps(SchedulerFixture().original)
        .replace("payments-api", "risk-sim")
        .replace('"payments"', '"risk"')
    )


def test_counterfactual_changes_only_retention_mode() -> None:
    """Reverse the reviewed edits and compare whole specs, including all unrelated settings."""
    document = baseline()
    original = deepcopy(document)
    control, retained = leak_specs(document)
    assert container(control)["image"] == IMAGE
    assert object_items(container(retained)["env"])[-1]["value"] == "retained-v1"
    object_items(container(retained)["env"])[-1]["value"] = "released-v1"
    assert retained == control and document == original
    container(control)["image"] = "payops-sandbox:local"
    container(control)["env"] = list[JsonValue](object_items(container(control)["env"])[:-1])
    control["strategy"] = object_value(document["spec"])["strategy"]
    assert control == document["spec"]


@pytest.mark.parametrize("change", ["role", "cpu", "memory", "command", "duplicate"])
def test_changed_baseline_rejects(change: str) -> None:
    """Unreviewed entrypoints, limits and ambiguous environment cannot enter the experiment."""
    document = baseline()
    item = container(object_value(document["spec"]))
    if change == "role":
        object_items(item["env"])[0]["value"] = "payments"
    elif change in {"cpu", "memory"}:
        object_value(object_value(item["resources"])["limits"])[change] = "1"
    elif change == "command":
        item["command"] = ["other"]
    else:
        item["env"] = [*object_items(item["env"]), object_items(item["env"])[0]]
    with pytest.raises(ValueError):
        leak_specs(document)


def gateway(tmp_path: Path) -> LeakGateway:
    """Construct with an explicit fixture config while intercepting process execution in tests."""
    config = tmp_path / "kubeconfig"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        return LeakGateway(config)


@pytest.mark.parametrize("previous", [False, True])
def test_log_read_has_fixed_scope_and_preserves_raw_text(tmp_path: Path, previous: bool) -> None:
    """Only the selected container stream differs; log content remains unmodified evidence."""
    adapter = gateway(tmp_path)
    pod: JsonObject = {
        "metadata": {"name": "risk-sim-abc-xyz", "namespace": "payops-sandbox", "uid": "uid"}
    }
    with patch("payops.scenarios.leak_gateway.bounded_read", return_value=b"raw\r\n") as read:
        result = adapter.read_log(pod, previous)
    args = read.call_args.args[0]
    assert "pod/risk-sim-abc-xyz" in args and "--container=sandbox" in args
    assert ("--previous=true" in args) is previous
    assert result["text"] == "raw\r\n" and result["pod_uid"] == "uid"
    assert read.call_args.args[1:] == (LOG_BYTES, 12)


def test_caps_and_invalid_log_targets_fail_closed(tmp_path: Path) -> None:
    """A capped log or foreign pod is an error, never proof of missing or completed workload."""
    adapter = gateway(tmp_path)
    pod: JsonObject = {
        "metadata": {"name": "risk-sim-abc-xyz", "namespace": "payops-sandbox", "uid": "uid"}
    }
    with patch("payops.scenarios.leak_gateway.bounded_read", return_value=b"x" * LOG_BYTES):
        with pytest.raises(ValueError):
            adapter.read_log(pod, True)
    replace_field(pod, ("metadata", "name"), "payments-api-abc-xyz")
    with patch("payops.scenarios.leak_gateway.bounded_read") as read:
        with pytest.raises(ValueError):
            adapter.read_log(pod, True)
    read.assert_not_called()
    for target in ("payments-api", "processor-adapter", "nodes"):
        with pytest.raises(ValueError):
            adapter.validate_target(target)


def test_snapshot_count_and_api_byte_caps(tmp_path: Path) -> None:
    """Reject multiplicity and truncated API responses before constructing provenance."""
    adapter = gateway(tmp_path)
    with (
        patch.object(adapter, "observe", return_value={"pods": []}),
        patch.object(adapter, "_json", return_value={"items": []}),
    ):
        assert adapter.snapshot()["replica_sets"] == []
    with (
        patch.object(adapter, "observe", return_value={"pods": [{}, {}, {}]}),
        patch.object(adapter, "_json", return_value={"items": []}),
    ):
        with pytest.raises(ValueError):
            adapter.snapshot()
    with patch("payops.scenarios.leak_gateway.bounded_read", return_value=b"x" * 262144):
        with pytest.raises(ValueError):
            adapter.deployment("risk-sim")
    with patch("payops.scenarios.leak_gateway.bounded_read", return_value=b'{"metadata":{}}'):
        assert adapter.deployment("risk-sim") == {"metadata": {}}


class ProcessGateway(LeakGateway):
    """Exercise the real subprocess reader with an inert JSON producer instead of Kubernetes."""

    def __init__(self) -> None:
        """Substitute an inert producer only in this test adapter."""
        self._prefix = (
            sys.executable,
            "-c",
            'import json; print(json.dumps({"metadata": {}}))',
        )


def test_api_budget_matches_real_subprocess_reader() -> None:
    """An API read must satisfy the shared reader's actual bounds before it can reach kubectl."""
    assert ProcessGateway().deployment("risk-sim") == {"metadata": {}}
