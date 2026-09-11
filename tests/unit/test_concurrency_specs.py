"""The concurrency treatment must never change the deployment's resource budget."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_scenarios import FakeCluster, document

from payops.scenarios.concurrency_specs import IMAGE, concurrency_spec
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container, fault_spec
from payops.scenarios.runner import LocalScenarioRunner


def baseline() -> JsonObject:
    """Include an unrelated resource field to detect destructive spec reconstruction."""
    original = document("payments-api")
    container(object_value(original["spec"]))["resources"] = {
        "requests": {"cpu": "50m", "memory": "96Mi", "ephemeral-storage": "64Mi"},
        "limits": {"cpu": "500m", "memory": "256Mi", "ephemeral-storage": "128Mi"},
    }
    return original


def test_enabled_spec_preserves_exact_original_and_resource_limits() -> None:
    """The same enabled spec supports both traffic controls and the parallel treatment."""
    original = baseline()
    saved = deepcopy(original)
    enabled = concurrency_spec(original)
    expected = deepcopy(object_value(original["spec"]))
    expected["strategy"] = {"type": "Recreate"}
    item = container(expected)
    item["image"] = IMAGE
    object_items(item["env"])[1]["value"] = '{"concurrency_memory":true}'
    assert enabled == expected and original == saved
    assert concurrency_spec(original) == enabled


@pytest.mark.parametrize(
    "fault",
    ["command", "init", "env", "role", "protocol", "capture", "active", "cpu", "memory"],
)
def test_uncontrolled_baselines_reject(fault: str) -> None:
    """Existing workloads, startup overrides and different quotas invalidate the contrast."""
    original = baseline()
    spec = object_value(original["spec"])
    item = container(spec)
    env = object_items(item["env"])
    if fault == "command":
        item["command"] = ["other"]
    elif fault == "init":
        object_value(object_value(spec["template"])["spec"])["initContainers"] = [{}]
    elif fault == "env":
        item["env"] = [*env, deepcopy(env[0])]
    elif fault == "role":
        env[0]["value"] = "risk"
    elif fault in {"protocol", "capture", "active"}:
        settings = {
            "protocol": {"risk_protocol": "v2"},
            "capture": {"cpu_capture": True},
            "active": {"concurrency_memory": True},
        }
        env[1]["value"] = json.dumps(settings[fault])
    else:
        object_value(object_value(item["resources"])["limits"])[fault] = "100m"
    with pytest.raises(ValueError):
        concurrency_spec(original)


def test_generic_injector_cannot_substitute_a_readiness_fault() -> None:
    """OOM-04 requires its own lifecycle instead of falling through to the default mutation."""
    original = baseline()
    with pytest.raises(ValueError, match="specialized"):
        fault_spec("OOM-04", object_value(original["spec"]))


def test_generic_runner_rejects_before_creating_a_run(tmp_path: Path) -> None:
    """The public runner cannot create a misleading receipt or mutate this specialized case."""
    gateway = FakeCluster("OOM-04")
    runner = LocalScenarioRunner(tmp_path / "config", tmp_path / "evidence", gateway)
    with pytest.raises(ValueError, match="specialized"):
        runner.run("OOM-04")
    assert gateway.patches == 0
    assert not runner.block_file.exists()
