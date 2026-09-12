"""A simulated OOM or foreign object cannot qualify an isolated kubelet eviction."""

from copy import deepcopy

import pytest
import yaml

from payops.scenarios.contracts import JsonObject, object_value
from payops.scenarios.eviction_contract import evicted, pressure_config, victim_pod
from payops.scenarios.eviction_harness import eviction_proof, node_recovered, webhook_healthy
from payops.scenarios.recipes import fault_spec


def proof() -> tuple[JsonObject, JsonObject, str]:
    """Represent independent API facts rather than a single trusted success flag."""
    original: JsonObject = {"metadata": {"uid": "victim"}}
    observed: JsonObject = {
        "pods": {
            "items": [
                {
                    "metadata": {"uid": "victim"},
                    "status": {
                        "phase": "Failed",
                        "reason": "Evicted",
                        "message": "The node was low on resource: memory.",
                    },
                }
            ]
        },
        "nodes": {
            "items": [{"status": {"conditions": [{"type": "MemoryPressure", "status": "True"}]}}]
        },
        "events": {"items": [{"involvedObject": {"uid": "victim"}, "reason": "Evicted"}]},
        "stats": {"node": {"memory": {"availableBytes": 1000}}},
        "configz": {"kubeletconfig": {"evictionHard": {"memory.available": "2000"}}},
    }
    return observed, original, "evictionHard:\n  memory.available: '2000'\n"


@pytest.mark.parametrize(
    "corruption",
    [None, "oom", "foreign", "no_pressure", "wrong_config", "no_event", "enough_memory"],
)
def test_eviction_requires_joined_kubelet_facts(corruption: str | None) -> None:
    """Require the owned pod, memory condition and configured signal for eviction."""
    observed, original, config = proof()
    if corruption == "oom":
        observed["pods"] = {
            "items": [
                {
                    "metadata": {"uid": "victim"},
                    "status": {"phase": "Failed", "reason": "OOMKilled", "message": "memory"},
                }
            ]
        }
    elif corruption == "foreign":
        original["metadata"] = {"uid": "other"}
    elif corruption == "no_pressure":
        observed["nodes"] = {"items": [{"status": {"conditions": []}}]}
    elif corruption == "wrong_config":
        observed["configz"] = {"kubeletconfig": {"evictionHard": {}}}
    elif corruption == "no_event":
        observed["events"] = {"items": []}
    elif corruption == "enough_memory":
        observed["stats"] = {"node": {"memory": {"availableBytes": 3000}}}
    assert eviction_proof(observed, original, config) is (corruption is None)


def test_calibrated_threshold_preserves_original_and_avoids_cgroup_resize() -> None:
    """Only eviction settings and enforcement change; no host memory allocation is requested."""
    original = "kind: KubeletConfiguration\nevictionHard:\n  nodefs.available: 0%\n"
    memory: JsonObject = {"availableBytes": 8 * 1024**3, "workingSetBytes": 1024**3}
    changed = yaml.safe_load(pressure_config(original, memory))
    assert changed["evictionHard"]["memory.available"] == str(8 * 1024**3 + 256 * 1024**2)
    assert changed["evictionHard"]["nodefs.available"] == "0%"
    assert changed["enforceNodeAllocatable"] == ["none"]
    assert "memory.available" not in yaml.safe_load(original)["evictionHard"]
    with pytest.raises(ValueError):
        pressure_config(original, {"availableBytes": 1, "workingSetBytes": 1})


def test_isolated_pod_has_no_operational_identity_and_generic_runner_rejects() -> None:
    """Use the isolated namespace default identity with no service-account token."""
    pod = victim_pod("a" * 32)
    spec = object_value(pod["spec"])
    assert spec["serviceAccountName"] == "default" and spec["automountServiceAccountToken"] is False
    assert spec["nodeName"] == "payops-eviction-control-plane"
    with pytest.raises(ValueError, match="specialized"):
        fault_spec("SCHED-04", deepcopy(spec))
    assert not evicted({"metadata": {"uid": "other"}, "status": {}}, pod)


def test_failed_reads_or_http_status_alone_cannot_prove_recovery() -> None:
    """Explicit read failures stay unqualified during kubelet restart."""
    assert not node_recovered({"acquisition_error": "CalledProcessError"})
    assert not eviction_proof({"acquisition_error": "CalledProcessError"}, {}, "")
    assert not webhook_healthy({"status": 200, "body": {"status": "accepted"}})


def test_clean_exit_requires_explicit_memory_disruption_and_termination() -> None:
    """Succeeded after a graceful shutdown qualifies only with kubelet's explicit memory cause."""
    original: JsonObject = {"metadata": {"uid": "owned"}}
    status: JsonObject = {
        "phase": "Succeeded",
        "conditions": [
            {
                "type": "DisruptionTarget",
                "status": "True",
                "reason": "TerminationByKubelet",
                "message": "The node was low on resource: memory.",
            }
        ],
        "containerStatuses": [
            {"state": {"terminated": {"finishedAt": "2026-09-12T00:00:00Z", "reason": "Completed"}}}
        ],
    }
    pod: JsonObject = {"metadata": {"uid": "owned"}, "status": status}
    assert evicted(pod, original)
    status["containerStatuses"] = []
    assert not evicted(pod, original)
    status["conditions"] = []
    assert not evicted(pod, original)
