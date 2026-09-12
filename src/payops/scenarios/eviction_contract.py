"""A calibrated kubelet reserve reproduces real eviction without exhausting the host."""

from copy import deepcopy

import yaml

from payops.scenarios.contracts import JsonObject, object_items, object_value

NODE = "payops-eviction-control-plane"
NAMESPACE = "payops-eviction"


def pressure_config(original: str, memory: JsonObject) -> str:
    """Raise the isolated node reserve above observed availability; disable cgroup resizing."""
    config = object_value(yaml.safe_load(original))
    available = memory.get("availableBytes")
    working = memory.get("workingSetBytes")
    if (
        type(available) is not int
        or type(working) is not int
        or available < 1024**3
        or working < 512 * 1024**2
    ):
        raise ValueError("node memory calibration outside reviewed bounds")
    enabled = deepcopy(config)
    hard = object_value(enabled.get("evictionHard", {}))
    hard["memory.available"] = str(available + 256 * 1024**2)
    enabled["evictionHard"] = hard
    enabled["evictionPressureTransitionPeriod"] = "1s"
    enabled["enforceNodeAllocatable"] = ["none"]
    return yaml.safe_dump(enabled, sort_keys=True)


def victim_pod(run_id: str, recovered: bool = False) -> JsonObject:
    """An isolated synthetic webhook has no peer or credential dependencies."""
    from payops.scenarios.noise_contract import noise_job

    # The hardened template supplies fixed image, resource ceilings and token suppression.
    job = noise_job(run_id)
    pod = deepcopy(object_value(object_value(object_value(job["spec"])["template"])["spec"]))
    item = object_items(pod["containers"])[0]
    item["name"] = "sandbox"
    item.pop("command")
    item["env"] = [
        {"name": "PAYOPS_SANDBOX_ROLE", "value": "webhook"},
        {"name": "PAYOPS_SANDBOX_CONFIG", "value": "{}"},
    ]
    object_value(object_value(item["resources"])["requests"])["memory"] = "16Mi"
    pod["nodeName"] = NODE
    pod["serviceAccountName"] = "default"
    pod["tolerations"] = [{"operator": "Exists", "effect": "NoSchedule"}]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "victim-recovered" if recovered else "victim-control",
            "namespace": NAMESPACE,
            "labels": {"app.kubernetes.io/part-of": "payops", "payops.dev/eviction-run": run_id},
        },
        "spec": pod,
    }


def evicted(pod: JsonObject, original: JsonObject) -> bool:
    """Require an owned terminal memory eviction, including the explicit kubelet disruption form."""
    status = object_value(pod.get("status", {}))
    if object_value(pod["metadata"]).get("uid") != object_value(original["metadata"]).get("uid"):
        return False
    if status.get("phase") == "Failed" and status.get("reason") == "Evicted":
        return memory_eviction_message(status.get("message"))
    if status.get("phase") not in {"Succeeded", "Failed"}:
        return False
    conditions = object_items(status.get("conditions", []))
    statuses = object_items(status.get("containerStatuses", []))
    termination = (
        object_value(object_value(statuses[0].get("state", {})).get("terminated", {}))
        if len(statuses) == 1
        else {}
    )
    return (
        bool(termination.get("finishedAt"))
        and termination.get("reason") != "OOMKilled"
        and any(
            row.get("type") == "DisruptionTarget"
            and row.get("status") == "True"
            and row.get("reason") == "TerminationByKubelet"
            and memory_eviction_message(row.get("message"))
            for row in conditions
        )
    )


def memory_eviction_message(value: object) -> bool:
    """An unrelated kubelet disruption cannot substitute the memory-pressure cause."""
    message = str(value).lower()
    return "memory" in message and "low on resource" in message
