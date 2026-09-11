"""Fixed admission envelope and actual scheduler evidence for the oversized-request variant."""

from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.memory_provenance import (
    current_pods,
    includes,
    owned_by,
    template_matches,
    timestamp,
)
from payops.scenarios.recipes import container, validate_baseline
from payops.scenarios.scheduler_gateway import Slot

SERVICES = {"payments-api", "processor-adapter", "risk-sim", "ledger-sim", "webhook-sim"}
QUOTA: JsonObject = {
    "hard": {
        "requests.cpu": "4",
        "limits.cpu": "8",
        "requests.memory": "2Gi",
        "limits.memory": "4Gi",
        "pods": "20",
        "services.nodeports": "0",
        "services.loadbalancers": "0",
    }
}
LIMITS: JsonObject = {
    "limits": [
        {
            "type": "Container",
            "default": {"cpu": "500m", "memory": "256Mi"},
            "defaultRequest": {"cpu": "50m", "memory": "96Mi"},
            "max": {"cpu": "2", "memory": "1Gi"},
        }
    ]
}


def node_identity(observed: JsonObject) -> JsonObject:
    """The fixed request is safe only while both known nodes still expose exactly22 CPUs."""
    nodes = object_items(observed.get("nodes", []))
    result: JsonObject = {}
    for node in nodes:
        metadata, status = object_value(node["metadata"]), object_value(node["status"])
        name, spec = str(metadata["name"]), object_value(node.get("spec", {}))
        conditions = {
            item.get("type"): item.get("status") for item in object_items(status["conditions"])
        }
        taints = (
            [{"key": "node-role.kubernetes.io/control-plane", "effect": "NoSchedule"}]
            if name.endswith("control-plane")
            else []
        )
        if (
            object_value(status["allocatable"]).get("cpu") != "22"
            or not metadata.get("uid")
            or spec.get("unschedulable", False) is not False
            or spec.get("taints", []) != taints
            or conditions.get("Ready") != "True"
            or any(
                conditions.get(key) != "False"
                for key in ("MemoryPressure", "DiskPressure", "PIDPressure")
            )
        ):
            raise ValueError("scheduler node capacity/health/taints outside reviewed baseline")
        result[name] = metadata["uid"]
    if set(result) != {"payops-dev-control-plane", "payops-dev-worker"} or len(nodes) != 2:
        raise ValueError("scheduler requires exactly the two reviewed nodes")
    return result


def healthy_users(observed: JsonObject) -> bool:
    """Five healthy, single-container service pods leave no unreviewed admission users."""
    pods = object_items(observed.get("namespace_pods", []))
    deployments = object_items(observed.get("deployments", []))
    names = {str(object_value(item["metadata"]).get("name")) for item in deployments}
    return (
        len(pods) == 5
        and len(deployments) == 5
        and names == SERVICES
        and not observed.get("other_workloads")
        and all(_healthy_user(pod) for pod in pods)
        and all(_owned_user(pod, observed) for pod in pods)
        and {
            str(
                object_value(object_value(pod["metadata"]).get("labels", {})).get(
                    "app.kubernetes.io/name"
                )
            )
            for pod in pods
        }
        == SERVICES
        and all(object_value(item["spec"]).get("replicas") == 1 for item in deployments)
    )


def _owned_user(pod: JsonObject, observed: JsonObject) -> bool:
    """Resolve every namespace service user through its actual controller ownership chain."""
    metadata = object_value(pod["metadata"])
    name = object_value(metadata.get("labels", {})).get("app.kubernetes.io/name")
    matching = [
        item
        for item in object_items(observed["deployments"])
        if object_value(item["metadata"]).get("name") == name
    ]
    if len(matching) != 1 or metadata.get("namespace") != "payops-sandbox":
        return False
    deployment = matching[0]
    parent = object_value(deployment["metadata"])
    expected = object_value(deployment["spec"])
    if parent.get("namespace") != "payops-sandbox" or not template_matches(pod, expected):
        return False
    return any(
        owned_by(object_value(replica["metadata"]), "Deployment", parent)
        and owned_by(metadata, "ReplicaSet", object_value(replica["metadata"]))
        and includes(object_value(replica["spec"]).get("template"), expected["template"])
        for replica in object_items(observed.get("replica_sets", []))
    )


def _healthy_user(pod: JsonObject) -> bool:
    """Each known service must use one ready unchanged normal image and bounded CPU/memory."""
    spec, status = object_value(pod["spec"]), object_value(pod["status"])
    containers, statuses = (
        object_items(spec.get("containers", [])),
        object_items(status.get("containerStatuses", [])),
    )
    if (
        len(containers) != 1
        or len(statuses) != 1
        or spec.get("initContainers")
        or spec.get("ephemeralContainers")
    ):
        return False
    item = containers[0]
    if any(item.get(key) for key in ("command", "args", "envFrom", "lifecycle")):
        return False
    env = object_items(item.get("env", []))
    if any(
        entry.get("name") not in {"PAYOPS_SANDBOX_ROLE", "PAYOPS_SANDBOX_CONFIG"}
        or "valueFrom" in entry
        for entry in env
    ):
        return False
    resources = object_value(item.get("resources", {}))
    return (
        item.get("image") == "payops-sandbox:local"
        and statuses[0].get("ready") is True
        and "running" in object_value(statuses[0].get("state", {}))
        and object_value(resources.get("requests", {})).get("cpu") == "50m"
        and object_value(resources.get("limits", {})).get("cpu") == "500m"
        and object_value(resources.get("requests", {})).get("memory") == "96Mi"
        and object_value(resources.get("limits", {})).get("memory") == "256Mi"
    )


def capture(observed: JsonObject) -> dict[Slot, JsonObject]:
    """Reject any changed admission field before copying all three full restoration documents."""
    quotas, limits = (
        object_items(observed.get("quotas", [])),
        object_items(observed.get("limit_ranges", [])),
    )
    if len(quotas) != 1 or len(limits) != 1 or not healthy_users(observed):
        raise ValueError("unexpected admission objects or namespace workloads")
    original: dict[Slot, JsonObject] = {
        "payments": object_value(observed["deployment"]),
        "quota": quotas[0],
        "limits": limits[0],
    }
    identities: tuple[tuple[Slot, str], ...] = (
        ("quota", "sandbox-budget"),
        ("limits", "sandbox-container-bounds"),
    )
    for slot, name in identities:
        document = original[slot]
        metadata = object_value(document["metadata"])
        if (
            metadata.get("name") != name
            or metadata.get("namespace") != "payops-sandbox"
            or not metadata.get("uid")
        ):
            raise ValueError("admission object identity outside reviewed scope")
    if quotas[0]["spec"] != QUOTA or limits[0]["spec"] != LIMITS:
        raise ValueError("admission non-CPU fields or original CPU bounds changed")
    if not quota_converged(quotas[0], QUOTA) or not quota_usage_restored(quotas[0]):
        raise ValueError("original quota accounting outside baseline")
    validate_baseline(original["payments"], "payments-api")
    node_identity(observed)
    return deepcopy(original)


def plans(original: dict[Slot, JsonObject]) -> dict[Slot, JsonObject]:
    """Only the reviewed CPU fields and rollout strategy differ from their captured specs."""
    result: dict[Slot, JsonObject] = {
        slot: deepcopy(object_value(document["spec"])) for slot, document in original.items()
    }
    object_value(result["quota"]["hard"]).update({"requests.cpu": "24", "limits.cpu": "25"})
    object_value(object_items(result["limits"]["limits"])[0]["max"])["cpu"] = "23"
    item = container(result["payments"])
    resources = object_value(item["resources"])
    object_value(resources["requests"])["cpu"] = "23"
    object_value(resources["limits"])["cpu"] = "23"
    result["payments"]["strategy"] = {"type": "Recreate"}
    return result


def quota_converged(document: JsonObject, expected: JsonObject) -> bool:
    """Admission and controller accounting must agree before the high request is submitted."""
    return document.get("spec") == expected and object_value(document.get("status", {})).get(
        "hard"
    ) == expected.get("hard")


def quota_usage_restored(document: JsonObject) -> bool:
    """A pending pod counts against quota until controller deletion/accounting finishes."""
    used = object_value(object_value(document.get("status", {})).get("used", {}))
    return all(
        _quantity(used.get(key)) <= _quantity(value)
        for key, value in object_value(QUOTA["hard"]).items()
    )


def _quantity(value: object) -> Decimal:
    """Parse only the finite CPU, memory and count forms emitted by this fixed local quota."""
    text = str(value)
    factors = {
        "m": Decimal("0.001"),
        "Ki": Decimal(1024),
        "Mi": Decimal(1024**2),
        "Gi": Decimal(1024**3),
    }
    suffix = next((item for item in factors if text.endswith(item)), "")
    try:
        number = Decimal(text[: -len(suffix)] if suffix else text) * factors.get(suffix, Decimal(1))
    except InvalidOperation as exc:
        raise ValueError("unsupported quota quantity") from exc
    if not number.is_finite() or number < 0:
        raise ValueError("invalid quota quantity")
    return number


def activated(
    observed: JsonObject,
    original: JsonObject,
    expected: JsonObject,
    requested_at: str,
    nodes: JsonObject,
    old_uids: tuple[str, ...],
) -> bool:
    """Ownership, actual scheduler state and fresh CPU rejection must all describe this rollout."""
    resource = object_value(container(expected)["resources"])
    if (
        node_identity(observed) != nodes
        or object_value(resource["requests"]).get("cpu") != "23"
        or object_value(resource["limits"]).get("cpu") != "23"
    ):
        return False
    return any(
        object_value(pod["metadata"])["uid"] not in old_uids
        and _pending_cpu(pod, observed, requested_at)
        for pod in current_pods(observed, original, expected, requested_at)
    )


def _pending_cpu(pod: JsonObject, observed: JsonObject, requested_at: str) -> bool:
    """An admitted unscheduled pod is distinct from admission rejection or a crashed container."""
    status = object_value(pod.get("status", {}))
    if (
        status.get("phase") != "Pending"
        or object_value(pod.get("spec", {})).get("nodeName")
        or status.get("containerStatuses")
    ):
        return False
    conditions = object_items(status.get("conditions", []))
    scheduled = [item for item in conditions if item.get("type") == "PodScheduled"]
    return (
        len(scheduled) == 1
        and scheduled[0].get("status") == "False"
        and scheduled[0].get("reason") == "Unschedulable"
        and _fresh(scheduled[0].get("lastTransitionTime"), requested_at)
        and any(
            _scheduler_event(event, pod, requested_at)
            for event in object_items(observed.get("events", []))
        )
    )


def _fresh(value: object, requested_at: str) -> bool:
    """Accept Kubernetes second-precision timestamps only inside this injection's time window."""
    observed, requested = timestamp(str(value)), timestamp(requested_at)
    return (
        observed is not None
        and requested is not None
        and requested.replace(microsecond=0) <= observed <= datetime.now(UTC)
    )


def _scheduler_event(event: JsonObject, pod: JsonObject, requested_at: str) -> bool:
    """A scheduler-owned event must name the current pod and actual insufficient-CPU result."""
    involved = object_value(event.get("involvedObject", {}))
    stamp = (
        object_value(event.get("series", {})).get("lastObservedTime")
        or event.get("lastTimestamp")
        or event.get("eventTime")
    )
    reporter = event.get("reportingComponent") or object_value(event.get("source", {})).get(
        "component"
    )
    return (
        object_value(event.get("metadata", {})).get("namespace") == "payops-sandbox"
        and bool(object_value(event.get("metadata", {})).get("name"))
        and involved.get("name") == object_value(pod["metadata"]).get("name")
        and involved.get("uid") == object_value(pod["metadata"])["uid"]
        and involved.get("kind") == "Pod"
        and involved.get("namespace") == "payops-sandbox"
        and event.get("reason") == "FailedScheduling"
        and reporter == "default-scheduler"
        and "Insufficient cpu" in str(event.get("message", ""))
        and _fresh(stamp, requested_at)
    )
