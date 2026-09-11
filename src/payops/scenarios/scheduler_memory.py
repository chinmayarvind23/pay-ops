"""Closed memory-request recipe; admission must succeed without permitting placement."""

from copy import deepcopy

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container
from payops.scenarios.scheduler_gateway import Slot
from payops.scenarios.scheduler_specs import node_identity


def memory_node_identity(observed: JsonObject) -> JsonObject:
    """Bind node UIDs and capacity; reject nodes capable of placing the 16Gi request."""
    return node_identity(observed, "memory")


def memory_plans(original: dict[Slot, JsonObject]) -> dict[Slot, JsonObject]:
    """Reserve pending-pod plus recovery-surge memory without altering CPU admission bounds."""
    result: dict[Slot, JsonObject] = {
        slot: deepcopy(object_value(document["spec"])) for slot, document in original.items()
    }
    # Four peers plus a replacement coexist with the pending pod during RollingUpdate recovery.
    object_value(result["quota"]["hard"]).update(
        {"requests.memory": "18Gi", "limits.memory": "20Gi"}
    )
    object_value(object_items(result["limits"]["limits"])[0]["max"])["memory"] = "16Gi"
    resources = object_value(container(result["payments"])["resources"])
    object_value(resources["requests"])["memory"] = "16Gi"
    object_value(resources["limits"])["memory"] = "16Gi"
    result["payments"]["strategy"] = {"type": "Recreate"}
    return result
