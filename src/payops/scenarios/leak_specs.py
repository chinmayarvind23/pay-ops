"""The retention counterfactual changes one startup mode under identical resource limits."""

from copy import deepcopy

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container, validate_baseline

IMAGE = "payops-sandbox:leak-9d4efd5"


def leak_specs(document: JsonObject) -> tuple[JsonObject, JsonObject]:
    """Capture only the normal risk baseline; preserve every field except image/mode/strategy."""
    original = validate_baseline(document, "risk-sim")
    item = container(original)
    pod = object_value(object_value(original["template"])["spec"])
    if any(key in item for key in ("command", "args", "envFrom", "lifecycle")) or any(
        pod.get(key) for key in ("initContainers", "ephemeralContainers")
    ):
        raise ValueError("retention experiment requires the normal risk entrypoint")
    env = object_items(item.get("env", []))
    values = {str(row.get("name")): row.get("value") for row in env}
    if (
        len(env) != 2
        or any(set(row) != {"name", "value"} for row in env)
        or set(values) != {"PAYOPS_SANDBOX_ROLE", "PAYOPS_SANDBOX_CONFIG"}
        or values["PAYOPS_SANDBOX_ROLE"] != "risk"
    ):
        raise ValueError("retention experiment requires exact risk startup configuration")
    resources = object_value(item["resources"])
    for kind, expected in (
        ("requests", {"cpu": "50m", "memory": "96Mi"}),
        ("limits", {"cpu": "500m", "memory": "256Mi"}),
    ):
        if any(object_value(resources[kind]).get(key) != value for key, value in expected.items()):
            raise ValueError("retention experiment requires unchanged normal resource bounds")
    control = deepcopy(original)
    control["strategy"] = {"type": "Recreate"}
    changed = container(control)
    changed["image"] = IMAGE
    changed["env"] = [
        *object_items(changed["env"]),
        {"name": "PAYOPS_SYNTHETIC_LEAK", "value": "released-v1"},
    ]
    retained = deepcopy(control)
    object_items(container(retained)["env"])[-1]["value"] = "retained-v1"
    return control, retained
