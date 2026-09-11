"""Keep serial and parallel memory experiments on exactly the same deployment spec."""

import json
from copy import deepcopy

from payops.sandbox.models import SandboxConfig
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container, validate_baseline

IMAGE = "payops-sandbox:concurrency-ab9a869"


def concurrency_spec(document: JsonObject) -> JsonObject:
    """Enable only the calibrated worker; all three stages share image and resource bounds."""
    original = validate_baseline(document, "payments-api")
    item = container(original)
    pod = object_value(object_value(original["template"])["spec"])
    if any(key in item for key in ("command", "args", "envFrom", "lifecycle")) or any(
        pod.get(key) for key in ("initContainers", "ephemeralContainers")
    ):
        raise ValueError("concurrency experiment requires the normal fixed entrypoint")
    env = object_items(item.get("env", []))
    values = {str(row.get("name")): row.get("value") for row in env}
    if (
        len(env) != 2
        or any(set(row) != {"name", "value"} for row in env)
        or set(values) != {"PAYOPS_SANDBOX_ROLE", "PAYOPS_SANDBOX_CONFIG"}
        or values["PAYOPS_SANDBOX_ROLE"] != "payments"
    ):
        raise ValueError("concurrency experiment requires exact payments startup configuration")
    config = SandboxConfig.model_validate_json(str(values["PAYOPS_SANDBOX_CONFIG"]))
    if config.risk_protocol != "v1" or config.timeout_seconds != 2 or config.cpu_capture:
        raise ValueError("concurrency experiment requires normal peer and capture configuration")
    resources = object_value(item["resources"])
    for kind, expected in (
        ("requests", {"cpu": "50m", "memory": "96Mi"}),
        ("limits", {"cpu": "500m", "memory": "256Mi"}),
    ):
        if any(object_value(resources[kind]).get(key) != value for key, value in expected.items()):
            raise ValueError("concurrency experiment requires unchanged normal resource bounds")
    enabled = deepcopy(original)
    enabled["strategy"] = {"type": "Recreate"}
    changed = container(enabled)
    changed["image"] = IMAGE
    for row in object_items(changed["env"]):
        if row["name"] == "PAYOPS_SANDBOX_CONFIG":
            settings = json.loads(str(row["value"]))
            settings["concurrency_memory"] = True
            row["value"] = json.dumps(settings, sort_keys=True, separators=(",", ":"))
    return enabled
