"""Four dependency experiments share a fixed image and narrowly scoped credentials."""

import json
from typing import Literal

from payops.scenarios.concurrency_specs import concurrency_spec
from payops.scenarios.contracts import CaseId, JsonObject, object_items, object_value
from payops.scenarios.recipes import container

DEPENDENCY_CASES = ("DEP-03", "DEP-04", "TELEM-02", "TELEM-04")
RUNTIME_IMAGE_DIGEST = "sha256:6fc7a1912e484c9e371bebbb6badf5e3443f8789d2da6f4ee30448558213317d"


def dependency_kind(case_id: str) -> Literal["postgres", "redis"]:
    """Reject unsupported cases before selecting any dependency authority."""
    if case_id not in DEPENDENCY_CASES:
        raise ValueError("unsupported dependency case")
    return "postgres" if case_id in {"DEP-03", "TELEM-04"} else "redis"


def dependency_spec(document: JsonObject, case_id: CaseId) -> JsonObject:
    """Reuse strict startup/resource checks, then project one read-only probe credential."""
    kind = dependency_kind(case_id)
    enabled = concurrency_spec(document)
    item = container(enabled)
    pod = object_value(object_value(enabled["template"])["spec"])
    if pod.get("volumes") or item.get("volumeMounts"):
        raise ValueError("dependency baseline must not contain credential projections")
    item["image"] = "payops-sandbox:dependencies"
    for row in object_items(item["env"]):
        if row["name"] == "PAYOPS_SANDBOX_CONFIG":
            config = json.loads(str(row["value"]))
            config["concurrency_memory"] = False
            config["dependency"] = kind
            config["telemetry_condition"] = {
                "TELEM-02": "archived_error",
                "TELEM-04": "delayed_metrics",
            }.get(case_id, "normal")
            row["value"] = json.dumps(config, sort_keys=True, separators=(",", ":"))
    pod["volumes"] = [
        {
            "name": "dependency",
            "secret": {"secretName": "payops-synthetic-" + kind, "defaultMode": 292},
        }
    ]
    item["volumeMounts"] = [
        {"name": "dependency", "mountPath": "/run/payops-dependency", "readOnly": True}
    ]
    return enabled
