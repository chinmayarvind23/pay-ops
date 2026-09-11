"""Closed wire-rollout states keep protocol mismatches reproducible and operator-owned."""

import json
from copy import deepcopy
from typing import Literal

from pydantic import BaseModel, ConfigDict

from payops.evidence.artifacts import JSON_OBJECT
from payops.sandbox.models import SandboxConfig
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container, validate_baseline

ProtocolStage = Literal["original", "mismatch", "matched", "final"]
ProtocolTarget = Literal["payments-api", "risk-sim"]


class ProtocolPlan(BaseModel):
    """Acquisition and acceptance choices require a source revision rather than run arguments."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    version: Literal["risk-wire-rollout-v1"] = "risk-wire-rollout-v1"
    samples_per_stage: Literal[1] = 1
    request_timeout_seconds: Literal[5] = 5
    capture_offset_seconds: Literal[12] = 12
    maximum_window_seconds: Literal[120] = 120
    risk_log_bytes: Literal[16384] = 16384
    clock_tolerance_seconds: Literal[1] = 1
    stages: tuple[
        Literal["original"], Literal["mismatch"], Literal["matched"], Literal["final"]
    ] = (
        "original",
        "mismatch",
        "matched",
        "final",
    )


PLAN = ProtocolPlan()


def protocol_spec(document: JsonObject, target: ProtocolTarget) -> JsonObject:
    """Derive one complete v2 spec from a captured normal v1 Deployment."""
    original = validate_baseline(document, target)
    metadata = object_value(document["metadata"])
    if (
        not metadata.get("uid")
        or not metadata.get("resourceVersion")
        or type(metadata.get("generation")) is not int
    ):
        raise ValueError("protocol rollout requires current Deployment identity")
    pod = object_value(object_value(original["template"])["spec"])
    entry = container(original)
    if any(key in entry for key in ("command", "args", "envFrom", "lifecycle")) or any(
        pod.get(key) for key in ("initContainers", "ephemeralContainers")
    ):
        raise ValueError("protocol rollout requires the normal fixed entrypoint")
    env = object_items(entry.get("env", []))
    values = {str(row.get("name")): row.get("value") for row in env}
    role = "payments" if target == "payments-api" else "risk"
    if (
        len(env) != 2
        or any(set(row) != {"name", "value"} for row in env)
        or set(values) != {"PAYOPS_SANDBOX_ROLE", "PAYOPS_SANDBOX_CONFIG"}
        or values.get("PAYOPS_SANDBOX_ROLE") != role
    ):
        raise ValueError("protocol rollout requires exact normal startup configuration")
    config = SandboxConfig.model_validate_json(str(values["PAYOPS_SANDBOX_CONFIG"]))
    if config.risk_protocol != "v1" or config.timeout_seconds != 2:
        raise ValueError("protocol rollout requires original v1 and two-second peer timeout")
    changed = deepcopy(original)
    changed["strategy"] = {"type": "Recreate"}
    settings = JSON_OBJECT.validate_json(str(values["PAYOPS_SANDBOX_CONFIG"]))
    settings["risk_protocol"] = "v2"
    changed_env = object_items(container(changed)["env"])
    for row in changed_env:
        if row["name"] == "PAYOPS_SANDBOX_CONFIG":
            row["value"] = json.dumps(settings, sort_keys=True, separators=(",", ":"))
    container(changed)["env"] = list(changed_env)
    return changed
