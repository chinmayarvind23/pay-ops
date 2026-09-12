"""Closed Deployment plans preserve every field except the specifically approved change."""

import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass

from pydantic import JsonValue

from payops.policy.contracts import ACTION, Action, PauseAction, RollbackAction, ScaleAction
from payops.policy.engine import SERVICES, action_digest
from payops.scenarios.contracts import JsonObject, object_items, object_value


@dataclass(frozen=True)
class DeploymentPlan:
    """The original spec is an atomic precondition, never a template supplied by a model."""

    action: Action
    before: JsonObject
    after: JsonObject

    def patch(self) -> list[JsonValue]:
        """UID and version tests prevent replacement-object and concurrent-writer races."""
        return [
            {"op": "test", "path": "/metadata/uid", "value": self.action.resource_uid},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": self.action.expected_version,
            },
            {"op": "test", "path": "/spec", "value": self.before},
            {"op": "replace", "path": "/spec", "value": self.after},
        ]


def validate_target(action: Action, inventory: Mapping[str, str]) -> None:
    """Operator-owned UID inventory excludes ledger, replacement objects and foreign modes."""
    ACTION.validate_json(action.model_dump_json())
    if (
        action.mode != "local_kind"
        or action.namespace != "payops-sandbox"
        or action.service not in SERVICES
        or inventory.get(action.service) != action.resource_uid
        or isinstance(action, PauseAction)
    ):
        raise PermissionError("EXECUTOR_TARGET_DENIED")


def deployment_spec(action: Action, current: JsonObject) -> JsonObject:
    """Reject stale identity and non-sandbox objects before constructing any patch."""
    metadata = object_value(current["metadata"])
    labels = object_value(metadata.get("labels", {}))
    if (
        current.get("kind") != "Deployment"
        or current.get("apiVersion") != "apps/v1"
        or (
            metadata.get("namespace"),
            metadata.get("name"),
            metadata.get("uid"),
            metadata.get("resourceVersion"),
        )
        != (action.namespace, action.service, action.resource_uid, action.expected_version)
        or labels.get("app.kubernetes.io/part-of") != "payops"
        or metadata.get("deletionTimestamp") is not None
    ):
        raise PermissionError("EXECUTOR_PRECONDITION_FAILED")
    return deepcopy(object_value(current["spec"]))


def rollback_image(revision: str, revisions: Mapping[str, str]) -> str:
    """An approved digest maps only to an immutable operator image, never an arbitrary tag."""
    image = revisions.get(revision, "")
    if re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9./:_-]*@sha256:" + re.escape(revision), image) is None:
        raise PermissionError("EXECUTOR_REVISION_DENIED")
    return image


def plan_deployment(
    action: Action,
    current: JsonObject,
    key: str,
    inventory: Mapping[str, str],
    revisions: Mapping[str, str],
) -> DeploymentPlan:
    """Restart, scale and image rollback have no shell, environment or manifest arguments."""
    validate_target(action, inventory)
    if key != action_digest(action):
        raise PermissionError("EXECUTOR_DIGEST_MISMATCH")
    before = deployment_spec(action, current)
    after = deepcopy(before)
    if isinstance(action, ScaleAction):
        after["replicas"] = action.replicas
    else:
        template = object_value(after["template"])
        if isinstance(action, RollbackAction):
            containers = object_items(object_value(template["spec"])["containers"])
            if len(containers) != 1 or containers[0].get("name") != "sandbox":
                raise PermissionError("EXECUTOR_CONTAINER_DENIED")
            containers[0]["image"] = rollback_image(action.revision_sha256, revisions)
        else:
            metadata = object_value(template["metadata"])
            annotations = object_value(metadata.get("annotations", {}))
            metadata["annotations"] = {**annotations, "payops.dev/remediation": key}
    return DeploymentPlan(action, before, after)


def rollout_ready(plan: DeploymentPlan, current: JsonObject) -> bool:
    """Controller readiness proves this exact desired spec, not business-level recovery."""
    metadata = object_value(current["metadata"])
    if metadata.get("uid") != plan.action.resource_uid or current.get("spec") != plan.after:
        raise PermissionError("EXECUTOR_POSTCHECK_DRIFT")
    if metadata.get("deletionTimestamp") is not None:
        raise PermissionError("EXECUTOR_POSTCHECK_DELETING")
    status = object_value(current.get("status", {}))
    replicas = plan.after.get("replicas", 1)
    generation = metadata.get("generation")
    return (
        type(replicas) is int
        and replicas > 0
        and type(generation) is int
        and status.get("observedGeneration") == generation
        and all(
            type(status.get(field)) is int and status[field] == replicas
            for field in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")
        )
        and status.get("unavailableReplicas", 0) == 0
    )
