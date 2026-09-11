"""Bind memory evidence to an owned rollout and its actual post-injection pod lifetime."""

from datetime import UTC, datetime
from typing import Literal

from pydantic import JsonValue

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container, oom_killed


def timestamp(value: JsonValue) -> datetime | None:
    """Malformed, naive or missing Kubernetes timestamps cannot establish causal order."""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def deployment_matches(
    observed: JsonObject,
    previous: JsonObject,
    expected: JsonObject,
    target: Literal["payments-api", "risk-sim"] = "payments-api",
) -> bool:
    """A matching spec must belong to the captured Deployment and an observed newer generation."""
    document = object_value(observed.get("deployment", {}))
    metadata = object_value(document.get("metadata", {}))
    prior = object_value(previous["metadata"])
    generation = metadata.get("generation")
    return (
        metadata.get("uid") == prior["uid"]
        and metadata.get("name") == prior.get("name") == target
        and metadata.get("namespace") == "payops-sandbox"
        and document.get("spec") == expected
        and type(generation) is int
        and generation > int(str(prior["generation"]))
        and object_value(document.get("status", {})).get("observedGeneration") == generation
    )


def owned_by(metadata: JsonObject, kind: str, parent: JsonObject) -> bool:
    """Controller ownership requires the exact API kind, UID and name, not matching labels."""
    references = object_items(metadata.get("ownerReferences", []))
    controllers = [reference for reference in references if reference.get("controller") is True]
    return len(controllers) == 1 and all(
        controllers[0].get(key) == value
        for key, value in {
            "apiVersion": "apps/v1",
            "kind": kind,
            "uid": parent.get("uid"),
            "name": parent.get("name"),
        }.items()
    )


def includes(actual: JsonValue, expected: JsonValue) -> bool:
    """Admission-added pod fields are allowed while every reviewed template field must match."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and includes(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(includes(left, right) for left, right in zip(actual, expected, strict=True))
        )
    return type(actual) is type(expected) and actual == expected


def template_matches(pod: JsonObject, expected: JsonObject) -> bool:
    """Bind configuration, resources and image exactly, allowing ordinary admission defaults."""
    template = object_value(expected["template"])
    if not includes(pod.get("spec"), template["spec"]):
        return False
    expected_item = container(expected)
    actual_items = object_items(object_value(pod.get("spec", {})).get("containers", []))
    if len(actual_items) != 1:
        return False
    pod_spec = object_value(pod["spec"])
    template_spec = object_value(template["spec"])
    return actual_items[0] == expected_item and all(
        pod_spec.get(key, []) == template_spec.get(key, [])
        for key in ("initContainers", "ephemeralContainers")
    )


def current_pods(
    observed: JsonObject,
    previous: JsonObject,
    expected: JsonObject,
    requested_at: str,
    target: Literal["payments-api", "risk-sim"] = "payments-api",
) -> list[JsonObject]:
    """Resolve Pod to ReplicaSet to original Deployment and reject pre-injection pod creation."""
    if not deployment_matches(observed, previous, expected, target):
        return []
    requested = timestamp(requested_at)
    if requested is None:
        return []
    # Kubernetes resource and termination timestamps have whole-second precision.
    lower = requested.replace(microsecond=0)
    deployment = object_value(object_value(observed["deployment"])["metadata"])
    replicas = object_items(observed.get("replica_sets", []))
    return [
        pod
        for pod in object_items(observed.get("pods", []))
        if _pod_matches(pod, replicas, deployment, expected, lower)
    ]


def _pod_matches(
    pod: JsonObject,
    replicas: list[JsonObject],
    deployment: JsonObject,
    expected: JsonObject,
    lower: datetime,
) -> bool:
    """Old ReplicaSets can be reused; the new pod itself must follow this injection."""
    metadata = object_value(pod.get("metadata", {}))
    created = timestamp(metadata.get("creationTimestamp"))
    if (
        not metadata.get("uid")
        or metadata.get("namespace") != "payops-sandbox"
        or created is None
        or not lower <= created <= datetime.now(UTC)
        or not template_matches(pod, expected)
    ):
        return False
    return any(
        _replica_matches(replica, deployment, expected)
        and owned_by(metadata, "ReplicaSet", object_value(replica["metadata"]))
        for replica in replicas
    )


def _replica_matches(replica: JsonObject, deployment: JsonObject, expected: JsonObject) -> bool:
    """The parent ReplicaSet must carry the reviewed template and original Deployment owner."""
    metadata = object_value(replica.get("metadata", {}))
    return (
        metadata.get("namespace") == "payops-sandbox"
        and bool(metadata.get("uid"))
        and owned_by(metadata, "Deployment", deployment)
        and includes(object_value(replica.get("spec", {})).get("template"), expected["template"])
    )


def fault_activated(
    observed: JsonObject,
    previous: JsonObject,
    expected: JsonObject,
    requested_at: str,
    control_uids: tuple[str, ...],
) -> bool:
    """An actual OOM must belong to a new fault pod and occur within its current lifetime."""
    return any(
        object_value(pod["metadata"])["uid"] not in control_uids and _recent_oom(pod)
        for pod in current_pods(observed, previous, expected, requested_at)
    )


def _recent_oom(pod: JsonObject) -> bool:
    """Exit 137 and restart need the kernel reason and ordered container termination times."""
    if not oom_killed(pod):
        return False
    created = timestamp(object_value(pod["metadata"]).get("creationTimestamp"))
    statuses = object_items(object_value(pod["status"])["containerStatuses"])
    status = next(item for item in statuses if item.get("name") == "sandbox")
    terminated = object_value(object_value(status["lastState"])["terminated"])
    started, finished = (
        timestamp(terminated.get("startedAt")),
        timestamp(terminated.get("finishedAt")),
    )
    return (
        created is not None
        and started is not None
        and finished is not None
        and created <= started <= finished <= datetime.now(UTC)
    )
