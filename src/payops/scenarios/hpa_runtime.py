"""Validate real scale-out without projecting multiple pods into a single-replica snapshot."""

from typing import Literal

from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.sampling_gateway import (
    SERVICES,
    deployment_identity,
    deployment_map,
    pod_identity,
)


def service_pods(state: JsonObject, name: str) -> list[JsonObject]:
    """Group only explicit service labels; later total-count checks reject unaccounted pods."""
    return [
        pod
        for pod in object_items(state["pods"])
        if object_value(object_value(pod["metadata"]).get("labels", {})).get(
            "app.kubernetes.io/name"
        )
        == name
    ]


def hpa_identities(
    state: JsonObject,
    original: JsonObject,
    payments: JsonObject,
    replicas: Literal[1, 2],
) -> dict[str, tuple[PodIdentity, ...]]:
    """Check exact controller counts, every pod owner/template/image, and unchanged peers."""
    if type(replicas) is not int or replicas not in (1, 2):
        raise ValueError("HPA runtime only supports one or two payments replicas")
    documents, prior = deployment_map(state), deployment_map(original)
    pods = object_items(state["pods"])
    if len(pods) != 4 + replicas or len(
        {object_value(p["metadata"]).get("uid") for p in pods}
    ) != len(pods):
        raise ValueError("HPA service runtime has extra or repeated pods")
    result: dict[str, tuple[PodIdentity, ...]] = {}
    for name in SERVICES:
        count = replicas if name == "payments-api" else 1
        expected = payments if name == "payments-api" else object_value(prior[name]["spec"])
        metadata = deployment_identity(documents[name], prior[name], expected, count)
        current, baseline = service_pods(state, name), service_pods(original, name)
        if len(current) != count or len(baseline) != 1:
            raise ValueError("HPA service pod set does not match the controller count")
        previous_status = object_items(object_value(baseline[0]["status"])["containerStatuses"])
        if len(previous_status) != 1:
            raise ValueError("HPA baseline container multiplicity differs")
        image = str(previous_status[0]["imageID"])
        identities = tuple(
            sorted(
                (
                    pod_identity(p, object_items(state["replicas"]), metadata, expected, image)
                    for p in current
                ),
                key=lambda p: p.pod_name,
            )
        )
        if len({p.container_id for p in identities}) != count:
            raise ValueError("HPA replicas repeat a container identity")
        if name != "payments-api":
            previous = pod_identity(
                baseline[0],
                object_items(original["replicas"]),
                object_value(prior[name]["metadata"]),
                expected,
                image,
            )
            if identities != (previous,):
                raise ValueError("unmodified HPA peer process changed")
        result[name] = identities
    return result
