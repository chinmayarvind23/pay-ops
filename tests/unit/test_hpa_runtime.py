"""Scale-out proof requires two distinct owned processes, not a desired replica count alone."""

from copy import deepcopy
from typing import Literal

import pytest
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.hpa_runtime import hpa_identities
from payops.scenarios.sampling_gateway import deployment_map


def runtime(replicas: Literal[1, 2]) -> tuple[JsonObject, JsonObject, JsonObject]:
    """Reconcile actual API-shaped controller counts and a second owned pod when scaled."""
    original = SamplingCluster(Clock()).state()
    state = deepcopy(original)
    payment = deployment_map(state)["payments-api"]
    spec = object_value(payment["spec"])
    spec["replicas"] = replicas
    object_value(payment["metadata"])["generation"] = 2
    object_value(payment["status"]).update(
        observedGeneration=2,
        replicas=replicas,
        updatedReplicas=replicas,
        readyReplicas=replicas,
        availableReplicas=replicas,
    )
    if replicas == 2:
        extra = deepcopy(object_items(state["pods"])[0])
        object_value(extra["metadata"]).update(name="payments-api-second", uid="second-pod")
        object_items(object_value(extra["status"])["containerStatuses"])[0]["containerID"] = (
            "second-container"
        )
        state["pods"] = [*object_items(state["pods"]), extra]
    return state, original, spec


@pytest.mark.parametrize("replicas", [1, 2])
def test_actual_scale_counts_and_owned_processes(replicas: Literal[1, 2]) -> None:
    """Every peer retains its identity while payments adds exactly one independently owned pod."""
    state, original, spec = runtime(replicas)
    identities = hpa_identities(state, original, spec, replicas)
    assert len(identities["payments-api"]) == replicas
    assert all(len(rows) == 1 for name, rows in identities.items() if name != "payments-api")


@pytest.mark.parametrize(
    "fault", ["extra", "duplicate_uid", "duplicate_container", "image", "owner", "count", "peer"]
)
def test_incomplete_or_foreign_scale_out_rejects(fault: str) -> None:
    """A second replica must have real readiness, ownership and unchanged code."""
    state, original, spec = runtime(2)
    pods = object_items(state["pods"])
    if fault == "extra":
        state["pods"] = [*pods, deepcopy(pods[-1])]
    elif fault == "duplicate_uid":
        object_value(pods[-1]["metadata"])["uid"] = object_value(pods[0]["metadata"])["uid"]
    elif fault in {"duplicate_container", "image"}:
        status = object_items(object_value(pods[-1]["status"])["containerStatuses"])[0]
        status["containerID" if fault == "duplicate_container" else "imageID"] = (
            object_items(object_value(pods[0]["status"])["containerStatuses"])[0]["containerID"]
            if fault == "duplicate_container"
            else "foreign"
        )
    elif fault == "owner":
        object_items(object_value(pods[-1]["metadata"])["ownerReferences"])[0]["uid"] = "foreign"
    elif fault == "count":
        object_value(deployment_map(state)["payments-api"]["status"])["readyReplicas"] = 1
    else:
        object_items(object_value(pods[1]["status"])["containerStatuses"])[0]["containerID"] = (
            "replacement"
        )
    with pytest.raises(ValueError):
        hpa_identities(state, original, spec, 2)
