"""A run label alone cannot hide an arbitrary pod from service validation."""

from copy import deepcopy

import pytest
from test_hpa_runtime import runtime

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.hpa_job import RUNTIME_IMAGE_DIGEST, load_job
from payops.scenarios.hpa_job_identity import split_load_pod
from payops.scenarios.hpa_runtime import hpa_identities

RUN = "b" * 32


def fixture() -> tuple[JsonObject, JsonObject, JsonObject, JsonObject]:
    """Add one API-shaped Job pod to a real two-replica service fixture."""
    state, original, payments = runtime(2)
    job = load_job(RUN)
    object_value(job["metadata"]).update(uid="job-uid", resourceVersion="9")
    template = deepcopy(object_value(object_value(job["spec"])["template"]))
    object_value(template["metadata"]).update(
        name="hpa-load-pod",
        uid="load-pod",
        namespace="payops-sandbox",
        ownerReferences=[
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": "hpa-load-" + RUN,
                "uid": "job-uid",
                "controller": True,
            }
        ],
    )
    template["status"] = {
        "containerStatuses": [
            {
                "name": "load",
                "restartCount": 0,
                "containerID": "load-process",
                "imageID": "image@" + RUNTIME_IMAGE_DIGEST,
                "state": {"running": {"startedAt": "2026-09-11T00:00:00Z"}},
            }
        ]
    }
    state["pods"] = [*object_items(state["pods"]), template]
    return state, job, original, payments


def test_only_owned_job_is_excluded() -> None:
    """The original snapshot stays intact and the actual service replicas still validate."""
    state, job, original, payments = fixture()
    saved = deepcopy(state)
    services, process = split_load_pod(state, job, RUN)
    assert state == saved and len(object_items(services["pods"])) == 6
    assert process.pod_uid == "load-pod"
    assert len(hpa_identities(services, original, payments, 2)["payments-api"]) == 2


@pytest.mark.parametrize("fault", ["owner", "duplicate", "image", "restart", "command", "job"])
def test_foreign_or_changed_job_pod_rejects(fault: str) -> None:
    """Each source boundary is checked before a pod can be excluded from runtime counts."""
    state, job, _, _ = fixture()
    pods = object_items(state["pods"])
    pod = pods[-1]
    if fault == "owner":
        object_items(object_value(pod["metadata"])["ownerReferences"])[0]["uid"] = "foreign"
    elif fault == "duplicate":
        state["pods"] = [*pods, deepcopy(pod)]
    elif fault == "image":
        object_items(object_value(pod["status"])["containerStatuses"])[0]["imageID"] = "foreign"
    elif fault == "restart":
        object_items(object_value(pod["status"])["containerStatuses"])[0]["restartCount"] = 1
    elif fault == "command":
        object_items(object_value(pod["spec"])["containers"])[0]["command"] = ["other"]
    else:
        object_value(job["spec"])["backoffLimit"] = 1
    with pytest.raises(ValueError):
        split_load_pod(state, job, RUN)


@pytest.mark.parametrize("fault", ["uid", "name", "container", "waiting", "ambiguous", "missing"])
def test_invalid_process_identity_rejects(fault: str) -> None:
    """Malformed identities and impossible lifetime states cannot become causal evidence."""
    state, job, _, _ = fixture()
    pod = object_items(state["pods"])[-1]
    meta = object_value(pod["metadata"])
    status = object_items(object_value(pod["status"])["containerStatuses"])[0]
    if fault in {"uid", "name"}:
        meta[fault] = 42
    elif fault == "container":
        status["containerID"] = True
    elif fault == "waiting":
        status["state"] = {"waiting": {"reason": "ContainerCreating"}}
    elif fault == "ambiguous":
        object_value(status["state"])["terminated"] = {"startedAt": "2026-09-11T00:00:00Z"}
    else:
        object_value(pod["status"])["containerStatuses"] = []
    with pytest.raises(ValueError):
        split_load_pod(state, job, RUN)


def test_terminated_process_is_identity_not_success() -> None:
    """Even a failed process has an attributable identity; lifecycle success remains separate."""
    state, job, _, _ = fixture()
    pod = object_items(state["pods"])[-1]
    status = object_items(object_value(pod["status"])["containerStatuses"])[0]
    status["state"] = {"terminated": {"startedAt": "2026-09-11T00:00:00Z", "exitCode": 1}}
    _, process = split_load_pod(state, job, RUN)
    assert process.container_id == "load-process"
