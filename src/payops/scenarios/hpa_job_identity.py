"""Account for the single owned load Job pod before checking the payment service runtime."""

from dataclasses import dataclass
from datetime import datetime

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.hpa_gateway import owned_metadata
from payops.scenarios.hpa_job import RUNTIME_IMAGE_DIGEST, load_job
from payops.scenarios.memory_provenance import includes, timestamp


@dataclass(frozen=True)
class LoadProcess:
    """The observed process identity remains separate from successful Job completion."""

    pod_name: str
    pod_uid: str
    container_id: str
    started: datetime


def split_load_pod(
    state: JsonObject, job: JsonObject, run_id: str
) -> tuple[JsonObject, LoadProcess]:
    """Remove only the exact owned, unchanged load pod; all other pods remain for validation."""
    metadata = owned_metadata("job", job, run_id)
    expected = object_value(load_job(run_id)["spec"])
    if not includes(job.get("spec"), expected):
        raise ValueError("load Job differs from the frozen workload envelope")
    pods = object_items(state["pods"])
    matches = [
        p
        for p in pods
        if object_value(object_value(p["metadata"]).get("labels", {})).get("payops.dev/hpa-run")
        == run_id
    ]
    if len(matches) != 1:
        raise ValueError("load Job does not have exactly one identifiable pod")
    pod = matches[0]
    _validate_owner(pod, metadata, expected)
    return {**state, "pods": [p for p in pods if p is not pod]}, _process(pod)


def _validate_owner(pod: JsonObject, metadata: JsonObject, expected: JsonObject) -> None:
    """Admission defaults cannot replace the owner chain or frozen workload template."""
    pod_meta = object_value(pod["metadata"])
    owners = [
        r for r in object_items(pod_meta.get("ownerReferences", [])) if r.get("controller") is True
    ]
    if (
        len(owners) != 1
        or any(
            owners[0].get(key) != value
            for key, value in {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": metadata["name"],
                "uid": metadata["uid"],
            }.items()
        )
        or pod_meta.get("namespace") != "payops-sandbox"
        or not isinstance(pod_meta.get("uid"), str)
        or not pod_meta.get("uid")
        or not isinstance(pod_meta.get("name"), str)
        or not pod_meta.get("name")
        or not includes(pod.get("spec"), object_value(expected["template"])["spec"])
        or object_value(pod["spec"]).get("initContainers")
        or object_value(pod["spec"]).get("ephemeralContainers")
    ):
        raise ValueError("load pod ownership or template differs")


def _process(pod: JsonObject) -> LoadProcess:
    """Accept one started, unrestarted process; successful completion is checked separately."""
    pod_meta = object_value(pod["metadata"])
    rows = object_items(object_value(pod.get("status", {})).get("containerStatuses", []))
    if len(rows) != 1:
        raise ValueError("load process container set differs")
    status = rows[0]
    phase = object_value(status.get("state", {}))
    if set(phase) not in ({"running"}, {"terminated"}):
        raise ValueError("load process lifetime is ambiguous or not started")
    lifetime = object_value(phase.get("running", phase.get("terminated", {})))
    started = timestamp(lifetime.get("startedAt"))
    if (
        status.get("name") != "load"
        or type(status.get("restartCount")) is not int
        or status.get("restartCount") != 0
        or not isinstance(status.get("containerID"), str)
        or not status.get("containerID")
        or str(status.get("imageID", "")).split("@")[-1] != RUNTIME_IMAGE_DIGEST
        or started is None
    ):
        raise ValueError("load process identity or image differs")
    return LoadProcess(
        str(pod_meta["name"]), str(pod_meta["uid"]), str(status["containerID"]), started
    )
