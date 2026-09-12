"""Fixed CPU-noise Job operations and processor-only deployment mutation."""

import re

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.dependency_specs import RUNTIME_IMAGE_DIGEST
from payops.scenarios.hpa_gateway import HpaGateway
from payops.scenarios.memory_provenance import includes
from payops.scenarios.noise_contract import noise_job
from payops.tools.traces import bounded_read


class NoiseGateway(HpaGateway):
    """Reuse bounded JSON transport; all scenario writes select fixed operator templates."""

    @staticmethod
    def validate_target(name: str) -> None:
        """Only the simulated processor can change its deployment state."""
        if name != "processor-adapter":
            raise ValueError("noise scenario only changes processor-adapter")

    def create_noise(self, run_id: str) -> JsonObject:
        """Create once; no existing Job can be adopted or overwritten."""
        return self._write(("create", "-f", "-", "-o", "json"), noise_job(run_id))

    def noise_state(self, run_id: str) -> JsonObject:
        """Read complete inventories so unknown jobs are not silently filtered out."""
        noise_job(run_id)
        return {
            "jobs": self._json(("get", "jobs"))["items"],
            "pods": self._json(("get", "pods"))["items"],
        }

    def remove_noise(self, document: JsonObject, run_id: str) -> JsonObject:
        """Delete only this run's Job under UID/version preconditions, with foreground cleanup."""
        metadata = noise_metadata(document, run_id)
        path = "/apis/batch/v1/namespaces/payops-sandbox/jobs/cpu-noise-" + run_id
        return self._write(
            ("delete", "--raw", path, "-f", "-"),
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "propagationPolicy": "Foreground",
                "preconditions": {
                    "uid": metadata["uid"],
                    "resourceVersion": metadata["resourceVersion"],
                },
            },
        )

    def noise_log(self, pod: JsonObject, run_id: str) -> JsonObject:
        """Keep raw per-second CPU measurements from the independently verified Job pod."""
        metadata = object_value(pod["metadata"])
        name = str(metadata["name"])
        if re.fullmatch("cpu-noise-" + run_id + r"-[a-z0-9]+", name) is None:
            raise ValueError("invalid noise log source")
        raw = bounded_read(
            (
                *self._prefix,
                "logs",
                name,
                "--container=load",
                "--timestamps=false",
                "--tail=150",
                "--limit-bytes=32768",
            ),
            32768,
            12,
        )
        if len(raw) >= 32768:
            raise ValueError("CPU noise log capped")
        return {"text": raw.decode(), "pod_uid": metadata["uid"], "pod_name": name}


def noise_metadata(document: JsonObject, run_id: str) -> JsonObject:
    """Validate kind, name, namespace and full reviewed Job shape before ownership is accepted."""
    expected = noise_job(run_id)
    metadata = object_value(document["metadata"])
    if (
        document.get("kind") != "Job"
        or document.get("apiVersion") != "batch/v1"
        or not includes(metadata, expected["metadata"])
        or not metadata.get("uid")
        or not metadata.get("resourceVersion")
        or not includes(document.get("spec"), expected["spec"])
    ):
        raise ValueError("CPU noise Job identity or template mismatch")
    return metadata


def noise_pod(state: JsonObject, job: JsonObject, run_id: str) -> JsonObject:
    """Require one actual owned process, pinned image and no restart before reading CPU logs."""
    metadata = noise_metadata(job, run_id)
    pods = [
        p
        for p in object_items(state["pods"])
        if str(object_value(p["metadata"])["name"]).startswith("cpu-noise-")
    ]
    if len(pods) != 1:
        raise ValueError("CPU noise pod multiplicity differs")
    pod = pods[0]
    owners = object_items(object_value(pod["metadata"]).get("ownerReferences", []))
    expected = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "name": metadata["name"],
        "uid": metadata["uid"],
        "controller": True,
    }
    if len(owners) != 1 or not includes(owners[0], expected):
        raise ValueError("CPU noise pod owner mismatch")
    spec = object_value(object_value(object_value(job["spec"])["template"])["spec"])
    if not includes(pod.get("spec"), spec):
        raise ValueError("CPU noise pod template mismatch")
    statuses = object_items(object_value(pod["status"]).get("containerStatuses", []))
    if (
        len(statuses) != 1
        or statuses[0].get("restartCount") != 0
        or str(statuses[0].get("imageID", "")).split("@")[-1] != RUNTIME_IMAGE_DIGEST
        or not object_value(statuses[0].get("state", {})).get("running")
    ):
        raise ValueError("CPU noise process is not running the pinned worker")
    return pod
