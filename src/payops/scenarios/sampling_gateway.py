"""Fixed runtime snapshots bind sampler changes to actual owned processes and image identities."""

from datetime import UTC, datetime
from time import monotonic

from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.kubectl import KubectlGateway
from payops.scenarios.memory_provenance import includes, owned_by, template_matches, timestamp
from payops.scenarios.recipes import container
from payops.scenarios.sampling_contract import sampling_payment_baseline, sampling_specs
from payops.tools.traces import bounded_read

SERVICES = ("payments-api", "processor-adapter", "risk-sim", "ledger-sim", "webhook-sim")


class SamplingGateway(KubectlGateway):
    """Read fixed namespace object kinds; inherited CAS still permits only closed Deployments."""

    def state(self, timeout_seconds: float = 30) -> JsonObject:
        """Small namespace bounds reject unexpected users instead of filtering them away."""
        result: JsonObject = {}
        if not 0 < timeout_seconds <= 30:
            raise ValueError("invalid sampling snapshot deadline")
        deadline = monotonic() + timeout_seconds
        for key, kind, maximum in (
            ("deployments", "deployments", 5),
            ("pods", "pods", 8),
            ("replicas", "replicasets", 64),
        ):
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError("sampling snapshot deadline exceeded")
            raw = bounded_read(
                (*self._prefix, "get", kind, "-o", "json"), 262144, min(12, remaining)
            )
            items = object_items(JSON_OBJECT.validate_json(raw)["items"])
            if len(items) > maximum:
                raise ValueError("sampling namespace object bound exceeded")
            result[key] = list(items)
        if monotonic() > deadline:
            raise TimeoutError("sampling snapshot deadline exceeded")
        return result


def deployment_map(state: JsonObject) -> dict[str, JsonObject]:
    """Exactly five distinct named deployments belong to the declared synthetic path."""
    documents = object_items(state["deployments"])
    result = {str(object_value(item["metadata"])["name"]): item for item in documents}
    if len(documents) != 5 or set(result) != set(SERVICES):
        raise ValueError("sampling namespace deployments differ from baseline")
    return result


def validate_runtime_baseline(state: JsonObject) -> None:
    """Check actual payments timeout and startup authority before either control or mutation."""
    documents = deployment_map(state)
    sampling_payment_baseline(documents["payments-api"])
    sampling_specs(documents["processor-adapter"])
    for document in documents.values():
        if container(object_value(document["spec"])).get("image") != "payops-sandbox:local":
            raise ValueError("sampling peers require the normal local sandbox image")
    runtime_identities(state, state, object_value(documents["processor-adapter"]["spec"]))


def _deployment(current: JsonObject, original: JsonObject, expected: JsonObject) -> JsonObject:
    """Exact UID/spec and observed current generation prevent stale controller readiness claims."""
    metadata = object_value(current["metadata"])
    prior = object_value(original["metadata"])
    status = object_value(current.get("status", {}))
    generation = metadata.get("generation")
    if (
        metadata.get("namespace") != "payops-sandbox"
        or metadata.get("uid") != prior["uid"]
        or current.get("spec") != expected
        or type(generation) is not int
        or generation < int(str(prior["generation"]))
        or status.get("observedGeneration") != generation
        or any(
            status.get(key) != 1
            for key in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")
        )
    ):
        raise ValueError("sampling Deployment has not converged to exact owned spec")
    return metadata


def _pod(state: JsonObject, name: str) -> JsonObject:
    """Only one current pod per service may satisfy the stage's evidence identity."""
    pods = object_items(state["pods"])
    if len(pods) != 5:
        raise ValueError("sampling requires exactly five settled pods")
    matches = [
        pod
        for pod in pods
        if object_value(object_value(pod["metadata"]).get("labels", {})).get(
            "app.kubernetes.io/name"
        )
        == name
    ]
    if len(matches) != 1:
        raise ValueError("sampling service pod identity is ambiguous")
    return matches[0]


def _status(pod: JsonObject) -> JsonObject:
    """The same image tag does not prove identical code; preserve actual imageID separately."""
    status = object_value(pod.get("status", {}))
    rows = object_items(status.get("containerStatuses", []))
    if (
        status.get("phase") != "Running"
        or len(rows) != 1
        or rows[0].get("name") != "sandbox"
        or rows[0].get("ready") is not True
        or rows[0].get("restartCount") != 0
        or "running" not in object_value(rows[0].get("state", {}))
        or not isinstance(rows[0].get("imageID"), str)
        or not rows[0].get("imageID")
    ):
        raise ValueError("sampling pod is not a healthy unchanged process")
    return rows[0]


def _identity(
    pod: JsonObject, replicas: list[JsonObject], deployment: JsonObject, expected: JsonObject
) -> PodIdentity:
    """Resolve controller ownership and the exact reviewed container template."""
    metadata = object_value(pod["metadata"])
    if metadata.get("namespace") != "payops-sandbox" or not template_matches(pod, expected):
        raise ValueError("sampling pod template or namespace differs")
    matches = [
        replica
        for replica in replicas
        if (
            object_value(replica["metadata"]).get("namespace") == "payops-sandbox"
            and owned_by(object_value(replica["metadata"]), "Deployment", deployment)
            and owned_by(metadata, "ReplicaSet", object_value(replica["metadata"]))
            and includes(object_value(replica["spec"]).get("template"), expected["template"])
        )
    ]
    if len(matches) != 1:
        raise ValueError("sampling pod owner chain does not match expected Deployment")
    status = _status(pod)
    return PodIdentity.model_validate(
        {
            "pod_name": metadata.get("name"),
            "pod_uid": metadata.get("uid"),
            "deployment_uid": deployment.get("uid"),
            "replica_set_uid": object_value(matches[0]["metadata"]).get("uid"),
            "container_id": status.get("containerID"),
            "restart_count": status.get("restartCount"),
        }
    )


def runtime_identities(
    state: JsonObject,
    original: JsonObject,
    processor_spec: JsonObject,
    requested_at: str | None = None,
    previous_processor: PodIdentity | None = None,
    *,
    risk_image_id: str | None = None,
    payments_image_id: str | None = None,
) -> tuple[PodIdentity, PodIdentity]:
    """All five services retain original ownership/code; a changed sampler requires a fresh pod."""
    documents, prior = deployment_map(state), deployment_map(original)
    identities: dict[str, PodIdentity] = {}
    for name in SERVICES:
        expected = (
            processor_spec if name == "processor-adapter" else object_value(prior[name]["spec"])
        )
        metadata = _deployment(documents[name], prior[name], expected)
        pod, previous = _pod(state, name), _pod(original, name)
        overrides = {"risk-sim": risk_image_id, "payments-api": payments_image_id}
        override = overrides.get(name)
        expected_image = _status(previous)["imageID"] if override is None else override
        if _status(pod)["imageID"] != expected_image:
            raise ValueError("sampling actual image identity changed")
        identities[name] = _identity(pod, object_items(state["replicas"]), metadata, expected)
    processor = identities["processor-adapter"]
    if requested_at is not None:
        _fresh_processor(
            _pod(state, "processor-adapter"), processor, previous_processor, requested_at
        )
    return identities["payments-api"], processor


def _fresh_processor(
    pod: JsonObject, current: PodIdentity, previous: PodIdentity | None, requested_at: str
) -> None:
    """Post-mutation creation and a distinct container reject reused processes."""
    requested = timestamp(requested_at)
    created = timestamp(object_value(pod["metadata"]).get("creationTimestamp"))
    if (
        requested is None
        or created is None
        or previous is None
        or not requested.replace(microsecond=0) <= created <= datetime.now(UTC)
        or current.pod_uid == previous.pod_uid
        or current.container_id == previous.container_id
    ):
        raise ValueError("sampler change lacks a fresh post-mutation process")
