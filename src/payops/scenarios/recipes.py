"""Reviewed fixed mutations and a replay workload cannot become generic patch requests."""

from copy import deepcopy

from payops.sandbox.models import FaultConfig
from payops.scenarios.contracts import (
    CaseId,
    DeploymentName,
    JsonObject,
    object_items,
    object_value,
)

VARIANTS: dict[CaseId, str] = {
    "OOM-01": "same bounded payments working set survives 256Mi control and OOMKills at 128Mi",
    "ROLLOUT-01": "local fixed bad image exits during Python startup",
    "ROLLOUT-02": "invalid PAYOPS_SANDBOX_CONFIG processor origin; not missing PROCESSOR_URL",
    "ROLLOUT-03": "readiness path mismatch on a running sandbox process",
    "DEP-01": "local processor replicas zero; not an AWS Lightsail outage",
    "DEP-02": "local processor B adds 400ms delay and returns 429; not Lightsail",
    "PAY-01": "local processor B declines every selected synthetic sample",
    "PAY-02": "local processor adds 600ms only to the eu synthetic region",
    "PAY-03": "local processor declines every debit synthetic sample",
    "PAY-04": "webhook exact replay succeeds; changed-payload retry conflicts; no pod mutation",
}

PACKAGE_A_CASES: tuple[CaseId, ...] = ("DEP-02", "PAY-01", "PAY-02", "PAY-03", "PAY-04")
PACKAGE_A_FAULTS: dict[CaseId, FaultConfig] = {
    "DEP-02": FaultConfig(processor="B", delay_ms=400, rate_limit_every=1),
    "PAY-01": FaultConfig(processor="B", decline_every=1),
    "PAY-02": FaultConfig(region="eu", delay_ms=600),
    "PAY-03": FaultConfig(payment_method="debit", decline_every=1),
}


def target(case_id: CaseId) -> DeploymentName:
    """Closed targets isolate faults to the synthetic dependency or explicit webhook probe."""
    if case_id == "PAY-04":
        return "webhook-sim"
    return "processor-adapter" if case_id in ("DEP-01", *PACKAGE_A_FAULTS) else "payments-api"


def container(spec: JsonObject) -> JsonObject:
    """Unexpected containers could broaden a captured patch, so reject them."""
    template = object_value(spec["template"])
    pod_spec = object_value(template["spec"])
    containers = object_items(pod_spec["containers"])
    if len(containers) != 1 or containers[0].get("name") != "sandbox":
        raise ValueError("expected one sandbox container")
    return containers[0]


def validate_baseline(document: JsonObject, name: DeploymentName) -> JsonObject:
    """Only the reviewed healthy sandbox image/config may become a restoration source."""
    metadata = object_value(document["metadata"])
    if metadata.get("name") != name or metadata.get("namespace") != "payops-sandbox":
        raise ValueError("Deployment outside sandbox scope")
    labels = object_value(metadata.get("labels", {}))
    if labels.get("app.kubernetes.io/part-of") != "payops":
        raise ValueError("Deployment is not owned by PayOps")
    spec = object_value(document["spec"])
    baseline_container = container(spec)
    if baseline_container.get("image") != "payops-sandbox:local" or spec.get("replicas") != 1:
        raise ValueError("baseline must use one healthy local sandbox replica")
    allowed_env = {"PAYOPS_SANDBOX_ROLE", "PAYOPS_SANDBOX_CONFIG"}
    env = object_items(baseline_container.get("env", []))
    if baseline_container.get("envFrom") or any(
        item.get("name") not in allowed_env for item in env
    ):
        raise ValueError("unexpected environment or active fault in baseline")
    if any("valueFrom" in item for item in env):
        raise ValueError("indirect environment is outside this local harness")
    return deepcopy(spec)


def fault_spec(case_id: CaseId, original: JsonObject) -> JsonObject:
    """Recreate makes rollout faults observable instead of leaving a healthy old replica."""
    spec = deepcopy(original)
    if case_id == "OOM-01":
        spec = memory_control_spec(original)
        object_value(object_value(container(spec)["resources"])["limits"])["memory"] = "128Mi"
        return spec
    item = container(spec)
    if case_id == "PAY-04":
        return spec
    if case_id in PACKAGE_A_FAULTS:
        spec["strategy"] = {"type": "Recreate"}
        env = object_items(item["env"])
        env.append(
            {"name": "PAYOPS_SANDBOX_FAULT", "value": PACKAGE_A_FAULTS[case_id].model_dump_json()}
        )
        item["env"] = list(env)
        return spec
    if case_id == "DEP-01":
        spec["replicas"] = 0
        return spec
    spec["strategy"] = {"type": "Recreate"}
    if case_id == "ROLLOUT-01":
        item["image"] = "payops-sandbox:revision-b"
    elif case_id == "ROLLOUT-02":
        env = object_items(item["env"])
        for entry in env:
            if entry.get("name") == "PAYOPS_SANDBOX_CONFIG":
                entry["value"] = '{"processor_url":""}'
    else:
        probe = object_value(item["readinessProbe"])
        object_value(probe["httpGet"])["path"] = "/synthetic-missing-health"
    return spec


def activation(case_id: CaseId, observation: JsonObject) -> bool:
    """Accept actual runtime/probe evidence rather than a successful patch response."""
    if case_id in PACKAGE_A_CASES:
        return observation.get("package_a_verified") is True
    pods = object_items(observation.get("pods", []))
    if case_id == "OOM-01":
        return False  # OOM requires the runner's captured rollout provenance, absent here.
    if case_id == "DEP-01":
        return not pods and observation.get("sample_status") == 503
    if case_id == "ROLLOUT-03":
        events = object_items(observation.get("events", []))
        return any(
            "Readiness probe failed" in str(event.get("message", ""))
            and "404" in str(event.get("message", ""))
            for event in events
        ) and any(_running_unready(pod) for pod in pods)
    return any(_crashed(pod) for pod in pods)


def memory_control_spec(original: JsonObject) -> JsonObject:
    """The control changes the image/worker flag while preserving its 256Mi memory cap."""
    spec = deepcopy(original)
    item = container(spec)
    limits = object_value(object_value(item.get("resources", {})).get("limits", {}))
    if limits.get("memory") != "256Mi" or item.get("command") or item.get("args"):
        raise ValueError("memory control requires unchanged 256Mi baseline and image entrypoint")
    spec["strategy"] = {"type": "Recreate"}
    item["image"] = "payops-sandbox:revision-c"
    env = object_items(item["env"])
    env.append({"name": "PAYOPS_SYNTHETIC_MEMORY_WORKLOAD", "value": "bounded-v1"})
    item["env"] = list(env)
    return spec


def oom_killed(pod: JsonObject) -> bool:
    """Require actual OOMKilled, exit 137 and a restart, rather than a generic termination code."""
    statuses = object_items(object_value(pod.get("status", {})).get("containerStatuses", []))
    for status in statuses:
        previous = object_value(object_value(status.get("lastState", {})).get("terminated", {}))
        if (
            status.get("name") == "sandbox"
            and previous.get("reason") == "OOMKilled"
            and previous.get("exitCode") == 137
            and type(status.get("restartCount")) is int
            and int(str(status["restartCount"])) >= 1
        ):
            return True
    return False


def _crashed(pod: JsonObject) -> bool:
    """A prior nonzero termination and observed restart prove startup failure occurred."""
    statuses = object_items(object_value(pod.get("status", {})).get("containerStatuses", []))
    for status in statuses:
        last_state = object_value(status.get("lastState", {}))
        terminated = object_value(last_state.get("terminated", {}))
        if terminated.get("exitCode") == 1 and int(str(status.get("restartCount", 0))) >= 1:
            return True
    return False


def _running_unready(pod: JsonObject) -> bool:
    """A running process with completed startup separates readiness failure from crash."""
    statuses = object_items(object_value(pod.get("status", {})).get("containerStatuses", []))
    return any(
        "running" in object_value(status.get("state", {}))
        and status.get("started") is True
        and status.get("ready") is False
        for status in statuses
    )
