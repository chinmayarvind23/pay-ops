"""Frozen operator-only sampling experiment; no model-selected fault parameters."""

from copy import deepcopy
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from payops.sandbox.models import FaultConfig, SandboxConfig
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container, validate_baseline
from payops.scenarios.traffic import SliceCount, Workload


class SamplingPlan(BaseModel):
    """Literal values force changed thresholds to become a reviewed source revision."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    version: Literal["processor-sampling-v1"] = "processor-sampling-v1"
    delay_ms: Literal[600] = 600
    samples_per_stage: Literal[8] = 8
    concurrency: Literal[2] = 2
    traffic_deadline_seconds: Literal[30] = 30
    request_timeout_seconds: Literal[5] = 5
    capture_offsets_seconds: tuple[Literal[12], Literal[17]] = (12, 17)
    maximum_window_seconds: Literal[120] = 120
    host_clock_tolerance_seconds: Literal[1] = 1
    stages: tuple[
        Literal["original"], Literal["suppressed"], Literal["restored_sampling"], Literal["final"]
    ] = ("original", "suppressed", "restored_sampling", "final")
    delayed_mean_min_seconds: float = Field(default=0.55, ge=0.55, le=0.55)
    delayed_mean_max_seconds: float = Field(default=1.5, ge=1.5, le=1.5)
    healthy_mean_max_seconds: float = Field(default=0.3, ge=0.3, le=0.3)
    counterfactual_mean_tolerance_seconds: float = Field(default=0.2, ge=0.2, le=0.2)
    caller_duration_max_seconds: float = Field(default=2.0, ge=2.0, le=2.0)
    evidence_scope: Literal["bounded_sample"] = "bounded_sample"


PLAN = SamplingPlan()


def sampling_workload() -> Workload:
    """Use unique driver IDs with exact equal slices and no request-controlled destination."""
    return Workload(
        distribution=tuple(
            SliceCount(processor=processor, region="us", payment_method="credit", count=4)
            for processor in ("A", "B")
        ),
        concurrency=PLAN.concurrency,
        request_timeout_seconds=float(PLAN.request_timeout_seconds),
        deadline_seconds=float(PLAN.traffic_deadline_seconds),
        seed=0,
    )


def _baseline(
    document: JsonObject,
    name: Literal["processor-adapter", "payments-api"] = "processor-adapter",
    role: Literal["processor", "payments"] = "processor",
) -> JsonObject:
    """Reject alternate entrypoints, indirect settings or already-faulted starting state."""
    original = validate_baseline(document, name)
    metadata = object_value(document["metadata"])
    if (
        not metadata.get("uid")
        or not metadata.get("resourceVersion")
        or type(metadata.get("generation")) is not int
    ):
        raise ValueError("captured Deployment identity and generation required")
    pod = object_value(object_value(original["template"])["spec"])
    item = container(original)
    if any(key in item for key in ("command", "args", "envFrom", "lifecycle")) or any(
        pod.get(key) for key in ("initContainers", "ephemeralContainers")
    ):
        raise ValueError("sampling requires the normal fixed image entrypoint")
    env = object_items(item.get("env", []))
    values = {str(entry.get("name")): entry.get("value") for entry in env}
    if len(env) != 2 or set(values) != {"PAYOPS_SANDBOX_ROLE", "PAYOPS_SANDBOX_CONFIG"}:
        raise ValueError("exact original startup environment required")
    if values["PAYOPS_SANDBOX_ROLE"] != role:
        raise ValueError("sampling target must run its expected role")
    config = SandboxConfig.model_validate_json(str(values["PAYOPS_SANDBOX_CONFIG"]))
    if config.timeout_seconds != 2:
        raise ValueError("captured peer timeout differs from frozen experiment")
    return original


def sampling_payment_baseline(document: JsonObject) -> JsonObject:
    """Validate the actual caller timeout; processor settings cannot establish caller behavior."""
    return _baseline(document, "payments-api", "payments")


def sampling_specs(document: JsonObject) -> tuple[JsonObject, JsonObject]:
    """Derive both future full specs before writes; sampling is their only difference."""
    counterfactual = _baseline(document)
    counterfactual["strategy"] = {"type": "Recreate"}
    item = container(counterfactual)
    env = object_items(item["env"])
    env.append(
        {
            "name": "PAYOPS_SANDBOX_FAULT",
            "value": FaultConfig(delay_ms=PLAN.delay_ms).model_dump_json(),
        }
    )
    item["env"] = list(env)
    suppressed = deepcopy(counterfactual)
    disabled_env = object_items(container(suppressed)["env"])
    disabled_env.append({"name": "OTEL_TRACES_SAMPLER", "value": "always_off"})
    container(suppressed)["env"] = list(disabled_env)
    return suppressed, counterfactual
