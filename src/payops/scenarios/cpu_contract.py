"""Freeze the calibrated workload, quota contrast and recovery criteria before live runs."""

import json
import math
from copy import deepcopy
from statistics import mean
from typing import Literal

from payops.evidence.trace_span import Immutable, PodIdentity
from payops.sandbox.models import SandboxConfig
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.cpu_counters import quota
from payops.scenarios.cpu_sources import CpuRecord, record_delta
from payops.scenarios.recipes import container, validate_baseline

CpuStage = Literal["original", "control", "restricted", "recovered", "final"]


class CpuPlan(Immutable):
    """Changing acquisition or qualification thresholds requires a reviewed source revision."""

    version: Literal["cpu-quota-v2"] = "cpu-quota-v2"
    rounds: Literal[50000] = 50000
    samples_per_stage: Literal[3] = 3
    request_timeout_seconds: Literal[5] = 5
    control_quota_usec: Literal[50000] = 50000
    restricted_quota_usec: Literal[10000] = 10000
    period_usec: Literal[100000] = 100000
    control_max_seconds: Literal[1] = 1
    restricted_min_seconds: Literal[1] = 1
    restricted_max_milliseconds: Literal[4500] = 4500
    latency_ratio: Literal[2] = 2
    throttled_min_milliseconds: Literal[500] = 500
    throttled_ratio: Literal[3] = 3
    stages: tuple[
        Literal["original"],
        Literal["control"],
        Literal["restricted"],
        Literal["recovered"],
        Literal["final"],
    ] = ("original", "control", "restricted", "recovered", "final")


PLAN = CpuPlan()


def cpu_specs(document: JsonObject) -> tuple[JsonObject, JsonObject]:
    """Derive complete work variants while preserving memory, requests and peer settings."""
    original = validate_baseline(document, "payments-api")
    item = container(original)
    pod = object_value(object_value(original["template"])["spec"])
    if any(key in item for key in ("command", "args", "envFrom", "lifecycle")) or any(
        pod.get(key) for key in ("initContainers", "ephemeralContainers")
    ):
        raise ValueError("CPU experiment requires the normal fixed entrypoint")
    env = object_items(item.get("env", []))
    values = {str(row.get("name")): row.get("value") for row in env}
    if (
        len(env) != 2
        or any(set(row) != {"name", "value"} for row in env)
        or set(values) != {"PAYOPS_SANDBOX_ROLE", "PAYOPS_SANDBOX_CONFIG"}
        or values["PAYOPS_SANDBOX_ROLE"] != "payments"
    ):
        raise ValueError("CPU experiment requires exact normal startup configuration")
    config = SandboxConfig.model_validate_json(str(values["PAYOPS_SANDBOX_CONFIG"]))
    limits = object_value(object_value(item["resources"])["limits"])
    if limits.get("cpu") != "500m" or config.risk_protocol != "v1" or config.timeout_seconds != 2:
        raise ValueError("CPU experiment requires normal 500m and v1 peer configuration")
    control = deepcopy(original)
    control["strategy"] = {"type": "Recreate"}
    changed = object_items(container(control)["env"])
    for row in changed:
        if row["name"] == "PAYOPS_SANDBOX_CONFIG":
            settings = json.loads(str(row["value"]))
            settings.update(cpu_rounds=PLAN.rounds, cpu_capture=True)
            row["value"] = json.dumps(settings, sort_keys=True, separators=(",", ":"))
    container(control)["env"] = list(changed)
    restricted = deepcopy(control)
    object_value(object_value(container(restricted)["resources"])["limits"])["cpu"] = "100m"
    return control, restricted


def stage_means(
    records: tuple[CpuRecord, ...], incident: str, identity: PodIdentity, maximum: int
) -> tuple[float, float]:
    """Revalidate every source and actual quota before aggregating equal-work observations."""
    if len(records) != PLAN.samples_per_stage or len({row.sample_id for row in records}) != 3:
        raise ValueError("CPU stage requires three distinct samples")
    checked = [CpuRecord.model_validate_json(row.model_dump_json()) for row in records]
    if any(
        a.after.completed_at > b.before.started_at
        for a, b in zip(checked, checked[1:], strict=False)
    ):
        raise ValueError("CPU stage requests overlap or are out of order")
    deltas = [record_delta(row, incident, identity) for row in checked]
    if any(quota(row.before.cpu_max) != (maximum, PLAN.period_usec) for row in checked):
        raise ValueError("CPU stage kernel quota differs from its treatment")
    if any(row.usage_usec <= 0 for row in deltas):
        raise ValueError("CPU stage lacks actual consumed CPU")
    return mean(row.wall_seconds for row in checked), mean(
        row.throttled_usec / 1e6 for row in deltas
    )


def compare(
    control: tuple[float, float], restricted: tuple[float, float], recovered: tuple[float, float]
) -> None:
    """Both normal-quota controls must establish recovery against the restricted treatment."""
    if not all(math.isfinite(value) for pair in (control, restricted, recovered) for value in pair):
        raise ValueError("CPU comparison requires finite measurements")
    for baseline in (control, recovered):
        if not (
            0 < baseline[0] <= PLAN.control_max_seconds
            and PLAN.restricted_min_seconds
            <= restricted[0]
            <= PLAN.restricted_max_milliseconds / 1000
            and restricted[0] >= PLAN.latency_ratio * baseline[0]
            and restricted[1] > PLAN.throttled_min_milliseconds / 1000
            and 0 <= baseline[1] < restricted[1] / PLAN.throttled_ratio
        ):
            raise ValueError("CPU quota contrast or recovery is insufficient")
