"""Closed autoscaler configuration and status checks for the local max-replica experiment."""

from dataclasses import dataclass
from typing import Literal

from payops.scenarios.contracts import JsonObject, object_items, object_value

HPA_NAME = "payments-cap-experiment"


def hpa_spec(maximum: Literal[1, 2]) -> JsonObject:
    """Only the cap changes between the saturated stage and the scale-out counterfactual."""
    if type(maximum) is not int or maximum not in (1, 2):
        raise ValueError("HPA maximum is outside the reviewed one/two replica contrast")
    return {
        "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "payments-api"},
        "minReplicas": 1,
        "maxReplicas": maximum,
        "metrics": [
            {
                "type": "Resource",
                "resource": {
                    "name": "cpu",
                    "target": {"type": "Utilization", "averageUtilization": 50},
                },
            }
        ],
        "behavior": {
            direction: {
                "stabilizationWindowSeconds": 0,
                "selectPolicy": "Max",
                "policies": [{"type": "Pods", "value": 1, "periodSeconds": 15}],
            }
            for direction in ("scaleUp", "scaleDown")
        },
    }


@dataclass(frozen=True)
class HpaDemand:
    """This is controller-reported utilization, not an independent raw Metrics API sample."""

    current_replicas: int
    desired_replicas: int
    cpu_utilization: int
    limited: bool


def _conditions(status: JsonObject, saturated: bool) -> None:
    """Missing metrics and duplicate/unknown condition states cannot establish cap saturation."""
    rows = object_items(status.get("conditions", []))
    values = {str(row.get("type")): row for row in rows}
    if len(values) != len(rows) or set(values) != {
        "AbleToScale",
        "ScalingActive",
        "ScalingLimited",
    }:
        raise ValueError("missing or ambiguous HPA conditions")
    if any(values[name].get("status") != "True" for name in ("AbleToScale", "ScalingActive")):
        raise ValueError("HPA cannot currently scale from valid metrics")
    limited = values["ScalingLimited"]
    if limited.get("status") != ("True" if saturated else "False"):
        raise ValueError("HPA cap condition disagrees with the experiment stage")
    if saturated and limited.get("reason") != "TooManyReplicas":
        raise ValueError("HPA is limited by something other than maxReplicas")


def hpa_demand(
    observed: JsonObject, uid: str, maximum: Literal[1, 2], *, saturated: bool
) -> HpaDemand:
    """Check identity/spec and demand; callers must separately bind fresh raw metrics."""
    metadata = object_value(observed.get("metadata", {}))
    status = object_value(observed.get("status", {}))
    generation = metadata.get("generation")
    observed_generation = status.get("observedGeneration")
    if (
        not uid
        or metadata.get("uid") != uid
        or metadata.get("namespace") != "payops-sandbox"
        or metadata.get("name") != HPA_NAME
        or observed.get("apiVersion") != "autoscaling/v2"
        or observed.get("kind") != "HorizontalPodAutoscaler"
        or observed.get("spec") != hpa_spec(maximum)
        or (generation is not None and (type(generation) is not int or generation < 1))
        or (
            observed_generation is not None
            and (type(observed_generation) is not int or observed_generation != generation)
        )
    ):
        raise ValueError("HPA identity, spec or observed generation differs")
    _conditions(status, saturated)
    current, desired = status.get("currentReplicas"), status.get("desiredReplicas")
    if (
        type(current) is not int
        or type(desired) is not int
        or current != maximum
        or desired != maximum
    ):
        raise ValueError("HPA is not converged at the configured maximum")
    metrics = object_items(status.get("currentMetrics", []))
    if len(metrics) != 1 or metrics[0].get("type") != "Resource":
        raise ValueError("HPA must report exactly its configured CPU resource metric")
    resource = object_value(metrics[0].get("resource", {}))
    value = object_value(resource.get("current", {})).get("averageUtilization")
    if resource.get("name") != "cpu" or type(value) is not int or not 0 <= value <= 10000:
        raise ValueError("HPA reported invalid CPU utilization")
    if (saturated and value < 100) or (not saturated and value > 50):
        raise ValueError("HPA utilization does not establish the intended demand stage")
    return HpaDemand(current, desired, value, saturated)
