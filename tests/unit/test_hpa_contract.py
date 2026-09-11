"""Autoscaler status must establish actual demand and a max-replica constraint together."""

from copy import deepcopy
from typing import Literal, cast

import pytest

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.hpa_contract import HPA_NAME, hpa_demand, hpa_spec


def observed(saturated: bool) -> JsonObject:
    """Independent API-shaped records distinguish healthy low demand from capped CPU pressure."""
    return {
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": HPA_NAME,
            "namespace": "payops-sandbox",
            "uid": "owned",
            "generation": 2,
        },
        "spec": hpa_spec(1),
        "status": {
            "observedGeneration": 2,
            "currentReplicas": 1,
            "desiredReplicas": 1,
            "conditions": [
                {"type": "AbleToScale", "status": "True"},
                {"type": "ScalingActive", "status": "True"},
                {
                    "type": "ScalingLimited",
                    "status": "True" if saturated else "False",
                    "reason": "TooManyReplicas" if saturated else "DesiredWithinRange",
                },
            ],
            "currentMetrics": [
                {
                    "type": "Resource",
                    "resource": {
                        "name": "cpu",
                        "current": {"averageUtilization": 200 if saturated else 10},
                    },
                }
            ],
        },
    }


def test_only_cap_changes_between_specs() -> None:
    """Scale-out control preserves resource target, policy and Deployment identity."""
    capped, expanded = hpa_spec(1), hpa_spec(2)
    expected = deepcopy(capped)
    expected["maxReplicas"] = 2
    assert expanded == expected
    for invalid in (0, 3, True):
        with pytest.raises(ValueError):
            hpa_spec(cast(Literal[1, 2], invalid))


@pytest.mark.parametrize("saturated", [True, False])
def test_status_contrast(saturated: bool) -> None:
    """Both demand states require a working controller and exact convergence."""
    result = hpa_demand(observed(saturated), "owned", 1, saturated=saturated)
    assert result.limited is saturated and result.current_replicas == 1
    assert result.cpu_utilization == (200 if saturated else 10)


def test_controller_without_observed_generation() -> None:
    """Kubernetes 1.35 setStatus omits this optional field; freshness needs raw metric windows."""
    row = observed(True)
    object_value(row["status"]).pop("observedGeneration")
    assert hpa_demand(row, "owned", 1, saturated=True).limited


@pytest.mark.parametrize(
    "fault",
    [
        "uid",
        "generation",
        "replicas",
        "duplicate",
        "inactive",
        "reason",
        "limited",
        "metric",
        "resource",
        "utilization",
        "low",
    ],
)
def test_incomplete_or_misleading_cap_evidence_rejects(fault: str) -> None:
    """A status flag alone cannot substitute for correct target, metric and cap evidence."""
    row = observed(True)
    status = object_value(row["status"])
    conditions = object_items(status["conditions"])
    metric = object_items(status["currentMetrics"])[0]
    if fault == "uid":
        object_value(row["metadata"])["uid"] = "foreign"
    elif fault == "generation":
        status["observedGeneration"] = 1
    elif fault == "replicas":
        status["currentReplicas"] = True
    elif fault == "duplicate":
        status["conditions"] = [*conditions, conditions[0]]
    elif fault == "inactive":
        conditions[1]["status"] = "False"
    elif fault == "reason":
        conditions[2]["reason"] = "ScaleUpLimit"
    elif fault == "limited":
        conditions[2]["status"] = "False"
    elif fault == "metric":
        status["currentMetrics"] = []
    elif fault == "resource":
        object_value(metric["resource"])["name"] = "memory"
    else:
        object_value(object_value(metric["resource"])["current"])["averageUtilization"] = (
            True if fault == "utilization" else 20
        )
    with pytest.raises(ValueError):
        hpa_demand(row, "owned", 1, saturated=True)
