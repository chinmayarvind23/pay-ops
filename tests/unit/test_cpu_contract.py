"""Calibrated contrasts require equivalent work, actual quota and recovered performance."""

import json
from copy import deepcopy
from datetime import timedelta

import pytest
from test_cpu_counters import pair
from test_cpu_sources import source
from test_scenarios import document

from payops.scenarios.contracts import object_items, object_value
from payops.scenarios.cpu_contract import PLAN, compare, cpu_specs, stage_means
from payops.scenarios.cpu_sources import CpuRecord
from payops.scenarios.recipes import container


def test_specs_change_only_work_configuration_strategy_and_cpu_limit() -> None:
    """Memory, requests and original config remain identical across the causal contrast."""
    original = document("payments-api")
    item = container(object_value(original["spec"]))
    item["resources"] = {"limits": {"cpu": "500m", "memory": "256Mi"}, "requests": {"cpu": "50m"}}
    saved = deepcopy(original)
    control, restricted = cpu_specs(original)
    assert original == saved
    expected = deepcopy(control)
    object_value(object_value(container(expected)["resources"])["limits"])["cpu"] = "100m"
    assert restricted == expected and control["strategy"] == {"type": "Recreate"}
    settings = json.loads(str(object_items(container(control)["env"])[1]["value"]))
    assert settings == {"cpu_capture": True, "cpu_rounds": 50000}
    assert object_value(container(control)["resources"]) == item["resources"]
    assert PLAN.stages == ("original", "control", "restricted", "recovered", "final")


@pytest.mark.parametrize("fault", ["command", "env", "role", "quota", "protocol", "active"])
def test_wrong_baseline_fails(fault: str) -> None:
    """Unrelated startup or quota changes cannot become an experiment's control."""
    original = document("payments-api")
    item = container(object_value(original["spec"]))
    item["resources"] = {"limits": {"cpu": "500m"}}
    env = object_items(item["env"])
    if fault == "command":
        item["command"] = ["other"]
    elif fault == "env":
        env.append(deepcopy(env[0]))
    elif fault == "role":
        env[0]["value"] = "risk"
    elif fault == "quota":
        object_value(object_value(item["resources"])["limits"])["cpu"] = "100m"
    elif fault == "protocol":
        env[1]["value"] = '{"risk_protocol":"v2"}'
    else:
        env[1]["value"] = '{"cpu_rounds":50000}'
    item["env"] = list(env)
    with pytest.raises(ValueError):
        cpu_specs(original)


def records() -> tuple[CpuRecord, ...]:
    """Three sequential requests have independently known equal counter increments."""
    row, _ = source()
    return tuple(
        row.model_copy(
            update={
                "sample_id": f"synthetic-stage-{index}",
                "before": row.before.model_copy(
                    update={
                        "started_at": row.before.started_at + timedelta(seconds=index * 3),
                        "completed_at": row.before.completed_at + timedelta(seconds=index * 3),
                    }
                ),
                "after": row.after.model_copy(
                    update={
                        "started_at": row.after.started_at + timedelta(seconds=index * 3),
                        "completed_at": row.after.completed_at + timedelta(seconds=index * 3),
                    }
                ),
            }
        )
        for index in range(3)
    )


@pytest.mark.parametrize("fault", ["none", "count", "duplicate", "overlap", "quota", "idle"])
def test_stage_aggregation_validates_sources(fault: str) -> None:
    """Missing, repeated, overlapping or idle measurements are not averaged into success."""
    rows = records()
    maximum = 10000
    if fault == "count":
        rows = rows[:2]
    elif fault == "duplicate":
        rows = (rows[0], rows[0], rows[2])
    elif fault == "overlap":
        rows = tuple(reversed(rows))
    elif fault == "quota":
        maximum = 50000
    elif fault == "idle":
        rows = tuple(
            row.model_copy(
                update={
                    "after": row.after.model_copy(
                        update={
                            "cpu_stat": row.after.cpu_stat.replace(
                                "usage_usec 201000", "usage_usec 1000"
                            )
                        }
                    )
                }
            )
            for row in rows
        )
    if fault == "none":
        assert stage_means(rows, "incident", pair()[0].identity, maximum) == (1.9, 1.5)
    else:
        with pytest.raises(ValueError):
            stage_means(rows, "incident", pair()[0].identity, maximum)


@pytest.mark.parametrize(
    "restricted", [(0.9, 1.5), (4.6, 1.5), (1.6, 0.5), (1.6, float("inf")), (1.6, float("nan"))]
)
def test_weak_or_invalid_restriction_fails(restricted: tuple[float, float]) -> None:
    """Small slowdowns, tiny throttling and nonfinite values cannot satisfy the contrast."""
    with pytest.raises(ValueError):
        compare((0.25, 0.1), restricted, (0.3, 0.1))


@pytest.mark.parametrize("baseline", [(1.1, 0.1), (0.9, 0.1), (0.3, 0.5), (0.0, 0.1), (0.3, -1.0)])
def test_both_controls_must_recover(baseline: tuple[float, float]) -> None:
    """Either unhealthy baseline invalidates the result, including failed post-fault recovery."""
    for first, last in ((baseline, (0.3, 0.1)), ((0.3, 0.1), baseline)):
        with pytest.raises(ValueError):
            compare(first, (1.6, 1.5), last)


def test_calibrated_contrast_passes() -> None:
    """Normal throttling is allowed when restriction is substantially worse and reversible."""
    compare((0.25, 0.1), (1.6, 1.5), (0.3, 0.12))
