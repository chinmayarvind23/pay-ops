"""Memory recipe admission, capacity and preservation checks, without live qualification."""

from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest
from test_memory import replace_field
from test_scheduler import SchedulerFixture, task

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container
from payops.scenarios.scheduler_memory import memory_node_identity, memory_plans
from payops.scenarios.scheduler_specs import capture, resource_quantity


def memory_observation() -> JsonObject:
    """Use the observed local capacity while retaining existing node health and ownership."""
    observed = SchedulerFixture().snapshot()
    for node in object_items(observed["nodes"]):
        object_value(object_value(node["status"])["allocatable"])["memory"] = "16124080Ki"
    return observed


@pytest.mark.parametrize("capacity", ["0", "16Gi", "32Gi", "NaN", "-1", None, "unknown"])
def test_memory_capacity_rejects_schedulable_or_invalid_nodes(capacity: str | None) -> None:
    """A large host or unknown quantity must abort before any admission expansion."""
    observed = memory_observation()
    replace_field(observed, ("nodes", 1, "status", "allocatable", "memory"), capacity)
    with pytest.raises(ValueError):
        memory_node_identity(observed)


def test_memory_identity_detects_capacity_and_uid_drift() -> None:
    """Health validation alone cannot hide a replacement node or a changed placement envelope."""
    observed = memory_observation()
    original = memory_node_identity(observed)
    replace_field(observed, ("nodes", 1, "status", "allocatable", "memory"), "15Gi")
    assert memory_node_identity(observed) != original
    observed = memory_observation()
    replace_field(observed, ("nodes", 1, "metadata", "uid"), "replacement")
    assert memory_node_identity(observed) != original


def test_memory_plan_preserves_every_unrelated_field_and_recovery_headroom() -> None:
    """Reverse only reviewed edits to prove exact preservation, including CPU bounds and env."""
    original = capture(memory_observation())
    untouched = deepcopy(original)
    injected = memory_plans(original)
    quota = object_value(injected["quota"]["hard"])
    normal = object_value(container(object_value(original["payments"]["spec"]))["resources"])
    for resource_kind in ("requests", "limits"):
        required = resource_quantity("16Gi") + 5 * resource_quantity(
            object_value(normal[resource_kind])["memory"]
        )
        assert resource_quantity(quota[resource_kind + ".memory"]) > required
        quota[resource_kind + ".memory"] = object_value(
            object_value(original["quota"]["spec"])["hard"]
        )[resource_kind + ".memory"]
        object_value(object_value(container(injected["payments"])["resources"])[resource_kind])[
            "memory"
        ] = object_value(normal[resource_kind])["memory"]
    object_value(object_items(injected["limits"]["limits"])[0]["max"])["memory"] = "1Gi"
    injected["payments"]["strategy"] = object_value(original["payments"]["spec"])["strategy"]
    assert injected == {key: value["spec"] for key, value in original.items()}
    assert original == untouched


@pytest.mark.parametrize("fail_at", range(7))
@pytest.mark.parametrize("after_apply", [False, True])
def test_memory_lifecycle_restores_independent_resources(
    tmp_path: Path, fail_at: int, after_apply: bool
) -> None:
    """Lost responses on all six writes must retain the established recovery semantics."""
    fixture = SchedulerFixture(fail_at, after_apply)
    runner = task(tmp_path, fixture)
    receipt = runner.run("SCHED-02")
    if fail_at == 0:
        assert receipt.activated and receipt.cleanup_verified and not receipt.failure
    elif fail_at <= 3:
        assert not receipt.activated and receipt.failure and receipt.cleanup_verified
    else:
        assert receipt.activated and receipt.cleanup_failure and runner.block_file.exists()
    if receipt.cleanup_verified:
        assert all(
            fixture.documents[key]["spec"] == value["spec"]
            for key, value in fixture.originals.items()
        )


@pytest.mark.parametrize("message", ["Insufficient cpu", "FailedCreate quota exceeded", ""])
def test_memory_requires_actual_memory_scheduler_event(tmp_path: Path, message: str) -> None:
    """CPU rejection and admission errors cannot qualify the memory scenario."""
    fixture = SchedulerFixture()
    snapshot = fixture.snapshot

    def changed_event() -> JsonObject:
        """Retain all correct ownership and pending state while corrupting the reason."""
        observed = snapshot()
        object_items(observed["events"])[0]["message"] = message
        return observed

    with patch.object(fixture, "snapshot", side_effect=changed_event):
        receipt = task(tmp_path, fixture).run("SCHED-02")
    assert not receipt.activated and receipt.failure and receipt.cleanup_verified


def test_memory_capacity_abort_precedes_all_writes(tmp_path: Path) -> None:
    """A schedulable request must not be submitted even with otherwise healthy preflight."""
    fixture = SchedulerFixture()
    observed = fixture.snapshot()
    replace_field(observed, ("nodes", 1, "status", "allocatable", "memory"), "32Gi")
    with patch.object(fixture, "snapshot", return_value=observed):
        receipt = task(tmp_path, fixture).run("SCHED-02")
    assert receipt.failure and not receipt.activated and not fixture.writes
