"""HTTP failures and memory growth qualify only when the tested owned process actually OOMs."""

import hashlib
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from test_concurrency_evidence import SAMPLES, START, records
from test_memory import replace_field
from test_scheduler import pending_evidence

from payops.evidence.trace_span import PodIdentity
from payops.scenarios.concurrency_lifetime import capture_concurrency_oom
from payops.scenarios.concurrency_specs import RUNTIME_IMAGE_DIGEST
from payops.scenarios.concurrency_traffic import TrafficWindow
from payops.scenarios.contracts import JsonObject, object_items, object_value


def evidence() -> tuple[JsonObject, JsonObject, JsonObject, PodIdentity, TrafficWindow, str]:
    """Use the existing controller-chain fixture with an explicit short OOM lifetime."""
    observed, original, expected, _, _ = pending_evidence()
    pod = object_items(observed["pods"])[0]
    metadata = object_value(pod["metadata"])
    metadata["creationTimestamp"] = START.isoformat()
    pod["status"] = {
        "containerStatuses": [
            {
                "name": "sandbox",
                "restartCount": 1,
                "imageID": "image@" + RUNTIME_IMAGE_DIGEST,
                "lastState": {
                    "terminated": {
                        "reason": "OOMKilled",
                        "exitCode": 137,
                        "containerID": "containerd://" + "a" * 64,
                        "startedAt": START.isoformat(),
                        "finishedAt": (START + timedelta(seconds=1)).isoformat(),
                    }
                },
            }
        ],
    }
    identity = PodIdentity(
        pod_name=str(metadata["name"]),
        pod_uid=str(metadata["uid"]),
        deployment_uid=str(object_value(original["metadata"])["uid"]),
        replica_set_uid="fixture-rs",
        container_id="containerd://" + "a" * 64,
        restart_count=0,
    )
    traffic = TrafficWindow(SAMPLES, START, START + timedelta(seconds=2), 1)
    raw = "\n".join(
        row.timestamp.isoformat() + " " + row.model_dump_json() for row in records(True)
    )
    return observed, original, expected, identity, traffic, raw


def test_joined_owned_oom_and_allocation_evidence() -> None:
    """Derive six actual overlapping allocations and preserve the source hash."""
    observed, original, expected, identity, traffic, raw = evidence()
    result = capture_concurrency_oom(
        observed, deepcopy(observed), original, expected, START.isoformat(), identity, traffic, raw
    )
    assert result.memory.peak_allocated == 6
    assert result.lifetime.container_id == identity.container_id
    assert result.lifetime.log_sha256 == hashlib.sha256(raw.encode()).hexdigest()


@pytest.mark.parametrize(
    "fault", ["owner", "race", "process", "restart", "image", "stale", "success", "events"]
)
def test_unrelated_or_racing_oom_rejects(fault: str) -> None:
    """Reject cross-process evidence, read races, unrelated failures and post-termination work."""
    observed, original, expected, identity, traffic, raw = evidence()
    after = deepcopy(observed)
    if fault == "owner":
        replace_field(after, ("pods", 0, "metadata", "ownerReferences", 0, "uid"), "foreign")
    elif fault == "race":
        replace_field(after, ("pods", 0, "status", "containerStatuses", 0, "restartCount"), 2)
    elif fault == "process":
        identity = identity.model_copy(update={"container_id": "containerd://" + "b" * 64})
    elif fault == "restart":
        identity = identity.model_copy(update={"restart_count": 1})
    elif fault == "image":
        for snapshot in (observed, after):
            replace_field(
                snapshot, ("pods", 0, "status", "containerStatuses", 0, "imageID"), "other"
            )
    elif fault == "stale":
        traffic = replace(
            traffic, started=START + timedelta(seconds=10), completed=START + timedelta(seconds=12)
        )
    elif fault == "success":
        traffic = replace(traffic, failures=0)
    else:
        source = [
            row.model_copy(update={"timestamp": row.timestamp + timedelta(seconds=3)})
            for row in records(True)
        ]
        raw = "\n".join(row.timestamp.isoformat() + " " + row.model_dump_json() for row in source)
    with pytest.raises(ValueError):
        capture_concurrency_oom(
            observed, after, original, expected, START.isoformat(), identity, traffic, raw
        )
