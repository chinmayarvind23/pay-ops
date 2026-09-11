"""Owned termination and read-race checks distinguish repeated OOMs from stale observations."""

import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from test_leak_evidence import END, START, sequence
from test_memory import replace_field
from test_scheduler import pending_evidence

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.leak_lifetime import capture_lifetime, repeated_oom


def evidence() -> tuple[JsonObject, JsonObject, JsonObject, str]:
    """Reuse actual controller-chain fixtures while selecting the closed risk target."""
    observed, original, expected, _, _ = pending_evidence()
    documents = json.loads(
        json.dumps([observed, original, expected]).replace("payments-api", "risk-sim")
    )
    observed, original, expected = documents
    pod = object_items(observed["pods"])[0]
    object_value(pod["metadata"])["creationTimestamp"] = START.isoformat()
    pod["status"] = {
        "containerStatuses": [
            {
                "name": "sandbox",
                "restartCount": 1,
                "imageID": "sha256:fixed-image",
                "lastState": {
                    "terminated": {
                        "reason": "OOMKilled",
                        "exitCode": 137,
                        "containerID": "containerd://" + "a" * 64,
                        "startedAt": START.isoformat(),
                        "finishedAt": END.isoformat(),
                    }
                },
            }
        ]
    }
    raw = "\n".join(
        r.timestamp.isoformat() + " " + r.model_dump_json() for r in sequence("retained-v1")
    )
    return observed, original, expected, raw


def test_distinct_adjacent_lifetimes_and_duplicate_rejection() -> None:
    """A retained allocation sequence binds to one actual OOM; the same frame cannot count twice."""
    observed, original, expected, raw = evidence()
    first = capture_lifetime(
        observed, deepcopy(observed), original, expected, START.isoformat(), raw
    )
    assert not repeated_oom(first, first)
    second = replace(
        first,
        container_id="containerd://" + "b" * 64,
        restart_count=2,
        started=END,
        finished=END + timedelta(seconds=30),
        log_sha256="different",
    )
    assert repeated_oom(first, second)
    for invalid in (
        replace(second, pod_uid="replacement"),
        replace(second, image_id="other"),
        replace(second, restart_count=3),
        replace(second, started=START),
        replace(second, log_sha256=""),
        replace(second, container_id=first.container_id),
    ):
        assert not repeated_oom(first, invalid)


@pytest.mark.parametrize(
    "field,value",
    [
        ("reason", "Error"),
        ("exitCode", 1),
        ("exitCode", True),
        ("containerID", ""),
        ("startedAt", "invalid"),
        ("finishedAt", "2099-01-01T00:00:00Z"),
    ],
)
def test_wrong_termination_rejects(field: str, value: str | int | bool) -> None:
    """Neither exit137 alone nor a fabricated/future termination can establish kernel OOM."""
    observed, original, expected, raw = evidence()
    replace_field(
        observed,
        ("pods", 0, "status", "containerStatuses", 0, "lastState", "terminated", field),
        value,
    )
    with pytest.raises(ValueError):
        capture_lifetime(observed, observed, original, expected, START.isoformat(), raw)


@pytest.mark.parametrize("change", ["container", "restart", "owner", "deployment"])
def test_previous_log_read_races_reject(change: str) -> None:
    """Before/after observations must retain the exact previous termination and ownership."""
    observed, original, expected, raw = evidence()
    after = deepcopy(observed)
    paths = {
        "container": (
            ("pods", 0, "status", "containerStatuses", 0, "lastState", "terminated", "containerID"),
            "containerd://" + "b" * 64,
        ),
        "restart": (("pods", 0, "status", "containerStatuses", 0, "restartCount"), 2),
        "owner": (("pods", 0, "metadata", "ownerReferences", 0, "uid"), "other"),
        "deployment": (("deployment", "metadata", "name"), "payments-api"),
    }
    path, value = paths[change]
    replace_field(after, path, value)
    with pytest.raises(ValueError):
        capture_lifetime(observed, after, original, expected, START.isoformat(), raw)
