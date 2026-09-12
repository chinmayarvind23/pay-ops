"""Reject distractor evidence without real CPU consumption, ownership or healthy controls."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.dependency_specs import RUNTIME_IMAGE_DIGEST
from payops.scenarios.noise_contract import noise_job, noise_window
from payops.scenarios.noise_gateway import noise_metadata, noise_pod
from payops.scenarios.noise_harness import noise_absent, processor_down, verify_peers
from payops.scenarios.recipes import fault_spec
from payops.scenarios.sampling_gateway import deployment_map

RUN = "a" * 32


def fixture() -> tuple[JsonObject, JsonObject]:
    """A defaulted Job pod includes only the fixed script and no operational credentials."""
    job = noise_job(RUN)
    object_value(job["metadata"]).update(uid="owned", resourceVersion="1")
    pod: JsonObject = {
        "metadata": {
            "name": "cpu-noise-" + RUN + "-abcde",
            "uid": "pod",
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": "cpu-noise-" + RUN,
                    "uid": "owned",
                    "controller": True,
                }
            ],
        },
        "spec": deepcopy(object_value(object_value(job["spec"])["template"])["spec"]),
        "status": {
            "containerStatuses": [
                {
                    "restartCount": 0,
                    "imageID": "image@" + RUNTIME_IMAGE_DIGEST,
                    "state": {"running": {"startedAt": "2026-09-12T00:00:00Z"}},
                }
            ]
        },
    }
    return job, pod


@pytest.mark.parametrize("corruption", [None, "owner", "image", "script", "restart", "duplicate"])
def test_noise_job_requires_owned_fixed_process(corruption: str | None) -> None:
    """Labels alone cannot substitute another process or worker program."""
    job, pod = fixture()
    state: JsonObject = {"pods": [pod]}
    if corruption == "owner":
        object_items(object_value(pod["metadata"])["ownerReferences"])[0]["uid"] = "foreign"
    elif corruption in {"image", "restart"}:
        status = object_items(object_value(pod["status"])["containerStatuses"])[0]
        status["imageID" if corruption == "image" else "restartCount"] = (
            "foreign" if corruption == "image" else 1
        )
    elif corruption == "script":
        object_items(object_value(pod["spec"])["containers"])[0]["command"] = ["foreign"]
    elif corruption == "duplicate":
        state["pods"] = [pod, pod]
    if corruption:
        with pytest.raises(ValueError):
            noise_pod(state, job, RUN)
    else:
        assert noise_pod(state, job, RUN) == pod
        assert noise_metadata(job, RUN)["uid"] == "owned"


@pytest.mark.parametrize("corruption", [None, "idle", "backwards", "uncovered", "duplicate"])
def test_actual_cpu_must_span_request_window(corruption: str | None) -> None:
    """A CPU label or a busy sample outside the incident window is not a distractor measurement."""
    start = datetime.now(UTC)
    rows = [
        {
            "event": "synthetic.cpu_noise",
            "at": (start + timedelta(seconds=i)).isoformat(),
            "elapsed": i + 1,
            "cpu_seconds": (i + 1) * 0.5,
        }
        for i in range(4)
    ]
    if corruption == "idle":
        for row in rows:
            row["cpu_seconds"] = 0.01 * float(row["elapsed"])
    elif corruption == "backwards":
        rows.reverse()
    elif corruption == "duplicate":
        rows.append(rows[-1])
    raw = "\n".join(json.dumps(row) for row in rows)
    end = start + timedelta(seconds=5 if corruption == "uncovered" else 2)
    if corruption:
        with pytest.raises(ValueError):
            noise_window(raw, start + timedelta(seconds=0.5), end)
    else:
        assert noise_window(raw, start + timedelta(seconds=0.5), end)["cpu_cores"] == 0.5


def test_outage_requires_no_pods_and_unchanged_peers() -> None:
    """A processor outage cannot hide an unrelated peer change or a still-running processor."""
    original = SamplingCluster(Clock()).state()
    state = deepcopy(original)
    off = deepcopy(object_value(deployment_map(original)["processor-adapter"]["spec"]))
    off["replicas"] = 0
    deployment_map(state)["processor-adapter"]["spec"] = off
    state["pods"] = [
        p
        for p in object_items(state["pods"])
        if not str(object_value(p["metadata"])["name"]).startswith("processor-adapter-")
    ]
    verify_peers(state, original, True, off)
    object_value(object_items(state["pods"])[0]["metadata"])["uid"] = "foreign"
    with pytest.raises(ValueError):
        verify_peers(state, original, True, off)
    assert not processor_down(
        {
            "deployment": {
                "metadata": {"generation": 2},
                "spec": off,
                "status": {"observedGeneration": 2},
            },
            "pods": [{"name": "still-running"}],
        }
    )
    assert not noise_absent({"jobs": [], "pods": [{"metadata": {"name": "cpu-noise-leftover"}}]})
    with pytest.raises(ValueError, match="specialized"):
        fault_spec("TELEM-01", off)
