"""Bind previous logs to stable Kubernetes termination state and distinguish repeated OOMs."""

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.leak_evidence import parse_records, validate_progression
from payops.scenarios.memory_provenance import current_pods, timestamp


@dataclass(frozen=True)
class OomLifetime:
    """One observed kernel termination; repeated snapshots of its container ID count only once."""

    pod_uid: str
    container_id: str
    image_id: str
    restart_count: int
    started: datetime
    finished: datetime
    log_sha256: str


def termination(pod: JsonObject) -> OomLifetime:
    """Exit137 alone is insufficient; require kubelet OOM reason and a real container identity."""
    metadata = object_value(pod["metadata"])
    statuses = object_items(object_value(pod.get("status", {})).get("containerStatuses", []))
    if len(statuses) != 1 or statuses[0].get("name") != "sandbox":
        raise ValueError("expected one risk container status")
    status = statuses[0]
    ended = object_value(object_value(status.get("lastState", {})).get("terminated", {}))
    restart = status.get("restartCount")
    container_id = str(ended.get("containerID", ""))
    if (
        ended.get("reason") != "OOMKilled"
        or type(ended.get("exitCode")) is not int
        or ended.get("exitCode") != 137
        or type(restart) is not int
        or restart < 1
        or re.fullmatch(r"containerd://[0-9a-f]{64}", container_id) is None
        or not status.get("imageID")
        or not metadata.get("uid")
    ):
        raise ValueError("missing actual OOM termination identity")
    created, started, finished = (
        timestamp(metadata.get("creationTimestamp")),
        timestamp(ended.get("startedAt")),
        timestamp(ended.get("finishedAt")),
    )
    if created is None or started is None or finished is None:
        raise ValueError("missing termination timestamps")
    if not created <= started < finished <= datetime.now(UTC):
        raise ValueError("invalid termination time order")
    return OomLifetime(
        str(metadata["uid"]), container_id, str(status["imageID"]), restart, started, finished, ""
    )


def capture_lifetime(
    before: JsonObject,
    after: JsonObject,
    original: JsonObject,
    expected: JsonObject,
    requested_at: str,
    raw: str,
) -> OomLifetime:
    """Bracket a previous-container log read with identical owned termination observations."""
    frames: list[OomLifetime] = []
    for observed in (before, after):
        pods = current_pods(observed, original, expected, requested_at, "risk-sim")
        if len(pods) != 1:
            raise ValueError("risk rollout ownership or pod multiplicity changed")
        frames.append(termination(pods[0]))
    if frames[0] != frames[1]:
        raise ValueError("previous container changed during log capture")
    frame = frames[0]
    validate_progression(parse_records(raw), "retained-v1", frame.started, frame.finished)
    return OomLifetime(
        frame.pod_uid,
        frame.container_id,
        frame.image_id,
        frame.restart_count,
        frame.started,
        frame.finished,
        hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    )


def repeated_oom(first: OomLifetime, second: OomLifetime) -> bool:
    """Require adjacent lifetimes of one pod/image, excluding repeated snapshots."""
    return (
        bool(first.log_sha256)
        and bool(second.log_sha256)
        and first.log_sha256 != second.log_sha256
        and first.pod_uid == second.pod_uid
        and first.image_id == second.image_id
        and first.container_id != second.container_id
        and second.restart_count == first.restart_count + 1
        and first.finished <= second.started
    )
