"""Join allocation and HTTP evidence to the exact payments process killed by the kernel."""

import hashlib
from dataclasses import dataclass, replace
from datetime import timedelta

from payops.evidence.trace_span import PodIdentity
from payops.scenarios.concurrency_evidence import MemoryProgress, parse_events, validate_events
from payops.scenarios.concurrency_specs import RUNTIME_IMAGE_DIGEST
from payops.scenarios.concurrency_traffic import TrafficWindow
from payops.scenarios.contracts import JsonObject
from payops.scenarios.leak_lifetime import OomLifetime, termination
from payops.scenarios.memory_provenance import current_pods


@dataclass(frozen=True)
class ConcurrencyOom:
    """Retain the raw-log digest alongside independently derived allocation and kernel facts."""

    lifetime: OomLifetime
    memory: MemoryProgress


def capture_concurrency_oom(
    before: JsonObject,
    after: JsonObject,
    original: JsonObject,
    expected: JsonObject,
    requested_at: str,
    identity: PodIdentity,
    traffic: TrafficWindow,
    raw: str,
) -> ConcurrencyOom:
    """Bracket previous logs, rejecting replacement processes, repeated restarts and stale OOMs."""
    frames: list[OomLifetime] = []
    for observed in (before, after):
        pods = current_pods(observed, original, expected, requested_at)
        if len(pods) != 1:
            raise ValueError("payments rollout ownership or pod multiplicity changed")
        frames.append(termination(pods[0]))
    if frames[0] != frames[1]:
        raise ValueError("previous payments container changed during log capture")
    frame = frames[0]
    if (
        frame.pod_uid != identity.pod_uid
        or frame.container_id != identity.container_id
        or frame.restart_count != identity.restart_count + 1
        or frame.image_id.split("@")[-1] != RUNTIME_IMAGE_DIGEST
        or traffic.failures < 1
        or not frame.started <= traffic.started
        or not traffic.started - timedelta(seconds=1)
        <= frame.finished
        <= traffic.completed + timedelta(seconds=1)
    ):
        raise ValueError("OOM does not belong to the tested process and traffic window")
    events = parse_events(raw)
    if any(
        not frame.started <= row.timestamp <= frame.finished + timedelta(seconds=1)
        for row in events
    ):
        raise ValueError("allocation record falls outside the terminated process lifetime")
    memory = validate_events(
        events, traffic.samples, traffic.started, traffic.completed, parallel=True
    )
    return ConcurrencyOom(
        replace(frame, log_sha256=hashlib.sha256(raw.encode("utf-8")).hexdigest()), memory
    )
