"""Read actual memory-workload evidence without granting public fault-control endpoints."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from pydantic import TypeAdapter

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.kubectl import KubectlGateway

MIB = 1024 * 1024


class MemoryObserver(Protocol):
    """Only a trusted operator collector supplies current pod state and runtime log records."""

    def collect(self) -> JsonObject:
        """Read the fixed payments workload's status and bounded current/previous logs."""
        ...


class MemoryRead:
    """Defer real gateway construction so fixture runners cannot accidentally launch kubectl."""

    def __init__(self, kubeconfig: Path) -> None:
        """Store only the explicit configuration for the dedicated sandbox gateway."""
        self._config = kubeconfig

    def collect(self) -> JsonObject:
        """The concrete gateway fixes service, context, namespace and byte limits."""
        return KubectlGateway(self._config).memory_observation()


def control_holding(observed: JsonObject) -> bool:
    """Require a healthy current pod and recent cgroup usage above the fault limit."""
    pods = object_items(observed.get("pods", []))
    current = {str(object_value(pod["metadata"])["uid"]) for pod in pods if _healthy_pod(pod)}
    if len(current) != 1:
        return False
    logs = [
        log
        for log in object_items(observed.get("memory_logs", []))
        if log.get("pod_uid") in current and log.get("previous") is False
    ]
    return len(logs) == 1 and _latest_holding(str(logs[0].get("text", "")))


def _healthy_pod(pod: JsonObject) -> bool:
    """The control allocation must survive without a hidden restart or unhealthy process."""
    statuses = object_items(object_value(pod.get("status", {})).get("containerStatuses", []))
    return (
        len(statuses) == 1
        and statuses[0].get("name") == "sandbox"
        and (
            statuses[0].get("ready") is True
            and statuses[0].get("restartCount") == 0
            and "running" in object_value(statuses[0].get("state", {}))
        )
    )


def _memory_event(line: str) -> tuple[datetime, JsonObject] | None:
    """Ignore unrelated log prose and reject malformed or untimestamped runtime events."""
    stamp, separator, body = line.partition(" ")
    if not separator:
        return None
    try:
        observed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        payload = TypeAdapter[JsonObject](JsonObject).validate_json(body)
    except (ValueError, TypeError):
        return None
    if observed.tzinfo is None or payload.get("event") != "synthetic.memory":
        return None
    return observed, payload


def _latest_holding(text: str) -> bool:
    """A newer release event invalidates a previous hold even if its timestamp is recent."""
    for line in reversed(text.splitlines()):
        event = _memory_event(line)
        if event is not None:
            observed, payload = event
            age = (datetime.now(UTC) - observed).total_seconds()
            return _holding_payload(payload) and 0 <= age <= 15
    return False


def _holding_payload(payload: JsonObject) -> bool:
    """Cgroup-accounted usage must exceed 128MiB while staying below the 256MiB control."""
    current = payload.get("cgroup_current_bytes")
    return (
        payload.get("event") == "synthetic.memory"
        and payload.get("phase") == "holding"
        and payload.get("allocated_bytes") == 128 * MIB
        and payload.get("cgroup_limit_bytes") == 256 * MIB
        and payload.get("hold_seconds") == 20
        and type(current) is int
        and 128 * MIB < current < 256 * MIB
    )
