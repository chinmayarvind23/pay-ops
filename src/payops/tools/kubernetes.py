"""Dedicated local Kubernetes reads project status rather than exposing arbitrary objects."""

import json
import re
import shutil
import subprocess
from collections.abc import Callable
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from pydantic import JsonValue

from payops.contracts import Source, utc_now
from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.normalize import Observation

SERVICES = frozenset({"payments-api", "risk-sim", "processor-adapter", "ledger-sim", "webhook-sim"})
MAX_BYTES = 262144
type JsonObject = dict[str, JsonValue]


def object_value(value: JsonValue) -> JsonObject:
    """Malformed source structure is a collection failure, never an empty healthy signal."""
    if not isinstance(value, dict):
        raise ValueError("expected Kubernetes object")
    return value


def items(value: JsonValue, maximum: int = 64) -> list[JsonObject]:
    """A collection budget prevents one source from consuming the incident context."""
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError("invalid or oversized Kubernetes collection")
    return [object_value(item) for item in value]


def selected(value: JsonValue, names: tuple[str, ...]) -> JsonObject:
    """Projection deliberately excludes environment values, annotations and managed fields."""
    obj = object_value(value)
    return {key: obj[key] for key in names if key in obj}


def project_deployment(value: JsonObject) -> JsonObject:
    """Retain rollout/resource context without copying deployment configuration secrets."""
    spec = object_value(value.get("spec", {}))
    template = object_value(object_value(spec.get("template", {})).get("spec", {}))
    containers: list[JsonValue] = []
    for container in items(template.get("containers", []), 8):
        projected = selected(container, ("name", "resources", "readinessProbe"))
        projected["image_fingerprint"] = sha256(
            str(container.get("image", "")).encode()
        ).hexdigest()
        projected["env_names"] = [entry.get("name") for entry in items(container.get("env", []))]
        containers.append(projected)
    return {
        "kind": "Deployment",
        "replicas": spec.get("replicas"),
        "containers": containers,
        "status": selected(
            value.get("status", {}),
            ("availableReplicas", "readyReplicas", "updatedReplicas", "conditions"),
        ),
    }


def project_pod(value: JsonObject) -> JsonObject:
    """Container termination states distinguish OOM/startup failures from scheduling gaps."""
    raw_status = object_value(value.get("status", {}))
    status = selected(raw_status, ("phase", "reason", "message", "conditions"))
    status["containerStatuses"] = [
        selected(item, ("name", "ready", "restartCount", "state", "lastState", "imageID"))
        for item in items(raw_status.get("containerStatuses", []), 8)
    ]
    return {"kind": "Pod", "status": status}


def run_read(args: tuple[str, ...]) -> str:
    """Only this module constructs argv; no shell interpreter or caller-supplied verbs exist."""
    result = subprocess.run(
        args,
        shell=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=12,
        check=True,
    )
    if len(result.stdout.encode("utf-8")) > MAX_BYTES:
        raise ValueError("Kubernetes response exceeds byte budget")
    return result.stdout


class KubernetesRead:
    """The model receives collect/log contracts, never the scenario mutation gateway."""

    def __init__(
        self, kubeconfig: Path, invoke: Callable[[tuple[str, ...]], str] = run_read
    ) -> None:
        """Pin trusted configuration and namespace; neither is accepted from model arguments."""
        executable = shutil.which("kubectl")
        if executable is None or not kubeconfig.is_file():
            raise ValueError("kubectl and explicit kubeconfig required")
        self._prefix = (
            executable,
            "--kubeconfig",
            str(kubeconfig.resolve()),
            "--context",
            "kind-payops-dev",
            "--namespace",
            "payops-sandbox",
            "--request-timeout=8s",
        )
        self._invoke = invoke

    def _get(self, kind: str, *arguments: str) -> JsonObject:
        """Internal call sites supply fixed kinds; source bytes are bounded and schema checked."""
        raw = self._invoke((*self._prefix, "get", kind, *arguments, "-o", "json"))
        if len(raw.encode("utf-8")) > MAX_BYTES:
            raise ValueError("Kubernetes response exceeds byte budget")
        return JSON_OBJECT.validate_json(raw)

    def collect(self, service: str) -> tuple[Observation, ...]:
        """Collect current snapshots of one approved service and its explicitly labeled pods."""
        if service not in SERVICES:
            raise ValueError("service outside read allowlist")
        deployment = self._get("deployment", service)
        if object_value(deployment.get("metadata", {})).get("name") != service:
            raise ValueError("deployment response has wrong resource identity")
        pods = self._get("pods", "-l", f"app.kubernetes.io/name={service}")
        observed = utc_now()
        results = [self._observation(deployment, project_deployment(deployment), "DEPLOYMENT")]
        for pod in items(pods.get("items", []), 32):
            labels = object_value(object_value(pod.get("metadata", {})).get("labels", {}))
            if labels.get("app.kubernetes.io/name") != service:
                raise ValueError("pod response has wrong service ownership")
            results.append(self._observation(pod, project_pod(pod), "KUBERNETES"))
        results.append(
            Observation(
                source="KUBERNETES",
                resource=service,
                observed_at=observed,
                query="pods.count",
                summary=f"Observed {len(results) - 1} pods",
                payload={"pod_count": len(results) - 1},
            )
        )
        return tuple(results)

    def _observation(self, raw: JsonObject, payload: JsonObject, source: Source) -> Observation:
        """Check scope again on response data before normalizing a source projection."""
        metadata = object_value(raw.get("metadata", {}))
        if metadata.get("namespace") != "payops-sandbox":
            raise ValueError("Kubernetes response crosses namespace")
        name = str(metadata.get("name", ""))
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,126}", name):
            raise ValueError("invalid resource name")
        payload["resource_uid"] = metadata.get("uid")
        payload["generation"] = metadata.get("generation")
        return Observation(
            source=source,
            resource=name,
            observed_at=utc_now(),
            query="kubernetes.status-snapshot",
            summary=json.dumps(payload)[:3500],
            payload=payload,
        )

    def logs(self, service: str) -> Observation:
        """Read one deployment's current container logs with explicit line/time/byte bounds."""
        if service not in SERVICES:
            raise ValueError("service outside read allowlist")
        raw = self._invoke(
            (
                *self._prefix,
                "logs",
                f"deployment/{service}",
                "--container=sandbox",
                "--tail=100",
                "--since=5m",
                "--timestamps=true",
                "--limit-bytes=32768",
            )
        )
        if len(raw.encode("utf-8")) > 32768:
            raise ValueError("log response exceeds byte budget")
        return Observation(
            source="LOG",
            resource=service,
            observed_at=utc_now(),
            query="logs.5m.100",
            summary=raw[-3500:] or "No current container log lines",
            payload={"lines": raw},
        )

    def events(self, service: str) -> tuple[Observation, ...]:
        """Use exact source event timestamps and current pod UID ownership, not relative ages."""
        if service not in SERVICES:
            raise ValueError("service outside read allowlist")
        pods = items(
            self._get("pods", "-l", f"app.kubernetes.io/name={service}").get("items", []), 32
        )
        pod_uids: set[str] = set()
        for pod in pods:
            metadata = object_value(pod.get("metadata", {}))
            uid = metadata.get("uid")
            labels = object_value(metadata.get("labels", {}))
            if (
                not isinstance(uid, str)
                or labels.get("app.kubernetes.io/name") != service
                or metadata.get("namespace") != "payops-sandbox"
            ):
                raise ValueError("event pod ownership is invalid")
            pod_uids.add(uid)
        raw = self._get("events", "--field-selector", "involvedObject.kind=Pod")
        observations: list[Observation] = []
        for event in items(raw.get("items", []), 256):
            involved = object_value(event.get("involvedObject", {}))
            if str(involved.get("uid")) not in pod_uids:
                continue
            if involved.get("namespace") != "payops-sandbox":
                raise ValueError("event response crosses namespace")
            if object_value(event.get("metadata", {})).get("namespace") != "payops-sandbox":
                raise ValueError("event metadata crosses namespace")
            series = object_value(event.get("series") or {})
            timestamp = (
                series.get("lastObservedTime")
                or event.get("lastTimestamp")
                or event.get("eventTime")
            )
            if not isinstance(timestamp, str):
                continue
            payload = selected(
                event,
                (
                    "reason",
                    "message",
                    "count",
                    "type",
                    "firstTimestamp",
                    "lastTimestamp",
                    "eventTime",
                    "series",
                    "involvedObject",
                ),
            )
            observations.append(
                Observation(
                    source="KUBERNETES",
                    resource=service,
                    observed_at=datetime.fromisoformat(timestamp.replace("Z", "+00:00")),
                    query="events.current-pod-uid",
                    summary=str(event.get("message", "event"))[:3500],
                    payload=payload,
                )
            )
        return tuple(observations)
