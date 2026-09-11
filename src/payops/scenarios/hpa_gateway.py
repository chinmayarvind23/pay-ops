"""Closed HPA/Job operations with server-side identity preconditions for experiment cleanup."""

import json
import re
import subprocess
from typing import Literal

from payops.evidence.artifacts import JSON_OBJECT
from payops.scenarios.concurrency_gateway import ConcurrencyGateway
from payops.scenarios.contracts import JsonObject, object_value
from payops.scenarios.hpa_contract import HPA_NAME, hpa_spec
from payops.scenarios.hpa_job import load_job
from payops.tools.traces import bounded_read

Kind = Literal["hpa", "job"]


def resource_path(kind: Kind, run_id: str) -> str:
    """Only a run's fixed local experiment objects can become raw API paths."""
    if kind not in {"hpa", "job"} or re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
        raise ValueError("invalid HPA experiment resource identity")
    group, resource, name = (
        ("autoscaling/v2", "horizontalpodautoscalers", HPA_NAME)
        if kind == "hpa"
        else ("batch/v1", "jobs", "hpa-load-" + run_id)
    )
    return f"/apis/{group}/namespaces/payops-sandbox/{resource}/{name}"


def owned_metadata(kind: Kind, document: JsonObject, run_id: str) -> JsonObject:
    """Run labels plus immutable UID and API version prevent deletion by name alone."""
    path = resource_path(kind, run_id)
    metadata = object_value(document.get("metadata", {}))
    expected_api, expected_kind = (
        ("autoscaling/v2", "HorizontalPodAutoscaler") if kind == "hpa" else ("batch/v1", "Job")
    )
    if (
        document.get("apiVersion") != expected_api
        or document.get("kind") != expected_kind
        or metadata.get("name") != path.rsplit("/", 1)[1]
        or metadata.get("namespace") != "payops-sandbox"
        or object_value(metadata.get("labels", {})).get("payops.dev/hpa-run") != run_id
        or not isinstance(metadata.get("uid"), str)
        or not metadata.get("uid")
        or re.fullmatch(r"[0-9]+", str(metadata.get("resourceVersion", ""))) is None
    ):
        raise ValueError("HPA experiment object is not owned by this run")
    return metadata


class HpaGateway(ConcurrencyGateway):
    """Reuse payments CAS and bounded reads; add no arbitrary resource or namespace selector."""

    def _write(self, args: tuple[str, ...], payload: object) -> JsonObject:
        """Send JSON on stdin, preserving literal data across Windows argument parsing."""
        self.verify_scope()
        encoded = json.dumps(payload)
        if len(encoded.encode()) > 65536:
            raise ValueError("HPA mutation payload exceeds reviewed bounds")
        result = subprocess.run(
            (*self._prefix, *args),
            input=encoded,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=True,
            timeout=15,
            shell=False,
        )
        if len(result.stdout.encode()) >= 262144:
            raise ValueError("HPA mutation response is capped")
        return JSON_OBJECT.validate_json(result.stdout)

    def create_hpa(self, run_id: str) -> JsonObject:
        """Create-only refuses an existing autoscaler instead of adopting another controller."""
        resource_path("hpa", run_id)
        document: JsonObject = {
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {
                "name": HPA_NAME,
                "namespace": "payops-sandbox",
                "labels": {"app.kubernetes.io/part-of": "payops", "payops.dev/hpa-run": run_id},
            },
            "spec": hpa_spec(1),
        }
        return self._write(("create", "-f", "-", "-o", "json"), document)

    def create_load(self, run_id: str) -> JsonObject:
        """Create one non-retrying Job from the fixed reviewed template."""
        return self._write(("create", "-f", "-", "-o", "json"), load_job(run_id))

    def read_resource(self, kind: Kind, run_id: str) -> JsonObject:
        """Names are derived from closed types and a validated experiment identity."""
        path = resource_path(kind, run_id)
        raw = bounded_read((*self._prefix, "get", "--raw", path), 262144, 12)
        if len(raw) >= 262144:
            raise ValueError("HPA resource response is capped")
        return JSON_OBJECT.validate_json(raw)

    def set_cap(self, expected: JsonObject, run_id: str, maximum: Literal[1, 2]) -> JsonObject:
        """Resource-version replacement atomically rejects controller or operator changes."""
        owned_metadata("hpa", expected, run_id)
        if expected.get("spec") not in (hpa_spec(1), hpa_spec(2)):
            raise ValueError("unknown HPA configuration cannot enter the contrast")
        document: JsonObject = {
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": dict(object_value(expected["metadata"])),
            "spec": hpa_spec(maximum),
        }
        return self._write(("replace", "-f", "-", "-o", "json"), document)

    def remove_owned(self, kind: Kind, expected: JsonObject, run_id: str) -> JsonObject:
        """UID/version DeleteOptions prevent removing a replacement after the ownership read."""
        metadata = owned_metadata(kind, expected, run_id)
        options = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Foreground",
            "preconditions": {
                "uid": metadata["uid"],
                "resourceVersion": metadata["resourceVersion"],
            },
        }
        return self._write(("delete", "--raw", resource_path(kind, run_id), "-f", "-"), options)
