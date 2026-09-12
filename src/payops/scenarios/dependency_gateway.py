"""Closed operator access to Redis availability and read-only PostgreSQL pressure proof."""

import json
import re
import time
from copy import deepcopy

import httpx

from payops.evidence.artifacts import JSON_OBJECT
from payops.scenarios.concurrency_gateway import ConcurrencyGateway
from payops.scenarios.contracts import JsonObject, object_items, object_value, utc_timestamp
from payops.tools.traces import bounded_read


class DependencyGateway(ConcurrencyGateway):
    """No model-facing arbitrary namespace, query, credential or mutation interface."""

    def postgres_forward_argv(self) -> tuple[str, ...]:
        """Expose only the fixed local PostgreSQL forward needed by owned pressure sessions."""
        return (
            *self._prefix[:6],
            "payops-data",
            *self._prefix[7:],
            "port-forward",
            "service/postgres",
            "35532:5432",
            "--address",
            "127.0.0.1",
        )

    def data_read(self, args: tuple[str, ...]) -> str:
        """Internal fixed callers retain the same context, timeout and output byte bound."""
        prefix = (*self._prefix[:6], "payops-data", *self._prefix[7:])
        raw = bounded_read((*prefix, *args), 262144, 12)
        if len(raw) >= 262144:
            raise ValueError("dependency response exceeds bound")
        return raw.decode("utf-8")

    def data_state(self) -> JsonObject:
        """Capture all data deployments and pods to detect concurrent changes."""
        return {
            "deployments": JSON_OBJECT.validate_json(
                self.data_read(("get", "deployments", "-o", "json"))
            ),
            "pods": JSON_OBJECT.validate_json(self.data_read(("get", "pods", "-o", "json"))),
            "recorded_at": utc_timestamp(),
        }

    def redis(self) -> JsonObject:
        """Redis is the only mutable data resource in this harness."""
        return JSON_OBJECT.validate_json(
            self.data_read(("get", "deployment", "redis", "-o", "json"))
        )

    def redis_replicas(self, expected: JsonObject, replicas: int) -> None:
        """Atomic UID/version/full-spec checks prevent overwriting concurrent Redis changes."""
        if replicas not in (0, 1):
            raise ValueError("Redis replicas must be zero or one")
        self.verify_scope()
        metadata = object_value(expected["metadata"])
        if metadata.get("name") != "redis" or metadata.get("namespace") != "payops-data":
            raise ValueError("Redis scope mismatch")
        spec = deepcopy(object_value(expected["spec"]))
        spec["replicas"] = replicas
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": metadata["resourceVersion"],
            },
            {"op": "test", "path": "/spec", "value": expected["spec"]},
            {"op": "replace", "path": "/spec", "value": spec},
        ]
        self.data_read(
            ("patch", "deployment", "redis", "--type=json", "--patch", json.dumps(patch))
        )

    def postgres_sessions(self, run_id: str) -> JsonObject:
        """The fixed administrator query reports counts and PIDs, never application data."""
        if re.fullmatch("[0-9a-f]{32}", run_id) is None:
            raise ValueError("invalid pressure run identity")
        query = (
            "SELECT json_build_object('limit', (SELECT rolconnlimit FROM pg_roles WHERE "
            "rolname='payops_synthetic'), 'total', count(*), 'owned', count(*) FILTER "
            f"(WHERE application_name='payops-scenario-{run_id}'), 'pids', "
            "coalesce(json_agg(pid), '[]'::json)) FROM pg_stat_activity "
            "WHERE usename='payops_synthetic'"
        )
        return JSON_OBJECT.validate_json(
            self.data_read(
                (
                    "exec",
                    "deployment/postgres",
                    "--",
                    "psql",
                    "-U",
                    "postgres",
                    "-d",
                    "payops",
                    "-Atc",
                    query,
                )
            )
        )

    def metrics(self) -> JsonObject:
        """Keep actual exposition bytes and capture timestamps; never retimestamp stale values."""
        with self._forward() as origin, httpx.Client(timeout=3, trust_env=False) as client:
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                try:
                    with client.stream("GET", origin + "/metrics") as response:
                        response.raise_for_status()
                        body = bytearray()
                        for chunk in response.iter_bytes():
                            body.extend(chunk)
                            if len(body) > 131072:
                                raise ValueError("metrics exceed bound")
                        return {
                            "text": body.decode(),
                            "snapshot": response.headers.get("x-payops-metrics-snapshot"),
                            "captured_at": utc_timestamp(),
                        }
                except httpx.RequestError:
                    time.sleep(0.1)
        raise TimeoutError("metrics forward unavailable")


def data_deployments(state: JsonObject) -> dict[str, JsonObject]:
    """Require the exact provisioned data service set before any outage."""
    items = object_items(object_value(state["deployments"])["items"])
    result = {str(object_value(item["metadata"])["name"]): item for item in items}
    if len(items) != 3 or set(result) != {"postgres", "redis", "elasticsearch"}:
        raise ValueError("unexpected data service set")
    return result
