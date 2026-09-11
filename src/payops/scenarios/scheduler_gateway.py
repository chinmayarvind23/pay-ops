"""A closed three-object operator gateway cannot become a generic admission editor."""

import json
from typing import Literal, Protocol

from payops.scenarios.contracts import ClusterGateway, JsonObject, object_items, object_value
from payops.scenarios.kubectl import KubectlGateway

Slot = Literal["payments", "quota", "limits"]
RESOURCES: dict[Slot, tuple[str, str]] = {
    "payments": ("deployment", "payments-api"),
    "quota": ("resourcequota", "sandbox-budget"),
    "limits": ("limitrange", "sandbox-container-bounds"),
}


class SchedulerAccess(ClusterGateway, Protocol):
    """Only the trusted scheduler runner receives these admission mutation capabilities."""

    def resource(self, slot: Slot) -> JsonObject:
        """Read one fixed object for capture or CAS verification."""
        ...

    def replace_resource(self, slot: Slot, expected: JsonObject, spec: JsonObject) -> None:
        """Replace one reviewed spec under current identity/version/spec tests."""
        ...

    def snapshot(self) -> JsonObject:
        """Read current namespace users, admission objects, node capacity and owned pod evidence."""
        ...


class SchedulerGateway(KubectlGateway):
    """Resource arguments are internal enum slots; context and namespace stay pinned."""

    def resource(self, slot: Slot) -> JsonObject:
        """Reject runtime strings outside the closed three-object table."""
        if slot not in RESOURCES:
            raise ValueError("scheduler resource outside allowlist")
        return self._json(("get", *RESOURCES[slot]))

    def replace_resource(self, slot: Slot, expected: JsonObject, spec: JsonObject) -> None:
        """A lost response is ambiguous; callers journal both possible specs before this write."""
        self.verify_scope()
        current = self.resource(slot)
        metadata = object_value(current["metadata"])
        if (
            current["spec"] != expected["spec"]
            or metadata["uid"] != object_value(expected["metadata"])["uid"]
        ):
            raise ValueError("scheduler resource changed before patch")
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
        self._invoke(("patch", *RESOURCES[slot], "--type=json", "--patch", json.dumps(patch)))

    def snapshot(self) -> JsonObject:
        """Bound object counts and retain all users so extra workloads cannot hide behind labels."""
        observed = self.observe("payments-api")
        queries = {
            "namespace_pods": "pods",
            "deployments": "deployments",
            "replica_sets": "replicasets",
            "quotas": "resourcequotas",
            "limit_ranges": "limitranges",
            "nodes": "nodes",
            "other_workloads": "jobs,cronjobs,daemonsets,statefulsets,replicationcontrollers",
        }
        for key, resource in queries.items():
            items = object_items(self._json(("get", resource))["items"])
            if len(items) > 32:
                raise ValueError("scheduler snapshot exceeds fixed resource count")
            observed[key] = list(items)
        return observed
