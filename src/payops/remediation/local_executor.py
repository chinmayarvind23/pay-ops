"""Operator-configured kind executor with one conditional write and bounded rollout postchecks."""

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from time import monotonic, sleep

from payops.contracts import utc_now
from payops.evidence.artifacts import JSON_OBJECT
from payops.policy.contracts import Action, ResourceSnapshot
from payops.policy.engine import SERVICES
from payops.remediation.contracts import EffectReceipt
from payops.remediation.deployment import plan_deployment, rollout_ready, validate_target
from payops.scenarios.contracts import JsonObject, object_value
from payops.tools.traces import ReadCommand, bounded_read


class LocalDeploymentExecutor:
    """This trusted backend is never registered as a model tool or public demo dependency."""

    def __init__(
        self,
        kubectl: Path,
        kubeconfig: Path,
        namespace_uid: str,
        inventory: Mapping[str, str],
        revisions: Mapping[str, Mapping[str, str]],
        *,
        command: ReadCommand = bounded_read,
        clock: Callable[[], float] = monotonic,
        wait: Callable[[float], None] = sleep,
        postcheck_seconds: float = 60,
    ) -> None:
        """Bind immutable operator scope and command budgets before receiving any action."""
        if (
            not namespace_uid
            or not inventory
            or not set(inventory) <= SERVICES
            or not set(revisions) <= set(inventory)
            or not 1 <= postcheck_seconds <= 120
        ):
            raise ValueError("invalid local executor configuration")
        self._prefix = (
            str(kubectl.resolve(strict=True)),
            "--kubeconfig",
            str(kubeconfig.resolve(strict=True)),
            "--context=kind-payops-dev",
            "--namespace=payops-sandbox",
            "--request-timeout=10s",
        )
        self._namespace_uid = namespace_uid
        self._inventory = dict(inventory)
        self._revisions = {service: dict(values) for service, values in revisions.items()}
        self._command, self._clock, self._wait = command, clock, wait
        self._seconds = postcheck_seconds

    def _json(self, args: tuple[str, ...]) -> JsonObject:
        """The bounded subprocess utility drains both pipes and never executes a shell."""
        raw = self._command((*self._prefix, *args, "-o=json"), 262144, 12)
        return JSON_OBJECT.validate_json(raw)

    def _namespace(self) -> None:
        """Namespace recreation invalidates operator authority even if its name is reused."""
        current = self._json(("get", "namespace", "payops-sandbox"))
        metadata = object_value(current["metadata"])
        if (
            metadata.get("uid") != self._namespace_uid
            or metadata.get("name") != "payops-sandbox"
            or metadata.get("deletionTimestamp") is not None
        ):
            raise PermissionError("EXECUTOR_NAMESPACE_CHANGED")

    def _read(self, service: str) -> JsonObject:
        """Only the configured four-service subset is addressable, including postchecks."""
        if service not in self._inventory or service not in SERVICES:
            raise PermissionError("EXECUTOR_SERVICE_DENIED")
        return self._json(("get", "deployment", service))

    def snapshot(self, action: Action) -> ResourceSnapshot:
        """Current resource versions are backend observations; proposals cannot refresh them."""
        validate_target(action, self._inventory)
        self._namespace()
        current = self._read(action.service)
        metadata = object_value(current["metadata"])
        labels = object_value(metadata.get("labels", {}))
        replicas = object_value(current["spec"]).get("replicas", 1)
        if type(replicas) is not int:
            raise ValueError("invalid deployment replica count")
        return ResourceSnapshot(
            namespace=str(metadata["namespace"]),
            service=str(metadata["name"]),
            uid=str(metadata["uid"]),
            version=str(metadata["resourceVersion"]),
            observed_at=utc_now(),
            replicas=replicas,
            synthetic=(
                labels.get("app.kubernetes.io/part-of") == "payops"
                and metadata.get("deletionTimestamp") is None
            ),
            approved_revisions=tuple(self._revisions.get(action.service, {})),
            mode="local_kind",
        )

    def execute(self, action: Action, idempotency_key: str) -> EffectReceipt:
        """The broker must claim dispatch first; any thrown error retains its UNKNOWN state."""
        validate_target(action, self._inventory)
        self._namespace()
        plan = plan_deployment(
            action,
            self._read(action.service),
            idempotency_key,
            self._inventory,
            self._revisions.get(action.service, {}),
        )
        current = self._json(
            (
                "patch",
                "deployment",
                action.service,
                "--type=json",
                "--patch=" + json.dumps(plan.patch(), separators=(",", ":")),
            )
        )
        # No write retry: a timeout may follow a committed patch. The broker preserves that claim.
        deadline = self._clock() + self._seconds
        while not rollout_ready(plan, current):
            if self._clock() >= deadline:
                return self._receipt(action, current, False)
            self._wait(min(1, max(0, deadline - self._clock())))
            self._namespace()
            current = self._read(action.service)
        return self._receipt(action, current, self._clock() <= deadline)

    @staticmethod
    def _receipt(action: Action, current: JsonObject, ready: bool) -> EffectReceipt:
        """Failed readiness can follow an applied patch and never implies automatic rollback."""
        return EffectReceipt(
            outcome="SUCCEEDED" if ready else "FAILED",
            resource_uid=action.resource_uid,
            previous_version=action.expected_version,
            resulting_version=str(object_value(current["metadata"])["resourceVersion"]),
        )
