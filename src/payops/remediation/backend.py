"""Compose authoritative identity, immutable evidence and local effects for the approval broker."""

from collections.abc import Callable

from payops.evidence.artifacts import ArtifactStore
from payops.memory.store import IncidentStore
from payops.policy.contracts import Action, PauseAction, Principal
from payops.policy.engine import PolicyContext
from payops.remediation.contracts import EffectReceipt
from payops.remediation.local_executor import LocalDeploymentExecutor
from payops.remediation.traffic_control import TrafficControl


class OperationalBackend:
    """Host wiring supplies IdentityService.principal; models cannot supply identity claims."""

    def __init__(
        self,
        principals: Callable[[str], Principal | None],
        incidents: IncidentStore,
        artifacts: ArtifactStore,
        executor: LocalDeploymentExecutor,
        traffic: TrafficControl | None = None,
    ) -> None:
        """Dependency lifetimes belong to the authenticated host, not individual API requests."""
        self._principals, self._incidents = principals, incidents
        self._artifacts, self._executor = artifacts, executor
        self._traffic = traffic

    def principal(self, subject: str) -> Principal | None:
        """Refresh grants through the configured identity authority on every broker check."""
        return self._principals(subject)

    def context(self, action: Action, subject: str) -> PolicyContext:
        """A missing incident cannot be replaced by caller-supplied evidence or report data."""
        incident = self._incidents.get(action.incident_id)
        if incident is None:
            raise PermissionError("INCIDENT_NOT_FOUND")
        if isinstance(action, PauseAction):
            if self._traffic is None:
                raise PermissionError("TRAFFIC_BACKEND_UNAVAILABLE")
            resource = self._traffic.snapshot(action)
        else:
            resource = self._executor.snapshot(action)
        return PolicyContext(self.principal(subject), incident, resource, self._artifacts)

    def execute(self, action: Action, idempotency_key: str) -> EffectReceipt:
        """Execution goes through the same operator inventory used during policy review."""
        if isinstance(action, PauseAction):
            if self._traffic is None:
                raise PermissionError("TRAFFIC_BACKEND_UNAVAILABLE")
            return self._traffic.pause(action, idempotency_key)
        return self._executor.execute(action, idempotency_key)
