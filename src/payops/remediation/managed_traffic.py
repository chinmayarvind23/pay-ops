"""Bind the existing bounded traffic driver to durable per-source SQL admission."""

from pathlib import Path

import httpx
from pydantic import JsonValue

from payops.remediation.traffic_control import TrafficControl, TrafficMode, TrafficService
from payops.scenarios.traffic import TrafficDriver, TrafficRole


class ManagedTrafficDriver(TrafficDriver):
    """Every synthetic attempt checks admission after acquiring its concurrency slot."""

    def __init__(
        self,
        control: TrafficControl,
        uid: str,
        service: TrafficService,
        mode: TrafficMode,
        kubeconfig: Path,
        output: Path,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Mode and service binding prevent a fixture gate from authorizing live traffic."""
        if mode != ("fixture_replay" if transport is not None else "local_kind") or service not in {
            "payments-api",
            "webhook-sim",
        }:
            raise PermissionError("TRAFFIC_DRIVER_SCOPE_DENIED")
        self._control, self._uid = control, uid
        self._service: TrafficService = service
        self._mode: TrafficMode = mode
        super().__init__(kubeconfig, output, transport, admission=self._admit)

    def _admit(self, role: TrafficRole) -> bool:
        """A wrong-role workload cannot spend admissions against another service's control."""
        if {"payments": "payments-api", "webhook": "webhook-sim"}.get(role) != self._service:
            raise PermissionError("TRAFFIC_DRIVER_ROLE_DENIED")
        return self._control.admit(self._uid, self._service, self._mode)

    def plan_metadata(self) -> dict[str, JsonValue]:
        """Persist control identity beside sample IDs to join a pause to actual attempts."""
        return {"traffic_control_uid": self._uid, "traffic_control_mode": self._mode}
