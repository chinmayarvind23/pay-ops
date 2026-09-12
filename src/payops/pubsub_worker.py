"""Consume incident references with current backend authority and durable report publication."""

import re
from collections.abc import Callable
from typing import Protocol

from google.cloud.pubsub_v1 import SubscriberClient  # pyright: ignore[reportMissingTypeStubs]
from google.cloud.pubsub_v1.subscriber.futures import (  # pyright: ignore[reportMissingTypeStubs]
    StreamingPullFuture,
)
from google.cloud.pubsub_v1.subscriber.message import (  # pyright: ignore[reportMissingTypeStubs]
    Message,
)
from google.cloud.pubsub_v1.types import FlowControl  # pyright: ignore[reportMissingTypeStubs]

from payops.contracts import Contract, Identifier, Incident
from payops.memory.store import IncidentStore
from payops.orchestrator.state import InvestigationState
from payops.policy.contracts import Principal
from payops.protected_api import Mode, authorize, scoped_incident


class InvestigationRequest(Contract):
    """Messages reference trusted stored incidents; they cannot carry grants or actions."""

    incident_id: Identifier


class DurableInvestigator(Protocol):
    """The configured host owns checkpointing, read budgets and per-operation authority."""

    def start(self, incident: Incident) -> InvestigationState:
        """Return existing state or start a budgeted investigation with immutable inputs."""
        ...

    def resume(self, incident_id: str) -> InvestigationState:
        """Resume recorded work without replacing inputs or replenishing spent budgets."""
        ...


class PubSubIncidentWorker:
    """Use one durable local host; this adapter makes no multi-host exactly-once claim."""

    def __init__(
        self,
        store: IncidentStore,
        investigator: DurableInvestigator,
        principal: Callable[[], Principal | None],
        *,
        mode: Mode,
    ) -> None:
        """Only trusted host wiring supplies authority, storage and the execution mode."""
        if mode not in {"local_kind", "cloud_gke", "fixture_replay"}:
            raise ValueError("invalid worker mode")
        self.store, self.investigator, self.principal, self.mode = (
            store,
            investigator,
            principal,
            mode,
        )

    def _authorized(self, incident_id: str) -> Incident:
        """Recheck fresh responder grants and service scope even on duplicate deliveries."""
        principal = self.principal()
        if principal is None:
            raise PermissionError("worker authority unavailable")
        incident = scoped_incident(self.store, incident_id, principal)
        authorize(principal, incident.request.namespace, write=True)
        return incident

    def process(self, data: bytes) -> None:
        """Acknowledgeable completion requires a scoped report committed to SQL."""
        if len(data) > 1024:
            raise ValueError("queue request exceeds byte budget")
        request = InvestigationRequest.model_validate_json(data)
        incident = self._authorized(request.incident_id)
        report = incident.report
        if report is None:
            state = self.investigator.start(incident)
            if state.phase != "FINISHED":
                state = self.investigator.resume(incident.incident_id)
            if state.phase != "FINISHED" or state.incident != incident:
                raise ValueError("investigation is unfinished or has different inputs")
            report = state.report
        if report is None or report.incident_id != incident.incident_id or report.mode != self.mode:
            raise ValueError("investigator report scope mismatch")
        self._authorized(incident.incident_id)
        saved = self.store.save_report(report)
        if saved.report is None or saved.report.mode != self.mode:
            raise ValueError("stored report mode mismatch")

    def handle(self, message: Message) -> None:
        """Failed work stays retryable; never leak request or exception contents into SDK logs."""
        try:
            self.process(message.data)
        except Exception:
            message.nack()
            return
        message.ack()

    def subscribe(self, client: SubscriberClient, subscription: str) -> StreamingPullFuture:
        """Return the live SDK handle so the operator can stop and drain the configured worker."""
        if (
            re.fullmatch(
                r"projects/[a-z][a-z0-9-]{4,61}[a-z0-9]/subscriptions/[A-Za-z][\w.-]{2,254}",
                subscription,
            )
            is None
        ):
            raise ValueError("invalid configured subscription")
        return client.subscribe(  # pyright: ignore[reportUnknownMemberType]
            subscription,
            callback=self.handle,
            flow_control=FlowControl(max_messages=1, max_bytes=65536, max_lease_duration=600),
            await_callbacks_on_shutdown=True,
        )
