"""Incident-scoped HTTP access to the durable broker; no request supplies an actor identity."""

from collections.abc import Callable

from fastapi import APIRouter, HTTPException
from pydantic import JsonValue

from payops.memory.store import IncidentStore
from payops.protected_api import Authenticator, Bearer, actor, authorize, scoped_incident
from payops.remediation.broker import RemediationBroker
from payops.remediation.contracts import ActionRecord, AuditEvent
from payops.remediation.store import TransitionConflict


def dispatch(operation: Callable[[], ActionRecord]) -> ActionRecord:
    """Expose stable failures without backend exception text or private policy evidence."""
    try:
        return operation()
    except PermissionError:
        raise HTTPException(403, detail="ACTION_DENIED") from None
    except KeyError:
        raise HTTPException(404, detail="ACTION_NOT_FOUND") from None
    except TransitionConflict:
        raise HTTPException(409, detail="ACTION_CHANGED") from None


def action_router(
    incidents: IncidentStore, identity: Authenticator, broker: RemediationBroker
) -> APIRouter:
    """Only explicit host wiring enables action routes; the public replay has no broker."""
    router = APIRouter(prefix="/api/incidents/{incident_id}/actions", tags=["remediation"])

    def scoped(incident_id: str, action_id: str, bearer: Bearer) -> ActionRecord:
        """Validate incident scope before looking up an action, then bind both identities."""
        incident = scoped_incident(incidents, incident_id, actor(identity, bearer))
        record = dispatch(lambda: broker.store.get(action_id))
        proposal = record.proposal
        if (proposal.incident_id, proposal.namespace, proposal.service, proposal.mode) != (
            incident_id,
            incident.request.namespace,
            incident.request.service,
            broker.mode,
        ):
            raise HTTPException(404, detail="ACTION_NOT_FOUND")
        return record

    @router.post("", status_code=201)
    def propose(incident_id: str, value: dict[str, JsonValue], bearer: Bearer) -> ActionRecord:
        """Bind the proposal body to the URL incident before independent broker policy review."""
        principal = actor(identity, bearer)
        incident = scoped_incident(incidents, incident_id, principal)
        authorize(principal, incident.request.namespace, write=True)
        if (value.get("incident_id"), value.get("namespace"), value.get("service")) != (
            incident_id,
            incident.request.namespace,
            incident.request.service,
        ):
            raise HTTPException(403, detail="ACTION_SCOPE_MISMATCH")
        result = dispatch(lambda: broker.propose(value, principal.subject))
        return scoped(incident_id, result.action_id, bearer)

    @router.get("/{action_id}")
    def get(incident_id: str, action_id: str, bearer: Bearer) -> ActionRecord:
        """A scoped reader may inspect a proposal without gaining execution authority."""
        return scoped(incident_id, action_id, bearer)

    @router.get("/{action_id}/audit")
    def audit(incident_id: str, action_id: str, bearer: Bearer) -> tuple[AuditEvent, ...]:
        """Audit access follows the same incident boundary as the immutable action record."""
        scoped(incident_id, action_id, bearer)
        events = broker.store.audit(action_id)
        scoped(incident_id, action_id, bearer)
        return events

    @router.post("/{action_id}/approve")
    def approve(incident_id: str, action_id: str, bearer: Bearer) -> ActionRecord:
        """The broker refreshes approver grants and rejects self-approval or stale evidence."""
        scoped(incident_id, action_id, bearer)
        principal = actor(identity, bearer)
        dispatch(lambda: broker.approve(action_id, principal.subject))
        return scoped(incident_id, action_id, bearer)

    @router.post("/{action_id}/execute")
    def execute(incident_id: str, action_id: str, bearer: Bearer) -> ActionRecord:
        """A durable claim precedes effects; replay reads the prior result without redispatch."""
        scoped(incident_id, action_id, bearer)
        principal = actor(identity, bearer)
        dispatch(lambda: broker.execute(action_id, principal.subject))
        return scoped(incident_id, action_id, bearer)

    return router
