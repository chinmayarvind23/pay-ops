"""Deny-default proposal evaluation independent of the model and operational executor."""

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from pydantic import JsonValue

from payops.contracts import EvidenceItem, Incident, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.payment_window import verify_payment_window
from payops.policy.contracts import (
    ACTION,
    Action,
    PauseAction,
    PolicyReview,
    Principal,
    ResourceSnapshot,
    RollbackAction,
    ScaleAction,
)

SERVICES = frozenset({"payments-api", "processor-adapter", "risk-sim", "webhook-sim"})


@dataclass(frozen=True)
class PolicyContext:
    """These inputs come from backend identity, incident storage and resource reads."""

    principal: Principal | None
    incident: Incident
    resource: ResourceSnapshot
    store: ArtifactStore


def action_digest(proposal: Action) -> str:
    """Canonical content binds every target, parameter, precondition and evidence reference."""
    payload = json.dumps(proposal.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return sha256(payload.encode()).hexdigest()


def identity_valid(principal: Principal | None, role: str, namespace: str, now: datetime) -> bool:
    """Expired, disabled or stale authenticated identities must be refreshed by the backend."""
    return (
        principal is not None
        and principal.enabled
        and role in principal.roles
        and namespace in principal.namespaces
        and principal.expires_at > now
        and 0 <= (now - principal.verified_at).total_seconds() <= 60
    )


def scope_failure(proposal: Action, context: PolicyContext, now: datetime) -> str | None:
    """Neither another incident nor a broad identity grant can bypass the sandbox allowlist."""
    incident, resource = context.incident, context.resource
    if not identity_valid(context.principal, "responder", proposal.namespace, now):
        return "IDENTITY_DENIED"
    if proposal.namespace != "payops-sandbox" or proposal.service not in SERVICES:
        return "SCOPE_DENIED"
    if (
        proposal.incident_id != incident.incident_id
        or proposal.namespace != incident.request.namespace
    ):
        return "INCIDENT_SCOPE_MISMATCH"
    if (proposal.namespace, proposal.service) != (resource.namespace, resource.service):
        return "RESOURCE_SCOPE_MISMATCH"
    if proposal.mode != resource.mode:
        return "OPERATION_MODE_MISMATCH"
    if not resource.synthetic or not 0 <= (now - resource.observed_at).total_seconds() <= 30:
        return "RESOURCE_NOT_CURRENT"
    if (proposal.resource_uid, proposal.expected_version) != (resource.uid, resource.version):
        return "STALE_PRECONDITION"
    expected_kind = "Traffic" if isinstance(proposal, PauseAction) else "Deployment"
    if resource.kind != expected_kind:
        return "RESOURCE_KIND_MISMATCH"
    if (
        isinstance(proposal, RollbackAction)
        and proposal.revision_sha256 not in resource.approved_revisions
    ):
        return "REVISION_NOT_APPROVED"
    return None


def operational_evidence(item: EvidenceItem, store: ArtifactStore) -> str | None:
    """Retrieved guidance cannot authorize effects, and derived numbers require valid lineage."""
    if item.source in {"RUNBOOK", "MEMORY"} or item.source == "TRACE":
        return "EVIDENCE_NOT_OPERATIONAL"
    if item.source == "PAYMENT":
        window = verify_payment_window(item, store)
        if window.status != "complete":
            return "EVIDENCE_NOT_OPERATIONAL"
    else:
        store.verify(item)
    return None


def evidence_failure(proposal: Action, context: PolicyContext, now: datetime) -> str | None:
    """Only recent verified incident evidence may justify a proposed operational change."""
    report = context.incident.report
    if report is None or report.terminal_state != "ESCALATED":
        return "INCIDENT_NOT_ACTIONABLE"
    if report.mode != context.resource.mode:
        return "EVIDENCE_MODE_MISMATCH"
    evidence = {item.evidence_id: item for item in report.evidence}
    try:
        for evidence_id in proposal.evidence_ids:
            item = evidence[evidence_id]
            if item.incident_id != proposal.incident_id:
                return "EVIDENCE_SCOPE_MISMATCH"
            if not 0 <= (now - item.observed_at).total_seconds() <= 300:
                return "EVIDENCE_STALE"
            if not 0 <= (now - item.collected_at).total_seconds() <= 300:
                return "EVIDENCE_STALE"
            failure = operational_evidence(item, context.store)
            if failure is not None:
                return failure
    except (KeyError, ValueError, OSError):
        return "EVIDENCE_INVALID"
    return None


def evaluate(value: dict[str, JsonValue], context: PolicyContext) -> PolicyReview:
    """Malformed or unauthorized proposals expose no executable object or action digest."""
    try:
        proposal = ACTION.validate_python(value)
    except ValueError:
        return PolicyReview(decision="DENY", reason="INVALID_PROPOSAL")
    now = utc_now()
    failure = scope_failure(proposal, context, now) or evidence_failure(proposal, context, now)
    if failure is not None:
        return PolicyReview(decision="DENY", reason=failure)
    risk = "R3" if isinstance(proposal, (ScaleAction, RollbackAction)) else "R2"
    return PolicyReview(
        decision="APPROVAL_REQUIRED",
        reason="REVIEW_REQUIRED",
        risk_tier=risk,
        proposal=proposal,
        action_digest=action_digest(proposal),
    )
