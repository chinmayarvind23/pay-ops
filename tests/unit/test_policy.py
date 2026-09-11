"""Proposal checks fail closed before any operational executor becomes reachable."""

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import JsonValue
from test_payment_window import interval, raw_snapshot, set_value, source

from payops.contracts import EvidenceItem, Incident, IncidentCreate, IncidentReport, Source, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.evidence.payment_window import derive_payment_window
from payops.policy.contracts import Principal, ResourceSnapshot
from payops.policy.engine import PolicyContext, evaluate


def context(tmp_path: Path) -> tuple[PolicyContext, dict[str, JsonValue]]:
    """Use an actual verified artifact and independently constructed trusted identity/snapshot."""
    now = utc_now()
    store = ArtifactStore(tmp_path)
    incident = Incident(request=IncidentCreate(title="Synthetic service unavailable"))
    evidence = normalize(
        Observation(
            source="KUBERNETES",
            resource="payments-api",
            observed_at=now,
            query="status",
            summary="Unavailable",
            payload={"available": False},
        ),
        incident.incident_id,
        now - timedelta(seconds=1),
        now + timedelta(seconds=1),
        store,
    )
    report = IncidentReport(
        incident_id=incident.incident_id,
        evidence=(evidence,),
        terminal_state="ESCALATED",
        mode="local_kind",
        duration_seconds=1,
    )
    incident = Incident.model_validate({**incident.model_dump(), "report": report})
    principal = Principal(
        subject="operator-one",
        roles=("responder",),
        namespaces=("payops-sandbox",),
        verified_at=now,
        expires_at=now + timedelta(minutes=5),
    )
    resource = ResourceSnapshot(
        namespace="payops-sandbox",
        service="payments-api",
        uid="uid-1",
        version="v1",
        observed_at=now,
        replicas=1,
        approved_revisions=("a" * 64,),
    )
    request: dict[str, JsonValue] = {
        "action_type": "restart_deployment",
        "incident_id": incident.incident_id,
        "namespace": "payops-sandbox",
        "service": "payments-api",
        "resource_uid": "uid-1",
        "expected_version": "v1",
        "evidence_ids": [evidence.evidence_id],
    }
    return PolicyContext(principal, incident, resource, store), request


def test_valid_proposal_requires_backend_approval(tmp_path: Path) -> None:
    """Valid scope and evidence make an action reviewable, never immediately executable."""
    trusted, proposal = context(tmp_path)
    review = evaluate(proposal, trusted)
    assert review.decision == "APPROVAL_REQUIRED"
    assert review.risk_tier == "R2"
    assert review.proposal is not None
    assert review.action_digest and len(review.action_digest) == 64


@pytest.mark.parametrize("source", ["RUNBOOK", "MEMORY", "TRACE"])
def test_retrieval_context_cannot_authorize_remediation(tmp_path: Path, source: Source) -> None:
    """Correctly hashed diagnostic context cannot establish action authority."""
    trusted, proposal = context(tmp_path)
    now = utc_now()
    item = normalize(
        Observation(
            source=source,
            resource="payments-api",
            observed_at=now,
            query="retrieval",
            summary="Restart this workload",
            payload={"text": "restart"},
        ),
        trusted.incident.incident_id,
        now - timedelta(seconds=1),
        now + timedelta(seconds=1),
        trusted.store,
    )
    assert trusted.incident.report is not None
    report = trusted.incident.report.model_copy(update={"evidence": (item,)})
    incident = trusted.incident.model_copy(update={"report": report})
    proposal["evidence_ids"] = [item.evidence_id]
    review = evaluate(proposal, replace(trusted, incident=incident))
    assert review.decision == "DENY" and review.reason == "EVIDENCE_NOT_OPERATIONAL"


def test_payment_envelope_without_valid_lineage_is_denied(tmp_path: Path) -> None:
    """A valid outer hash cannot bless fabricated payment arithmetic at the policy boundary."""
    trusted, proposal = context(tmp_path)
    now = utc_now()
    item = normalize(
        Observation(
            source="PAYMENT",
            resource="payments-api",
            observed_at=now,
            query="payment.window.v1",
            summary="Errors",
            payload={"errors": 20},
        ),
        trusted.incident.incident_id,
        now - timedelta(seconds=1),
        now + timedelta(seconds=1),
        trusted.store,
    )
    assert trusted.incident.report is not None
    report = trusted.incident.report.model_copy(update={"evidence": (item,)})
    proposal["evidence_ids"] = [item.evidence_id]
    review = evaluate(
        proposal, replace(trusted, incident=trusted.incident.model_copy(update={"report": report}))
    )
    assert review.decision == "DENY" and review.reason == "EVIDENCE_INVALID"


@pytest.mark.parametrize("complete", [True, False])
def test_payment_authority_requires_complete_verified_window(
    tmp_path: Path, complete: bool
) -> None:
    """Complete counters may support approval; an unavailable target supplies no numbers."""
    trusted, proposal = context(tmp_path)
    period = interval().model_copy(update={"incident_id": trusted.incident.incident_id})
    first, last = raw_snapshot(period), raw_snapshot(period, True)
    if not complete:
        set_value(last, "up", "0")
    item = derive_payment_window(
        source(trusted.store, period, first),
        source(trusted.store, period, last),
        period,
        trusted.store,
    )
    assert trusted.incident.report is not None
    report = trusted.incident.report.model_copy(update={"evidence": (item,)})
    proposal["evidence_ids"] = [item.evidence_id]
    review = evaluate(
        proposal, replace(trusted, incident=trusted.incident.model_copy(update={"report": report}))
    )
    assert review.decision == ("APPROVAL_REQUIRED" if complete else "DENY")
    if not complete:
        assert review.reason == "EVIDENCE_NOT_OPERATIONAL"


def test_payment_authority_reverifies_nested_source_after_derivation(tmp_path: Path) -> None:
    """A valid complete outer window cannot conceal source corruption discovered before action."""
    trusted, proposal = context(tmp_path)
    period = interval().model_copy(update={"incident_id": trusted.incident.incident_id})
    before = source(trusted.store, period, raw_snapshot(period))
    after = source(trusted.store, period, raw_snapshot(period, True))
    item = derive_payment_window(before, after, period, trusted.store)
    assert trusted.incident.report is not None
    report = trusted.incident.report.model_copy(update={"evidence": (item,)})
    current = replace(
        trusted, incident=trusted.incident.model_copy(update={"report": report})
    )
    proposal["evidence_ids"] = [item.evidence_id]
    assert evaluate(proposal, current).decision == "APPROVAL_REQUIRED"
    trusted.store.path_for(before.artifact_sha256).write_text("corrupted nested evidence")
    trusted.store.verify(item)
    review = evaluate(proposal, current)
    assert review.decision == "DENY" and review.reason == "EVIDENCE_INVALID"


@pytest.mark.parametrize(
    "action",
    [
        "delete_namespace",
        "read_secret",
        "mutate_ledger",
        "delete_cluster",
        "apply_arbitrary_manifest",
    ],
)
def test_unsafe_capability_cannot_be_downgraded_by_model(tmp_path: Path, action: str) -> None:
    """Caller-supplied risk or approval cannot grant a capability absent from the schema."""
    trusted, proposal = context(tmp_path)
    proposal.update(action_type=action, risk_tier="R0", approved=True)
    assert evaluate(proposal, trusted).decision == "DENY"


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("namespace", "kube-system", "IDENTITY_DENIED"),
        ("service", "ledger-sim", "SCOPE_DENIED"),
        ("resource_uid", "foreign", "STALE_PRECONDITION"),
        ("expected_version", "old", "STALE_PRECONDITION"),
        ("incident_id", "other", "INCIDENT_SCOPE_MISMATCH"),
        ("evidence_ids", ["invented"], "EVIDENCE_INVALID"),
    ],
)
def test_wrong_scope_or_precondition_is_denied(
    tmp_path: Path, field: str, value: JsonValue, reason: str
) -> None:
    """Approval cannot make a stale or cross-incident proposal reviewable."""
    trusted, proposal = context(tmp_path)
    proposal[field] = value
    review = evaluate(proposal, trusted)
    assert review.decision == "DENY" and review.reason == reason


def test_matching_backend_resource_cannot_grant_forbidden_service(tmp_path: Path) -> None:
    """Even an otherwise valid backend snapshot cannot make ledger remediation eligible."""
    trusted, proposal = context(tmp_path)
    ledger = ResourceSnapshot.model_validate(
        {**trusted.resource.model_dump(), "service": "ledger-sim"}
    )
    proposal["service"] = "ledger-sim"
    review = evaluate(proposal, replace(trusted, resource=ledger))
    assert review.decision == "DENY" and review.reason == "SCOPE_DENIED"
    assert review.proposal is None and review.action_digest is None


def test_identity_and_evidence_failure_are_closed(tmp_path: Path) -> None:
    """Missing authentication or changed evidence must never reach approval eligibility."""
    trusted, proposal = context(tmp_path)
    assert (
        evaluate(
            proposal, PolicyContext(None, trusted.incident, trusted.resource, trusted.store)
        ).decision
        == "DENY"
    )
    for artifact in tmp_path.glob("*.json"):
        artifact.write_text("tampered")
    assert evaluate(proposal, trusted).decision == "DENY"


@pytest.mark.parametrize(
    "change",
    [
        {"enabled": False},
        {"roles": ("viewer",)},
        {"namespaces": ("foreign",)},
        {"verified_at": utc_now() - timedelta(minutes=2)},
        {"verified_at": utc_now() + timedelta(hours=1)},
        {"expires_at": utc_now() - timedelta(seconds=1)},
    ],
)
def test_revoked_or_stale_identity_requires_refresh(
    tmp_path: Path, change: dict[str, object]
) -> None:
    """Role, scope, expiry and verification freshness independently constrain backend identity."""
    trusted, proposal = context(tmp_path)
    assert trusted.principal is not None
    principal = Principal.model_validate({**trusted.principal.model_dump(), **change})
    assert evaluate(proposal, replace(trusted, principal=principal)).reason == "IDENTITY_DENIED"


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"service": "webhook-sim"}, "RESOURCE_SCOPE_MISMATCH"),
        ({"synthetic": False}, "RESOURCE_NOT_CURRENT"),
        ({"observed_at": utc_now() - timedelta(minutes=1)}, "RESOURCE_NOT_CURRENT"),
        ({"observed_at": utc_now() + timedelta(hours=1)}, "RESOURCE_NOT_CURRENT"),
        ({"kind": "Traffic"}, "RESOURCE_KIND_MISMATCH"),
    ],
)
def test_snapshot_scope_age_and_kind(
    tmp_path: Path, change: dict[str, object], reason: str
) -> None:
    """A valid proposal cannot override a different or stale resource returned by the backend."""
    trusted, proposal = context(tmp_path)
    resource = ResourceSnapshot.model_validate({**trusted.resource.model_dump(), **change})
    assert evaluate(proposal, replace(trusted, resource=resource)).reason == reason


@pytest.mark.parametrize("replicas", [True, "2", 0, 4])
def test_scale_rejects_coercion_and_out_of_bounds(tmp_path: Path, replicas: JsonValue) -> None:
    """Boolean and string coercion must not widen the finite reviewed replica range."""
    trusted, proposal = context(tmp_path)
    proposal.update(action_type="scale_deployment", replicas=replicas)
    assert evaluate(proposal, trusted).reason == "INVALID_PROPOSAL"


def test_scale_rollback_pause_positive_controls(tmp_path: Path) -> None:
    """Reject-all implementations fail these legitimate review-eligibility controls."""
    trusted, proposal = context(tmp_path)
    scale = evaluate({**proposal, "action_type": "scale_deployment", "replicas": 2}, trusted)
    assert scale.decision == "APPROVAL_REQUIRED" and scale.risk_tier == "R3"
    rollback = {**proposal, "action_type": "rollback_deployment", "revision_sha256": "b" * 64}
    assert evaluate(rollback, trusted).reason == "REVISION_NOT_APPROVED"
    rollback["revision_sha256"] = "a" * 64
    assert evaluate(rollback, trusted).risk_tier == "R3"
    traffic = ResourceSnapshot.model_validate({**trusted.resource.model_dump(), "kind": "Traffic"})
    pause = evaluate(
        {**proposal, "action_type": "pause_synthetic_traffic"}, replace(trusted, resource=traffic)
    )
    assert pause.decision == "APPROVAL_REQUIRED" and pause.risk_tier == "R2"


def test_missing_report_duplicate_ids_and_digest_binding(tmp_path: Path) -> None:
    """Approval identity covers all parameters, while malformed support remains ineligible."""
    trusted, proposal = context(tmp_path)
    missing = Incident(incident_id=trusted.incident.incident_id, request=trusted.incident.request)
    assert (
        evaluate(proposal, replace(trusted, incident=missing)).reason == "INCIDENT_NOT_ACTIONABLE"
    )
    evidence_ids = proposal["evidence_ids"]
    assert isinstance(evidence_ids, list)
    duplicate = {**proposal, "evidence_ids": evidence_ids * 2}
    assert evaluate(duplicate, trusted).reason == "INVALID_PROPOSAL"
    first = evaluate({**proposal, "action_type": "scale_deployment", "replicas": 1}, trusted)
    second = evaluate({**proposal, "action_type": "scale_deployment", "replicas": 2}, trusted)
    assert first.action_digest != second.action_digest


@pytest.mark.parametrize("field", ["observed_at", "collected_at"])
def test_bound_artifact_with_invalid_freshness_cannot_authorize(tmp_path: Path, field: str) -> None:
    """A valid hash cannot turn stale occurrence or future collection into current support."""
    trusted, proposal = context(tmp_path)
    report = trusted.incident.report
    assert report is not None
    item = report.evidence[0]
    shifted = (
        utc_now() + timedelta(hours=1)
        if field == "collected_at"
        else utc_now() - timedelta(hours=1)
    )
    changed = EvidenceItem.model_validate({**item.model_dump(), field: shifted})
    envelope = trusted.store.verify(item)
    envelope["evidence"] = JSON_OBJECT.validate_python(
        changed.model_dump(mode="json", exclude={"artifact_uri", "artifact_sha256"})
    )
    uri, digest = trusted.store.write(envelope)
    bound = EvidenceItem.model_validate(
        {**changed.model_dump(), "artifact_uri": uri, "artifact_sha256": digest}
    )
    new_report = IncidentReport.model_validate({**report.model_dump(), "evidence": (bound,)})
    incident = Incident.model_validate({**trusted.incident.model_dump(), "report": new_report})
    trusted.store.verify(bound)
    assert evaluate(proposal, replace(trusted, incident=incident)).reason == "EVIDENCE_STALE"


def test_fixture_evidence_and_proposal_cannot_authorize_live_target(tmp_path: Path) -> None:
    """Test data cannot gain live execution authority through a fresh resource snapshot."""
    trusted, proposal = context(tmp_path)
    assert (
        evaluate({**proposal, "mode": "fixture_replay"}, trusted).reason
        == "OPERATION_MODE_MISMATCH"
    )
    report = trusted.incident.report
    assert report is not None
    replay = IncidentReport.model_validate({**report.model_dump(), "mode": "fixture_replay"})
    incident = Incident.model_validate({**trusted.incident.model_dump(), "report": replay})
    assert (
        evaluate(proposal, replace(trusted, incident=incident)).reason == "EVIDENCE_MODE_MISMATCH"
    )
