"""Durable approvals must survive restarts without becoming reusable execution authority."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier

import pytest
from pydantic import JsonValue
from sqlalchemy import update
from sqlalchemy.orm import Session

from payops.contracts import Incident, IncidentCreate, IncidentReport, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.policy.contracts import Action, Principal, ResourceSnapshot, Role
from payops.policy.engine import PolicyContext
from payops.remediation.broker import RemediationBroker
from payops.remediation.contracts import ActionRecord, ActionState, Approval, EffectReceipt
from payops.remediation.store import ActionRow, ActionStore, AuditRow, TransitionConflict


class Backend:
    """Mutable trusted dependencies let tests revoke identity or change resources after approval."""

    def __init__(self, root: Path) -> None:
        """Construct real evidence, three distinct principals, and a versioned synthetic target."""
        now = utc_now()
        self.store = ArtifactStore(root / "artifacts")
        incident = Incident(request=IncidentCreate(title="Synthetic service unavailable"))
        item = normalize(
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
            self.store,
        )
        report = IncidentReport(
            incident_id=incident.incident_id,
            evidence=(item,),
            terminal_state="ESCALATED",
            mode="local_kind",
            duration_seconds=1,
        )
        self.incident = Incident.model_validate({**incident.model_dump(), "report": report})
        self.resource = ResourceSnapshot(
            namespace="payops-sandbox",
            service="payments-api",
            uid="uid-1",
            version="v1",
            observed_at=now,
        )
        roles: tuple[tuple[str, Role], ...] = (
            ("alice", "responder"),
            ("bob", "approver"),
            ("worker", "executor"),
        )
        self.identities = {
            subject: Principal(
                subject=subject,
                roles=(role,),
                namespaces=("payops-sandbox",),
                verified_at=now,
                expires_at=now + timedelta(minutes=10),
            )
            for subject, role in roles
        }
        self.proposal: dict[str, JsonValue] = {
            "action_type": "restart_deployment",
            "incident_id": incident.incident_id,
            "namespace": "payops-sandbox",
            "service": "payments-api",
            "resource_uid": "uid-1",
            "expected_version": "v1",
            "evidence_ids": [item.evidence_id],
        }
        self.effects: list[str] = []
        self.fail = False

    def principal(self, subject: str) -> Principal | None:
        """Lookup simulates independently refreshed authentication and role assignments."""
        return self.identities.get(subject)

    def context(self, action: Action, subject: str) -> PolicyContext:
        """Backend incident/resource reads are independent of untrusted proposal contents."""
        return PolicyContext(self.principal(subject), self.incident, self.resource, self.store)

    def execute(self, action: Action, idempotency_key: str) -> EffectReceipt:
        """The fixture records an effect before a possible ambiguous transport failure."""
        self.effects.append(idempotency_key)
        if self.fail:
            raise TimeoutError("sensitive backend detail must not enter audit")
        return EffectReceipt(
            outcome="SUCCEEDED",
            resource_uid=action.resource_uid,
            previous_version=action.expected_version,
            resulting_version="v2",
        )


def setup(root: Path) -> tuple[Backend, ActionStore, RemediationBroker]:
    """Use a real file database so separate broker instances share atomic claims."""
    backend = Backend(root)
    store = ActionStore(f"sqlite:///{root / 'actions.db'}")
    return backend, store, RemediationBroker(store, backend, mode="local_kind")


def test_approval_persists_and_executes_once(tmp_path: Path) -> None:
    """A reconstructed broker can execute an approved action but never repeat the effect."""
    backend, store, broker = setup(tmp_path)
    proposed = broker.propose(backend.proposal, "alice")
    assert proposed.state == "PROPOSED"
    assert broker.propose(backend.proposal, "alice") == proposed
    approved = broker.approve(proposed.action_id, "bob")
    assert approved.state == "APPROVED" and not backend.effects
    store.close()
    reopened = ActionStore(f"sqlite:///{tmp_path / 'actions.db'}")
    broker = RemediationBroker(reopened, backend, mode="local_kind")
    result = broker.execute(proposed.action_id, "worker")
    assert result.state == "SUCCEEDED" and len(backend.effects) == 1
    assert broker.execute(proposed.action_id, "worker") == result
    assert [event.state for event in reopened.audit(proposed.action_id)] == [
        "PROPOSED",
        "APPROVED",
        "EXECUTING",
        "SUCCEEDED",
    ]
    reopened.close()


def test_self_approval_and_missing_approval_cannot_execute(tmp_path: Path) -> None:
    """Even a principal with both roles cannot approve their own proposal."""
    backend, store, broker = setup(tmp_path)
    backend.identities["alice"] = backend.identities["alice"].model_copy(
        update={"roles": ("responder", "approver")}
    )
    action = broker.propose(backend.proposal, "alice")
    with pytest.raises(PermissionError):
        broker.approve(action.action_id, "alice")
    with pytest.raises(PermissionError):
        broker.execute(action.action_id, "worker")
    assert not backend.effects
    assert store.get(action.action_id).state == "PROPOSED"
    store.close()


@pytest.mark.parametrize("revoked", ["alice", "bob", "worker"])
def test_execution_rechecks_each_identity(tmp_path: Path, revoked: str) -> None:
    """Revocation after approval blocks dispatch, including the worker's own authority."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    del backend.identities[revoked]
    with pytest.raises(PermissionError):
        broker.execute(action.action_id, "worker")
    assert not backend.effects
    store.close()


@pytest.mark.parametrize("change", ["version", "uid", "mode", "artifact"])
def test_execution_rechecks_evidence_and_target(tmp_path: Path, change: str) -> None:
    """An old approval cannot authorize a replaced target, altered evidence, or another mode."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    if change == "artifact":
        for path in (tmp_path / "artifacts").rglob("*.json"):
            path.write_text("{}")
    else:
        value = "fixture_replay" if change == "mode" else "changed"
        backend.resource = backend.resource.model_copy(update={change: value})
    with pytest.raises(PermissionError):
        broker.execute(action.action_id, "worker")
    assert not backend.effects
    store.close()


def test_expired_approval_cannot_execute(tmp_path: Path) -> None:
    """Approval lifetime is backend time, independent of model parameters or client timestamps."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    approved = broker.approve(action.action_id, "bob")
    assert approved.approval is not None
    expired = approved.approval.expires_at + timedelta(seconds=1)
    broker.clock = lambda: expired
    backend.identities["worker"] = backend.identities["worker"].model_copy(
        update={"verified_at": expired}
    )
    with pytest.raises(PermissionError, match="APPROVAL_REQUIRED"):
        broker.execute(action.action_id, "worker")
    assert not backend.effects
    store.close()


@pytest.mark.parametrize("case", ["invalid", "forbidden", "mode", "foreign_identity"])
def test_proposal_boundary_denies_untrusted_inputs(tmp_path: Path, case: str) -> None:
    """Reaching the broker does not let model data acquire identity or executor configuration."""
    backend, store, broker = setup(tmp_path)
    if case == "invalid":
        backend.proposal["approved"] = True
    elif case == "forbidden":
        backend.proposal["service"] = "ledger-sim"
    elif case == "mode":
        backend.proposal["mode"] = "fixture_replay"
    else:
        backend.identities["alice"] = backend.identities["alice"].model_copy(
            update={"subject": "mallory"}
        )
    with pytest.raises(PermissionError):
        broker.propose(backend.proposal, "alice")
    assert not backend.effects
    store.close()


def test_mode_is_checked_for_previously_approved_records(tmp_path: Path) -> None:
    """A process configured for replay cannot consume an existing live approval."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    replay = RemediationBroker(store, backend, mode="fixture_replay")
    with pytest.raises(PermissionError, match="OPERATION_MODE_MISMATCH"):
        replay.execute(action.action_id, "worker")
    assert not backend.effects
    store.close()


def test_same_digest_cannot_change_proposer(tmp_path: Path) -> None:
    """A second responder cannot take ownership by resubmitting an identical proposal."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    backend.identities["mallory"] = backend.identities["alice"].model_copy(
        update={"subject": "mallory"}
    )
    with pytest.raises(PermissionError, match="another identity"):
        broker.propose(backend.proposal, "mallory")
    assert store.get(action.action_id).proposer == "alice"
    store.close()


def test_store_rejects_missing_or_invalid_transitions(tmp_path: Path) -> None:
    """Missing state and backward transitions cannot reset an execution claim."""
    backend, store, broker = setup(tmp_path)
    with pytest.raises(KeyError):
        store.get("unknown")
    action = broker.propose(backend.proposal, "alice")
    approved = broker.approve(action.action_id, "bob")
    with pytest.raises(ValueError, match="new action"):
        store.create(approved)
    with pytest.raises(TransitionConflict):
        store.transition(approved, state="PROPOSED", actor="alice", reason="RETRY")
    store.close()


@pytest.mark.parametrize(
    "corruption",
    [
        "digest",
        "approval_missing",
        "self_approved",
        "approval_digest",
        "approval_lifetime",
        "result_missing",
        "result_unexpected",
        "result_wrong_target",
    ],
)
def test_persisted_invariants_reject_corruption(tmp_path: Path, corruption: str) -> None:
    """Database deserialization validates approvals and results instead of trusting stored JSON."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    approved = broker.approve(action.action_id, "bob")
    assert approved.approval is not None
    payload = approved.model_dump()
    approval = approved.approval.model_dump()
    if corruption == "digest":
        payload["action_id"] = "0" * 64
    elif corruption == "approval_missing":
        payload["approval"] = None
    elif corruption == "self_approved":
        payload["approval"] = {**approval, "subject": "alice"}
    elif corruption == "approval_digest":
        payload["approval"] = {**approval, "action_digest": "0" * 64}
    elif corruption == "approval_lifetime":
        payload["approval"] = {
            **approval,
            "expires_at": approved.approval.approved_at + timedelta(seconds=301),
        }
    elif corruption == "result_missing":
        payload["state"] = "SUCCEEDED"
    else:
        payload["result"] = EffectReceipt(
            outcome="SUCCEEDED", resource_uid="other", previous_version="v1"
        )
        if corruption == "result_wrong_target":
            payload["state"] = "SUCCEEDED"
    with pytest.raises(ValueError):
        ActionRecord.model_validate(payload)
    store.close()


def test_receipt_mismatch_stays_unknown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A response naming a different target is not successful verification of this action."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")

    def mismatched(action: Action, idempotency_key: str) -> EffectReceipt:
        """Simulate a confused executor returning another resource's success receipt."""
        backend.effects.append(idempotency_key)
        return EffectReceipt(outcome="SUCCEEDED", resource_uid="other", previous_version="v1")

    monkeypatch.setattr(backend, "execute", mismatched)
    assert broker.execute(action.action_id, "worker").state == "UNKNOWN"
    assert len(backend.effects) == 1
    store.close()


def test_crash_after_claim_never_dispatches_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interrupted process leaves EXECUTING durable, requiring operator reconciliation."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")

    def interrupted(action: Action, idempotency_key: str) -> EffectReceipt:
        """Record an effect before simulating process interruption rather than ordinary failure."""
        backend.effects.append(idempotency_key)
        raise KeyboardInterrupt

    monkeypatch.setattr(backend, "execute", interrupted)
    with pytest.raises(KeyboardInterrupt):
        broker.execute(action.action_id, "worker")
    assert store.get(action.action_id).state == "EXECUTING"
    assert broker.execute(action.action_id, "worker").state == "EXECUTING"
    assert len(backend.effects) == 1
    store.close()


def test_approval_expiring_during_resource_read_blocks_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A slow trusted resource refresh cannot stretch a previously valid approval lifetime."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    approved = broker.approve(action.action_id, "bob")
    assert approved.approval is not None
    expiry = approved.approval.expires_at
    original = backend.context

    def slow_context(action: Action, subject: str) -> PolicyContext:
        """Advance only the broker clock after its initial identity and approval checks."""
        broker.clock = lambda: expiry
        return original(action, subject)

    monkeypatch.setattr(backend, "context", slow_context)
    with pytest.raises(PermissionError, match="APPROVAL_EXPIRED"):
        broker.execute(action.action_id, "worker")
    assert store.get(action.action_id).state == "APPROVED"
    assert not backend.effects
    store.close()


def test_claim_contention_cannot_extend_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the database claim consumes remaining authority, dispatch is recorded as failed."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    approved = broker.approve(action.action_id, "bob")
    assert approved.approval is not None
    expiry = approved.approval.expires_at
    original = store.transition

    def delayed(
        before: ActionRecord,
        *,
        state: ActionState,
        actor: str,
        reason: str,
        approval: Approval | None = None,
        result: EffectReceipt | None = None,
    ) -> ActionRecord:
        """Simulate a database wait ending after the approval expires."""
        changed = original(
            before, state=state, actor=actor, reason=reason, approval=approval, result=result
        )
        if state == "EXECUTING":
            broker.clock = lambda: expiry
        return changed

    monkeypatch.setattr(store, "transition", delayed)
    assert broker.execute(action.action_id, "worker").state == "FAILED"
    assert store.audit(action.action_id)[-1].reason == "DISPATCH_AUTHORITY_DENIED"
    assert not backend.effects
    store.close()


@pytest.mark.parametrize("table", ["action", "audit_identity", "audit_revision"])
def test_valid_payload_swapped_between_rows_is_rejected(tmp_path: Path, table: str) -> None:
    """Valid internal checksums do not excuse a payload stored under a different row key."""
    backend, store, broker = setup(tmp_path)
    first = broker.propose(backend.proposal, "alice")
    backend.proposal.update(action_type="scale_deployment", replicas=2)
    second = broker.propose(backend.proposal, "alice")
    with Session(store.engine) as session:
        if table == "action":
            session.execute(
                update(ActionRow)
                .where(ActionRow.action_id == first.action_id)
                .values(payload=second.model_dump_json())
            )
        else:
            event = store.audit(second.action_id)[0]
            if table == "audit_revision":
                event = store.audit(first.action_id)[0].model_copy(update={"revision": 1})
            session.execute(
                update(AuditRow)
                .where(AuditRow.action_id == first.action_id)
                .values(payload=event.model_dump_json())
            )
        session.commit()
    with pytest.raises(ValueError, match="row identity mismatch"):
        store.get(first.action_id) if table == "action" else store.audit(first.action_id)
    store.close()


def test_audit_failure_rolls_back_claim_before_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A claim without its audit event cannot commit or reach the executor."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")

    def fail_audit(session: Session, record: ActionRecord, actor: str, reason: str) -> None:
        """Simulate failing audit persistence in the same transaction as the claim."""
        raise OSError("audit store unavailable")

    monkeypatch.setattr(store, "_audit", fail_audit)
    with pytest.raises(OSError):
        broker.execute(action.action_id, "worker")
    assert store.get(action.action_id).state == "APPROVED"
    assert not backend.effects
    store.close()


def test_broker_race_dispatches_one_effect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two fully authorized workers with simultaneous claims still produce one backend effect."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    barrier = Barrier(2)
    original = store.transition

    def synchronized(
        before: ActionRecord,
        *,
        state: ActionState,
        actor: str,
        reason: str,
        approval: Approval | None = None,
        result: EffectReceipt | None = None,
    ) -> ActionRecord:
        """Place both authorized workers immediately before their contended SQL claim."""
        if state == "EXECUTING":
            barrier.wait(timeout=5)
        return original(
            before, state=state, actor=actor, reason=reason, approval=approval, result=result
        )

    def execute() -> str:
        """The loser reports conflict and cannot invoke the executor."""
        try:
            return broker.execute(action.action_id, "worker").state
        except TransitionConflict:
            return "CONFLICT"

    monkeypatch.setattr(store, "transition", synchronized)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(execute) for _ in range(2)]
        assert sorted(future.result(timeout=10) for future in futures) == ["CONFLICT", "SUCCEEDED"]
    assert len(backend.effects) == 1
    store.close()


@pytest.mark.parametrize("revoked", ["alice", "bob", "worker"])
def test_identity_revoked_during_context_refresh_blocks_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, revoked: str
) -> None:
    """A resource lookup may take long enough for earlier identity authority to be revoked."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    original = backend.context

    def revoking(action: Action, subject: str) -> PolicyContext:
        """Invalidate an earlier identity lookup before returning current resource information."""
        result = original(action, subject)
        del backend.identities[revoked]
        return result

    monkeypatch.setattr(backend, "context", revoking)
    assert broker.execute(action.action_id, "worker").state == "FAILED"
    assert not backend.effects
    store.close()


def test_approval_refreshes_approver_after_resource_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The initial approver lookup cannot survive revocation during proposal revalidation."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    original = backend.context

    def revoking(action: Action, subject: str) -> PolicyContext:
        """Return a valid proposal context while revoking the separate approval identity."""
        del backend.identities["bob"]
        return original(action, subject)

    monkeypatch.setattr(backend, "context", revoking)
    with pytest.raises(PermissionError, match="IDENTITY_DENIED"):
        broker.approve(action.action_id, "bob")
    assert store.get(action.action_id).state == "PROPOSED"
    store.close()


def test_final_identity_lookups_have_a_shared_freshness_cutoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refreshing later identities must not silently age the first beyond its authority window."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    original = backend.principal

    def slow_final(subject: str) -> Principal | None:
        """Advance the clock at the last lookup after the execution claim has committed."""
        if subject == "alice" and store.get(action.action_id).state == "EXECUTING":
            future = utc_now() + timedelta(seconds=61)
            broker.clock = lambda: future
            backend.identities["alice"] = backend.identities["alice"].model_copy(
                update={"verified_at": future}
            )
        return original(subject)

    monkeypatch.setattr(backend, "principal", slow_final)
    assert broker.execute(action.action_id, "worker").state == "FAILED"
    assert not backend.effects
    store.close()


@pytest.mark.parametrize("expiry_source", ["resource", "evidence"])
def test_policy_freshness_survives_database_and_identity_waits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expiry_source: str
) -> None:
    """Fresh approval and identities cannot extend the validity of an old policy snapshot."""
    backend, store, broker = setup(tmp_path)
    delay = 31
    if expiry_source == "evidence":
        now = utc_now()
        item = normalize(
            Observation(
                source="KUBERNETES",
                resource="payments-api",
                observed_at=now - timedelta(seconds=295),
                query="status",
                summary="Unavailable",
                payload={"available": False},
            ),
            backend.incident.incident_id,
            now - timedelta(seconds=300),
            now + timedelta(seconds=1),
            backend.store,
        )
        assert backend.incident.report is not None
        report = backend.incident.report.model_copy(update={"evidence": (item,)})
        backend.incident = backend.incident.model_copy(update={"report": report})
        backend.proposal["evidence_ids"] = [item.evidence_id]
        delay = 6
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    original = backend.principal

    def slow_final(subject: str) -> Principal | None:
        """Age the policy inputs while keeping all identity snapshots within sixty seconds."""
        if store.get(action.action_id).state == "EXECUTING":
            future = utc_now() + timedelta(seconds=delay)
            broker.clock = lambda: future
        return original(subject)

    monkeypatch.setattr(backend, "principal", slow_final)
    assert broker.execute(action.action_id, "worker").state == "FAILED"
    assert not backend.effects
    store.close()


def test_approval_refuses_expired_policy_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A slow second approver refresh cannot approve a policy view that is already stale."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    original = backend.principal
    calls = 0

    def slow_approver(subject: str) -> Principal | None:
        """Advance time only on the final approver refresh, leaving identity within its cutoff."""
        nonlocal calls
        if subject == "bob":
            calls += 1
            if calls == 2:
                future = utc_now() + timedelta(seconds=31)
                broker.clock = lambda: future
        return original(subject)

    monkeypatch.setattr(backend, "principal", slow_approver)
    with pytest.raises(PermissionError, match="POLICY_EXPIRED"):
        broker.approve(action.action_id, "bob")
    assert store.get(action.action_id).state == "PROPOSED"
    store.close()


def test_ambiguous_effect_is_never_retried(tmp_path: Path) -> None:
    """A lost response after dispatch stays UNKNOWN; another request cannot duplicate the effect."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    broker.approve(action.action_id, "bob")
    backend.fail = True
    result = broker.execute(action.action_id, "worker")
    assert result.state == "UNKNOWN"
    assert broker.execute(action.action_id, "worker") == result
    assert len(backend.effects) == 1
    assert "sensitive" not in str(store.audit(action.action_id))
    store.close()


def test_concurrent_claim_has_one_winner(tmp_path: Path) -> None:
    """Two independent connections attempting the same CAS cannot both dispatch."""
    backend, store, broker = setup(tmp_path)
    action = broker.propose(backend.proposal, "alice")
    approved = broker.approve(action.action_id, "bob")
    gate = Barrier(2)

    def claim() -> bool:
        """Synchronize stale reads and then contend on the SQL update predicate."""
        gate.wait(timeout=5)
        try:
            store.transition(approved, state="EXECUTING", actor="worker", reason="DISPATCH")
            return True
        except TransitionConflict:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [pool.submit(claim) for _ in range(2)]
        assert sorted(result.result(timeout=10) for result in results) == [False, True]
    assert [event.state for event in store.audit(action.action_id)].count("EXECUTING") == 1
    store.close()
