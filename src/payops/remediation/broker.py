"""Backend-authenticated approval and execution are separate from untrusted action proposals."""

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol

from pydantic import JsonValue

from payops.contracts import utc_now
from payops.evidence.artifacts import JSON_OBJECT
from payops.policy.contracts import ACTION, Action, OperationMode, Principal
from payops.policy.engine import PolicyContext, action_digest, evaluate, identity_valid
from payops.remediation.contracts import ActionRecord, Approval, EffectReceipt
from payops.remediation.store import ActionStore


class TrustedBackend(Protocol):
    """Configured implementations own identity refresh, resource reads and exact effects."""

    def principal(self, subject: str) -> Principal | None:
        """Refresh a known authenticated subject from the authoritative identity provider."""
        ...

    def context(self, action: Action, subject: str) -> PolicyContext:
        """Fetch incident, artifacts and current resource state independently of model data."""
        ...

    def execute(self, action: Action, idempotency_key: str) -> EffectReceipt:
        """Apply an atomic resource UID/version precondition and never blindly retry a write."""
        ...


class RemediationBroker:
    """Only authenticated API/worker code supplies subjects; this object is not a model tool."""

    def __init__(
        self,
        store: ActionStore,
        backend: TrustedBackend,
        *,
        mode: OperationMode,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """Bind executor mode and trusted dependencies at process startup, never from a proposal."""
        self.store, self.backend, self.mode, self.clock = store, backend, mode, clock

    def _identity(self, subject: str, role: str, namespace: str) -> Principal:
        """A refreshed backend identity must match the requested authenticated subject exactly."""
        principal = self.backend.principal(subject)
        if (
            not identity_valid(principal, role, namespace, self.clock())
            or principal is None
            or principal.subject != subject
        ):
            raise PermissionError("IDENTITY_DENIED")
        return principal

    def _review(self, action: Action, subject: str) -> datetime:
        """Re-evaluate current policy and artifact bytes at approval and again before dispatch."""
        principal = self._identity(subject, "responder", action.namespace)
        context = self.backend.context(action, subject)
        if action.mode != self.mode:
            raise PermissionError("BACKEND_CONTEXT_MISMATCH")
        trusted = PolicyContext(principal, context.incident, context.resource, context.store)
        review = evaluate(JSON_OBJECT.validate_json(action.model_dump_json()), trusted)
        if review.decision != "APPROVAL_REQUIRED":
            raise PermissionError(review.reason)
        assert context.incident.report is not None
        deadlines = [context.resource.observed_at + timedelta(seconds=30)]
        for item in context.incident.report.evidence:
            if item.evidence_id in action.evidence_ids:
                deadlines.extend(
                    (
                        item.observed_at + timedelta(seconds=300),
                        item.collected_at + timedelta(seconds=300),
                    )
                )
        return min(deadlines)

    def propose(self, value: dict[str, JsonValue], subject: str) -> ActionRecord:
        """Untrusted JSON becomes a persisted proposal only after deterministic backend review."""
        try:
            action = ACTION.validate_python(value)
        except ValueError:
            raise PermissionError("INVALID_PROPOSAL") from None
        self._review(action, subject)
        now = self.clock()
        return self.store.create(
            ActionRecord(
                action_id=action_digest(action),
                proposal=action,
                proposer=subject,
                created_at=now,
                updated_at=now,
            )
        )

    def approve(self, action_id: str, subject: str) -> ActionRecord:
        """A distinct current approver binds a five-minute approval to the immutable proposal."""
        record = self.store.get(action_id)
        self._identity(subject, "approver", record.proposal.namespace)
        if subject == record.proposer or record.state != "PROPOSED":
            raise PermissionError("APPROVAL_DENIED")
        policy_deadline = self._review(record.proposal, record.proposer)
        self._identity(subject, "approver", record.proposal.namespace)
        now = self.clock()
        if now > policy_deadline:
            raise PermissionError("POLICY_EXPIRED")
        approval = Approval(
            subject=subject,
            action_digest=record.action_id,
            approved_at=now,
            expires_at=now + timedelta(seconds=300),
        )
        return self.store.transition(
            record, state="APPROVED", actor=subject, reason="HUMAN_APPROVED", approval=approval
        )

    def execute(self, action_id: str, subject: str) -> ActionRecord:
        """Commit a single execution claim before any side effect; ambiguity never enables retry."""
        record = self.store.get(action_id)
        self._identity(subject, "executor", record.proposal.namespace)
        if record.proposal.mode != self.mode:
            raise PermissionError("OPERATION_MODE_MISMATCH")
        if record.state in {"EXECUTING", "SUCCEEDED", "FAILED", "UNKNOWN"}:
            return record
        approval = record.approval
        if (
            record.state != "APPROVED"
            or approval is None
            or not approval.approved_at <= self.clock() < approval.expires_at
        ):
            raise PermissionError("APPROVAL_REQUIRED")
        self._identity(approval.subject, "approver", record.proposal.namespace)
        policy_deadline = self._review(record.proposal, record.proposer)
        self._approval_current(approval)
        claimed = self.store.transition(record, state="EXECUTING", actor=subject, reason="DISPATCH")
        return self._dispatch(claimed, subject, policy_deadline)

    def _approval_current(self, approval: Approval) -> None:
        """Slow resource reads and database contention must not extend approval authority."""
        if not approval.approved_at <= self.clock() < approval.expires_at:
            raise PermissionError("APPROVAL_EXPIRED")

    def _dispatch_authority(
        self, record: ActionRecord, subject: str, policy_deadline: datetime
    ) -> None:
        """Refresh identities after context/database waits, then check all claims at one time."""
        assert record.approval is not None
        identities = (
            (self._identity(subject, "executor", record.proposal.namespace), "executor"),
            (
                self._identity(record.approval.subject, "approver", record.proposal.namespace),
                "approver",
            ),
            (self._identity(record.proposer, "responder", record.proposal.namespace), "responder"),
        )
        now = self.clock()
        if not all(
            identity_valid(principal, role, record.proposal.namespace, now)
            for principal, role in identities
        ):
            raise PermissionError("IDENTITY_DENIED")
        self._approval_current(record.approval)
        if self.clock() > policy_deadline:
            raise PermissionError("POLICY_EXPIRED")

    def _dispatch(
        self, record: ActionRecord, subject: str, policy_deadline: datetime
    ) -> ActionRecord:
        """Retain ambiguous claims without storing raw exception text or allowing retries."""
        assert record.approval is not None
        try:
            self._dispatch_authority(record, subject, policy_deadline)
        except PermissionError:
            result = EffectReceipt(
                outcome="FAILED",
                resource_uid=record.proposal.resource_uid,
                previous_version=record.proposal.expected_version,
            )
            return self.store.transition(
                record,
                state="FAILED",
                actor=subject,
                reason="DISPATCH_AUTHORITY_DENIED",
                result=result,
            )
        try:
            result = self.backend.execute(record.proposal, record.action_id)
            if (result.resource_uid, result.previous_version) != (
                record.proposal.resource_uid,
                record.proposal.expected_version,
            ):
                raise ValueError("executor receipt precondition mismatch")
        except Exception:
            return self.store.transition(
                record, state="UNKNOWN", actor=subject, reason="EXECUTOR_RESULT_UNKNOWN"
            )
        return self.store.transition(
            record, state=result.outcome, actor=subject, reason="EXECUTOR_RESULT", result=result
        )
