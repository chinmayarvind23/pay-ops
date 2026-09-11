"""Immutable records bind approvals, execution outcomes and audit transitions."""

from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from payops.contracts import Contract, Identifier
from payops.policy.contracts import Action, Digest
from payops.policy.engine import action_digest

ActionState = Literal["PROPOSED", "APPROVED", "EXECUTING", "SUCCEEDED", "FAILED", "UNKNOWN"]


class Approval(Contract):
    """Only a backend-authenticated second person may approve this exact action digest."""

    subject: Identifier
    action_digest: Digest
    approved_at: AwareDatetime
    expires_at: AwareDatetime
    policy_version: Literal["payops-policy-v1"] = "payops-policy-v1"


class EffectReceipt(Contract):
    """Executor output reports precondition identity without arbitrary commands or backend text."""

    outcome: Literal["SUCCEEDED", "FAILED"]
    resource_uid: Identifier
    previous_version: Identifier
    resulting_version: Identifier | None = None


class ActionRecord(Contract):
    """Every database read revalidates digest, state and two-person approval invariants."""

    action_id: Digest
    proposal: Action
    proposer: Identifier
    state: ActionState = "PROPOSED"
    revision: int = Field(default=0, ge=0, strict=True)
    created_at: AwareDatetime
    updated_at: AwareDatetime
    approval: Approval | None = None
    result: EffectReceipt | None = None

    @model_validator(mode="after")
    def consistent(self) -> Self:
        """Corrupt persisted JSON cannot manufacture approval or relabel a completed effect."""
        if self.action_id != action_digest(self.proposal):
            raise ValueError("action digest mismatch")
        if (self.state == "PROPOSED") != (self.approval is None):
            raise ValueError("approval/state mismatch")
        if self.approval is not None:
            self._validate_approval(self.approval)
        if self.state in {"SUCCEEDED", "FAILED"}:
            if self.result is None or self.result.outcome != self.state:
                raise ValueError("result/state mismatch")
            if (self.result.resource_uid, self.result.previous_version) != (
                self.proposal.resource_uid,
                self.proposal.expected_version,
            ):
                raise ValueError("result precondition mismatch")
        elif self.result is not None:
            raise ValueError("unexpected effect result")
        return self

    def _validate_approval(self, approval: Approval) -> None:
        """A bounded approval binds the proposer-independent actor and canonical action."""
        if approval.subject == self.proposer or approval.action_digest != self.action_id:
            raise ValueError("approval identity/digest mismatch")
        lifetime = (approval.expires_at - approval.approved_at).total_seconds()
        if not 0 < lifetime <= 300:
            raise ValueError("approval lifetime outside bounds")


class AuditEvent(Contract):
    """A transaction records each successful state transition without raw exception contents."""

    action_id: Digest
    revision: int = Field(ge=0, strict=True)
    state: ActionState
    actor: Identifier
    reason: Identifier
    recorded_at: AwareDatetime
