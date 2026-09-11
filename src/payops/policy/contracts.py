"""Closed action schemas separate untrusted proposals from backend identity and state."""

from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, StringConstraints, TypeAdapter, model_validator

from payops.contracts import Contract, Identifier

Digest = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
Role = Literal["viewer", "responder", "approver", "executor"]
OperationMode = Literal["local_kind", "cloud_gke", "fixture_replay"]


class Principal(Contract):
    """Only verified backend authentication constructs these claims, never model arguments."""

    subject: Identifier
    roles: tuple[Role, ...] = Field(min_length=1, max_length=4)
    namespaces: tuple[Identifier, ...] = Field(min_length=1, max_length=8)
    verified_at: AwareDatetime
    expires_at: AwareDatetime
    enabled: bool = Field(default=True, strict=True)


class ResourceSnapshot(Contract):
    """Trusted current resource identity and revision constrain every proposed change."""

    namespace: Identifier
    service: Identifier
    uid: Identifier
    version: Identifier
    observed_at: AwareDatetime
    kind: Literal["Deployment", "Traffic"] = "Deployment"
    synthetic: bool = Field(default=True, strict=True)
    replicas: int = Field(default=1, ge=0, le=100, strict=True)
    approved_revisions: tuple[Digest, ...] = ()
    mode: OperationMode = "local_kind"


class ActionBase(Contract):
    """Exact preconditions and evidence references are part of the approval's immutable digest."""

    incident_id: Identifier
    namespace: Identifier
    service: Identifier
    resource_uid: Identifier
    expected_version: Identifier
    evidence_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=8)
    mode: OperationMode = "local_kind"

    @model_validator(mode="after")
    def distinct_evidence(self) -> Self:
        """Repeated citations must not masquerade as additional remediation support."""
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("duplicate action evidence")
        return self


class RestartAction(ActionBase):
    """A rollout restart has no shell, manifest, environment or optional arbitrary parameters."""

    action_type: Literal["restart_deployment"]


class ScaleAction(ActionBase):
    """Only a bounded integer replica target is expressible."""

    action_type: Literal["scale_deployment"]
    replicas: int = Field(ge=1, le=3, strict=True)


class RollbackAction(ActionBase):
    """The operator inventory must separately approve the requested immutable image revision."""

    action_type: Literal["rollback_deployment"]
    revision_sha256: Digest


class PauseAction(ActionBase):
    """Pause applies only to an identified synthetic traffic run, never payment settlement."""

    action_type: Literal["pause_synthetic_traffic"]


Action = Annotated[
    RestartAction | ScaleAction | RollbackAction | PauseAction, Field(discriminator="action_type")
]
ACTION = TypeAdapter[Action](Action)


class PolicyReview(Contract):
    """A review reports backend-computed risk; approval eligibility is not execution authority."""

    decision: Literal["DENY", "APPROVAL_REQUIRED"]
    reason: str
    policy_version: Literal["payops-policy-v1"] = "payops-policy-v1"
    risk_tier: Literal["R2", "R3"] | None = None
    proposal: Action | None = None
    action_digest: Digest | None = None
