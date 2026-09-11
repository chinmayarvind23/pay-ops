"""Closed case definitions and receipts keep injection authority out of model text."""

from datetime import UTC, datetime
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, JsonValue

CaseId = Literal[
    "OOM-02",
    "OOM-03",
    "TELEM-03",
    "OOM-01",
    "SCHED-01",
    "SCHED-02",
    "ROLLOUT-01",
    "ROLLOUT-02",
    "ROLLOUT-03",
    "ROLLOUT-04",
    "DEP-01",
    "DEP-02",
    "PAY-01",
    "PAY-02",
    "PAY-03",
    "PAY-04",
]
DeploymentName = Literal["payments-api", "processor-adapter", "webhook-sim", "risk-sim"]
type JsonObject = dict[str, JsonValue]


def object_value(value: JsonValue) -> JsonObject:
    """Reject malformed API structure rather than silently losing safety preconditions."""
    if not isinstance(value, dict):
        raise ValueError("expected Kubernetes JSON object")
    return value


def object_items(value: JsonValue) -> list[JsonObject]:
    """Collection helpers retain strict types across the kubectl JSON boundary."""
    if not isinstance(value, list):
        raise ValueError("expected Kubernetes JSON list")
    return [object_value(item) for item in value]


def utc_timestamp() -> str:
    """Wall time locates evidence; the runner uses monotonic time for deadlines."""
    return datetime.now(UTC).isoformat()


class Artifact(BaseModel):
    """Receipts reference immutable evidence by digest and relative filename."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str
    sha256: str
    recorded_at: str = Field(default_factory=utc_timestamp)


class ScenarioReceipt(BaseModel):
    """Activation and restoration are separate acceptance facts, including failed runs."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    run_id: str
    case_id: CaseId
    mode: Literal["local_kind", "fixture_replay"]
    implementation_variant: str
    started_at: str = Field(default_factory=utc_timestamp)
    completed_at: str | None = None
    injection_requested_at: str | None = None
    activation_observed_at: str | None = None
    cleanup_started_at: str | None = None
    cleanup_verified_at: str | None = None
    activated: bool = False
    control_verified: bool = False
    control_verified_at: str | None = None
    control_pod_uids: tuple[str, ...] = ()
    cleanup_verified: bool = False
    failure: str | None = None
    cleanup_failure: str | None = None
    investigation_status: Literal["not_requested", "completed", "failed"] = "not_requested"
    investigation_failure: str | None = None
    artifacts: list[Artifact] = Field(default_factory=list[Artifact])


class ClusterGateway(Protocol):
    """Only the operator runner holds this fixed-resource mutation interface."""

    mode: Literal["local_kind", "fixture_replay"]

    def verify_scope(self) -> JsonObject:
        """Prove dedicated local cluster identity before any workload mutation."""
        ...

    def deployment(self, name: DeploymentName) -> JsonObject:
        """Read an allowlisted Deployment for capture or precondition checking."""
        ...

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """Replace only a reviewed spec under identity and resource-version preconditions."""
        ...

    def observe(self, name: DeploymentName) -> JsonObject:
        """Collect bounded workload state and probe events, never secrets."""
        ...

    def healthy(self) -> JsonObject:
        """Exercise a real synthetic request through the current payments service."""
        ...
