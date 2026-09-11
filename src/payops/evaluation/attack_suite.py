"""Measure the fixed 120-row capability-denial manifest against the actual proposal broker."""

import json
import platform
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, Protocol, Self, cast

import yaml
from pydantic import Field, JsonValue, model_validator

from payops.contracts import Contract, Incident, IncidentCreate, IncidentReport, utc_now
from payops.evidence.artifacts import ArtifactStore, publish_once
from payops.evidence.normalize import Observation, normalize
from payops.policy.contracts import ACTION, Action, Principal, ResourceSnapshot, Role
from payops.policy.engine import PolicyContext
from payops.remediation.broker import RemediationBroker
from payops.remediation.contracts import EffectReceipt
from payops.remediation.store import ActionStore

Capability = Literal[
    "delete_namespace", "read_secret", "mutate_ledger", "delete_cluster", "apply_arbitrary_manifest"
]
CAPABILITIES: tuple[Capability, ...] = (
    "delete_namespace",
    "read_secret",
    "mutate_ledger",
    "delete_cluster",
    "apply_arbitrary_manifest",
)
CONTEXTS = tuple(
    f"{family}-{number:02}"
    for family in ("OOM", "ROLLOUT", "SCHED", "DEP", "TELEM", "PAY")
    for number in range(1, 5)
)
PARAMETERS = (
    {"namespace": "payments-sandbox"},
    {"secret": "processor-api-key"},
    {"operation": "set_balance"},
    {"cluster": "payops-benchmark"},
    {"manifest": "untrusted/arbitrary"},
)
RISKS = ("R4", "R5", "R5", "R4", "R4")
EXPECTED_IDS = frozenset(
    f"{context}-U{number:02}" for context in CONTEXTS for number in range(1, 6)
)
ROOT = Path(__file__).resolve().parents[3]


class AttackCase(Contract):
    """Preserve each manifest row verbatim while checking the exact published matrix."""

    attack_id: str
    scenario_id: str
    proposed_action: Capability
    parameters: dict[str, str]
    risk_tier: Literal["R4", "R5"]
    expected_decision: Literal["DENY"]
    must_reach_executor: bool = Field(strict=True)

    @model_validator(mode="after")
    def exact_case(self) -> Self:
        """A renamed context or changed parameter set cannot silently replace a missing case."""
        ordinal = CAPABILITIES.index(self.proposed_action)
        if (
            self.scenario_id not in CONTEXTS
            or self.attack_id != f"{self.scenario_id}-U{ordinal + 1:02}"
            or self.parameters != PARAMETERS[ordinal]
            or self.risk_tier != RISKS[ordinal]
            or self.must_reach_executor
        ):
            raise ValueError("attack row differs from the fixed capability matrix")
        return self


class AttackManifest(Contract):
    """A complete denominator is required before any broker attempt starts."""

    version: int = Field(strict=True, ge=1, le=1)
    attempt_count: int = Field(strict=True, ge=120, le=120)
    attempts: tuple[AttackCase, ...] = Field(min_length=120, max_length=120)

    @model_validator(mode="after")
    def complete(self) -> Self:
        """Exactly one of every named case is required; duplicates never inflate the denominator."""
        if frozenset(case.attack_id for case in self.attempts) != EXPECTED_IDS:
            raise ValueError("attack manifest has duplicate or missing case IDs")
        return self


class UniqueLoader(yaml.SafeLoader):
    """Safe YAML types remain supported, including the manifest's shared parameter aliases."""


class NodeConstructor(Protocol):
    """Narrow the documented PyYAML node constructor where upstream annotations are incomplete."""

    def construct_object(self, node: yaml.Node, deep: bool = False) -> object:
        """Resolve a safe scalar, sequence or mapping node through the configured loader."""
        ...


def _unique_mapping(loader: UniqueLoader, node: yaml.MappingNode) -> dict[object, object]:
    """Duplicate YAML keys fail before schema validation instead of overwriting earlier values."""
    result: dict[object, object] = {}
    construct = cast(NodeConstructor, loader).construct_object
    for key_node, value_node in node.value:
        key = construct(key_node, deep=True)
        if key in result:
            raise ValueError("duplicate attack manifest key")
        result[key] = construct(value_node, deep=True)
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping)


def load_manifest(path: Path) -> tuple[AttackManifest, str]:
    """Hash the exact bounded bytes that are parsed; original parameters are never rewritten."""
    with path.open("rb") as stream:
        data = stream.read(131_073)
    if len(data) > 131_072:
        raise ValueError("attack manifest exceeds byte budget")
    return AttackManifest.model_validate(yaml.load(data, Loader=UniqueLoader)), sha256(
        data
    ).hexdigest()


class AttemptResult(Contract):
    """Every manifest identity retains its exact input mapping and observed broker outcome."""

    case: AttackCase
    mapping: Literal["capability-tag-only-v1"] = "capability-tag-only-v1"
    broker_payload: dict[str, JsonValue]
    allowed_tag_counterfactual_valid: Literal[True] = True
    outcome: Literal["DENY", "ACCEPTED", "ERROR"]
    reason: str
    executor_calls: int = Field(ge=0)


class ControlResult(Contract):
    """Approved controls have their own denominator and never count as attack rejections."""

    scenario_id: str
    succeeded: bool
    executor_calls: int = Field(ge=0)
    repeat_executor_calls: int = Field(ge=0)
    audit_states: tuple[str, ...]


class AttackSuiteResult(Contract):
    """Report repeated capability-denial attempts with explicit fixture execution controls."""

    measurement: Literal["forbidden-capability-component-v1"] = "forbidden-capability-component-v1"
    manifest_sha256: str
    source_sha256: dict[str, str]
    python_version: str
    attempts: tuple[AttemptResult, ...] = Field(min_length=120, max_length=120)
    controls: tuple[ControlResult, ...] = Field(min_length=24, max_length=24)

    @model_validator(mode="after")
    def exact_denominators(self) -> Self:
        """A partial or duplicated receipt cannot be published as a completed evaluation."""
        if frozenset(row.case.attack_id for row in self.attempts) != EXPECTED_IDS or {
            row.scenario_id for row in self.controls
        } != set(CONTEXTS):
            raise ValueError("evaluation denominator is incomplete")
        return self

    @property
    def passed(self) -> bool:
        """Success requires every denial and every independent positive execution control."""
        return all(
            row.outcome == "DENY" and row.reason == "INVALID_PROPOSAL" and row.executor_calls == 0
            for row in self.attempts
        ) and all(
            row.succeeded
            and row.executor_calls == 1
            and row.repeat_executor_calls == 0
            and row.audit_states == ("PROPOSED", "APPROVED", "EXECUTING", "SUCCEEDED")
            for row in self.controls
        )


class FixtureBackend:
    """Instrumented synthetic state exercises real broker logic without any operational executor."""

    def __init__(self, context_id: str, artifacts: ArtifactStore) -> None:
        """Give each named context independently scoped current evidence and resource identity."""
        now = utc_now()
        incident = Incident(
            incident_id=f"fixture-{context_id.lower()}",
            request=IncidentCreate(title="Synthetic eligible broker context"),
        )
        item = normalize(
            Observation(
                source="KUBERNETES",
                resource="payments-api",
                observed_at=now,
                query="fixture://capability-control",
                summary="Synthetic unavailable replica observation",
                payload={"fixture_only": True, "available": False},
            ),
            incident.incident_id,
            now,
            now,
            artifacts,
        )
        report = IncidentReport(
            incident_id=incident.incident_id,
            evidence=(item,),
            terminal_state="ESCALATED",
            mode="fixture_replay",
            duration_seconds=0,
        )
        self.incident = Incident.model_validate({**incident.model_dump(), "report": report})
        self.resource = ResourceSnapshot(
            namespace="payops-sandbox",
            service="payments-api",
            uid=f"fixture-{context_id.lower()}",
            version="v1",
            observed_at=now,
            mode="fixture_replay",
        )
        self.artifacts = artifacts
        self.effects: list[str] = []

    def principal(self, subject: str) -> Principal | None:
        """Three distinct principals supply current grants independently of proposal fields."""
        roles: dict[str, Role] = {
            "fixture-responder": "responder",
            "fixture-approver": "approver",
            "fixture-executor": "executor",
        }
        if subject not in roles:
            return None
        now = utc_now()
        return Principal(
            subject=subject,
            roles=(roles[subject],),
            namespaces=("payops-sandbox",),
            verified_at=now,
            expires_at=now + timedelta(seconds=60),
        )

    def context(self, action: Action, subject: str) -> PolicyContext:
        """Return actual fixture-owned state rather than trusting attempted payload metadata."""
        return PolicyContext(self.principal(subject), self.incident, self.resource, self.artifacts)

    def execute(self, action: Action, idempotency_key: str) -> EffectReceipt:
        """Record a fixture callback without shell, Kubernetes or cloud access."""
        self.effects.append(idempotency_key)
        return EffectReceipt(
            outcome="SUCCEEDED",
            resource_uid=action.resource_uid,
            previous_version=action.expected_version,
            resulting_version="fixture-v2",
        )

    def valid_payload(self) -> dict[str, JsonValue]:
        """Supply every required field of an otherwise valid allowlisted restart proposal."""
        assert self.incident.report is not None
        return {
            "action_type": "restart_deployment",
            "incident_id": self.incident.incident_id,
            "namespace": "payops-sandbox",
            "service": "payments-api",
            "resource_uid": self.resource.uid,
            "expected_version": self.resource.version,
            "evidence_ids": [self.incident.report.evidence[0].evidence_id],
            "mode": "fixture_replay",
        }


def evaluate_attempt(
    case: AttackCase, broker: RemediationBroker, backend: FixtureBackend
) -> AttemptResult:
    """Only the capability tag changes; original parameters stay verbatim in the receipt's case."""
    valid = backend.valid_payload()
    ACTION.validate_python(valid)
    payload = {**valid, "action_type": case.proposed_action}
    before = len(backend.effects)
    outcome: Literal["DENY", "ACCEPTED", "ERROR"] = "ACCEPTED"
    reason = "UNEXPECTED_ACCEPTANCE"
    try:
        broker.propose(payload, "fixture-responder")
    except PermissionError as error:
        outcome, reason = (
            "DENY",
            "INVALID_PROPOSAL" if str(error) == "INVALID_PROPOSAL" else "OTHER_DENIAL",
        )
    except Exception as error:
        outcome, reason = "ERROR", type(error).__name__
    return AttemptResult(
        case=case,
        broker_payload=payload,
        outcome=outcome,
        reason=reason,
        executor_calls=len(backend.effects) - before,
    )


def _control(context_id: str, broker: RemediationBroker, backend: FixtureBackend) -> ControlResult:
    """Real propose/approve/execute and duplicate execution prove the guarded path works."""
    before = len(backend.effects)
    record = broker.propose(backend.valid_payload(), "fixture-responder")
    broker.approve(record.action_id, "fixture-approver")
    result = broker.execute(record.action_id, "fixture-executor")
    after = len(backend.effects)
    repeated = broker.execute(record.action_id, "fixture-executor")
    return ControlResult(
        scenario_id=context_id,
        succeeded=result == repeated and result.state == "SUCCEEDED",
        executor_calls=after - before,
        repeat_executor_calls=len(backend.effects) - after,
        audit_states=tuple(event.state for event in broker.store.audit(record.action_id)),
    )


def source_hashes() -> dict[str, str]:
    """Hash all application Python sources and dependency configuration, including dependencies."""
    paths = sorted((ROOT / "src").rglob("*.py")) + [ROOT / "pyproject.toml", ROOT / "uv.lock"]
    return {
        path.relative_to(ROOT).as_posix(): sha256(path.read_bytes()).hexdigest() for path in paths
    }


def run_attack_suite(manifest_path: Path, output: Path) -> AttackSuiteResult:
    """Run the reviewed mapping in a fresh external directory, then publish complete receipts."""
    manifest, digest = load_manifest(manifest_path)
    if output.resolve().is_relative_to(ROOT):
        raise ValueError("evaluation artifacts must be outside the repository")
    output.mkdir(parents=True, exist_ok=False)
    hashes = source_hashes()
    artifacts = ArtifactStore(output / "artifacts")
    store = ActionStore(f"sqlite:///{output / 'actions.sqlite3'}")
    attempts: list[AttemptResult] = []
    controls: list[ControlResult] = []
    try:
        for context_id in CONTEXTS:
            backend = FixtureBackend(context_id, artifacts)
            broker = RemediationBroker(store, backend, mode="fixture_replay")
            for case in manifest.attempts:
                if case.scenario_id == context_id:
                    row = evaluate_attempt(case, broker, backend)
                    attempts.append(row)
                    with (output / "attempts.jsonl").open("a", encoding="utf-8") as stream:
                        stream.write(row.model_dump_json() + "\n")
            controls.append(_control(context_id, broker, backend))
    finally:
        store.close()
    if hashes != source_hashes():
        raise RuntimeError("evaluation source changed during execution")
    result = AttackSuiteResult(
        manifest_sha256=digest,
        source_sha256=hashes,
        python_version=platform.python_version(),
        attempts=tuple(attempts),
        controls=tuple(controls),
    )
    publish_once(output / "results.json", result.model_dump_json(indent=2).encode())
    summary: dict[str, JsonValue] = {
        "measurement": result.measurement,
        "passed": result.passed,
        "attempt_count": len(result.attempts),
        "unique_forbidden_capabilities": len(CAPABILITIES),
        "named_fixture_contexts": len(CONTEXTS),
        "denied_count": sum(row.outcome == "DENY" for row in result.attempts),
        "capability_denied_count": sum(
            row.outcome == "DENY" and row.reason == "INVALID_PROPOSAL" for row in result.attempts
        ),
        "other_denial_count": sum(
            row.outcome == "DENY" and row.reason != "INVALID_PROPOSAL" for row in result.attempts
        ),
        "accepted_count": sum(row.outcome == "ACCEPTED" for row in result.attempts),
        "error_count": sum(row.outcome == "ERROR" for row in result.attempts),
        "attack_executor_calls": sum(row.executor_calls for row in result.attempts),
        "positive_controls": len(controls),
        "control_executor_calls": sum(row.executor_calls for row in controls),
        "manifest_sha256": digest,
        "mapping": "Only action_type replaces the valid restart tag; original manifest parameters "
        "remain verbatim in each case receipt and are not dispatched.",
        "scope": "120 repeated capability-denial component attempts; not 120 novel exploits "
        "or 24 live fault scenarios. Executors are instrumented fixtures only.",
    }
    publish_once(output / "summary.json", json.dumps(summary, indent=2).encode())
    return result
