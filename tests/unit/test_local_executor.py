"""The operational executor must preserve exact preconditions and never retry ambiguous writes."""

import json
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import JsonValue

from payops.contracts import IncidentCreate, IncidentReport, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.memory.store import IncidentStore
from payops.policy.contracts import ACTION, Action, Principal, Role
from payops.policy.engine import action_digest
from payops.remediation.backend import OperationalBackend
from payops.remediation.broker import RemediationBroker
from payops.remediation.deployment import plan_deployment, rollout_ready
from payops.remediation.local_executor import LocalDeploymentExecutor
from payops.remediation.store import ActionStore
from payops.scenarios.contracts import JsonObject, object_items, object_value


def action(kind: str = "restart_deployment", **changes: JsonValue) -> Action:
    """Closed proposals carry fixture identities, never operational credentials."""
    value: JsonObject = {
        "incident_id": "incident",
        "namespace": "payops-sandbox",
        "service": "payments-api",
        "resource_uid": "uid-1",
        "expected_version": "v1",
        "evidence_ids": ["evidence"],
        "action_type": kind,
        **changes,
    }
    return ACTION.validate_python(value)


def deployment() -> JsonObject:
    """Retain extra fields so tests detect a patch that accidentally replaces unrelated spec."""
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "payments-api",
            "namespace": "payops-sandbox",
            "uid": "uid-1",
            "resourceVersion": "v1",
            "generation": 1,
            "labels": {"app.kubernetes.io/part-of": "payops"},
        },
        "spec": {
            "replicas": 1,
            "strategy": {"type": "RollingUpdate"},
            "template": {
                "metadata": {"annotations": {"existing": "keep"}},
                "spec": {
                    "containers": [
                        {
                            "name": "sandbox",
                            "image": "old",
                            "env": [{"name": "KEEP", "value": "yes"}],
                        }
                    ]
                },
            },
        },
        "status": {
            "replicas": 1,
            "updatedReplicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
            "observedGeneration": 1,
        },
    }


@pytest.mark.parametrize("kind", ["restart_deployment", "scale_deployment", "rollback_deployment"])
def test_closed_plan_preserves_other_fields(kind: str) -> None:
    """Only one approved field changes, with UID, resourceVersion and full-spec atomic tests."""
    revision = "a" * 64
    options: JsonObject = {"replicas": 2} if kind == "scale_deployment" else {}
    if kind == "rollback_deployment":
        options["revision_sha256"] = revision
    proposal = action(kind, **options)
    original = deployment()
    plan = plan_deployment(
        proposal,
        original,
        action_digest(proposal),
        {"payments-api": "uid-1"},
        {revision: "payops/sandbox@sha256:" + revision},
    )
    expected = deepcopy(object_value(original["spec"]))
    template = object_value(expected["template"])
    if kind == "scale_deployment":
        expected["replicas"] = 2
    elif kind == "rollback_deployment":
        object_items(object_value(template["spec"])["containers"])[0]["image"] = (
            "payops/sandbox@sha256:" + revision
        )
    else:
        object_value(object_value(template["metadata"])["annotations"])[
            "payops.dev/remediation"
        ] = action_digest(proposal)
    assert plan.after == expected and original == deployment()
    assert [object_value(item)["path"] for item in plan.patch()] == [
        "/metadata/uid",
        "/metadata/resourceVersion",
        "/spec",
        "/spec",
    ]
    assert plan.before == original["spec"]


@pytest.mark.parametrize(
    "change",
    ["namespace", "service", "mode", "uid", "version", "digest", "pause", "label", "deleting"],
)
def test_plan_rejects_unsafe_target(change: str) -> None:
    """A valid schema alone cannot authorize a different resource or bypass the closed inventory."""
    options: JsonObject = {}
    if change in {"namespace", "service", "mode"}:
        options[change] = "fixture_replay" if change == "mode" else "foreign"
    if change in {"uid", "version"}:
        options["resource_uid" if change == "uid" else "expected_version"] = "different"
    proposal = action(
        "pause_synthetic_traffic" if change == "pause" else "restart_deployment", **options
    )
    current = deployment()
    metadata = object_value(current["metadata"])
    if change == "label":
        metadata["labels"] = {}
    if change == "deleting":
        metadata["deletionTimestamp"] = "now"
    with pytest.raises(PermissionError):
        plan_deployment(
            proposal,
            current,
            "wrong" if change == "digest" else action_digest(proposal),
            {"payments-api": "uid-1"},
            {},
        )


@pytest.mark.parametrize(
    "image", [None, "payops:latest", "payops@sha256:" + "b" * 64, "--option@sha256:" + "a" * 64]
)
def test_rollback_requires_exact_immutable_inventory(image: str | None) -> None:
    """An allowed digest cannot resolve to an unpinned or mismatched runtime image."""
    proposal = action("rollback_deployment", revision_sha256="a" * 64)
    with pytest.raises(PermissionError):
        plan_deployment(
            proposal,
            deployment(),
            action_digest(proposal),
            {"payments-api": "uid-1"},
            {} if image is None else {"a" * 64: image},
        )


class Command:
    """In-memory API transport applies only exact CAS patches and counts attempted effects."""

    def __init__(self) -> None:
        """A mutable fake server can race, time out or fail rollout without a real cluster."""
        self.current = deployment()
        self.namespace = "namespace-1"
        self.calls: list[tuple[str, ...]] = []
        self.writes = 0
        self.failure: str | None = None
        self.time = 0.0

    def clock(self) -> float:
        """A monotonic fake clock makes timeout boundaries deterministic."""
        return self.time

    def wait(self, seconds: float) -> None:
        """Advance time without slowing safety tests."""
        self.time += seconds

    def __call__(self, args: tuple[str, ...], maximum: int, timeout: float) -> bytes:
        """Model a committed write followed by timeout separately from a rejected CAS."""
        self.calls.append(args)
        assert maximum == 262144 and timeout == 12 and "--context=kind-payops-dev" in args
        if "namespace" in args:
            return json.dumps(
                {"metadata": {"name": "payops-sandbox", "uid": self.namespace}}
            ).encode()
        if "patch" in args:
            self.writes += 1
            patch = json.loads(
                next(
                    value.removeprefix("--patch=") for value in args if value.startswith("--patch=")
                )
            )
            assert patch[0]["value"] == "uid-1" and patch[1]["value"] == "v1"
            assert patch[2]["value"] == self.current["spec"]
            if self.failure == "race":
                raise ValueError("server rejected resourceVersion test")
            self.current["spec"] = JSON_OBJECT.validate_python(patch[3]["value"])
            object_value(self.current["metadata"])["resourceVersion"] = "v2"
            if self.failure == "timeout":
                raise TimeoutError("response lost after commit")
            if self.failure == "unready":
                self.current["status"] = {}
        return json.dumps(self.current).encode()


def executor(tmp_path: Path, command: Command, **changes: object) -> LocalDeploymentExecutor:
    """Bind existing dummy paths; the injected transport prevents all process execution."""
    executable, config = tmp_path / "kubectl", tmp_path / "kubeconfig"
    executable.touch()
    config.touch()
    assert not changes
    return LocalDeploymentExecutor(
        executable,
        config,
        "namespace-1",
        {"payments-api": "uid-1"},
        {},
        command=command,
        clock=command.clock,
        wait=command.wait,
        postcheck_seconds=1,
    )


def test_executor_snapshot_conditional_effect_and_postcheck(tmp_path: Path) -> None:
    """Trusted snapshots and execution use the same fixed context, namespace and UID inventory."""
    command = Command()
    runtime = executor(tmp_path, command)
    proposal = action()
    snapshot = runtime.snapshot(proposal)
    assert snapshot.uid == "uid-1" and snapshot.version == "v1" and snapshot.synthetic
    result = runtime.execute(proposal, action_digest(proposal))
    assert result.outcome == "SUCCEEDED" and result.resulting_version == "v2"
    assert command.writes == 1
    with pytest.raises(PermissionError):
        runtime.execute(proposal, action_digest(proposal))
    assert command.writes == 1


@pytest.mark.parametrize("failure", ["race", "timeout", "unready", "namespace"])
def test_executor_never_retries_write(tmp_path: Path, failure: str) -> None:
    """A readiness failure does not undo a committed patch; a transport ambiguity is raised."""
    command = Command()
    command.failure = failure
    if failure == "namespace":
        command.namespace = "recreated"
    runtime = executor(tmp_path, command)
    proposal = action()
    if failure == "unready":
        result = runtime.execute(proposal, action_digest(proposal))
        assert result.outcome == "FAILED" and result.resulting_version == "v2"
    else:
        with pytest.raises((ValueError, PermissionError, TimeoutError)):
            runtime.execute(proposal, action_digest(proposal))
    assert command.writes == (0 if failure == "namespace" else 1)


@pytest.mark.parametrize("change", ["uid", "spec", "deleting", "generation", "ready", "zero"])
def test_postcheck_cannot_accept_other_rollout(change: str) -> None:
    """A healthy unrelated rollout or obsolete controller observation is not success."""
    proposal = action()
    plan = plan_deployment(
        proposal, deployment(), action_digest(proposal), {"payments-api": "uid-1"}, {}
    )
    current = deployment()
    current["spec"] = deepcopy(plan.after)
    metadata = object_value(current["metadata"])
    if change == "uid":
        metadata["uid"] = "replaced"
    elif change == "spec":
        object_value(current["spec"])["replicas"] = 2
    elif change == "deleting":
        metadata["deletionTimestamp"] = "now"
    elif change == "generation":
        metadata["generation"] = 2
    elif change == "ready":
        object_value(current["status"])["readyReplicas"] = 0
    else:
        plan.after["replicas"] = 0
        current["spec"] = deepcopy(plan.after)
    if change in {"uid", "spec", "deleting"}:
        with pytest.raises(PermissionError):
            rollout_ready(plan, current)
    else:
        assert not rollout_ready(plan, current)


def test_operational_backend_broker_integration(tmp_path: Path) -> None:
    """Real broker/executor wiring uses fixture identities and intercepted Kubernetes."""
    command = Command()
    runtime = executor(tmp_path, command)
    artifacts = ArtifactStore(tmp_path / "artifacts")
    incidents = IncidentStore(f"sqlite:///{tmp_path / 'incidents.db'}")
    actions = ActionStore(f"sqlite:///{tmp_path / 'actions.db'}")
    now = utc_now()
    roles: dict[str, Role] = {"alice": "responder", "bob": "approver", "worker": "executor"}

    def principal(subject: str) -> Principal | None:
        """Distinct synthetic principals exercise composition without pretending human approval."""
        if subject not in roles:
            return None
        return Principal(
            subject=subject,
            roles=(roles[subject],),
            namespaces=("payops-sandbox",),
            verified_at=utc_now(),
            expires_at=now + timedelta(minutes=5),
        )

    try:
        incident = incidents.create(IncidentCreate(title="Unavailable"), None)
        item = normalize(
            Observation(
                source="KUBERNETES",
                resource="payments-api",
                observed_at=now,
                query="status",
                summary="Unavailable",
                payload={"ready": False},
            ),
            incident.incident_id,
            now - timedelta(seconds=1),
            now + timedelta(seconds=1),
            artifacts,
        )
        incidents.save_report(
            IncidentReport(
                incident_id=incident.incident_id,
                evidence=(item,),
                terminal_state="ESCALATED",
                mode="local_kind",
                duration_seconds=1,
            )
        )
        backend = OperationalBackend(principal, incidents, artifacts, runtime)
        broker = RemediationBroker(actions, backend, mode="local_kind")
        proposal = action(incident_id=incident.incident_id, evidence_ids=[item.evidence_id])
        record = broker.propose(JSON_OBJECT.validate_json(proposal.model_dump_json()), "alice")
        broker.approve(record.action_id, "bob")
        assert broker.execute(record.action_id, "worker").state == "SUCCEEDED"
        assert broker.execute(record.action_id, "worker").state == "SUCCEEDED"
        assert command.writes == 1
        with pytest.raises(PermissionError, match="INCIDENT_NOT_FOUND"):
            backend.context(action(), "alice")
        assert backend.principal("unknown") is None
    finally:
        actions.close()
        incidents.close()


def test_executor_invalid_configuration_and_replica_shape(tmp_path: Path) -> None:
    """Configuration and malformed source numbers fail before any mutation is attempted."""
    with pytest.raises(ValueError, match="configuration"):
        LocalDeploymentExecutor(tmp_path, tmp_path, "uid", {"ledger-sim": "uid"}, {})
    command = Command()
    runtime = executor(tmp_path, command)
    object_value(command.current["spec"])["replicas"] = True
    with pytest.raises(ValueError, match="replica"):
        runtime.snapshot(action())
    assert command.writes == 0


def test_rollback_rejects_extra_containers() -> None:
    """Image replacement cannot silently choose one container in an unexpected pod template."""
    current = deployment()
    template = object_value(object_value(current["spec"])["template"])
    object_value(template["spec"])["containers"] = []
    proposal = action("rollback_deployment", revision_sha256="a" * 64)
    with pytest.raises(PermissionError, match="CONTAINER_DENIED"):
        plan_deployment(proposal, current, action_digest(proposal), {"payments-api": "uid-1"}, {})
