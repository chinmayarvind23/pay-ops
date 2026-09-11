"""Failure-oriented injector tests prove restoration and retained negative evidence."""

import hashlib
import json
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Literal, cast

import pytest
from pydantic import JsonValue

from payops.scenarios.contracts import (
    CaseId,
    DeploymentName,
    JsonObject,
    object_items,
    object_value,
)
from payops.scenarios.kubectl import KubectlGateway
from payops.scenarios.recipes import activation, container, fault_spec, target, validate_baseline
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner, sample_healthy
from payops.scenarios.startup_failure import main


def document(name: DeploymentName) -> JsonObject:
    """A minimal API-shaped baseline preserves the exact fields restored by the runner."""
    return {
        "metadata": {
            "name": name,
            "namespace": "payops-sandbox",
            "uid": "fixture-uid",
            "resourceVersion": "1",
            "generation": 1,
            "labels": {"app.kubernetes.io/part-of": "payops"},
        },
        "spec": {
            "replicas": 1,
            "strategy": {"type": "RollingUpdate"},
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "sandbox",
                            "image": "payops-sandbox:local",
                            "env": [
                                {"name": "PAYOPS_SANDBOX_ROLE", "value": "payments"},
                                {"name": "PAYOPS_SANDBOX_CONFIG", "value": "{}"},
                            ],
                            "readinessProbe": {"httpGet": {"path": "/health", "port": "http"}},
                        }
                    ]
                }
            },
        },
        "status": {
            "observedGeneration": 1,
            "replicas": 1,
            "updatedReplicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
        },
    }


class FakeCluster:
    """A fixture gateway models controller state without claiming Kubernetes execution."""

    mode: Literal["local_kind", "fixture_replay"] = "fixture_replay"

    def __init__(self, case_id: CaseId, activate: bool = True, restore_error: bool = False) -> None:
        """Failure switches exercise the runner's finally path and contamination latch."""
        self.case_id: CaseId = case_id
        self.original = document(target(case_id))
        self.current = deepcopy(self.original)
        self.activate = activate
        self.restore_error = restore_error
        self.patches = 0

    def verify_scope(self) -> JsonObject:
        """The fake is labeled independently from real local-kind evidence."""
        return {"namespace": "payops-sandbox", "test_fixture": True}

    def deployment(self, name: DeploymentName) -> JsonObject:
        """Return detached data to catch accidental mutation of the restoration source."""
        assert name == target(self.case_id)
        return deepcopy(self.current)

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """Track real runner mutation calls and reject cleanup when explicitly injected."""
        assert expected == self.current and name == target(self.case_id)
        self.patches += 1
        if self.restore_error and self.patches == 2:
            raise RuntimeError("injected cleanup API failure")
        self.current["spec"] = deepcopy(spec)

    def observe(self, name: DeploymentName) -> JsonObject:
        """Supply concrete exit or readiness evidence only after a reviewed mutation."""
        assert name == target(self.case_id)
        if self.current["spec"] == self.original["spec"]:
            return {"deployment": self.current, "pods": [], "events": []}
        if not self.activate:
            return {"deployment": self.current, "pods": [], "events": []}
        status: JsonObject = {"restartCount": 1, "lastState": {"terminated": {"exitCode": 1}}}
        events: list[JsonObject] = []
        if self.case_id == "ROLLOUT-03":
            status = {"started": True, "ready": False, "state": {"running": {}}}
            events = [{"message": "Readiness probe failed: HTTP probe failed with statuscode: 404"}]
        pods: list[JsonObject] = [{"status": {"containerStatuses": [status]}}]
        if self.case_id == "DEP-01":
            pods = []
        return {
            "deployment": self.current,
            "pods": list[JsonValue](pods),
            "events": list[JsonValue](events),
        }

    def healthy(self) -> JsonObject:
        """Processor loss is visible to an end-to-end request; healthy replays are explicit."""
        if self.case_id == "DEP-01" and self.current["spec"] != self.original["spec"]:
            return {"sample_status": 503, "sample_body": "processor unavailable"}
        return {
            "sample_status": 200,
            "sample_body": json.dumps(
                {
                    "sample_id": "synthetic-check",
                    "role": "payments",
                    "status": "accepted",
                    "synthetic": True,
                }
            ),
        }


@pytest.mark.parametrize("case_id", ["ROLLOUT-01", "ROLLOUT-02", "ROLLOUT-03", "DEP-01"])
def test_activation_and_exact_cleanup(case_id: CaseId, tmp_path: Path) -> None:
    """All four recipes must restore captured state and write verifiable artifact hashes."""
    cluster = FakeCluster(case_id)
    runner = LocalScenarioRunner(tmp_path / "unused", tmp_path / "evidence", cluster, 0.01, 0.001)
    receipt = runner.run(case_id)
    assert receipt.activated and receipt.cleanup_verified
    assert receipt.mode == "fixture_replay"
    assert receipt.failure is None and receipt.cleanup_failure is None
    assert cluster.current == cluster.original and cluster.patches == 2
    assert not runner.block_file.exists()
    for artifact in receipt.artifacts:
        path = runner.root / receipt.run_id / artifact.name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact.sha256
    assert (runner.root / receipt.run_id / "receipt.json").is_file()


def test_timeout_preserves_failure_and_restores(tmp_path: Path) -> None:
    """Missing activation cannot be reported as a successful scenario or skip cleanup."""
    cluster = FakeCluster("ROLLOUT-01", activate=False)
    runner = LocalScenarioRunner(tmp_path / "unused", tmp_path, cluster, 0.005, 0.001)
    receipt = runner.run("ROLLOUT-01")
    assert not receipt.activated and receipt.cleanup_verified
    assert receipt.failure is not None and "TimeoutError" in receipt.failure
    assert cluster.current == cluster.original
    assert any("activation" in artifact.name for artifact in receipt.artifacts)


def test_cleanup_failure_blocks_new_process(tmp_path: Path) -> None:
    """A failed restore persists a latch that survives runner reconstruction."""
    cluster = FakeCluster("ROLLOUT-01", restore_error=True)
    runner = LocalScenarioRunner(tmp_path / "unused", tmp_path, cluster, 0.01, 0.001)
    receipt = runner.run("ROLLOUT-01")
    assert receipt.activated and not receipt.cleanup_verified
    assert receipt.cleanup_failure is not None
    restarted = LocalScenarioRunner(tmp_path / "unused", tmp_path, cluster)
    with pytest.raises(CleanupUnverified):
        restarted.run("ROLLOUT-01")
    assert cluster.patches == 2


def test_existing_latch_prevents_any_cluster_access(tmp_path: Path) -> None:
    """Concurrent or interrupted scenario ownership is denied before even preflight reads."""
    cluster = FakeCluster("DEP-01")
    runner = LocalScenarioRunner(tmp_path / "unused", tmp_path, cluster)
    runner.block_file.write_text("pending operator run")
    with pytest.raises(CleanupUnverified):
        runner.run("DEP-01")
    assert cluster.patches == 0


def test_invalid_baseline_cannot_become_restore_source(tmp_path: Path) -> None:
    """Unowned resources and unexpected env/config must never be captured then replayed."""
    cluster = FakeCluster("ROLLOUT-01")
    object_value(cluster.current["metadata"])["namespace"] = "production"
    runner = LocalScenarioRunner(tmp_path / "unused", tmp_path, cluster)
    receipt = runner.run("ROLLOUT-01")
    assert receipt.failure is not None and cluster.patches == 0
    assert not runner.block_file.exists()


def test_recipe_never_changes_original() -> None:
    """Restoration requires a detached baseline, not an alias modified by fault injection."""
    baseline = document("payments-api")
    spec = validate_baseline(baseline, "payments-api")
    injected = fault_spec("ROLLOUT-02", spec)
    assert spec == baseline["spec"] and injected != spec
    assert "PROCESSOR_URL" not in json.dumps(injected)
    assert "processor_url" in json.dumps(injected)


def test_scope_rejects_arbitrary_resource() -> None:
    """A runtime type cast does not authorize a new Kubernetes resource."""
    with pytest.raises(ValueError, match="allowlist"):
        KubectlGateway.validate_target("production-api")


def test_activation_requires_real_signals() -> None:
    """A successful patch, empty pod list or process-running flag alone cannot pass."""
    assert not activation("ROLLOUT-01", {"pods": [{"status": {"containerStatuses": []}}]})
    assert not activation("DEP-01", {"pods": [], "sample_status": 200})
    assert not activation("ROLLOUT-03", {"pods": [], "events": []})
    assert not sample_healthy({"sample_status": 503})


def test_fixed_startup_failure_has_no_arguments() -> None:
    """The bad-image module is a deterministic regression fixture, not a command interpreter."""
    with pytest.raises(RuntimeError, match="synthetic startup regression"):
        main()


def test_unknown_case_and_unbounded_time_rejected(tmp_path: Path) -> None:
    """Operator input cannot expand the closed scenario recipes or polling budget."""
    cluster = FakeCluster("DEP-01")
    with pytest.raises(ValueError, match="timing"):
        LocalScenarioRunner(tmp_path / "kubeconfig", tmp_path, cluster, timeout_seconds=10000)
    runner = LocalScenarioRunner(tmp_path / "kubeconfig", tmp_path, cluster)
    with pytest.raises(ValueError):
        runner.run(cast(CaseId, "DELETE-CLUSTER"))


@pytest.mark.parametrize("fail", [False, True])
def test_active_callback_always_cleans_up(tmp_path: Path, fail: bool) -> None:
    """Collectors see active faults without labels; failure still triggers cleanup."""
    cluster = FakeCluster("ROLLOUT-01")
    runner = LocalScenarioRunner(tmp_path / "kubeconfig", tmp_path, cluster, 0.01, 0.001)

    def investigate() -> None:
        """The closure observes active state; the runner passes no gold or case arguments."""
        assert cluster.current != cluster.original
        if fail:
            raise RuntimeError("collector unavailable")

    receipt = runner.run("ROLLOUT-01", after_activation=investigate)
    assert receipt.cleanup_verified and cluster.current == cluster.original
    assert receipt.investigation_status == ("failed" if fail else "completed")
    assert (receipt.investigation_failure is not None) == fail


@pytest.mark.parametrize("change", ["identity", "spec"])
def test_cleanup_refuses_concurrent_operator_change(tmp_path: Path, change: str) -> None:
    """Restoration cannot overwrite a replacement Deployment or another operator's edit."""
    cluster = FakeCluster("ROLLOUT-01")
    runner = LocalScenarioRunner(tmp_path / "kubeconfig", tmp_path, cluster, 0.01, 0.001)

    def concurrent_change() -> None:
        """Simulate an external edit after activation but before the final cleanup check."""
        if change == "identity":
            object_value(cluster.current["metadata"])["uid"] = "replacement"
        else:
            object_value(cluster.current["spec"])["replicas"] = 2

    receipt = runner.run("ROLLOUT-01", after_activation=concurrent_change)
    assert not receipt.cleanup_verified and runner.block_file.exists()
    assert cluster.patches == 1


class KubectlResponses:
    """A typed subprocess replacement verifies argv and raw Kubernetes read contracts."""

    def __init__(self) -> None:
        """Fixtures include an unrelated event to verify UID-based event filtering."""
        self.server = "https://127.0.0.1:6443"
        self.nodes = ["payops-dev-control-plane", "payops-dev-worker"]
        self.owner = "payops"
        self.calls: list[tuple[str, ...]] = []
        self.current = document("payments-api")

    def execute(self, args: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        """No invocation may enable a shell, omit the context or drop API time bounds."""
        assert kwargs["shell"] is False and kwargs["timeout"] == 15
        assert args[3:8] == (
            "--context",
            "kind-payops-dev",
            "--namespace",
            "payops-sandbox",
            "--request-timeout=10s",
        )
        self.calls.append(args)
        command = args[8:]
        if command[0] == "config":
            output = self.server
        elif command[0] == "patch":
            output = "deployment.apps/payments-api patched"
        else:
            output = json.dumps(self.response(command[1]))
        return subprocess.CompletedProcess(args, 0, output, "")

    def response(self, kind: str) -> JsonObject:
        """Only the gateway's known object categories exist in this fake API."""
        if kind == "nodes":
            return {"items": [{"metadata": {"name": name}} for name in self.nodes]}
        if kind == "namespace":
            return {"metadata": {"labels": {"app.kubernetes.io/part-of": self.owner}}}
        if kind == "deployment":
            return self.current
        if kind == "pods":
            return {"items": [{"metadata": {"uid": "current-pod"}}]}
        return {
            "items": [
                {"involvedObject": {"uid": "current-pod"}, "message": "current"},
                {"involvedObject": {"uid": "old-pod"}, "message": "stale"},
            ]
        }


def gateway_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[KubectlGateway, KubectlResponses]:
    """A real gateway is tested with a harmless kubeconfig marker and fake native process."""
    path = tmp_path / "kubeconfig"
    path.write_text("fixture only")
    responses = KubectlResponses()

    def installed(name: str) -> str:
        """Resolve the harmless executable marker without consulting host PATH."""
        return name

    monkeypatch.setattr("payops.scenarios.kubectl.shutil.which", installed)
    monkeypatch.setattr(subprocess, "run", responses.execute)
    return KubectlGateway(path), responses


def test_kubectl_scope_patch_and_event_filter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wire-level tests require UID, resourceVersion and spec tests on each fixed patch."""
    gateway, responses = gateway_fixture(tmp_path, monkeypatch)
    assert gateway.verify_scope()["namespace"] == "payops-sandbox"
    observed = gateway.observe("payments-api")
    assert len(object_items(observed["events"])) == 1
    original = gateway.deployment("payments-api")
    gateway.replace_spec(
        "payments-api", original, fault_spec("ROLLOUT-01", object_value(original["spec"]))
    )
    patch_call = next(call for call in responses.calls if "patch" in call)
    patch = json.loads(patch_call[-1])
    assert [item["op"] for item in patch] == ["test", "test", "test", "replace"]
    assert [item["path"] for item in patch] == [
        "/metadata/uid",
        "/metadata/resourceVersion",
        "/spec",
        "/spec",
    ]


@pytest.mark.parametrize("failure", ["server", "nodes", "owner", "changed"])
def test_kubectl_scope_drift_denied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """API origin, node identity, namespace ownership and concurrent changes fail closed."""
    gateway, responses = gateway_fixture(tmp_path, monkeypatch)
    expected = deepcopy(responses.current)
    if failure == "server":
        responses.server = "https://production.example"
    elif failure == "nodes":
        responses.nodes = ["production-worker"]
    elif failure == "owner":
        responses.owner = "other-project"
    else:
        object_value(responses.current["spec"])["replicas"] = 2
    with pytest.raises(ValueError):
        gateway.replace_spec("payments-api", expected, object_value(expected["spec"]))
    assert not any("patch" in call for call in responses.calls)


def test_missing_local_requirements_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing tool or kubeconfig cannot cause implicit use of the global cluster."""

    def missing(name: str) -> None:
        """Force the missing-tool branch independently of developer machine setup."""
        return None

    monkeypatch.setattr("payops.scenarios.kubectl.shutil.which", missing)
    with pytest.raises(ValueError, match="explicit"):
        KubectlGateway(tmp_path / "missing")


@pytest.mark.parametrize("value", [None, "string", 42])
def test_malformed_json_shapes_rejected(value: JsonValue) -> None:
    """Invalid nested objects never become empty permissive defaults during validation."""
    with pytest.raises(ValueError):
        object_value(value)
    with pytest.raises(ValueError):
        object_items(value)


@pytest.mark.parametrize("after_applied", [False, True])
def test_ambiguous_patch_error_attempts_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, after_applied: bool
) -> None:
    """An API failure may arrive before or after application; both paths recover safely."""
    cluster = FakeCluster("ROLLOUT-01")
    original_replace = cluster.replace_spec
    attempts = 0

    def uncertain(name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """Model a lost response after an atomic patch, or an API rejection before it."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            if after_applied:
                original_replace(name, expected, spec)
            raise RuntimeError("patch acknowledgment lost")
        original_replace(name, expected, spec)

    monkeypatch.setattr(cluster, "replace_spec", uncertain)
    runner = LocalScenarioRunner(tmp_path / "kubeconfig", tmp_path, cluster, 0.01, 0.001)
    receipt = runner.run("ROLLOUT-01")
    assert receipt.failure is not None and receipt.cleanup_verified
    assert cluster.current == cluster.original and not runner.block_file.exists()


@pytest.mark.parametrize("unsafe", ["owner", "image", "env", "indirect", "container"])
def test_unreviewed_restore_source_rejected(unsafe: str) -> None:
    """Captured live state must not introduce unexpected code, containers or secret references."""
    baseline = document("payments-api")
    item = container(object_value(baseline["spec"]))
    if unsafe == "owner":
        object_value(baseline["metadata"])["labels"] = {}
    elif unsafe == "image":
        item["image"] = "arbitrary-image:latest"
    elif unsafe == "env":
        item["env"] = [{"name": "PAYOPS_SANDBOX_FAULT", "value": "{}"}]
    elif unsafe == "indirect":
        item["env"] = [{"name": "PAYOPS_SANDBOX_ROLE", "valueFrom": {}}]
    else:
        item["name"] = "unreviewed-container"
    with pytest.raises(ValueError):
        validate_baseline(baseline, "payments-api")


def test_two_output_roots_share_cluster_latch(tmp_path: Path) -> None:
    """Changing report destinations cannot bypass ownership of the same target cluster."""
    cluster = FakeCluster("DEP-01")
    config = tmp_path / "kubeconfig"
    first = LocalScenarioRunner(config, tmp_path / "first", cluster, 0.01, 0.001)
    second = LocalScenarioRunner(config, tmp_path / "second", cluster, 0.01, 0.001)
    assert first.block_file == second.block_file

    def overlapping() -> None:
        """A second instance must fail before any mutation while the first fault is active."""
        with pytest.raises(CleanupUnverified):
            second.run("DEP-01")

    receipt = first.run("DEP-01", after_activation=overlapping)
    assert receipt.cleanup_verified and cluster.patches == 2


def test_keyboard_interrupt_still_restores(tmp_path: Path) -> None:
    """Operator interruption propagates only after finally restores the sandbox."""
    cluster = FakeCluster("ROLLOUT-01")
    runner = LocalScenarioRunner(tmp_path / "kubeconfig", tmp_path, cluster, 0.01, 0.001)

    def interrupt() -> None:
        """Exercise a BaseException rather than the ordinary collector error path."""
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        runner.run("ROLLOUT-01", after_activation=interrupt)
    assert cluster.current == cluster.original and not runner.block_file.exists()
    receipt = json.loads(next(tmp_path.glob("*/receipt.json")).read_text())
    assert receipt["investigation_status"] == "failed"
    assert "KeyboardInterrupt" in receipt["failure"]
