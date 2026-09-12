"""Exercise the actual lifecycle with owned runtime objects and verified fixture artifacts."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import pytest
import test_sampling_observation as observations
from test_sampling import document

from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.trace_span import PodIdentity, verify_trace_log
from payops.scenarios.contracts import (
    DeploymentName,
    JsonObject,
    ScenarioReceipt,
    object_items,
    object_value,
)
from payops.scenarios.recipes import container, fault_spec
from payops.scenarios.runner import CleanupUnverified, LocalScenarioRunner
from payops.scenarios.sampling import SamplingHarness
from payops.scenarios.sampling_gateway import (
    SERVICES,
)
from payops.scenarios.sampling_observation import SamplingObservation, Stage
from payops.tools.traces import TraceCollection


class Clock:
    """A historical fixture clock advances through captures without any live wait or process."""

    def __init__(self) -> None:
        """Retained source timestamps precede the real artifact collection clock."""
        self.value = datetime(2024, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        """Return the virtual runtime wall time used by fake pods and requests."""
        return self.value


class SamplingCluster:
    """Every CAS can fail before or after applying, independently of later recovery attempts."""

    mode: Literal["local_kind", "fixture_replay"] = "fixture_replay"

    def __init__(self, clock: Clock, fail_at: int = 0, after_apply: bool = False) -> None:
        """Create five deployments and pods with explicit controller ownership."""
        self.clock, self.fail_at, self.after_apply = clock, fail_at, after_apply
        self.documents: dict[str, JsonObject] = {}
        self.created: dict[str, str] = {}
        self.writes: list[JsonObject] = []
        self.journal_root: Path | None = None
        self.foreign = False
        roles = ("payments", "processor", "risk", "ledger", "webhook")
        for name, role in zip(SERVICES, roles, strict=True):
            item = document()
            metadata = object_value(item["metadata"])
            metadata.update(name=name, uid=name + "-deployment")
            entry = container(object_value(item["spec"]))
            env = object_items(entry["env"])
            env[0]["value"] = role
            entry["env"] = list(env)
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 1,
                "updatedReplicas": 1,
                "readyReplicas": 1,
                "availableReplicas": 1,
            }
            self.documents[name] = item
            self.created[name] = clock.now().isoformat()
        self.originals = deepcopy(self.documents)

    def verify_scope(self) -> JsonObject:
        """The fixture never constructs kubectl or borrows a global Kubernetes context."""
        return {"fixture": True, "namespace": "payops-sandbox"}

    def deployment(self, name: DeploymentName) -> JsonObject:
        """API reads return copies so captured original state cannot mutate in place."""
        result = deepcopy(self.documents[name])
        if self.foreign:
            object_value(result["metadata"])["uid"] = "foreign"
        return result

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """Require a complete persisted three-spec journal before the first fake mutation."""
        assert name == "processor-adapter"
        assert self.journal_root is not None
        journals = tuple(self.journal_root.glob("*/*-sampling-journal.json"))
        assert len(journals) == 1
        assert expected["spec"] == self.documents[name]["spec"]
        assert (
            object_value(expected["metadata"])["uid"]
            == object_value(self.documents[name]["metadata"])["uid"]
        )
        self.writes.append(deepcopy(spec))
        failed = len(self.writes) == self.fail_at
        if failed and not self.after_apply:
            raise OSError("write rejected before apply")
        current = self.documents[name]
        current["spec"] = deepcopy(spec)
        metadata = object_value(current["metadata"])
        generation = int(str(metadata["generation"])) + 1
        metadata.update(generation=generation, resourceVersion=str(generation))
        object_value(current["status"])["observedGeneration"] = generation
        self.created[name] = self.clock.now().isoformat()
        if failed:
            raise OSError("response lost after apply")

    def observe(self, name: DeploymentName) -> JsonObject:
        """Unused compatibility method keeps the generic mutation gateway protocol closed."""
        return {"deployment": self.deployment(name)}

    def healthy(self) -> JsonObject:
        """Any hidden ninth synthetic request is a fixture assertion failure."""
        raise AssertionError("extra synthetic health request outside frozen stage plan")

    def state(self, timeout_seconds: float = 30) -> JsonObject:
        """Reconcile fixture pods from actual current templates."""
        assert 0 < timeout_seconds <= 30
        pods: list[JsonObject] = []
        replicas: list[JsonObject] = []
        for name, deployment in self.documents.items():
            metadata = object_value(deployment["metadata"])
            generation = str(metadata["generation"])
            rs_name, rs_uid = name + "-rs" + generation, name + "-rsuid" + generation
            replicas.append(
                {
                    "metadata": {
                        "name": rs_name,
                        "uid": rs_uid,
                        "namespace": "payops-sandbox",
                        "ownerReferences": [
                            {
                                "apiVersion": "apps/v1",
                                "kind": "Deployment",
                                "name": name,
                                "uid": metadata["uid"],
                                "controller": True,
                            }
                        ],
                    },
                    "spec": {"template": deepcopy(object_value(deployment["spec"])["template"])},
                }
            )
            template = object_value(object_value(deployment["spec"])["template"])
            pods.append(
                {
                    "metadata": {
                        "name": name + "-pod" + generation,
                        "uid": name + "-uid" + generation,
                        "namespace": "payops-sandbox",
                        "creationTimestamp": self.created[name],
                        "labels": {"app.kubernetes.io/name": name},
                        "ownerReferences": [
                            {
                                "apiVersion": "apps/v1",
                                "kind": "ReplicaSet",
                                "name": rs_name,
                                "uid": rs_uid,
                                "controller": True,
                            }
                        ],
                    },
                    "spec": deepcopy(template["spec"]),
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [
                            {
                                "name": "sandbox",
                                "ready": True,
                                "restartCount": 0,
                                "imageID": "sha256:normal",
                                "containerID": "containerd://" + name + generation,
                                "state": {"running": {}},
                            }
                        ],
                    },
                }
            )
        documents = deepcopy(list(self.documents.values()))
        if self.foreign:
            object_value(documents[1]["metadata"])["uid"] = "foreign"
        return JSON_OBJECT.validate_python(
            {"deployments": documents, "pods": pods, "replicas": replicas}
        )


class FixtureObserver:
    """Verify real metric and LOG artifacts from historical fixture observations."""

    def __init__(self, clock: Clock, fail_stage: Stage | None = None) -> None:
        """Inject one controlled collection failure while retaining final-stage verification."""
        self.clock, self.fail_stage = clock, fail_stage
        self.stages: list[Stage] = []
        self.interrupt = False

    def collect(
        self,
        stage: Stage,
        incident: str,
        identities: tuple[PodIdentity, PodIdentity],
        ready_at: datetime,
        directory: Path,
        store: ArtifactStore,
    ) -> SamplingObservation:
        """Bind fixture sources to the lifecycle's exact identities and incident."""
        self.stages.append(stage)
        if stage == self.fail_stage:
            if self.interrupt:
                raise KeyboardInterrupt("fixture interruption")
            raise ValueError("fixture collection failed")
        ages = {"original": 240, "suppressed": 180, "restored_sampling": 120, "final": 60}
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                observations,
                "utc_now",
                lambda: self.clock.now() + timedelta(seconds=ages[stage] + 1),
            )
            observed = observations.fixture(store, stage)
        captures: list[TraceCollection] = []
        for capture in observed.captures:
            logs = [verify_trace_log(item, store) for item in capture.sources]
            rebound = tuple(
                item.model_copy(
                    update={
                        "scope": item.scope.model_copy(update={"incident_id": incident}),
                        "identity": selected,
                    }
                )
                for item, selected in zip(logs, identities, strict=True)
            )
            captures.append(observations.collection(store, (rebound[0], rebound[1])))
        mean = 0.6 if stage in {"suppressed", "restored_sampling"} else 0.01
        observed = observed.model_copy(
            update={
                "captures": tuple(captures),
                "payments_identity": identities[0],
                "processor_identity": identities[1],
                "metrics": (
                    observations.metrics(
                        store, observed.traffic, "payments-api", mean, incident=incident
                    ),
                    observations.metrics(
                        store, observed.traffic, "processor-adapter", mean, incident=incident
                    ),
                ),
            }
        )
        assert observed.traffic.started_at >= ready_at
        self.clock.value = observed.traffic.completed_at + timedelta(seconds=19)
        return observed


def setup(
    tmp_path: Path, fail_at: int = 0, after_apply: bool = False, fail_stage: Stage | None = None
) -> tuple[SamplingHarness, SamplingCluster, FixtureObserver]:
    """Each fixture owns a separate canonical kubeconfig sibling lock and evidence root."""
    clock = Clock()
    cluster, observer = (
        SamplingCluster(clock, fail_at, after_apply),
        FixtureObserver(clock, fail_stage),
    )
    root = tmp_path / "runs"
    cluster.journal_root = root
    # Real artifact writes need scheduling slack on busy Windows hosts; retries remain bounded.
    runner = SamplingHarness(
        tmp_path / "kubeconfig", root, cluster, observer, 1.0, 0.001, clock.now
    )
    return runner, cluster, observer


def test_full_lifecycle_restores_exact_spec_and_releases_latch(tmp_path: Path) -> None:
    """Run the callback under suppression and retain exactly eight final requests."""
    runner, cluster, observer = setup(tmp_path)

    def callback() -> None:
        """Inspect only actual current deployment settings at the investigation boundary."""
        env = object_items(
            container(object_value(cluster.documents["processor-adapter"]["spec"]))["env"]
        )
        assert any(item.get("name") == "OTEL_TRACES_SAMPLER" for item in env)

    receipt = runner.run(after_activation=callback)
    assert receipt.activated and receipt.control_verified and receipt.cleanup_verified
    assert receipt.failure is None and receipt.investigation_status == "completed"
    assert observer.stages == ["original", "suppressed", "restored_sampling", "final"]
    assert len(cluster.writes) == 3 and not runner.block_file.exists()
    assert (
        cluster.documents["processor-adapter"]["spec"]
        == cluster.originals["processor-adapter"]["spec"]
    )


@pytest.mark.parametrize("number,after", [(1, False), (1, True), (2, False), (2, True)])
def test_ambiguous_transition_always_restores(tmp_path: Path, number: int, after: bool) -> None:
    """Lost responses cannot hide a known intermediate spec from finally restoration."""
    runner, cluster, _ = setup(tmp_path, number, after)
    receipt = runner.run()
    assert receipt.failure and receipt.cleanup_verified
    assert (
        cluster.documents["processor-adapter"]["spec"]
        == cluster.originals["processor-adapter"]["spec"]
    )
    assert not runner.block_file.exists()


@pytest.mark.parametrize("stage", ["original", "suppressed", "restored_sampling", "final"])
def test_collection_failures_do_not_skip_restoration(tmp_path: Path, stage: Stage) -> None:
    """Unavailable metrics/traces cannot leave a sampler override running after the test."""
    runner, cluster, _ = setup(tmp_path, fail_stage=stage)
    receipt = runner.run()
    assert (
        cluster.documents["processor-adapter"]["spec"]
        == cluster.originals["processor-adapter"]["spec"]
    )
    assert runner.block_file.exists() is (stage == "final")
    assert receipt.failure or receipt.cleanup_failure


def test_keyboard_interrupt_still_restores(tmp_path: Path) -> None:
    """Interruption propagates only after the finally path restores and verifies the runtime."""
    runner, cluster, observer = setup(tmp_path, fail_stage="suppressed")
    observer.interrupt = True
    with pytest.raises(KeyboardInterrupt):
        runner.run()
    assert (
        cluster.documents["processor-adapter"]["spec"]
        == cluster.originals["processor-adapter"]["spec"]
    )
    assert not runner.block_file.exists()


def test_audit_failure_during_cleanup_never_skips_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every cleanup record can fail while the actual original spec still receives its CAS."""
    runner, cluster, _ = setup(tmp_path)
    original_save = runner._save  # pyright: ignore[reportPrivateUsage]

    def failing(directory: Path, receipt: ScenarioReceipt, name: str, data: JsonObject) -> None:
        """Fail disk writes only after the fault/counterfactual has actually run."""
        if receipt.cleanup_started_at is not None:
            raise OSError("fixture disk unavailable")
        original_save(directory, receipt, name, data)

    monkeypatch.setattr(runner, "_save", failing)
    receipt = runner.run()
    assert len(cluster.writes) == 3
    assert (
        cluster.documents["processor-adapter"]["spec"]
        == cluster.originals["processor-adapter"]["spec"]
    )
    assert not receipt.cleanup_verified and receipt.cleanup_failure and runner.block_file.exists()
    another = SamplingHarness(
        tmp_path / "kubeconfig", tmp_path / "another", cluster, runner.observer
    )
    with pytest.raises(CleanupUnverified):
        another.run()


def test_journal_storage_failure_prevents_all_mutations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No fault can begin without durable original and both possible intermediate specs."""
    runner, cluster, _ = setup(tmp_path)
    original_save = runner._save  # pyright: ignore[reportPrivateUsage]

    def failing(directory: Path, receipt: ScenarioReceipt, name: str, data: JsonObject) -> None:
        """The original observation is retained while the journal publish fails."""
        if name == "sampling-journal":
            raise OSError("journal disk unavailable")
        original_save(directory, receipt, name, data)

    monkeypatch.setattr(runner, "_save", failing)
    receipt = runner.run()
    assert receipt.failure and not cluster.writes and not runner.block_file.exists()


def test_generic_integration_cannot_misroute_sampling(tmp_path: Path) -> None:
    """The new CaseId cannot fall through to the older startup/readiness recipes."""
    runner, cluster, _ = setup(tmp_path)
    with pytest.raises(ValueError):
        runner.run("DEP-01")
    with pytest.raises(ValueError):
        LocalScenarioRunner(tmp_path / "kubeconfig", tmp_path / "generic", cluster).run("TELEM-03")
    with pytest.raises(ValueError):
        fault_spec("TELEM-03", object_value(document()["spec"]))


@pytest.mark.parametrize("foreign", [True, False])
def test_unknown_cleanup_state_is_never_overwritten(tmp_path: Path, foreign: bool) -> None:
    """A conflicting UID or spec leaves the blocking latch and does not receive restore CAS."""
    runner, cluster, _ = setup(tmp_path)

    def change() -> None:
        """Represent an external operator changing a resource while investigation runs."""
        if foreign:
            cluster.foreign = True
        else:
            object_value(cluster.documents["processor-adapter"]["spec"])["replicas"] = 2

    receipt = runner.run(after_activation=change)
    assert not receipt.cleanup_verified and runner.block_file.exists()
    assert receipt.cleanup_failure and "restore" in receipt.cleanup_failure


@pytest.mark.parametrize("after", [True, False])
def test_restore_response_failure_retains_uncertainty(tmp_path: Path, after: bool) -> None:
    """Even an applied restore with a lost response remains explicitly unverified."""
    runner, cluster, _ = setup(tmp_path, fail_at=3, after_apply=after)
    receipt = runner.run()
    assert not receipt.cleanup_verified and runner.block_file.exists()
    assert receipt.cleanup_failure and "restore" in receipt.cleanup_failure
    assert len(cluster.writes) == 3
    assert (
        cluster.documents["processor-adapter"]["spec"]
        == cluster.originals["processor-adapter"]["spec"]
    ) is after


def test_callback_failure_aborts_counterfactual_but_still_cleans_up(tmp_path: Path) -> None:
    """Investigation failure is separate from the physical sampling acceptance result."""
    runner, _, observer = setup(tmp_path)

    def fail() -> None:
        """A model or collector may fail while the trusted lifecycle continues."""
        raise ValueError("investigator unavailable")

    receipt = runner.run(after_activation=fail)
    assert receipt.investigation_status == "failed" and receipt.cleanup_verified
    assert observer.stages == ["original", "suppressed", "final"]


@pytest.mark.parametrize("mismatch", ["mode", "time", "identity"])
def test_observer_cannot_supply_wrong_mode_or_prior_measurement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mismatch: str
) -> None:
    """Valid artifact content still has to belong to the runtime collection being performed."""
    runner, cluster, observer = setup(tmp_path)
    collect = observer.collect

    def changed(
        stage: Stage,
        incident: str,
        identities: tuple[PodIdentity, PodIdentity],
        ready_at: datetime,
        directory: Path,
        store: ArtifactStore,
    ) -> SamplingObservation:
        """Change the boundary receipt before the first mutation is authorized."""
        result = collect(stage, incident, identities, ready_at, directory, store)
        if mismatch == "identity":
            return result.model_copy(
                update={
                    "processor_identity": identities[1].model_copy(
                        update={"container_id": "foreign-container"}
                    )
                }
            )
        traffic = result.traffic.model_copy(
            update={"mode": "local_kind"}
            if mismatch == "mode"
            else {"started_at": ready_at - timedelta(seconds=1)}
        )
        return result.model_copy(update={"traffic": traffic})

    monkeypatch.setattr(observer, "collect", changed)
    receipt = runner.run()
    assert receipt.failure and "readiness" in receipt.failure
    assert not cluster.writes and not runner.block_file.exists()


def test_latch_release_failure_retains_explicit_blocking_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A healthy saved receipt cannot override the canonical latch after unlink fails."""
    runner, cluster, _ = setup(tmp_path)
    unlink = Path.unlink

    def fail(path: Path, missing_ok: bool = False) -> None:
        """Only the coordination latch is made unavailable for release."""
        if path == runner.block_file:
            raise OSError("latch release denied")
        unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail)
    receipt = runner.run()
    assert not receipt.cleanup_verified and receipt.cleanup_failure
    assert "latch release" in receipt.cleanup_failure
    blocked = ScenarioReceipt.model_validate_json(runner.block_file.read_text(encoding="utf-8"))
    assert blocked == receipt
    assert (
        cluster.documents["processor-adapter"]["spec"]
        == cluster.originals["processor-adapter"]["spec"]
    )


def test_final_receipt_disk_failure_keeps_latch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Original restoration remains real even when its final receipt cannot be persisted."""
    runner, cluster, _ = setup(tmp_path)
    original_open = Path.open

    def fail(path: Path, *args: Any, **kwargs: Any) -> Any:
        """Only the final receipt publish fails, after every recovery operation."""
        if path.name == "receipt.json":
            raise OSError("final receipt disk unavailable")
        return cast(Any, original_open(path, *args, **kwargs))

    monkeypatch.setattr(Path, "open", fail)
    receipt = runner.run()
    assert not receipt.cleanup_verified and receipt.cleanup_failure
    assert "receipt persistence" in receipt.cleanup_failure and runner.block_file.exists()
    assert len(cluster.writes) == 3


def test_latch_update_failure_preserves_original_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even failure to write detailed recovery status cannot remove the existing reserved lock."""
    runner, cluster, _ = setup(tmp_path, fail_stage="final")
    write = Path.write_text

    def fail(path: Path, *args: Any, **kwargs: Any) -> Any:
        """The initial exclusive latch creation succeeds; only the final update fails."""
        if path == runner.block_file:
            raise OSError("latch disk unavailable")
        return write(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", fail)
    with pytest.raises(CleanupUnverified, match="latch persistence"):
        runner.run()
    assert runner.block_file.exists() and len(cluster.writes) == 3


def test_timing_bounds_reject_unbounded_operator_parameters(tmp_path: Path) -> None:
    """Trusted constructor parameters cannot silently widen the frozen runtime allowance."""
    _, cluster, observer = setup(tmp_path)
    for timeout, poll in ((91, 2), (90, 3), (0, 1)):
        with pytest.raises(ValueError):
            SamplingHarness(
                tmp_path / "kubeconfig", tmp_path / "other", cluster, observer, timeout, poll
            )
