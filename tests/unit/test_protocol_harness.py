"""Two-resource recovery survives ambiguous writes, observation failures and unavailable audit."""

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from test_protocol_observation import fixture
from test_sampling_harness import Clock, SamplingCluster

from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import DeploymentName, JsonObject, ScenarioReceipt, object_value
from payops.scenarios.protocol import ProtocolHarness
from payops.scenarios.protocol_contract import ProtocolStage
from payops.scenarios.protocol_observation import ProtocolObservation
from payops.scenarios.runner import CleanupUnverified


class Cluster(SamplingCluster):
    """Synthetic Kubernetes documents actually change spec and process identity on each write."""

    def __init__(self, clock: Clock, fail_at: int = 0, after_apply: bool = False) -> None:
        """One selected write may fail either side of its application boundary."""
        super().__init__(clock, fail_at, after_apply)
        self.targets: list[str] = []

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """Check the complete journal and CAS predicate before changing either target."""
        assert name in {"payments-api", "risk-sim"} and self.journal_root is not None
        assert len(tuple(self.journal_root.glob("*/*-protocol-journal.json"))) == 1
        current = self.documents[name]
        assert expected["spec"] == current["spec"]
        assert object_value(expected["metadata"])["uid"] == object_value(current["metadata"])["uid"]
        self.writes.append(deepcopy(spec))
        self.targets.append(name)
        failed = len(self.writes) == self.fail_at
        if failed and not self.after_apply:
            raise OSError("fixture write rejected")
        current["spec"] = deepcopy(spec)
        metadata = object_value(current["metadata"])
        generation = int(str(metadata["generation"])) + 1
        metadata.update(generation=generation, resourceVersion=str(generation))
        object_value(current["status"])["observedGeneration"] = generation
        self.created[name] = self.clock.now().isoformat()
        if failed:
            raise OSError("fixture response lost after apply")

    def risk_log(self, identity: PodIdentity, start: datetime) -> JsonObject:
        """The fixture observer publishes its own access source; no network read occurs."""
        raise AssertionError("unexpected runtime log call in fixture")


class Observer:
    """Real evidence verification remains enabled while only collection transport is replaced."""

    def __init__(self, clock: Clock, fail_stage: ProtocolStage | None = None) -> None:
        """Inject one phase failure without suppressing finally-stage observation."""
        self.clock, self.fail_stage = clock, fail_stage
        self.stages: list[ProtocolStage] = []
        self.interrupt = False

    def collect(
        self,
        stage: ProtocolStage,
        incident: str,
        identities: dict[str, PodIdentity],
        directory: Path,
        store: ArtifactStore,
    ) -> ProtocolObservation:
        """Known source times progress beyond each capture before the next rollout request."""
        self.stages.append(stage)
        if stage == self.fail_stage:
            if self.interrupt:
                raise KeyboardInterrupt("fixture cancellation")
            raise ValueError("fixture observation unavailable")
        start = self.clock.now() + timedelta(seconds=1)
        result = fixture(store, stage, start, identities, incident)
        self.clock.value = start + timedelta(seconds=15)
        return result


def setup(
    tmp_path: Path,
    fail_at: int = 0,
    after_apply: bool = False,
    fail_stage: ProtocolStage | None = None,
) -> tuple[ProtocolHarness, Cluster, Observer]:
    """Each fixture owns an isolated kubeconfig sibling latch and artifact tree."""
    clock = Clock()
    cluster, observer = Cluster(clock, fail_at, after_apply), Observer(clock, fail_stage)
    cluster.journal_root = tmp_path / "runs"
    return (
        ProtocolHarness(
            tmp_path / "kubeconfig", cluster.journal_root, cluster, observer, 0.1, 0.001, clock.now
        ),
        cluster,
        observer,
    )


def restored(cluster: Cluster) -> bool:
    """Both exact originals are required; matching only the last restore is insufficient."""
    return all(
        cluster.documents[name]["spec"] == cluster.originals[name]["spec"]
        for name in ("payments-api", "risk-sim")
    )


def test_full_protocol_lifecycle_and_exact_two_resource_cleanup(tmp_path: Path) -> None:
    """Real fixture artifacts must prove all controls while the journals gate four CAS writes."""
    runner, cluster, observer = setup(tmp_path)
    receipt = runner.run()
    assert receipt.activated and receipt.control_verified and receipt.cleanup_verified
    assert not receipt.failure and not receipt.cleanup_failure
    assert observer.stages == ["original", "mismatch", "matched", "final"]
    assert cluster.targets == ["risk-sim", "payments-api", "payments-api", "risk-sim"]
    assert restored(cluster) and not runner.block_file.exists()


@pytest.mark.parametrize("write,after", [(1, False), (1, True), (2, False), (2, True)])
def test_ambiguous_forward_write_always_restores_both(
    tmp_path: Path, write: int, after: bool
) -> None:
    """A lost response cannot conceal a known v2 state from finally restoration."""
    runner, cluster, _ = setup(tmp_path, write, after)
    receipt = runner.run()
    assert receipt.failure and receipt.cleanup_verified and restored(cluster)
    assert not runner.block_file.exists()


@pytest.mark.parametrize("write,after", [(3, False), (3, True), (4, False), (4, True)])
def test_failed_restore_still_attempts_other_resource(
    tmp_path: Path, write: int, after: bool
) -> None:
    """Both restore operations occur even if the first response fails ambiguously."""
    runner, cluster, _ = setup(tmp_path, write, after)
    receipt = runner.run()
    assert cluster.targets[-2:] == ["payments-api", "risk-sim"]
    assert not receipt.cleanup_verified and receipt.cleanup_failure and runner.block_file.exists()
    other = "risk-sim" if write == 3 else "payments-api"
    assert cluster.documents[other]["spec"] == cluster.originals[other]["spec"]
    assert restored(cluster) is after


@pytest.mark.parametrize("stage", ["original", "mismatch", "matched", "final"])
def test_observation_failures_restore_known_states(tmp_path: Path, stage: ProtocolStage) -> None:
    """Missing wire/trace proof never becomes a qualified case or skips physical recovery."""
    runner, cluster, _ = setup(tmp_path, fail_stage=stage)
    receipt = runner.run()
    assert restored(cluster) and (receipt.failure or receipt.cleanup_failure)
    assert runner.block_file.exists() is (stage == "final")


def test_interruption_restores_before_propagating(tmp_path: Path) -> None:
    """Even a BaseException during the mismatch probe restores both resources."""
    runner, cluster, observer = setup(tmp_path, fail_stage="mismatch")
    observer.interrupt = True
    with pytest.raises(KeyboardInterrupt):
        runner.run()
    assert restored(cluster) and not runner.block_file.exists()


def test_every_cleanup_save_can_fail_without_skipping_a_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disk errors remain explicit, while both actual CAS calls and health verification run."""
    runner, cluster, observer = setup(tmp_path)
    original = runner._save  # pyright: ignore[reportPrivateUsage]

    def save(directory: Path, receipt: ScenarioReceipt, name: str, value: JsonObject) -> None:
        """Fail only after both forward mutations have completed."""
        if receipt.cleanup_started_at is not None:
            raise OSError("fixture cleanup disk failed")
        original(directory, receipt, name, value)

    monkeypatch.setattr(runner, "_save", save)
    receipt = runner.run()
    assert cluster.targets[-2:] == ["payments-api", "risk-sim"] and restored(cluster)
    assert observer.stages[-1] == "final" and not receipt.cleanup_verified
    assert runner.block_file.exists() and receipt.cleanup_failure
    another = ProtocolHarness(tmp_path / "kubeconfig", tmp_path / "another", cluster, observer)
    with pytest.raises(CleanupUnverified):
        another.run()


def test_journal_failure_prevents_fault_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful control does not authorize a fault until both complete originals persist."""
    runner, cluster, _ = setup(tmp_path)
    original = runner._save  # pyright: ignore[reportPrivateUsage]

    def save(directory: Path, receipt: ScenarioReceipt, name: str, value: JsonObject) -> None:
        """The journal alone is unavailable; all earlier control artifacts remain real."""
        if name == "protocol-journal":
            raise OSError("fixture journal unavailable")
        original(directory, receipt, name, value)

    monkeypatch.setattr(runner, "_save", save)
    receipt = runner.run()
    assert receipt.failure and not cluster.targets and not runner.block_file.exists()
