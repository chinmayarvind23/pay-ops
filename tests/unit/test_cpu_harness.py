"""Five-stage CPU recovery is tested with real source verification and API-shaped fixtures."""

from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from test_cpu_sources import source
from test_protocol_observation import fixture
from test_sampling_harness import Clock, SamplingCluster

from payops.contracts import EvidenceItem
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.trace_span import PodIdentity, publish_trace_log, verify_trace_log
from payops.scenarios.contracts import DeploymentName, JsonObject, object_value
from payops.scenarios.cpu_contract import CpuStage
from payops.scenarios.cpu_harness import CpuHarness
from payops.scenarios.cpu_observer import CpuObservation
from payops.scenarios.cpu_sources import CpuGateway
from payops.scenarios.protocol_observation import ProtocolObservation


class Cluster(SamplingCluster):
    """Actual fixture CAS changes process identity, including lost responses after apply."""

    def replace_spec(self, name: DeploymentName, expected: JsonObject, spec: JsonObject) -> None:
        """Every mutation requires the complete journal and exact current expected spec."""
        assert name == "payments-api" and self.journal_root is not None
        assert len(tuple(self.journal_root.glob("*/*-cpu-journal.json"))) == 1
        current = self.documents[name]
        assert expected["spec"] == current["spec"] and expected["metadata"] == current["metadata"]
        self.writes.append(deepcopy(spec))
        failed = len(self.writes) == self.fail_at
        if failed and not self.after_apply:
            raise OSError("rejected before apply")
        current["spec"] = deepcopy(spec)
        metadata = object_value(current["metadata"])
        generation = int(str(metadata["generation"])) + 1
        metadata.update(generation=generation, resourceVersion=str(generation))
        object_value(current["status"])["observedGeneration"] = generation
        self.created[name] = self.clock.now().isoformat()
        if failed:
            raise OSError("response lost after apply")


def path_fixture(
    store: ArtifactStore, clock: Clock, identities: dict[str, PodIdentity], incident: str
) -> ProtocolObservation:
    """Extend the known valid path's observation window to accommodate real-work duration."""
    path = fixture(store, "original", clock.now() + timedelta(seconds=1), identities, incident)
    sources: list[EvidenceItem] = []
    for item in path.capture.sources:
        log = verify_trace_log(item, store)
        records = tuple(
            record.model_copy(
                update={
                    "span": record.span.model_copy(
                        update={
                            "start_time": path.probe.started_at
                            + timedelta(
                                seconds=0 if record.span.name == "sandbox.payments" else 2.1
                            ),
                            "end_time": path.probe.started_at + timedelta(seconds=2.5),
                        }
                    ),
                    "log_start": path.probe.started_at + timedelta(seconds=4),
                    "log_end": path.probe.started_at + timedelta(seconds=4),
                }
            )
            for record in log.parsed.spans
        )
        log = log.model_copy(
            update={
                "parsed": log.parsed.model_copy(update={"spans": records}),
                "scope": log.scope.model_copy(update={"end": log.scope.end + timedelta(seconds=2)}),
                "captured_start": log.captured_start + timedelta(seconds=2),
                "captured_end": log.captured_end + timedelta(seconds=2),
            }
        )
        sources.append(publish_trace_log(log, store))
    clock.value += timedelta(seconds=18)
    return path.model_copy(
        update={
            "probe": path.probe.model_copy(
                update={"completed_at": path.probe.completed_at + timedelta(seconds=2)}
            ),
            "capture": path.capture.model_copy(update={"sources": tuple(sources)}),
        }
    )


class Observer:
    """Only transport is replaced; production path and kernel semantic verifiers stay enabled."""

    def __init__(self, clock: Clock, fail: CpuStage | None = None) -> None:
        """Select one failing phase without suppressing recovery observations."""
        self.clock, self.fail = clock, fail
        self.stages: list[CpuStage] = []
        self.interrupt = False

    def collect(
        self,
        stage: CpuStage,
        incident: str,
        identities: dict[str, PodIdentity],
        directory: Path,
        store: ArtifactStore,
        original: JsonObject,
        payments: JsonObject,
        risk: JsonObject,
    ) -> CpuObservation:
        """Return three independently sourced successful requests with known kernel deltas."""
        self.stages.append(stage)
        if stage == self.fail:
            if self.interrupt:
                raise KeyboardInterrupt("cancelled observation")
            raise ValueError("observation failed")
        paths: list[ProtocolObservation] = []
        logs: list[str] = []
        for _ in range(3):
            path = path_fixture(store, self.clock, identities, incident)
            paths.append(path)
            if stage in {"original", "final"}:
                logs.append("")
                continue
            row, _ = source()
            start = path.probe.started_at
            restricted = stage == "restricted"
            quota = "10000 100000\n" if restricted else "50000 100000\n"
            row = row.model_copy(
                update={
                    "sample_id": path.probe.sample.sample_id,
                    "wall_seconds": 1.6 if restricted else 0.25,
                    "before": row.before.model_copy(
                        update={
                            "started_at": start,
                            "completed_at": start + timedelta(milliseconds=1),
                            "cpu_max": quota,
                        }
                    ),
                    "after": row.after.model_copy(
                        update={
                            "started_at": start + timedelta(seconds=2),
                            "completed_at": start + timedelta(seconds=2.001),
                            "cpu_max": quota,
                            "cpu_stat": row.after.cpu_stat
                            if restricted
                            else row.after.cpu_stat.replace("1500500", "100500"),
                        }
                    ),
                }
            )
            logs.append(f"{(start + timedelta(seconds=2.1)).isoformat()} {row.model_dump_json()}\n")
        return CpuObservation(paths=tuple(paths), logs=tuple(logs))


def setup(
    tmp_path: Path, write: int = 0, after: bool = False, fail: CpuStage | None = None
) -> tuple[CpuHarness, Cluster, Observer]:
    """Each test owns its latch and artifact root; it cannot access a real cluster."""
    clock = Clock()
    cluster, observer = Cluster(clock, write, after), Observer(clock, fail)
    cluster.journal_root = tmp_path / "runs"
    runner = CpuHarness(
        tmp_path / "kubeconfig",
        cluster.journal_root,
        cast(CpuGateway, cluster),
        observer,
        0.1,
        0.001,
        clock.now,
    )
    return runner, cluster, observer


def test_complete_lifecycle(tmp_path: Path) -> None:
    """Fifteen fresh full paths and both quota contrasts precede successful exact cleanup."""
    runner, cluster, observer = setup(tmp_path)
    receipt = runner.run()
    assert receipt.failure is None and receipt.cleanup_failure is None
    assert receipt.activated and receipt.control_verified and receipt.cleanup_verified
    assert observer.stages == ["original", "control", "restricted", "recovered", "final"]
    assert len(cluster.writes) == 4
    assert cluster.documents["payments-api"]["spec"] == cluster.originals["payments-api"]["spec"]
    assert not runner.block_file.exists()


@pytest.mark.parametrize(
    "write,after", [(1, False), (1, True), (2, False), (2, True), (3, False), (3, True)]
)
def test_ambiguous_writes_restore_known_state(tmp_path: Path, write: int, after: bool) -> None:
    """A lost response never changes the set of recognized states allowed for rollback."""
    runner, cluster, _ = setup(tmp_path, write, after)
    receipt = runner.run()
    assert receipt.failure and receipt.cleanup_verified
    assert cluster.documents["payments-api"]["spec"] == cluster.originals["payments-api"]["spec"]
    assert not runner.block_file.exists()


@pytest.mark.parametrize("stage", ["original", "control", "restricted", "recovered", "final"])
def test_failed_observation_still_restores(tmp_path: Path, stage: CpuStage) -> None:
    """Failed final evidence blocks reuse even after the exact deployment spec was restored."""
    runner, cluster, _ = setup(tmp_path, fail=stage)
    receipt = runner.run()
    assert cluster.documents["payments-api"]["spec"] == cluster.originals["payments-api"]["spec"]
    assert receipt.cleanup_verified == (stage != "final")
    assert runner.block_file.exists() == (stage == "final")


def test_foreign_identity_is_not_overwritten(tmp_path: Path) -> None:
    """An external replacement is retained for operator review and leaves the latch blocked."""
    runner, cluster, _ = setup(tmp_path)
    receipt = runner.run(after_activation=lambda: setattr(cluster, "foreign", True))
    assert receipt.cleanup_failure and not receipt.cleanup_verified
    assert len(cluster.writes) == 3 and runner.block_file.exists()


def test_cancellation_restores_before_propagating(tmp_path: Path) -> None:
    """Keyboard interruption retains the same finally restoration guarantee."""
    runner, cluster, observer = setup(tmp_path, fail="restricted")
    observer.interrupt = True
    with pytest.raises(KeyboardInterrupt):
        runner.run()
    assert cluster.documents["payments-api"]["spec"] == cluster.originals["payments-api"]["spec"]
    assert not runner.block_file.exists()


def test_rejected_restore_blocks_reuse(tmp_path: Path) -> None:
    """A rejected final write cannot be reported as recovered just because prior probes passed."""
    runner, cluster, _ = setup(tmp_path, write=4)
    receipt = runner.run()
    assert receipt.cleanup_failure and not receipt.cleanup_verified
    assert runner.block_file.exists()
    assert cluster.documents["payments-api"]["spec"] != cluster.originals["payments-api"]["spec"]


def test_unknown_spec_is_not_overwritten(tmp_path: Path) -> None:
    """Concurrent operator edits invalidate CAS and are never replaced during cleanup."""
    runner, cluster, _ = setup(tmp_path)

    def change() -> None:
        """Introduce a real unexpected spec rather than only altering a response flag."""
        object_value(cluster.documents["payments-api"]["spec"])["replicas"] = 2

    receipt = runner.run(after_activation=change)
    assert receipt.failure and receipt.cleanup_failure and runner.block_file.exists()
    assert object_value(cluster.documents["payments-api"]["spec"])["replicas"] == 2


def test_artifact_store_startup_failure_releases_unused_latch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure before any captured mutation still produces a receipt and releases the latch."""
    runner, cluster, _ = setup(tmp_path)

    def fail(path: Path) -> ArtifactStore:
        """Model an unavailable evidence root at store construction."""
        raise OSError("evidence store unavailable")

    monkeypatch.setattr("payops.scenarios.cpu_harness.ArtifactStore", fail)
    receipt = runner.run()
    assert receipt.failure and not cluster.writes and not runner.block_file.exists()
