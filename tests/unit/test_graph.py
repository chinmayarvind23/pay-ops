"""Durable investigation transitions preserve evidence, budgets and incident identity."""

from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest
from filelock import FileLock, Timeout
from test_payment_window import interval, raw_snapshot, source

from payops.contracts import Incident, IncidentCreate, RootCauseHypothesis, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.evidence.payment_window import derive_payment_window
from payops.orchestrator import nodes
from payops.orchestrator.graph import InvestigationWorker
from payops.orchestrator.nodes import incident_directory, route
from payops.orchestrator.state import InvestigationBudget, InvestigationState, pack
from payops.tools.collect import Collection


def collector(calls: list[str]) -> Callable[[Incident, Path], Collection]:
    """Record collection invocation independently of graph state to detect accidental replay."""

    def collect(incident: Incident, _output: Path) -> Collection:
        """An empty but valid collection must end with insufficient evidence, not a diagnosis."""
        calls.append(incident.incident_id)
        return Collection(incident_id=incident.incident_id, evidence=(), failures=())

    return collect


def test_native_checkpoint_resume_does_not_recollect(tmp_path: Path) -> None:
    """Reconstruct a worker and database connection after a real LangGraph pause."""
    calls: list[str] = []
    incident = Incident(request=IncidentCreate(title="Synthetic payment alert"))
    first = InvestigationWorker(tmp_path, collector(calls), pause_before_ranking=True)
    paused = first.start(incident)
    assert paused.phase == "EVIDENCE_COLLECTED"
    assert paused.report is None
    second = InvestigationWorker(tmp_path, collector(calls))
    completed = second.resume(incident.incident_id)
    assert completed.report is not None
    assert completed.report.terminal_state == "EVIDENCE_INSUFFICIENT"
    assert calls == [incident.incident_id]
    assert second.start(incident) == completed
    assert second.resume(incident.incident_id) == completed
    assert len(second.history(incident.incident_id)) >= 6


@pytest.mark.parametrize(
    "limit,expected", [(33, "BUDGET_EXHAUSTED"), (34, "EVIDENCE_INSUFFICIENT")]
)
def test_payment_backend_reservation_precedes_collection(
    tmp_path: Path, limit: int, expected: str
) -> None:
    """Twenty logical calls cannot conceal34 backend reads or replenish them on resume."""
    calls: list[str] = []
    incident = Incident(request=IncidentCreate(title="Alert"))
    worker = InvestigationWorker(
        tmp_path, collector(calls), collection_profile="payment_windows_v1"
    )
    state = worker.start(incident, InvestigationBudget(max_backend_reads=limit))
    assert state.report is not None and state.report.terminal_state == expected
    assert len(calls) == (1 if limit == 34 else 0)
    assert state.backend_reads_reserved == (34 if limit == 34 else 0)
    assert worker.resume(incident.incident_id) == state
    with pytest.raises(ValueError, match="collection profile"):
        InvestigationWorker(tmp_path, collector(calls)).resume(incident.incident_id)


def test_legacy_checkpoint_cannot_dispatch_without_backend_reservation(tmp_path: Path) -> None:
    """A pre-budget checkpoint may retain evidence, but cannot authorize new unreserved reads."""
    calls: list[str] = []
    incident = Incident(request=IncidentCreate(title="Alert"))
    state = InvestigationState(
        incident=incident,
        budget=InvestigationBudget(),
        phase="READS_RESERVED",
        tool_calls_reserved=20,
    )
    result = nodes.unpack(nodes.InvestigationNodes(tmp_path, collector(calls)).collect(pack(state)))
    assert result.terminal == "BUDGET_EXHAUSTED" and not calls


@pytest.mark.parametrize("limit", ["steps", "tools", "deadline"])
def test_exhausted_budget_never_collects(tmp_path: Path, limit: str) -> None:
    """Changing a restart or thread ID cannot reset the persisted investigation budget."""
    calls: list[str] = []
    incident = Incident(request=IncidentCreate(title="Alert"))
    budget = InvestigationBudget(
        max_steps=0 if limit == "steps" else 4,
        max_tool_calls=0 if limit == "tools" else 20,
        node_start_deadline=utc_now() + timedelta(seconds=-1 if limit == "deadline" else 60),
    )
    worker = InvestigationWorker(tmp_path, collector(calls))
    state = worker.start(incident, budget)
    assert state.report is not None
    assert state.report.terminal_state == "BUDGET_EXHAUSTED"
    assert not calls
    assert worker.start(incident) == state


def test_scope_conflict_and_unknown_resume_fail(tmp_path: Path) -> None:
    """No thread override or foreign namespace can silently reuse a trusted collection binding."""
    calls: list[str] = []
    worker = InvestigationWorker(tmp_path, collector(calls))
    incident = Incident(request=IncidentCreate(title="Foreign", namespace="other"))
    state = worker.start(incident)
    assert state.report is not None and state.report.terminal_state == "SECURITY_BLOCK"
    assert not calls
    with pytest.raises(ValueError, match="different incident"):
        worker.start(
            Incident(incident_id=incident.incident_id, request=IncidentCreate(title="New"))
        )
    with pytest.raises(KeyError):
        worker.resume("unknown")


def test_expected_collection_failure_is_durable(tmp_path: Path) -> None:
    """A source outage becomes an explicit terminal report, with no sensitive error text."""

    def collect(_incident: Incident, _output: Path) -> Collection:
        """Simulate a source exception which must not reach user-visible audit output."""
        raise OSError("private credential detail")

    worker = InvestigationWorker(tmp_path, collect)
    incident = Incident(request=IncidentCreate(title="Unavailable"))
    result = worker.start(incident)
    assert result.report is not None
    assert result.report.terminal_state == "DEPENDENCY_UNAVAILABLE"
    assert "private credential" not in result.model_dump_json()
    assert worker.resume(incident.incident_id) == result


def test_crash_after_dispatch_is_not_retried(tmp_path: Path) -> None:
    """At-most-once collection sacrifices uncertain evidence instead of exceeding its budget."""
    calls: list[str] = []

    def crash(incident: Incident, _output: Path) -> Collection:
        """A worker fault after external reads is ambiguous and must not cause another batch."""
        calls.append(incident.incident_id)
        raise RuntimeError("worker stopped")

    worker = InvestigationWorker(tmp_path, crash)
    incident = Incident(request=IncidentCreate(title="Interrupted"))
    with pytest.raises(RuntimeError, match="worker stopped"):
        worker.start(incident)
    resumed = InvestigationWorker(tmp_path, collector(calls)).resume(incident.incident_id)
    assert resumed.report is not None
    assert resumed.report.terminal_state == "DEPENDENCY_UNAVAILABLE"
    assert resumed.tool_calls_reserved == 20
    assert len(calls) == 1


def test_retained_batch_recovers_after_checkpoint_boundary_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Completed source output survives a crash before LangGraph commits the next checkpoint."""
    calls: list[str] = []
    incident = Incident(request=IncidentCreate(title="Retained output"))
    worker = InvestigationWorker(tmp_path, collector(calls))

    def crash(_state: InvestigationState, _result: Collection) -> InvestigationState:
        """Interrupt exactly after the durable batch file was closed."""
        raise RuntimeError("before checkpoint")

    monkeypatch.setattr(worker.nodes, "_collected", crash)
    with pytest.raises(RuntimeError):
        worker.start(incident, InvestigationBudget(max_steps=6))
    result = InvestigationWorker(tmp_path, collector(calls)).resume(incident.incident_id)
    assert result.report is not None and result.report.terminal_state == "EVIDENCE_INSUFFICIENT"
    assert len(calls) == 1


def test_artifact_changed_after_pause_is_blocked(tmp_path: Path) -> None:
    """A checkpoint never grants permanent trust to bytes that were valid before restart."""
    paths: list[Path] = []

    def collect(incident: Incident, output: Path) -> Collection:
        """Retain a real artifact then let the test corrupt it after collection checkpoints."""
        store = ArtifactStore(output / "artifacts")
        observed = utc_now()
        item = normalize(
            Observation(
                source="LOG",
                resource="payments-api",
                observed_at=observed,
                query="test",
                summary="healthy",
                payload={"lines": "healthy"},
            ),
            incident.incident_id,
            observed - timedelta(seconds=1),
            observed + timedelta(seconds=1),
            store,
        )
        paths.extend((output / "artifacts").glob("*.json"))
        return Collection(incident_id=incident.incident_id, evidence=(item,), failures=())

    incident = Incident(request=IncidentCreate(title="Corruption"))
    worker = InvestigationWorker(tmp_path, collect, pause_before_ranking=True)
    assert worker.start(incident).phase == "EVIDENCE_COLLECTED"
    assert paths
    paths[0].write_text("changed")
    result = InvestigationWorker(tmp_path, collect).resume(incident.incident_id)
    assert result.report is not None and result.report.terminal_state == "SECURITY_BLOCK"


def test_nested_payment_artifact_changed_after_pause_is_blocked(tmp_path: Path) -> None:
    """Changed source bytes invalidate a derived envelope after checkpointing."""
    paths: list[Path] = []

    def collect(incident: Incident, output: Path) -> Collection:
        """Build a complete payment window from real retained fixture artifacts."""
        store = ArtifactStore(output / "artifacts")
        period = interval().model_copy(update={"incident_id": incident.incident_id})
        first = source(store, period, raw_snapshot(period))
        last = source(store, period, raw_snapshot(period, True))
        item = derive_payment_window(first, last, period, store)
        paths.append(output / "artifacts" / f"{first.artifact_sha256}.json")
        return Collection(incident_id=incident.incident_id, evidence=(item,), failures=())

    incident = Incident(request=IncidentCreate(title="Payment corruption"))
    worker = InvestigationWorker(tmp_path, collect, pause_before_ranking=True)
    assert worker.start(incident).phase == "EVIDENCE_COLLECTED"
    paths[0].write_text("tampered")
    result = InvestigationWorker(tmp_path, collect).resume(incident.incident_id)
    assert result.report is not None and result.report.terminal_state == "SECURITY_BLOCK"


def test_cross_incident_batch_never_enters_checkpoint(tmp_path: Path) -> None:
    """A transport cannot attach another incident's collection to a valid workflow."""

    def collect(_incident: Incident, _output: Path) -> Collection:
        """Return a structurally valid but foreign collection identity."""
        return Collection(incident_id="foreign", evidence=(), failures=())

    state = InvestigationWorker(tmp_path, collect).start(
        Incident(request=IncidentCreate(title="Alert"))
    )
    assert state.report is not None and state.report.terminal_state == "SECURITY_BLOCK"
    assert not state.evidence


def test_duplicate_worker_lock_and_illegal_transition(tmp_path: Path) -> None:
    """Contention fails before execution, and unknown phase edges cannot skip lifecycle gates."""
    calls: list[str] = []
    incident = Incident(request=IncidentCreate(title="Locked"))
    worker = InvestigationWorker(tmp_path, collector(calls))
    lock_path = incident_directory(tmp_path, incident.incident_id).with_suffix(".lock")
    with FileLock(lock_path, timeout=0), pytest.raises(Timeout):
        worker.start(incident)
    assert not calls
    initial = InvestigationState(incident=incident, budget=InvestigationBudget())
    with pytest.raises(ValueError, match="illegal"):
        route(pack(initial))
    assert incident_directory(tmp_path, "..").parent == tmp_path


def test_rank_crashes_consume_attempt_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unexpected worker failures cannot replenish the step allowance on repeated resume."""
    rank_calls: list[int] = []

    def crash(*_args: object) -> tuple[RootCauseHypothesis, ...]:
        """Stop before returning the rank node update, after its attempt was durably reserved."""
        rank_calls.append(1)
        raise RuntimeError("rank worker stopped")

    monkeypatch.setattr(nodes, "rank_evidence", crash)
    incident = Incident(request=IncidentCreate(title="Rank interrupted"))
    worker = InvestigationWorker(tmp_path, collector([]))
    with pytest.raises(RuntimeError):
        worker.start(incident, InvestigationBudget(max_steps=5))
    completed: list[InvestigationState] = []
    for _ in range(3):
        try:
            completed.append(worker.resume(incident.incident_id))
        except RuntimeError:
            pass
    assert len(rank_calls) == 2
    assert completed
    result = completed[-1]
    assert result.steps_used == 5
    assert result.report is not None and result.report.terminal_state == "BUDGET_EXHAUSTED"


def test_fixture_mode_survives_restart_and_cannot_be_relabeled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A replay cannot become a live result by reconstructing its worker with another mode."""

    def rank(*_args: object) -> tuple[RootCauseHypothesis, ...]:
        """Isolate the graph's escalation path; diagnosis semantics have separate evidence tests."""
        return (RootCauseHypothesis(cause_code="FIXTURE_CAUSE", confidence=0.5),)

    monkeypatch.setattr(nodes, "rank_evidence", rank)
    incident = Incident(request=IncidentCreate(title="Replay"))
    worker = InvestigationWorker(
        tmp_path, collector([]), mode="fixture_replay", pause_before_ranking=True
    )
    assert worker.start(incident).mode == "fixture_replay"
    with pytest.raises(ValueError, match="mode"):
        InvestigationWorker(tmp_path, collector([])).resume(incident.incident_id)
    result = InvestigationWorker(tmp_path, collector([]), mode="fixture_replay").resume(
        incident.incident_id
    )
    assert result.report is not None and result.report.mode == "fixture_replay"
    assert result.report.terminal_state == "ESCALATED"
