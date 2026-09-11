"""Native LangGraph checkpoints integrate durable fixture reasoning without hidden recollection."""

from pathlib import Path

import pytest
from test_context import item
from test_reasoning_loop import Harness, read_decision
from test_reasoning_loop import harness as harness

from payops.contracts import Incident
from payops.evidence.artifacts import ArtifactStore
from payops.orchestrator import nodes
from payops.orchestrator.graph import InvestigationWorker
from payops.orchestrator.graph_reasoning import ReasonerFactory
from payops.orchestrator.loop import ReasoningLoop
from payops.orchestrator.state import InvestigationBudget, InvestigationState
from payops.tools.collect import Collection


class GraphHarness:
    """Use the graph's actual incident artifact directory with real fixture transports and SQL."""

    def __init__(self, fixture: Harness) -> None:
        """The fixture actor and model remain bound in trusted factory construction."""
        self.fixture = fixture
        self.collections = 0

    def collect(self, incident: Incident, output: Path) -> Collection:
        """Actual source artifacts are collected once before the native ranking checkpoint."""
        self.collections += 1
        h = self.fixture
        h.store = ArtifactStore(output / "artifacts")
        h.initial, h.additional = item(h.store, text="initial"), item(h.store, text="read result")
        h.responses(read_decision(), h.finish(h.additional))
        return Collection(incident_id=incident.incident_id, evidence=(h.initial,), failures=())

    def factory(self, state: InvestigationState, store: ArtifactStore) -> ReasoningLoop:
        """Supply explicit remaining allowances and the graph's verified artifact store."""
        h = self.fixture
        return ReasoningLoop(
            h.root / "runs",
            store,
            h.ledger,
            h.runtime,
            h.factory,
            subject="actor",
            causes=h.loop.causes,
            limits=h.limits,
        )

    def worker(self, *, pause: bool = False, profile: str = "fixture-v1") -> InvestigationWorker:
        """Model mode and profile are durable host configuration, never checkpoint overrides."""
        return InvestigationWorker(
            self.fixture.root / "graph",
            self.collect,
            pause_before_ranking=pause,
            mode="fixture_replay",
            reasoner_factory=self.factory,
            ranking_profile=profile,
            ranking_method="model_fixture",
        )


def allowance() -> InvestigationBudget:
    """Fixed collection consumes20 logical/30 backend reads before model-selected reads."""
    return InvestigationBudget(max_steps=6, max_tool_calls=24, max_backend_reads=38)


def test_native_pause_resume_uses_model_evidence_and_reports_method(harness: Harness) -> None:
    """A fresh worker resumes ranking and retains receipts, selected evidence and exact charges."""
    h, graph = harness, GraphHarness(harness)
    paused = graph.worker(pause=True).start(h.incident, allowance())
    assert paused.phase == "EVIDENCE_COLLECTED" and not h.adapter.calls
    result = graph.worker().resume("incident")
    assert result.report is not None and result.report.terminal_state == "ESCALATED"
    assert result.report.ranking_method == "model_fixture"
    assert result.report.reasoning_stop_reason == "FINISHED"
    assert len(result.report.reasoning_receipts) == 3
    assert result.report.ranked_root_causes[0].supporting_evidence_ids == (
        h.additional.evidence_id,
    )
    assert result.tool_calls_reserved == 21 and result.backend_reads_reserved == 31
    assert graph.collections == 1 and len(h.adapter.calls) == 2 and len(h.read_calls) == 1
    assert graph.worker().resume("incident") == result


def test_crash_after_loop_completion_replays_without_dispatch(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A completed loop survives loss of the enclosing graph-node checkpoint."""
    h, graph = harness, GraphHarness(harness)
    original = nodes.reason

    def crash(
        state: InvestigationState, store: ArtifactStore, factory: ReasonerFactory
    ) -> InvestigationState:
        """Lose only the graph update after the actual SQL and artifact publications finish."""
        original(state, store, factory)
        raise RuntimeError("lost graph checkpoint")

    with monkeypatch.context() as patch:
        patch.setattr(nodes, "reason", crash)
        with pytest.raises(RuntimeError, match="lost graph checkpoint"):
            graph.worker().start(h.incident, allowance())
    assert len(h.adapter.calls) == 2 and len(h.read_calls) == 1
    resumed = graph.worker().resume("incident")
    assert resumed.report is not None and resumed.report.reasoning_stop_reason == "FINISHED"
    assert len(h.adapter.calls) == 2 and len(h.read_calls) == 1 and graph.collections == 1
    assert resumed.tool_calls_reserved == 21


@pytest.mark.parametrize("fault", ["corrupt", "allowance", "mode"])
def test_model_security_failure_retains_selected_method(harness: Harness, fault: str) -> None:
    """Rejected model paths remain in their model evaluation denominator with no fake baseline."""
    h, graph = harness, GraphHarness(harness)
    graph.worker(pause=True).start(h.incident, allowance())
    if fault == "corrupt":
        h.store.path_for(h.initial.artifact_sha256).write_bytes(b"corrupt")
    elif fault == "allowance":
        h.limits = h.limits.model_copy(update={"tool_calls": 5})
    else:
        h.runtime.settings = h.runtime.settings.model_copy(update={"mode": "provider"})
    result = graph.worker().resume("incident")
    assert result.report is not None and result.report.terminal_state == "SECURITY_BLOCK"
    assert result.report.ranking_method == "model_fixture"
    assert result.report.reasoning_stop_reason == "SECURITY_BLOCK"
    assert not result.hypotheses and not h.adapter.calls and not h.read_calls


def test_changed_model_profile_cannot_resume_existing_checkpoint(harness: Harness) -> None:
    """A new host profile cannot switch a saved incident to another ranking implementation."""
    graph = GraphHarness(harness)
    graph.worker(pause=True).start(harness.incident, allowance())
    with pytest.raises(ValueError, match="ranking profile"):
        graph.worker(profile="fixture-v2").resume("incident")
    assert not harness.adapter.calls
