"""Model failures and host-profile mismatches cannot become successful baseline reports."""

import pytest
from test_graph_reasoning import GraphHarness, allowance
from test_reasoning_loop import Harness
from test_reasoning_loop import harness as harness

from payops.contracts import (
    EvidenceItem,
    Incident,
    RankingMethod,
    RootCauseHypothesis,
    TerminalState,
)
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.orchestrator.graph import InvestigationWorker
from payops.orchestrator.graph_reasoning import reason, terminal
from payops.orchestrator.loop import LoopResult, ReasoningLoop, StopReason
from payops.orchestrator.state import InvestigationState


@pytest.mark.parametrize(
    "stop,expected",
    [
        ("FINISHED", "EVIDENCE_INSUFFICIENT"),
        ("REFUSED", "EVIDENCE_INSUFFICIENT"),
        ("INVALID_OUTPUT", "EVIDENCE_INSUFFICIENT"),
        ("BUDGET_EXHAUSTED", "BUDGET_EXHAUSTED"),
        ("DENIED", "SECURITY_BLOCK"),
        ("UNKNOWN_COMPLETION", "DEPENDENCY_UNAVAILABLE"),
        ("ERROR", "DEPENDENCY_UNAVAILABLE"),
        ("TIMEOUT", "DEPENDENCY_UNAVAILABLE"),
        ("BUSY", "DEPENDENCY_UNAVAILABLE"),
    ],
)
def test_every_model_stop_has_an_explicit_terminal(
    stop: StopReason, expected: TerminalState
) -> None:
    """Refusal, unknown effects and unavailable providers remain in the failed-run denominator."""
    result = LoopResult(
        run_id="incident",
        mode="fixture",
        stop_reason=stop,
        evidence=(),
        receipt_sha256s=(),
    )
    assert terminal(result) == expected


def test_finished_supported_hypothesis_requires_escalation() -> None:
    """A diagnostic answer does not imply that a remediation ran or the incident resolved."""
    result = LoopResult(
        run_id="incident",
        mode="fixture",
        stop_reason="FINISHED",
        evidence=(),
        receipt_sha256s=(),
        hypotheses=(RootCauseHypothesis(cause_code="dependency_unavailable", confidence=0.7),),
    )
    assert terminal(result) == "ESCALATED"


@pytest.mark.parametrize(
    "model,profile,method",
    [
        (True, "deterministic-v1", "model_fixture"),
        (False, "fixture-v1", "deterministic"),
        (True, "fixture-v1", "deterministic"),
        (False, "deterministic-v1", "model_provider"),
    ],
)
def test_factory_profile_and_method_must_agree(
    harness: Harness, model: bool, profile: str, method: RankingMethod
) -> None:
    """A misleading host label is rejected before collection or model submission."""
    graph = GraphHarness(harness)
    with pytest.raises(ValueError, match="ranking"):
        InvestigationWorker(
            harness.root / "guard",
            graph.collect,
            reasoner_factory=graph.factory if model else None,
            ranking_profile=profile,
            ranking_method=method,
        )
    assert not harness.adapter.calls and graph.collections == 0


def test_saved_method_cannot_switch_even_when_profile_matches(harness: Harness) -> None:
    """The native checkpoint separately binds fixture/provider classification and profile."""
    graph = GraphHarness(harness)
    graph.worker(pause=True).start(harness.incident, allowance())
    changed = InvestigationWorker(
        harness.root / "graph",
        graph.collect,
        mode="fixture_replay",
        reasoner_factory=graph.factory,
        ranking_profile="fixture-v1",
        ranking_method="model_provider",
    )
    with pytest.raises(ValueError, match="ranking method"):
        changed.resume("incident")
    assert not harness.adapter.calls and graph.collections == 1


@pytest.mark.parametrize("boundary", ["backend", "store", "result-incident"])
def test_graph_reasoner_rejects_foreign_host_bindings(
    harness: Harness, boundary: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Independent backend allowance, artifact root and returned incident checks fail closed."""
    h, graph = harness, GraphHarness(harness)
    state = graph.worker(pause=True).start(h.incident, allowance())
    loop = graph.factory(state, h.store)
    if boundary == "backend":
        loop.limits = loop.limits.model_copy(update={"backend_reads": 9})
    elif boundary == "store":
        loop.store = ArtifactStore(h.root / "other-artifacts")
    else:

        def foreign_result(incident: Incident, initial: tuple[EvidenceItem, ...]) -> LoopResult:
            """Return a valid-shaped foreign result without dispatching any transport."""
            return LoopResult(
                run_id="foreign",
                mode="fixture",
                stop_reason="FINISHED",
                evidence=(),
                receipt_sha256s=(),
            )

        monkeypatch.setattr(loop, "run", foreign_result)

    def bound_factory(state: InvestigationState, store: ArtifactStore) -> ReasoningLoop:
        """Select the deliberately altered host binding for the boundary under test."""
        return loop

    with pytest.raises(EvidenceIntegrityError):
        reason(state, h.store, bound_factory)
    assert not h.adapter.calls and not h.read_calls
