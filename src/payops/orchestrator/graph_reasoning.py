"""Host-bound reasoning integrates with native graph ranking and its remaining read allowances."""

from collections.abc import Callable

from payops.contracts import TerminalState
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.orchestrator.budget import ReadCharge
from payops.orchestrator.loop import LoopResult, ReasoningLoop
from payops.orchestrator.state import InvestigationState

ReasonerFactory = Callable[[InvestigationState, ArtifactStore], ReasoningLoop]


def reason(
    state: InvestigationState, store: ArtifactStore, factory: ReasonerFactory
) -> InvestigationState:
    """The factory supplies transport and actor; graph state can only reduce read allowances."""
    loop = factory(state, store)
    method = "model_fixture" if loop.runtime.settings.mode == "fixture" else "model_provider"
    if (
        loop.store.root != store.root
        or method != state.ranking_method
        or loop.limits.tool_calls > state.budget.max_tool_calls - state.tool_calls_reserved
        or loop.limits.backend_reads > state.budget.max_backend_reads - state.backend_reads_reserved
    ):
        raise EvidenceIntegrityError("reasoner store or remaining read allowance differs")
    result = loop.run(state.incident, state.evidence)
    if result.run_id != state.incident.incident_id:
        raise EvidenceIntegrityError("reasoning result belongs to another incident")
    charges = loop.ledger.get(result.run_id).charges
    reads = [charge for charge in charges if isinstance(charge, ReadCharge)]
    return InvestigationState.model_validate(
        {
            **state.model_dump(),
            "phase": "RANKED",
            "evidence": result.evidence,
            "hypotheses": result.hypotheses,
            "terminal": terminal(result),
            "ranking_method": "model_fixture" if result.mode == "fixture" else "model_provider",
            "reasoning_stop_reason": result.stop_reason,
            "reasoning_receipts": result.receipt_sha256s,
            "tool_calls_reserved": state.tool_calls_reserved + sum(len(x.requests) for x in reads),
            "backend_reads_reserved": state.backend_reads_reserved
            + sum(x.backend_reads() for x in reads),
        }
    )


def terminal(result: LoopResult) -> TerminalState:
    """Failed or ambiguous model work never silently becomes a deterministic scored answer."""
    if result.stop_reason == "FINISHED":
        return "ESCALATED" if result.hypotheses else "EVIDENCE_INSUFFICIENT"
    if result.stop_reason == "DENIED":
        return "SECURITY_BLOCK"
    if result.stop_reason == "BUDGET_EXHAUSTED":
        return "BUDGET_EXHAUSTED"
    if result.stop_reason in {"REFUSED", "INVALID_OUTPUT"}:
        return "EVIDENCE_INSUFFICIENT"
    return "DEPENDENCY_UNAVAILABLE"
