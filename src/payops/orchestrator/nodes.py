"""Bounded read-only lifecycle nodes with explicit failure and budget transitions."""

import os
import time
from collections.abc import Callable
from hashlib import sha256
from pathlib import Path

from payops.contracts import EvidenceItem, Incident, IncidentReport, utc_now
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.payment_window import verify_payment_window
from payops.orchestrator.baseline import rank_evidence
from payops.orchestrator.state import Envelope, InvestigationState, Phase, StepRecord, pack, unpack
from payops.tools.collect import Collection, CollectionFailure
from payops.tools.kubernetes import SERVICES

Collector = Callable[[Incident, Path], Collection]


def incident_directory(root: Path, incident_id: str) -> Path:
    """Hash identifiers so even a valid '..' identifier cannot escape the runtime root."""
    return root / sha256(incident_id.encode()).hexdigest()


def updated(state: InvestigationState, **changes: object) -> InvestigationState:
    """Validated reconstruction prevents model_copy from bypassing field invariants."""
    return InvestigationState.model_validate({**state.model_dump(), **changes})


def reserve_attempt(root: Path, state: InvestigationState, node: str) -> InvestigationState:
    """Durable attempt files count crashed nodes even if their graph update never committed."""
    directory = incident_directory(root, state.incident.incident_id) / "attempts"
    directory.mkdir(parents=True, exist_ok=True)
    used = max(state.steps_used, len(tuple(directory.glob("attempt-*.json"))))
    if used >= state.budget.max_steps or utc_now() >= state.budget.node_start_deadline:
        return updated(state, steps_used=used, terminal="BUDGET_EXHAUSTED")
    record = StepRecord(node=node, observed_at=utc_now(), duration_seconds=0)
    with (directory / f"attempt-{used + 1:02d}.json").open("xb") as stream:
        stream.write(record.model_dump_json().encode())
        stream.flush()
        os.fsync(stream.fileno())
    return updated(state, steps_used=used + 1)


class InvestigationNodes:
    """Trusted construction binds readers and artifact storage; graph state contains no tools."""

    def __init__(self, root: Path, collect: Collector) -> None:
        """Each worker receives an operator-owned read function, never a command string."""
        self.root, self.collector = root, collect

    def _step(
        self,
        envelope: Envelope,
        name: str,
        operation: Callable[[InvestigationState], InvestigationState],
    ) -> Envelope:
        """Reserve attempts durably; the cutoff prevents new nodes, not in-flight I/O."""
        state = unpack(envelope)
        started = time.perf_counter()
        reserved = reserve_attempt(self.root, state, name)
        result = reserved if reserved.terminal is not None else operation(reserved)
        record = StepRecord(
            node=name, observed_at=utc_now(), duration_seconds=time.perf_counter() - started
        )
        return pack(updated(result, steps=(*state.steps, record)))

    def triage(self, state: Envelope) -> Envelope:
        """Reject foreign workload scope before reserving or dispatching operational reads."""

        def operation(state: InvestigationState) -> InvestigationState:
            """Alert text never changes the fixed sandbox reader boundary."""
            request = state.incident.request
            terminal = (
                None
                if request.namespace == "payops-sandbox" and request.service in SERVICES
                else "SECURITY_BLOCK"
            )
            return updated(state, phase="TRIAGED", terminal=terminal)

        return self._step(state, "triage", operation)

    def reserve(self, state: Envelope) -> Envelope:
        """Checkpoint logical operations and backend commands/queries before collection dispatch."""

        def operation(state: InvestigationState) -> InvestigationState:
            """Reserve 34 payment or 30 instant backend reads; failed calls are not refunded."""
            reserved = state.tool_calls_reserved + 20
            backend = state.backend_reads_reserved + (
                34 if state.collection_profile == "payment_windows_v1" else 30
            )
            if reserved > state.budget.max_tool_calls:
                return updated(state, terminal="BUDGET_EXHAUSTED")
            if backend > state.budget.max_backend_reads:
                return updated(state, terminal="BUDGET_EXHAUSTED")
            return updated(
                state,
                phase="READS_RESERVED",
                tool_calls_reserved=reserved,
                backend_reads_reserved=backend,
            )

        return self._step(state, "reserve", operation)

    def collect(self, state: Envelope) -> Envelope:
        """An ambiguous crashed collection is never repeated against its reserved budget."""
        return self._step(state, "collect", self._collect)

    def _collect(self, state: InvestigationState) -> InvestigationState:
        """An exclusive dispatch marker distinguishes retained output from an uncertain attempt."""
        required_reads = 34 if state.collection_profile == "payment_windows_v1" else 30
        if state.tool_calls_reserved < 20 or state.backend_reads_reserved < required_reads:
            return updated(state, terminal="BUDGET_EXHAUSTED")
        output = incident_directory(self.root, state.incident.incident_id)
        output.mkdir(parents=True, exist_ok=True)
        result_path = output / "graph-collection.json"
        try:
            if result_path.exists():
                result = Collection.model_validate_json(result_path.read_bytes())
                return self._collected(state, result)
            with (output / "collection-dispatched").open("xb") as marker:
                marker.write(b"one bounded read batch\n")
            result = self.collector(state.incident, output)
            with result_path.open("xb") as retained:
                retained.write(result.model_dump_json().encode())
            return self._collected(state, result)
        except EvidenceIntegrityError:
            return updated(state, terminal="SECURITY_BLOCK")
        except (OSError, ValueError) as error:
            failure = CollectionFailure(
                tool="collection",
                resource=state.incident.request.service,
                error_type=type(error).__name__,
            )
            return updated(
                state, terminal="DEPENDENCY_UNAVAILABLE", failures=(*state.failures, failure)
            )

    def _collected(self, state: InvestigationState, result: Collection) -> InvestigationState:
        """Collector output must belong to this incident and resolve every artifact."""
        if result.incident_id != state.incident.incident_id:
            raise EvidenceIntegrityError("collection belongs to another incident")
        store = ArtifactStore(
            incident_directory(self.root, state.incident.incident_id) / "artifacts"
        )
        for item in result.evidence:
            if item.incident_id != state.incident.incident_id:
                raise EvidenceIntegrityError("evidence belongs to another incident")
            verify_collected_item(item, store)
        return updated(
            state, phase="EVIDENCE_COLLECTED", evidence=result.evidence, failures=result.failures
        )

    def rank(self, state: Envelope) -> Envelope:
        """Only verified observations reach deterministic ranking; no scenario labels enter."""

        def operation(state: InvestigationState) -> InvestigationState:
            """Reverify after restart so modified artifacts cannot inherit checkpoint trust."""
            store = ArtifactStore(
                incident_directory(self.root, state.incident.incident_id) / "artifacts"
            )
            try:
                for item in state.evidence:
                    verify_collected_item(item, store)
                hypotheses = rank_evidence(state.evidence, store)
            except EvidenceIntegrityError:
                return updated(state, terminal="SECURITY_BLOCK")
            terminal = "ESCALATED" if hypotheses else "EVIDENCE_INSUFFICIENT"
            return updated(state, phase="RANKED", hypotheses=hypotheses, terminal=terminal)

        return self._step(state, "rank", operation)

    def finish(self, state: Envelope) -> Envelope:
        """Final reporting always runs, even after a budget denial; it has no external reads."""
        current = unpack(state)
        report = IncidentReport(
            incident_id=current.incident.incident_id,
            evidence=current.evidence,
            ranked_root_causes=current.hypotheses,
            terminal_state=current.terminal or "UNRECOVERABLE",
            mode=current.mode,
            duration_seconds=sum(step.duration_seconds for step in current.steps),
        )
        return pack(updated(current, phase="FINISHED", report=report))


def verify_collected_item(item: EvidenceItem, store: ArtifactStore) -> None:
    """Retained derived evidence is revalidated after restart as well as before checkpointing."""
    try:
        store.verify(item)
        if item.source == "PAYMENT":
            verify_payment_window(item, store)
    except ValueError:
        raise EvidenceIntegrityError("collected evidence failed verification") from None


def route(envelope: Envelope) -> str:
    """Only explicit legal phases schedule a next node; terminal outcomes go directly to report."""
    state = unpack(envelope)
    if state.terminal is not None:
        return "finish"
    transitions: dict[Phase, str] = {
        "TRIAGED": "reserve",
        "READS_RESERVED": "collect",
        "EVIDENCE_COLLECTED": "rank",
    }
    if state.phase not in transitions:
        raise ValueError("illegal investigation transition")
    return transitions[state.phase]
