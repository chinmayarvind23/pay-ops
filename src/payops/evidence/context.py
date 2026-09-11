"""Bounded model context retains verified source facts and makes omissions explicit."""

from typing import Literal

from pydantic import Field, JsonValue

from payops.contracts import Contract, EvidenceItem, Source
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, EvidenceIntegrityError
from payops.evidence.payment_window import verify_payment_window
from payops.evidence.trace_span import verify_trace_span
from payops.evidence.verification import verify_evidence
from payops.memory.data_clients import verify_retrieval_evidence

PRIORITY: dict[Source, int] = {
    "PAYMENT": 0,
    "DEPLOYMENT": 1,
    "KUBERNETES": 2,
    "PROMETHEUS": 3,
    "TRACE": 4,
    "LOG": 5,
    "RUNBOOK": 6,
    "MEMORY": 7,
}


class ContextEntry(Contract):
    """Omitting a large payload never changes its summary, timestamps or artifact identity."""

    evidence: EvidenceItem
    facts: dict[str, JsonValue] | None
    facts_omitted: bool


class ReasoningContext(Contract):
    """Only included IDs may resolve model citations; omitted records remain outside the prompt."""

    treatment: Literal["untrusted_evidence_data"] = "untrusted_evidence_data"
    entries: tuple[ContextEntry, ...] = Field(max_length=64)
    omitted_count: int = Field(ge=0, le=256)

    def evidence_ids(self) -> frozenset[str]:
        """Resolve citations against exactly the evidence submitted to this model decision."""
        return frozenset(entry.evidence.evidence_id for entry in self.entries)


def _facts(item: EvidenceItem, store: ArtifactStore) -> dict[str, JsonValue]:
    """Derived numbers retain their provenance IDs without duplicating full source envelopes."""
    if item.source == "PAYMENT":
        window = verify_payment_window(item, store)
        return JSON_OBJECT.validate_python(
            {
                **window.model_dump(mode="json", exclude={"inputs"}),
                "input_evidence_ids": [source.evidence_id for source in window.inputs],
            }
        )
    if item.source == "TRACE":
        span = verify_trace_span(item, store)
        return JSON_OBJECT.validate_python(
            {
                **span.model_dump(mode="json", exclude={"source"}),
                "source_evidence_id": span.source.evidence_id,
            }
        )
    if item.source in {"RUNBOOK", "MEMORY"}:
        lineage = verify_retrieval_evidence(item, store)
        return {
            "retrieval_only": True,
            "source_evidence_id": lineage.source.evidence_id,
            "original_summary": lineage.source.summary,
            "retrieved_at": lineage.retrieved_at.isoformat(),
        }
    return JSON_OBJECT.validate_python(store.verify(item).get("payload"))


def build_context(
    evidence: tuple[EvidenceItem, ...],
    store: ArtifactStore,
    incident_id: str,
    *,
    max_characters: int = 24000,
    max_items: int = 64,
) -> ReasoningContext:
    """Verify even dropped evidence; fixed source priority uses no scenario IDs or gold causes."""
    if not 128 <= max_characters <= 24000 or not 0 <= max_items <= 64 or len(evidence) > 256:
        raise ValueError("context budget outside bounds")
    if len({item.evidence_id for item in evidence}) != len(evidence):
        raise EvidenceIntegrityError("duplicate context evidence")
    for item in evidence:
        if item.incident_id != incident_id:
            raise EvidenceIntegrityError("context incident mismatch")
        verify_evidence(item, store)
    result = ReasoningContext(entries=(), omitted_count=len(evidence))
    for item in sorted(evidence, key=lambda entry: PRIORITY[entry.source]):
        if len(result.entries) >= max_items:
            break
        entry = ContextEntry(evidence=item, facts=_facts(item, store), facts_omitted=False)
        result = _append(result, entry, max_characters)
    return result


def _append(bundle: ReasoningContext, entry: ContextEntry, limit: int) -> ReasoningContext:
    """Try complete facts, then a clearly marked metadata-only entry, otherwise omit the item."""
    for candidate in (entry, ContextEntry(evidence=entry.evidence, facts=None, facts_omitted=True)):
        updated = ReasoningContext(
            entries=(*bundle.entries, candidate), omitted_count=bundle.omitted_count - 1
        )
        if len(updated.model_dump_json()) <= limit:
            return updated
    return bundle
