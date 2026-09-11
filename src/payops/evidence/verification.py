"""Shared retained-evidence verification for graph checkpoints and model context."""

from payops.contracts import EvidenceItem
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.payment_window import verify_payment_window
from payops.evidence.trace_span import LOG_QUERY, verify_trace_log, verify_trace_span
from payops.memory.data_clients import verify_retrieval_evidence


def verify_evidence(item: EvidenceItem, store: ArtifactStore) -> None:
    """A derived envelope hash cannot replace verification of its original source chain."""
    try:
        store.verify(item)
        if item.source == "PAYMENT":
            verify_payment_window(item, store)
        elif item.source == "TRACE":
            verify_trace_span(item, store)
        elif item.source == "LOG" and item.query == LOG_QUERY:
            verify_trace_log(item, store)
        elif item.source in {"RUNBOOK", "MEMORY"}:
            verify_retrieval_evidence(item, store)
    except ValueError:
        raise EvidenceIntegrityError("evidence source verification failed") from None
