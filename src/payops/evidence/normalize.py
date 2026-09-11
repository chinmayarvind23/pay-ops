"""Keep evidence selection independent of scenario labels and model instructions."""

from datetime import datetime

from pydantic import AwareDatetime, Field, JsonValue

from payops.contracts import Contract, EvidenceItem, Identifier, Source, Summary, new_id, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, EvidenceIntegrityError
from payops.evidence.redact import redact, redact_text


class Observation(Contract):
    """Collector output describes a signal; gold labels are never part of this contract."""

    source: Source
    resource: Identifier
    observed_at: AwareDatetime
    query: Summary
    summary: Summary
    payload: dict[str, JsonValue] = Field(default_factory=dict)


def normalize(
    observation: Observation, incident_id: str, start: datetime, end: datetime, store: ArtifactStore
) -> EvidenceItem:
    """Reject out-of-window signals and hash the exact sanitized representation."""
    if start.tzinfo is None or end.tzinfo is None or not start <= observation.observed_at <= end:
        raise ValueError("observation outside aware incident window")
    metadata = {
        "evidence_id": new_id(),
        "incident_id": incident_id,
        "source": observation.source,
        "resource": observation.resource,
        "observed_at": observation.observed_at,
        "collected_at": utc_now(),
        "query": redact_text(observation.query),
        "summary": redact_text(observation.summary),
        "untrusted_text": True,
    }
    # Validate before storing; the temporary digest only completes the typed envelope.
    provisional = EvidenceItem.model_validate(
        {**metadata, "artifact_uri": "pending://artifact", "artifact_sha256": "0" * 64}
    )
    envelope = JSON_OBJECT.validate_python(
        {
            "evidence": provisional.model_dump(
                mode="json", exclude={"artifact_uri", "artifact_sha256"}
            ),
            "payload": redact(observation.payload),
            "transformation": "common-secret-redaction-v1",
        }
    )
    uri, digest = store.write(envelope)
    return EvidenceItem.model_validate({**metadata, "artifact_uri": uri, "artifact_sha256": digest})


def select_context(
    evidence: tuple[EvidenceItem, ...],
    store: ArtifactStore,
    max_characters: int = 24000,
    max_items: int = 64,
) -> tuple[EvidenceItem, ...]:
    """Validate before truncating; a dropped corrupt item must still invalidate the bundle."""
    if max_characters < 0 or not 0 <= max_items <= 64:
        raise ValueError("invalid context budget")
    if len({item.incident_id for item in evidence}) > 1:
        raise EvidenceIntegrityError("mixed incident context")
    if len({item.evidence_id for item in evidence}) != len(evidence):
        raise EvidenceIntegrityError("duplicate context evidence")
    selected: list[EvidenceItem] = []
    used = 0
    for item in evidence:
        store.verify(item)
        size = len(item.model_dump_json())
        if len(selected) < max_items and used + size <= max_characters:
            selected.append(item)
            used += size
    return tuple(selected)
