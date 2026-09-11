"""Explicit mock path validates plumbing without claiming diagnostic performance."""

from hashlib import sha256
from time import perf_counter

from payops.contracts import EvidenceItem, Incident, IncidentReport, utc_now

MOCK_ARTIFACT = b'{"mode":"mock","observation":"collector not connected"}'


def investigate_mock(incident: Incident) -> IncidentReport:
    """Missing real observations require escalation, never invented root causes."""
    start = perf_counter()
    observed = utc_now()
    evidence = EvidenceItem(
        evidence_id="mock-collector",
        incident_id=incident.incident_id,
        source="LOG",
        observed_at=observed,
        collected_at=observed,
        query="mock://collector-status",
        resource=incident.request.service,
        artifact_uri="package://payops.orchestrator.mock/MOCK_ARTIFACT",
        artifact_sha256=sha256(MOCK_ARTIFACT).hexdigest(),
        summary="Mock collector: operational evidence is not connected.",
    )
    return IncidentReport(
        incident_id=incident.incident_id,
        evidence=(evidence,),
        terminal_state="EVIDENCE_INSUFFICIENT",
        mode="mock",
        duration_seconds=perf_counter() - start,
    )
