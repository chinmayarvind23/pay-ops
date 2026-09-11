"""Reject lineages that are well-formed but internally inconsistent."""

import pytest
from pydantic import ValidationError

from payops.contracts import Incident, IncidentCreate, IncidentReport, RootCauseHypothesis
from payops.orchestrator.mock import investigate_mock


def test_nested_report_cannot_cross_incidents() -> None:
    """Durable JSON revalidation must reject a report attached to the wrong parent."""
    report = investigate_mock(Incident(request=IncidentCreate(title="First")))
    with pytest.raises(ValidationError):
        Incident(incident_id="another", request=IncidentCreate(title="Second"), report=report)


@pytest.mark.parametrize(
    "corruption", ["duplicate_evidence", "cross_incident", "duplicate_cause", "contradiction"]
)
def test_report_rejects_inconsistent_lineage(corruption: str) -> None:
    """Valid identifiers alone cannot establish evidence scope or a unique ranking."""
    report = investigate_mock(Incident(request=IncidentCreate(title="Test")))
    evidence = report.evidence[0]
    hypothesis = RootCauseHypothesis(
        cause_code="test", confidence=0.5, supporting_evidence_ids=(evidence.evidence_id,)
    )
    payload = report.model_dump()
    if corruption == "duplicate_evidence":
        payload["evidence"] = (evidence, evidence)
    elif corruption == "cross_incident":
        payload["incident_id"] = "another-incident"
    elif corruption == "duplicate_cause":
        payload["ranked_root_causes"] = (hypothesis, hypothesis)
    else:
        payload["ranked_root_causes"] = (
            RootCauseHypothesis(
                cause_code="test",
                confidence=0.5,
                supporting_evidence_ids=(evidence.evidence_id,),
                refuting_evidence_ids=(evidence.evidence_id,),
            ),
        )
    with pytest.raises(ValidationError):
        IncidentReport.model_validate(payload)
