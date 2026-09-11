"""Contracts reject ambiguous times, hidden fields and unsupported citations."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from payops.contracts import EvidenceItem, IncidentCreate, IncidentReport, RootCauseHypothesis


def test_incident_forbids_unrecognized_fields() -> None:
    """Extra fields must not become an accidental command channel."""
    with pytest.raises(ValidationError):
        IncidentCreate.model_validate({"title": "Outage", "command": "delete"})


def test_contract_freeze_prevents_in_place_authority_changes() -> None:
    """Validated identity/scope must not be edited through ordinary attribute assignment."""
    request = IncidentCreate(title="Outage")
    with pytest.raises(ValidationError):
        request.namespace = "production"


@pytest.mark.parametrize("confidence", [-0.1, 1.1, float("nan"), float("inf")])
def test_confidence_requires_finite_unit_interval(confidence: float) -> None:
    """Confidence is bounded even before calibration; NaN cannot bypass comparisons."""
    with pytest.raises(ValidationError):
        RootCauseHypothesis(cause_code="cause", confidence=confidence)


def test_incident_rejects_empty_title() -> None:
    """An alert needs a meaningful bounded description for investigation."""
    with pytest.raises(ValidationError):
        IncidentCreate(title=" ")


def test_evidence_rejects_naive_time() -> None:
    """Timezone ambiguity would invalidate cross-source correlation."""
    with pytest.raises(ValidationError):
        EvidenceItem.model_validate(
            {
                "evidence_id": "e1",
                "incident_id": "i1",
                "source": "LOG",
                "observed_at": datetime(2026, 9, 11),
                "collected_at": datetime.now(UTC),
                "query": "mock",
                "summary": "mock",
                "artifact_sha256": "a" * 64,
                "artifact_uri": "memory://mock",
                "resource": "payments-api",
            }
        )


def test_report_rejects_unresolvable_citation() -> None:
    """A plausible diagnosis is invalid if its cited evidence cannot be audited."""
    with pytest.raises(ValidationError):
        IncidentReport.model_validate(
            {
                "incident_id": "i1",
                "evidence": [],
                "ranked_root_causes": [
                    {
                        "cause_code": "unknown",
                        "confidence": 0.5,
                        "supporting_evidence_ids": ["absent"],
                        "refuting_evidence_ids": [],
                        "missing_evidence": [],
                    }
                ],
                "terminal_state": "ESCALATED",
                "mode": "mock",
                "duration_seconds": 0.1,
            }
        )
