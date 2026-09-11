"""Exercise database failure boundaries apart from HTTP routing."""

from pathlib import Path

import pytest

from payops.contracts import IncidentReport
from payops.memory.store import IncidentStore


def test_report_cannot_create_missing_incident(tmp_path: Path) -> None:
    """An unsolicited report must never manufacture durable incident authority."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'store.db'}")
    with pytest.raises(KeyError):
        store.save_report(
            IncidentReport(
                incident_id="absent", terminal_state="ESCALATED", mode="mock", duration_seconds=0
            )
        )
    assert store.get("absent") is None
    store.close()
