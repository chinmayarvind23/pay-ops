"""Exercise database failure boundaries apart from HTTP routing."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from sqlalchemy import event, update
from sqlalchemy.engine import Connection, ExecutionContext
from sqlalchemy.engine.interfaces import DBAPICursor
from sqlalchemy.orm import Session

from payops.contracts import IncidentCreate, IncidentReport
from payops.memory.store import IncidentRow, IncidentStore


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


def test_first_report_is_immutable(tmp_path: Path) -> None:
    """A retried investigation must return the committed report rather than overwrite it."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'store.db'}")
    incident = store.create(IncidentCreate(title="Unavailable"), None)
    first = IncidentReport(
        incident_id=incident.incident_id,
        terminal_state="ESCALATED",
        mode="local_kind",
        duration_seconds=1,
    )
    different = first.model_copy(update={"terminal_state": "EVIDENCE_INSUFFICIENT"})
    store.save_report(first)
    assert store.save_report(different).report == first
    saved = store.get(incident.incident_id)
    assert saved is not None and saved.report == first
    store.close()


def test_racing_report_writers_return_one_canonical_report(tmp_path: Path) -> None:
    """Concurrent investigators cannot return separate reports as the same durable result."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'store.db'}")
    incident = store.create(IncidentCreate(title="Unavailable"), None)
    gate = Barrier(2)

    def before_update(
        conn: Connection,
        cursor: DBAPICursor,
        statement: str,
        parameters: object,
        context: ExecutionContext,
        executemany: bool,
    ) -> None:
        """Force both workers past their old-state read before either conditional update runs."""
        if statement.startswith("UPDATE incidents"):
            gate.wait(timeout=5)

    event.listen(store.engine, "before_cursor_execute", before_update)

    def write(duration: int) -> IncidentReport | None:
        """Start both writers together with independently generated report identities."""
        report = IncidentReport(
            incident_id=incident.incident_id,
            terminal_state="ESCALATED",
            mode="local_kind",
            duration_seconds=duration,
        )
        return store.save_report(report).report

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, (1, 2)))
    saved = store.get(incident.incident_id)
    assert saved is not None and results == [saved.report, saved.report]
    store.close()


def test_swapped_incident_payload_is_not_read_as_another_record(tmp_path: Path) -> None:
    """A valid foreign incident payload cannot replace the authority of the requested row."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'store.db'}")
    first = store.create(IncidentCreate(title="First"), "first")
    second = store.create(IncidentCreate(title="Second"), "second")
    with Session(store.engine) as session:
        session.execute(
            update(IncidentRow)
            .where(IncidentRow.incident_id == first.incident_id)
            .values(payload=second.model_dump_json())
        )
        session.commit()
    with pytest.raises(ValueError, match="row identity mismatch"):
        store.get(first.incident_id)
    with pytest.raises(ValueError, match="row identity mismatch"):
        store.create(first.request, "first")
    with pytest.raises(ValueError, match="row identity mismatch"):
        store.save_report(
            IncidentReport(
                incident_id=first.incident_id,
                terminal_state="ESCALATED",
                mode="local_kind",
                duration_seconds=1,
            )
        )
    store.close()
