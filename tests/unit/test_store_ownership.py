"""Shared bounded database pools belong to the host factory, not either borrowed store."""

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from payops.contracts import IncidentCreate
from payops.memory.store import IncidentStore
from payops.remediation.store import ActionStore


def test_borrowed_engine_survives_each_store_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Closing one store must not dispose a shared pool while the other service is using it."""
    engine = create_engine(f"sqlite:///{tmp_path / 'shared.db'}", pool_size=2, max_overflow=0)
    incidents, actions = IncidentStore(engine), ActionStore(engine)
    original = engine.dispose
    calls: list[bool] = []

    def record_disposal(close: bool = True) -> None:
        """Track disposal directly because SQLAlchemy can silently reopen a disposed pool."""
        calls.append(close)
        original(close=close)

    monkeypatch.setattr(engine, "dispose", record_disposal)
    try:
        actions.close()
        incident = incidents.create(IncidentCreate(title="Borrowed pool"), None)
        incidents.close()
        assert incidents.get(incident.incident_id) == incident
        assert calls == []
    finally:
        engine.dispose()
    assert calls == [True]
