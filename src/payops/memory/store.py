"""SQL-backed walking-skeleton persistence with atomic idempotent creation."""

from sqlalchemy import Engine, String, Text, create_engine, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from payops.contracts import Incident, IncidentCreate, IncidentReport


class Base(DeclarativeBase):
    """Shared SQL metadata supports both local SQLite and PostgreSQL."""


class IncidentRow(Base):
    """Unique ingestion keys prevent duplicate alerts across racing workers."""

    __tablename__ = "incidents"
    incident_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), unique=True)
    payload: Mapped[str] = mapped_column(Text)


class IdempotencyConflict(ValueError):
    """A reused key with different input must be visible to the caller."""


class IncidentStore:
    """Commit before acknowledging state; never use Redis as incident truth."""

    def __init__(self, database_url: str) -> None:
        """Schema creation is local bootstrap; production migrations come later."""
        self.engine: Engine = create_engine(database_url)
        Base.metadata.create_all(self.engine)

    def close(self) -> None:
        """Release pooled connections when the application lifespan ends."""
        self.engine.dispose()

    def create(self, request: IncidentCreate, key: str | None) -> Incident:
        """The unique constraint resolves races rather than a check-then-write lock."""
        incident = Incident(request=request)
        with Session(self.engine) as session:
            session.add(
                IncidentRow(
                    incident_id=incident.incident_id,
                    idempotency_key=key,
                    payload=incident.model_dump_json(),
                )
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                row = session.scalar(select(IncidentRow).where(IncidentRow.idempotency_key == key))
                if row is None:
                    raise
                previous = self._decode(row)
                if previous.request != request:
                    raise IdempotencyConflict("idempotency key has a different payload") from None
                return previous
        return incident

    def get(self, incident_id: str) -> Incident | None:
        """Revalidate persisted JSON to detect corrupt or incompatible records."""
        with Session(self.engine) as session:
            row = session.get(IncidentRow, incident_id)
            return self._decode(row) if row else None

    @staticmethod
    def _decode(row: IncidentRow) -> Incident:
        """Bind valid nested incident JSON to its storage key before returning it to a caller."""
        incident = Incident.model_validate_json(row.payload)
        if incident.incident_id != row.incident_id:
            raise ValueError("incident row identity mismatch")
        return incident

    def save_report(self, report: IncidentReport) -> Incident:
        """Publish the first report atomically; all racing or retried callers return that winner."""
        with Session(self.engine) as session:
            row = session.get(IncidentRow, report.incident_id)
            if row is None:
                raise KeyError(report.incident_id)
            incident = self._decode(row)
            if incident.report is not None:
                return incident
            updated = Incident.model_validate({**incident.model_dump(), "report": report})
            winner = session.execute(
                update(IncidentRow)
                .where(
                    IncidentRow.incident_id == report.incident_id,
                    IncidentRow.payload == row.payload,
                )
                .values(payload=updated.model_dump_json())
                .returning(IncidentRow.incident_id)
            ).scalar_one_or_none()
            session.commit()
            if winner is not None:
                return updated
        saved = self.get(report.incident_id)
        if saved is None or saved.report is None:
            raise RuntimeError("committed incident report is unavailable")
        return saved
