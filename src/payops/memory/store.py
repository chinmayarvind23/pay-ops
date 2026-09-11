"""SQL-backed walking-skeleton persistence with atomic idempotent creation."""

from sqlalchemy import Engine, String, Text, create_engine, select
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
                previous = Incident.model_validate_json(row.payload)
                if previous.request != request:
                    raise IdempotencyConflict("idempotency key has a different payload") from None
                return previous
        return incident

    def get(self, incident_id: str) -> Incident | None:
        """Revalidate persisted JSON to detect corrupt or incompatible records."""
        with Session(self.engine) as session:
            row = session.get(IncidentRow, incident_id)
            return Incident.model_validate_json(row.payload) if row else None

    def save_report(self, report: IncidentReport) -> Incident:
        """Reports attach only to existing incidents; no implicit cross-scope upsert."""
        with Session(self.engine) as session:
            row = session.get(IncidentRow, report.incident_id)
            if row is None:
                raise KeyError(report.incident_id)
            incident = Incident.model_validate_json(row.payload)
            updated = Incident.model_validate({**incident.model_dump(), "report": report})
            row.payload = updated.model_dump_json()
            session.commit()
            return updated
