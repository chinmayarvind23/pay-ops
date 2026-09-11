"""SQL transactions atomically publish execution claims and their corresponding audit events."""

from sqlalchemy import Engine, Integer, String, Text, create_engine, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from payops.contracts import utc_now
from payops.remediation.contracts import (
    ActionRecord,
    ActionState,
    Approval,
    AuditEvent,
    EffectReceipt,
)


class Base(DeclarativeBase):
    """Separate metadata keeps action persistence independent of incident bootstrap."""


class ActionRow(Base):
    """The digest primary key deduplicates exact proposals across independent workers."""

    __tablename__ = "remediation_actions"
    action_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    payload: Mapped[str] = mapped_column(Text)


class AuditRow(Base):
    """Composite action/revision identity prevents duplicate transition events."""

    __tablename__ = "remediation_audit"
    action_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    payload: Mapped[str] = mapped_column(Text)


class TransitionConflict(RuntimeError):
    """Another transaction already changed this action; the caller must not dispatch."""


TRANSITIONS: dict[ActionState, frozenset[ActionState]] = {
    "PROPOSED": frozenset({"APPROVED"}),
    "APPROVED": frozenset({"EXECUTING"}),
    "EXECUTING": frozenset({"SUCCEEDED", "FAILED", "UNKNOWN"}),
    "SUCCEEDED": frozenset(),
    "FAILED": frozenset(),
    "UNKNOWN": frozenset(),
}


class ActionStore:
    """Database truth, never a cache entry, determines whether an action was dispatched."""

    def __init__(self, database_url: str | Engine) -> None:
        """Use local bootstrap for now; production migrations and database roles are separate."""
        self._owns_engine = isinstance(database_url, str)
        self.engine = create_engine(database_url) if isinstance(database_url, str) else database_url
        Base.metadata.create_all(self.engine)

    def close(self) -> None:
        """Release owned database connections."""
        if self._owns_engine:
            self.engine.dispose()

    def get(self, action_id: str) -> ActionRecord:
        """Missing or corrupt records cannot be interpreted as unclaimed actions."""
        with Session(self.engine) as session:
            row = session.get(ActionRow, action_id)
            if row is None:
                raise KeyError(action_id)
            record = ActionRecord.model_validate_json(row.payload)
            if record.action_id != row.action_id:
                raise ValueError("action row identity mismatch")
            return record

    @staticmethod
    def _audit(session: Session, record: ActionRecord, actor: str, reason: str) -> None:
        """Insert an audit event in the same transaction as the action change."""
        event = AuditEvent(
            action_id=record.action_id,
            revision=record.revision,
            state=record.state,
            actor=actor,
            reason=reason,
            recorded_at=record.updated_at,
        )
        session.add(
            AuditRow(
                action_id=record.action_id,
                revision=record.revision,
                payload=event.model_dump_json(),
            )
        )

    def create(self, record: ActionRecord) -> ActionRecord:
        """Exact duplicate proposals are idempotent only for the same authenticated proposer."""
        if record.state != "PROPOSED" or record.revision != 0:
            raise ValueError("new action must be proposed")
        with Session(self.engine) as session:
            session.add(ActionRow(action_id=record.action_id, payload=record.model_dump_json()))
            self._audit(session, record, record.proposer, "POLICY_REVIEWED")
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                previous = self.get(record.action_id)
                if previous.proposer != record.proposer or previous.proposal != record.proposal:
                    raise PermissionError("proposal belongs to another identity") from None
                return previous
        return record

    def transition(
        self,
        before: ActionRecord,
        *,
        state: ActionState,
        actor: str,
        reason: str,
        approval: Approval | None = None,
        result: EffectReceipt | None = None,
    ) -> ActionRecord:
        """Compare the entire validated old record so only one concurrent claimant can win."""
        if state not in TRANSITIONS[before.state]:
            raise TransitionConflict("invalid action transition")
        after = ActionRecord.model_validate(
            {
                **before.model_dump(),
                "state": state,
                "revision": before.revision + 1,
                "updated_at": utc_now(),
                "approval": approval or before.approval,
                "result": result,
            }
        )
        with Session(self.engine) as session:
            changed = session.execute(
                update(ActionRow)
                .where(
                    ActionRow.action_id == before.action_id,
                    ActionRow.payload == before.model_dump_json(),
                )
                .values(payload=after.model_dump_json())
                .returning(ActionRow.action_id)
            ).scalar_one_or_none()
            if changed is None:
                raise TransitionConflict("action changed concurrently")
            self._audit(session, after, actor, reason)
            session.commit()
        return after

    def audit(self, action_id: str) -> tuple[AuditEvent, ...]:
        """Return a validated ordered history; this API exposes no event update or deletion."""
        with Session(self.engine) as session:
            rows = session.scalars(
                select(AuditRow).where(AuditRow.action_id == action_id).order_by(AuditRow.revision)
            )
            return tuple(self._read_event(row) for row in rows)

    @staticmethod
    def _read_event(row: AuditRow) -> AuditEvent:
        """Bind validated event contents to their database identity, not just their own schema."""
        event = AuditEvent.model_validate_json(row.payload)
        if (event.action_id, event.revision) != (row.action_id, row.revision):
            raise ValueError("audit row identity mismatch")
        return event
