"""Durable synthetic traffic admission and pause share a single SQL serialization boundary."""

from typing import Literal
from uuid import uuid4

from sqlalchemy import Boolean, Engine, Integer, String, create_engine, update
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from payops.contracts import utc_now
from payops.policy.contracts import ACTION, PauseAction, ResourceSnapshot
from payops.policy.engine import action_digest
from payops.remediation.contracts import EffectReceipt

TrafficService = Literal["payments-api", "webhook-sim"]
TrafficMode = Literal["local_kind", "fixture_replay"]


class Base(DeclarativeBase):
    """Control metadata has no connection to payment settlement or ledger state."""


class TrafficRow(Base):
    """Only trusted host code registers runs; pause never modifies workload or destinations."""

    __tablename__ = "synthetic_traffic_control"
    uid: Mapped[str] = mapped_column(String(64), primary_key=True)
    service: Mapped[str] = mapped_column(String(32))
    mode: Mapped[str] = mapped_column(String(32))
    version: Mapped[int] = mapped_column(Integer, default=0)
    paused: Mapped[bool] = mapped_column(Boolean, default=False)
    admitted: Mapped[int] = mapped_column(Integer, default=0)


class TrafficControl:
    """A pause stops future admissions; already admitted requests may complete or be cancelled."""

    def __init__(self, database: str | Engine) -> None:
        """Use SQL truth across processes; a cached enabled flag would race an approved pause."""
        self._owns_engine = isinstance(database, str)
        self.engine = create_engine(database) if isinstance(database, str) else database
        Base.metadata.create_all(self.engine)

    def close(self) -> None:
        """Release only this store's owned connection pool."""
        if self._owns_engine:
            self.engine.dispose()

    def register(self, service: TrafficService, mode: TrafficMode) -> str:
        """The trusted host creates one durable identity per managed synthetic traffic source."""
        if service not in {"payments-api", "webhook-sim"} or mode not in {
            "local_kind",
            "fixture_replay",
        }:
            raise PermissionError("TRAFFIC_SCOPE_DENIED")
        uid = uuid4().hex
        with Session(self.engine) as session:
            session.add(TrafficRow(uid=uid, service=service, mode=mode))
            session.commit()
        return uid

    def snapshot(self, action: PauseAction) -> ResourceSnapshot:
        """Bind control identity to action scope independently of any caller-supplied snapshot."""
        if not isinstance(ACTION.validate_json(action.model_dump_json()), PauseAction):
            raise PermissionError("TRAFFIC_ACTION_DENIED")
        with Session(self.engine) as session:
            row = session.get(TrafficRow, action.resource_uid)
            if (
                row is None
                or row.service != action.service
                or row.mode != action.mode
                or action.namespace != "payops-sandbox"
            ):
                raise PermissionError("TRAFFIC_SCOPE_DENIED")
            return ResourceSnapshot(
                namespace="payops-sandbox",
                service=row.service,
                uid=row.uid,
                version=str(row.version),
                observed_at=utc_now(),
                kind="Traffic",
                replicas=0 if row.paused else 1,
                mode=action.mode,
            )

    def admit(self, uid: str, service: TrafficService, mode: TrafficMode) -> bool:
        """The conditional UPDATE orders admission before or after pause in every worker."""
        if service not in {"payments-api", "webhook-sim"} or mode not in {
            "local_kind",
            "fixture_replay",
        }:
            raise PermissionError("TRAFFIC_SCOPE_DENIED")
        with Session(self.engine) as session:
            admitted = session.execute(
                update(TrafficRow)
                .where(
                    TrafficRow.uid == uid,
                    TrafficRow.service == service,
                    TrafficRow.mode == mode,
                    TrafficRow.paused.is_(False),
                )
                .values(admitted=TrafficRow.admitted + 1)
                .returning(TrafficRow.uid)
            ).scalar_one_or_none()
            session.commit()
            return admitted is not None

    def pause(self, action: PauseAction, key: str) -> EffectReceipt:
        """One versioned update closes admission; stale or repeated requests never change state."""
        self.snapshot(action)
        if key != action_digest(action):
            raise PermissionError("TRAFFIC_DIGEST_MISMATCH")
        with Session(self.engine) as session:
            changed = session.execute(
                update(TrafficRow)
                .where(
                    TrafficRow.uid == action.resource_uid,
                    TrafficRow.service == action.service,
                    TrafficRow.mode == action.mode,
                    TrafficRow.version.cast(String) == action.expected_version,
                    TrafficRow.paused.is_(False),
                )
                .values(paused=True, version=TrafficRow.version + 1)
                .returning(TrafficRow.version)
            ).scalar_one_or_none()
            if changed is None:
                raise PermissionError("TRAFFIC_PRECONDITION_FAILED")
            session.commit()
        return EffectReceipt(
            outcome="SUCCEEDED",
            resource_uid=action.resource_uid,
            previous_version=action.expected_version,
            resulting_version=str(changed),
        )
