"""SQL compare-and-set reserves reasoning work before dispatch and never refunds uncertain calls."""

from typing import Annotated, Literal, Self

from pydantic import Field, TypeAdapter, model_validator
from sqlalchemy import Engine, String, Text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from payops.contracts import Contract, Identifier
from payops.orchestrator.reasoning import ReadRequest, TextPrice, TokenAccounting
from payops.tools.registry import CATALOG

Amount = Annotated[int, Field(strict=True, ge=0, le=1_000_000_000_000)]
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class ReasoningBudget(Contract):
    """Trusted configuration binds remaining allowances after any initial fixed collection."""

    model_calls: int = Field(strict=True, ge=0, le=10)
    tokens: int = Field(strict=True, ge=0, le=100000)
    cost_nano_usd: Amount
    tool_calls: int = Field(strict=True, ge=0, le=40)
    backend_reads: int = Field(strict=True, ge=0, le=64)
    provider_requests: int = Field(default=20, strict=True, ge=0, le=20)


class ModelCharge(Contract):
    """Reserve exact prepared input and capped output at conservative trusted text rates."""

    kind: Literal["model"] = "model"
    operation_id: Identifier
    prompt_sha256: Digest
    input_tokens: int = Field(strict=True, ge=0, le=100000)
    output_token_limit: int = Field(strict=True, ge=1, le=16384)
    price: TextPrice
    token_accounting: TokenAccounting = "fixture_exact"
    provider_requests: int = Field(default=0, strict=True, ge=0, le=2)

    @model_validator(mode="after")
    def request_census(self) -> Self:
        """Ceiling accounting reserves one count and one generation request before either starts."""
        if self.provider_requests != (2 if self.token_accounting == "provider_ceiling" else 0):
            raise ValueError("provider request reservation differs from accounting contract")
        return self

    def tokens(self) -> int:
        """Uncertain calls retain both input and maximum output allowance after a crash."""
        return self.input_tokens + self.output_token_limit

    def cost(self) -> int:
        """Cache reads cannot exceed reservation even if their configured price is higher."""
        return self.input_tokens * max(
            self.price.input_nano_usd, self.price.cached_input_nano_usd
        ) + (self.output_token_limit * self.price.output_nano_usd)


class ReadCharge(Contract):
    """The stored request batch determines backend cost; callers cannot submit a cheaper count."""

    kind: Literal["reads"] = "reads"
    operation_id: Identifier
    requests: tuple[ReadRequest, ...] = Field(min_length=1, max_length=2)
    backend_read_count: int = Field(strict=True, ge=1, le=4)

    @model_validator(mode="after")
    def distinct(self) -> Self:
        """A duplicate request cannot become two effects hidden behind one operation identity."""
        if len({item.model_dump_json() for item in self.requests}) != len(self.requests):
            raise ValueError("duplicate charged read")
        if self.backend_read_count != sum(
            CATALOG[item.tool].backend_reads for item in self.requests
        ):
            raise ValueError("stored read cost differs from catalog")
        return self

    def backend_reads(self) -> int:
        """Use the same closed catalog as dispatch, with no model-supplied price fields."""
        return self.backend_read_count


Charge = Annotated[ModelCharge | ReadCharge, Field(discriminator="kind")]
CHARGE = TypeAdapter[Charge](Charge)


class Completion(Contract):
    """A separately fsynced result becomes replayable only after its digest is anchored in SQL."""

    operation_id: Identifier
    artifact_sha256: Digest


class BudgetRecord(Contract):
    """Binding covers incident, provider configuration and initial evidence in the owning loop."""

    run_id: Identifier
    binding_sha256: Digest
    limits: ReasoningBudget
    charges: tuple[Charge, ...] = Field(default=(), max_length=50)
    completions: tuple[Completion, ...] = Field(default=(), max_length=50)

    @model_validator(mode="after")
    def within_limits(self) -> Self:
        """Derive every total from immutable charges rather than trusting mutable counters."""
        if len({charge.operation_id for charge in self.charges}) != len(self.charges):
            raise ValueError("duplicate budget operation")
        completed = {item.operation_id for item in self.completions}
        if len(completed) != len(self.completions) or not completed <= {
            charge.operation_id for charge in self.charges
        }:
            raise ValueError("completion lacks one unique reserved operation")
        model = [charge for charge in self.charges if isinstance(charge, ModelCharge)]
        reads = [charge for charge in self.charges if isinstance(charge, ReadCharge)]
        if (
            len(model) > self.limits.model_calls
            or sum(charge.tokens() for charge in model) > self.limits.tokens
            or sum(charge.cost() for charge in model) > self.limits.cost_nano_usd
            or sum(charge.provider_requests for charge in model) > self.limits.provider_requests
            or sum(len(charge.requests) for charge in reads) > self.limits.tool_calls
            or sum(charge.backend_reads() for charge in reads) > self.limits.backend_reads
        ):
            raise ValueError("reasoning allowance exhausted")
        return self


class Base(DeclarativeBase):
    """Local bootstrap metadata; cloud deployment requires explicit migrations and grants."""


class BudgetRow(Base):
    """One run's complete validated record is the compare-and-set budget authority."""

    __tablename__ = "reasoning_budgets"
    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    payload: Mapped[str] = mapped_column(Text)


class BudgetConflict(RuntimeError):
    """Another worker changed the ledger; this caller must not dispatch an unclaimed operation."""


class BudgetLedger:
    """Borrow the host's SQL engine; no model chooses the database or persistence lifetime."""

    def __init__(self, engine: Engine) -> None:
        """Commit reservations independently of graph checkpoints to cover interrupted nodes."""
        self.engine = engine
        Base.metadata.create_all(engine)

    def open(self, run_id: str, binding_sha256: str, limits: ReasoningBudget) -> BudgetRecord:
        """Reopening cannot replace bindings, reset charges or enlarge the original budget."""
        record = BudgetRecord(run_id=run_id, binding_sha256=binding_sha256, limits=limits)
        with Session(self.engine) as session:
            session.add(BudgetRow(run_id=run_id, payload=record.model_dump_json()))
            try:
                session.commit()
                return record
            except IntegrityError:
                session.rollback()
        existing = self.get(run_id)
        if existing.binding_sha256 != binding_sha256 or existing.limits != limits:
            raise BudgetConflict("run budget binding differs")
        return existing

    def get(self, run_id: str) -> BudgetRecord:
        """Revalidate row identity and derived totals before authorizing another attempt."""
        with Session(self.engine) as session:
            row = session.get(BudgetRow, run_id)
            if row is None:
                raise KeyError(run_id)
            record = BudgetRecord.model_validate_json(row.payload)
            if record.run_id != row.run_id:
                raise BudgetConflict("budget row identity differs")
            return record

    def reserve(
        self, expected: BudgetRecord, charge: Charge
    ) -> Literal["NEW", "EXISTING", "DENIED"]:
        """Only NEW grants dispatch; EXISTING requires a separately retained result for replay."""
        # Revalidate even model_construct/model_copy objects from trusted integration code.
        expected = BudgetRecord.model_validate_json(expected.model_dump_json())
        charge = CHARGE.validate_json(charge.model_dump_json())
        existing = next(
            (item for item in expected.charges if item.operation_id == charge.operation_id), None
        )
        if existing is not None:
            if existing != charge:
                raise BudgetConflict("operation identity reused with different work")
            if self.get(expected.run_id) != expected:
                raise BudgetConflict("budget changed before replay")
            return "EXISTING"
        try:
            updated = BudgetRecord.model_validate(
                {**expected.model_dump(), "charges": (*expected.charges, charge)}
            )
        except ValueError:
            return "DENIED"
        self._replace(expected, updated)
        return "NEW"

    def complete(
        self, expected: BudgetRecord, operation_id: str, artifact_sha256: str
    ) -> Literal["NEW", "EXISTING"]:
        """The first completion digest is immutable; a conflicting response cannot replace it."""
        expected = BudgetRecord.model_validate_json(expected.model_dump_json())
        receipt = Completion(operation_id=operation_id, artifact_sha256=artifact_sha256)
        existing = next(
            (item for item in expected.completions if item.operation_id == operation_id), None
        )
        if existing is not None:
            if existing != receipt or self.get(expected.run_id) != expected:
                raise BudgetConflict("completion differs or budget changed")
            return "EXISTING"
        updated = BudgetRecord.model_validate(
            {**expected.model_dump(), "completions": (*expected.completions, receipt)}
        )
        self._replace(expected, updated)
        return "NEW"

    def _replace(self, expected: BudgetRecord, updated: BudgetRecord) -> None:
        """Budget and response publications share one atomic compare-and-set implementation."""
        with Session(self.engine) as session:
            row = session.get(BudgetRow, expected.run_id)
            if row is None or BudgetRecord.model_validate_json(row.payload) != expected:
                raise BudgetConflict("budget changed before reservation")
            # Preserve CAS against old serialized shapes while writing the current validated schema.
            raw_payload = row.payload
            winner = session.execute(
                update(BudgetRow)
                .where(
                    BudgetRow.run_id == expected.run_id,
                    BudgetRow.payload == raw_payload,
                )
                .values(payload=updated.model_dump_json())
                .returning(BudgetRow.run_id)
            ).scalar_one_or_none()
            session.commit()
        if winner is None:
            raise BudgetConflict("budget changed before reservation")
