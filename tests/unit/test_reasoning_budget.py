"""Actual SQL reservations survive reopen, reject stale workers and retain uncertain-call costs."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from sqlalchemy import create_engine, update

from payops.orchestrator.budget import (
    BudgetConflict,
    BudgetLedger,
    BudgetRecord,
    BudgetRow,
    ModelCharge,
    ReadCharge,
    ReasoningBudget,
)
from payops.orchestrator.reasoning import ReadRequest, TextPrice


def limits(**changes: int) -> ReasoningBudget:
    """Use small fixture allowances whose boundaries can be independently recomputed."""
    return ReasoningBudget(
        **{
            "model_calls": 2,
            "tokens": 240,
            "cost_nano_usd": 200000,
            "tool_calls": 2,
            "backend_reads": 3,
            **changes,
        }
    )


def model(operation: str = "model-1") -> ModelCharge:
    """Fixture prices are arithmetic inputs, never a current provider quote."""
    return ModelCharge(
        operation_id=operation,
        prompt_sha256="b" * 64,
        input_tokens=100,
        output_token_limit=20,
        price=TextPrice(input_nano_usd=250, cached_input_nano_usd=25, output_nano_usd=2000),
    )


def reads(operation: str = "read-1") -> ReadCharge:
    """One status request costs two actual Kubernetes commands in the fixed catalog."""
    return ReadCharge(
        operation_id=operation,
        requests=(ReadRequest(tool="workload_status", service="payments-api", query=None),),
        backend_read_count=2,
    )


@pytest.fixture
def ledger(tmp_path: Path) -> Iterator[BudgetLedger]:
    """Use a real file-backed engine across sessions and worker threads."""
    engine = create_engine(f"sqlite:///{(tmp_path / 'budgets.sqlite').as_posix()}")
    yield BudgetLedger(engine)
    engine.dispose()


def test_reservation_survives_reopen_and_does_not_refill_unknown_usage(
    ledger: BudgetLedger,
) -> None:
    """A crashed provider call has no response, but its full reservation remains charged."""
    start = ledger.open("run", "a" * 64, limits())
    assert ledger.reserve(start, model()) == "NEW"
    reopened = BudgetLedger(ledger.engine).open("run", "a" * 64, limits())
    assert reopened.charges == (model(),)
    assert ledger.reserve(reopened, model()) == "EXISTING"
    assert ledger.reserve(reopened, model("model-2")) == "NEW"
    assert ledger.reserve(ledger.get("run"), model("model-3")) == "DENIED"
    assert len(ledger.get("run").charges) == 2
    assert model().tokens() == 120 and model().cost() == 65000


@pytest.mark.parametrize("change", [{"model_calls": 0}, {"tokens": 119}, {"cost_nano_usd": 64999}])
def test_model_allowances_fail_before_authorizing_dispatch(
    ledger: BudgetLedger, change: dict[str, int]
) -> None:
    """Every independent model ceiling rejects the first over-budget reservation."""
    record = ledger.open("run", "a" * 64, limits(**change))
    assert ledger.reserve(record, model()) == "DENIED"
    assert ledger.get("run").charges == ()


@pytest.mark.parametrize("change", [{"tool_calls": 0}, {"backend_reads": 1}])
def test_read_cost_cannot_hide_behind_logical_call_count(
    ledger: BudgetLedger, change: dict[str, int]
) -> None:
    """One logical call cannot fit an allowance that lacks its two backend reads."""
    record = ledger.open("run", "a" * 64, limits(**change))
    assert ledger.reserve(record, reads()) == "DENIED"


def test_read_reservation_and_changed_operation_identity(ledger: BudgetLedger) -> None:
    """Identical replay is not new dispatch; different work cannot reuse the operation ID."""
    record = ledger.open("run", "a" * 64, limits())
    assert ledger.reserve(record, reads()) == "NEW"
    current = ledger.get("run")
    assert ledger.reserve(current, reads()) == "EXISTING"
    with pytest.raises(BudgetConflict, match="different work"):
        ledger.reserve(current, model("read-1"))
    assert ledger.reserve(current, reads("read-2")) == "DENIED"


def test_racing_workers_cannot_both_spend_same_remaining_allowance(ledger: BudgetLedger) -> None:
    """Actual concurrent transactions return exactly one new reservation from a shared snapshot."""
    record = ledger.open("run", "a" * 64, limits(model_calls=1))
    barrier = Barrier(2)

    def reserve(operation: str) -> str:
        """Synchronize two distinct operations before the SQL compare-and-set."""
        barrier.wait(timeout=2)
        try:
            return ledger.reserve(record, model(operation))
        except BudgetConflict:
            return "CONFLICT"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, ["model-1", "model-2"]))
    assert sorted(results) == ["CONFLICT", "NEW"]
    assert len(ledger.get("run").charges) == 1


def test_binding_budget_and_stale_replay_cannot_reset(ledger: BudgetLedger) -> None:
    """Reopening with more funds or another prompt/provider binding never replaces the row."""
    record = ledger.open("run", "a" * 64, limits())
    for digest, budget in [("b" * 64, limits()), ("a" * 64, limits(tokens=1000))]:
        with pytest.raises(BudgetConflict):
            ledger.open("run", digest, budget)
    assert ledger.reserve(record, model()) == "NEW"
    first = ledger.get("run")
    assert ledger.reserve(first, model("model-2")) == "NEW"
    with pytest.raises(BudgetConflict, match="before replay"):
        ledger.reserve(first, model())


def test_missing_or_misbound_sql_row_is_not_fresh_budget(ledger: BudgetLedger) -> None:
    """Storage keys bind validated JSON, independently of the caller's current expectation."""
    with pytest.raises(KeyError):
        ledger.get("absent")
    record = ledger.open("run", "a" * 64, limits())
    foreign = BudgetRecord(run_id="foreign", binding_sha256="a" * 64, limits=record.limits)
    with ledger.engine.begin() as connection:
        connection.execute(
            update(BudgetRow)
            .where(BudgetRow.run_id == "run")
            .values(payload=foreign.model_dump_json())
        )
    with pytest.raises(BudgetConflict, match="row identity"):
        ledger.get("run")


def test_constructed_invalid_charge_cannot_bypass_revalidation(ledger: BudgetLedger) -> None:
    """Pydantic instance reuse must not preserve invalid negative token counts."""
    record = ledger.open("run", "a" * 64, limits())
    with pytest.raises(ValueError):
        ledger.reserve(record, model().model_copy(update={"input_tokens": -1}))
    assert ledger.get("run").charges == ()


def test_cached_price_and_read_cost_census() -> None:
    """Reserve higher cache prices and verify stored read costs against the catalog."""
    charged = model().model_copy(
        update={"price": TextPrice(input_nano_usd=1, cached_input_nano_usd=3, output_nano_usd=2)}
    )
    assert charged.cost() == 340
    for altered in [
        reads().model_copy(update={"backend_read_count": 1}),
        reads().model_copy(update={"requests": reads().requests * 2}),
    ]:
        with pytest.raises(ValueError):
            ReadCharge.model_validate_json(altered.model_dump_json())
    with pytest.raises(ValueError, match="duplicate budget operation"):
        BudgetRecord(
            run_id="run", binding_sha256="a" * 64, limits=limits(), charges=(model(), model())
        )


def test_exact_model_and_read_budget_boundaries_are_usable(ledger: BudgetLedger) -> None:
    """Inclusive ceilings permit exactly the charged work and reject the next operation."""
    record = ledger.open(
        "run",
        "a" * 64,
        limits(model_calls=1, tokens=120, cost_nano_usd=65000, tool_calls=1, backend_reads=2),
    )
    assert ledger.reserve(record, model()) == "NEW"
    assert ledger.reserve(ledger.get("run"), reads()) == "NEW"
    assert ledger.reserve(ledger.get("run"), reads("read-2")) == "DENIED"


@pytest.mark.parametrize("field", ["prompt_sha256", "price", "output_token_limit"])
def test_same_model_operation_cannot_change_prompt_or_price(
    ledger: BudgetLedger, field: str
) -> None:
    """A call identity binds its exact prompt, token cap and trusted prices."""
    record = ledger.open("run", "a" * 64, limits())
    assert ledger.reserve(record, model()) == "NEW"
    values: dict[str, object] = {
        "prompt_sha256": "c" * 64,
        "output_token_limit": 21,
        "price": TextPrice(input_nano_usd=1, cached_input_nano_usd=1, output_nano_usd=1),
    }
    with pytest.raises(BudgetConflict, match="different work"):
        ledger.reserve(ledger.get("run"), model().model_copy(update={field: values[field]}))
