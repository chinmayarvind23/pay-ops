"""Durable SQL receipts drive real fixture model and registry execution across restarts."""

import json
from collections.abc import Iterator
from pathlib import Path
from threading import Event
from typing import Any

import pytest
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from test_context import item
from test_model_runtime import Adapter, reply
from test_reasoning import finished

from payops.contracts import EvidenceItem, Incident, IncidentCreate
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.verification import verify_evidence
from payops.orchestrator.budget import (
    BudgetConflict,
    BudgetLedger,
    BudgetRecord,
    BudgetRow,
    ModelCharge,
    ReadCharge,
    ReasoningBudget,
)
from payops.orchestrator.loop import (
    LoopResult,
    LoopSession,
    LoopStopped,
    ReasoningLoop,
    prompt_context,
)
from payops.orchestrator.loop_records import (
    ModelReceipt,
    PreparedTurn,
    ReadReceipt,
    restore,
    retain,
)
from payops.orchestrator.model_runtime import ModelRuntime
from payops.orchestrator.reasoning import ReadRequest, parse_decision
from payops.tools.registry import CATALOG, ReadRegistry, ReadResult, Reserve


def read_decision() -> dict[str, Any]:
    """A model requests one closed read; backend scope and execution stay host-bound."""
    return {
        "decision": "read",
        "summary": "Need recent logs",
        "hypotheses": [],
        "reads": [{"tool": "recent_logs", "service": "payments-api", "query": None}],
    }


class Harness:
    """Each case owns a file-backed SQL engine, actual artifacts and explicit fixture transports."""

    def __init__(self, root: Path) -> None:
        """Use public incident and source contracts, never scenario gold or live client setup."""
        self.root = root
        self.url = f"sqlite:///{(root / 'budget.sqlite3').as_posix()}"
        self.engine = create_engine(self.url)
        self.store = ArtifactStore(root / "artifacts")
        self.incident = Incident(incident_id="incident", request=IncidentCreate(title="synthetic"))
        self.initial = item(self.store, text="untrusted initial source")
        self.additional = item(self.store, text="untrusted collected source")
        self.limits = ReasoningBudget(
            model_calls=4, tokens=1000, cost_nano_usd=10000, tool_calls=4, backend_reads=8
        )
        self.read_calls: list[ReadRequest] = []
        self.read_failure: Exception | None = None
        self.registries: list[ReadRegistry] = []
        self.bind()

    def bind(self) -> None:
        """New process-equivalent clients share only durable engine files and artifacts."""
        self.adapter = Adapter()
        self.runtime = ModelRuntime(self.adapter, lambda: self.adapter.allow)
        self.ledger = BudgetLedger(self.engine)
        self.loop = ReasoningLoop(
            self.root / "runs",
            self.store,
            self.ledger,
            self.runtime,
            self.factory,
            subject="actor",
            causes=frozenset({"dependency_unavailable"}),
            limits=self.limits,
        )

    def factory(self, reserve: Reserve) -> ReadRegistry:
        """Registry invokes the loop's actual SQL reservation before fixture transport dispatch."""
        registry = ReadRegistry(
            {name: self.read for name in CATALOG},
            lambda: self.adapter.allow,
            reserve,
            lambda evidence: verify_evidence(evidence, self.store),
            "incident",
        )
        self.registries.append(registry)
        return registry

    def read(self, request: ReadRequest) -> tuple[EvidenceItem, ...]:
        """The handler checks that its read operation is already durably charged."""
        assert any(charge.kind == "reads" for charge in self.ledger.get("incident").charges)
        self.read_calls.append(request)
        if self.read_failure is not None:
            raise self.read_failure
        return (self.additional,)

    def finish(self, evidence: EvidenceItem | None = None) -> dict[str, Any]:
        """Generate one valid cause using the actual submitted artifact's opaque evidence ID."""
        result = finished()
        result["hypotheses"][0]["supporting_evidence_ids"] = [
            (evidence or self.initial).evidence_id
        ]
        return result

    def responses(self, *values: dict[str, Any] | str) -> None:
        """Run actual installed LangChain fake messages with bounded synthetic usage metadata."""
        self.adapter.chat = FakeMessagesListChatModel(
            responses=[
                reply(json.dumps(value) if isinstance(value, dict) else value) for value in values
            ]
        )

    def run(self) -> LoopResult:
        """Every call reopens the same immutable run binding and reconstructs durable history."""
        return self.loop.run(self.incident, (self.initial,))

    def restart(self) -> None:
        """Dispose connections and construct new runtime/ledger instances over retained state."""
        self.close()
        self.engine = create_engine(self.url)
        self.registries = []
        self.read_calls = []
        self.bind()

    def close(self) -> None:
        """Drain only bounded fixture workers before disposing the owned SQL engine."""
        for registry in self.registries:
            registry.close()
            registry._pool.shutdown(wait=True)  # pyright: ignore[reportPrivateUsage]
        self.runtime.close()
        self.runtime._pool.shutdown(wait=True)  # pyright: ignore[reportPrivateUsage]
        self.engine.dispose()


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Harness]:
    """Complete fixture cleanup even when testing deliberate interrupted publications."""
    value = Harness(tmp_path)
    try:
        yield value
    finally:
        value.close()


def test_read_finish_restart_replays_without_model_or_read_calls(harness: Harness) -> None:
    """The real second model sees newly read evidence; restart consumes exact receipts only."""
    h = harness
    h.responses(read_decision(), h.finish(h.additional))
    first = h.run()
    assert first.stop_reason == "FINISHED" and first.mode == "fixture"
    assert first.hypotheses[0].supporting_evidence_ids == (h.additional.evidence_id,)
    assert len(h.adapter.calls) == 2 and len(h.read_calls) == 1
    assert len(first.receipt_sha256s) == 3
    record = h.ledger.get("incident")
    assert [charge.operation_id for charge in record.charges] == [
        "model-01",
        "reads-01",
        "model-02",
    ]
    assert len(record.completions) == 3
    second_prompt = h.adapter.calls[1][0]
    assert isinstance(second_prompt[1].content, str)
    data = json.loads(second_prompt[1].content)
    assert data["prior_results"] == [{"tool": "recent_logs", "status": "OK"}]
    assert len(data["context"]["entries"]) == 2
    h.restart()
    assert h.run() == first
    assert not h.adapter.calls and not h.read_calls
    assert h.ledger.get("incident") == record


def test_refusal_has_no_hypotheses_or_read_dispatch(harness: Harness) -> None:
    """A structured refusal ends the durable run without pretending to identify a cause."""
    h = harness
    h.responses({"decision": "refuse", "summary": "Cannot continue", "reads": [], "hypotheses": []})
    result = h.run()
    assert result.stop_reason == "REFUSED" and not result.hypotheses
    assert len(h.adapter.calls) == 1 and not h.read_calls


@pytest.mark.parametrize("malformed", [1, 2])
def test_one_malformed_retry_and_two_stop(harness: Harness, malformed: int) -> None:
    """Only one schema-repair turn is allowed, and raw invalid text never enters feedback."""
    h = harness
    h.responses(*(["private invalid output"] * malformed), h.finish())
    result = h.run()
    assert result.stop_reason == ("FINISHED" if malformed == 1 else "INVALID_OUTPUT")
    assert len(h.adapter.calls) == 2 and not h.read_calls
    content = h.adapter.calls[1][0][1].content
    assert isinstance(content, str) and "private invalid output" not in content
    assert json.loads(content)["prior_results"] == [
        {"status": "INVALID_OUTPUT", "instruction": "Use schema"}
    ]


@pytest.mark.parametrize("limits", [{"model_calls": 0}, {"tokens": 119}, {"cost_nano_usd": 139}])
def test_model_budget_prevents_dispatch(harness: Harness, limits: dict[str, int]) -> None:
    """Exact prompt plus maximum output reservation is denied before any provider invocation."""
    h = harness
    h.loop.limits = h.limits.model_copy(update=limits)
    h.responses(h.finish())
    result = h.run()
    assert result.stop_reason == "BUDGET_EXHAUSTED"
    assert not h.adapter.calls and not h.read_calls
    assert not h.ledger.get("incident").charges


@pytest.mark.parametrize("limits", [{"tool_calls": 0}, {"backend_reads": 0}])
def test_read_budget_prevents_handler_dispatch(harness: Harness, limits: dict[str, int]) -> None:
    """A model can request a read, but SQL denial stops the batch before its first handler."""
    h = harness
    h.loop.limits = h.limits.model_copy(update=limits)
    h.responses(read_decision())
    assert h.run().stop_reason == "BUDGET_EXHAUSTED"
    assert len(h.adapter.calls) == 1 and not h.read_calls
    assert len(h.ledger.get("incident").charges) == 1


@pytest.mark.parametrize("phase", ["model", "reads"])
def test_crash_after_reservation_never_repeats_unknown_operation(
    harness: Harness, phase: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An effect may finish before publication; a restart preserves its charge and stops unknown."""
    import payops.orchestrator.loop as module

    h = harness
    h.responses(read_decision(), h.finish(h.additional))
    publish = module.publish

    def interrupted(
        ledger: BudgetLedger, store: ArtifactStore, receipt: ModelReceipt | ReadReceipt
    ) -> str:
        """Interrupt only the selected receipt after its real fixture effect was performed."""
        if isinstance(receipt, ModelReceipt) == (phase == "model"):
            raise KeyboardInterrupt
        return publish(ledger, store, receipt)

    with monkeypatch.context() as patch:
        patch.setattr(module, "publish", interrupted)
        with pytest.raises(KeyboardInterrupt):
            h.run()
    record = h.ledger.get("incident")
    assert len(record.charges) == (1 if phase == "model" else 2)
    assert len(record.completions) == (0 if phase == "model" else 1)
    h.restart()
    assert h.run().stop_reason == "UNKNOWN_COMPLETION"
    assert not h.adapter.calls and not h.read_calls
    assert h.ledger.get("incident") == record


@pytest.mark.parametrize("target", ["prompt", "model-result", "read-result", "source"])
def test_corrupt_replay_artifact_fails_before_new_calls(harness: Harness, target: str) -> None:
    """Each persisted layer is independently checked; a matching run ID cannot hide bad bytes."""
    h = harness
    h.responses(read_decision(), h.finish(h.additional))
    h.run()
    record = h.ledger.get("incident")
    charge = record.charges[0]
    assert isinstance(charge, ModelCharge)
    digest = {
        "prompt": charge.prompt_sha256,
        "model-result": record.completions[0].artifact_sha256,
        "read-result": record.completions[1].artifact_sha256,
        "source": h.additional.artifact_sha256,
    }[target]
    h.store.path_for(digest).write_bytes(b"corrupted")
    h.restart()
    with pytest.raises(EvidenceIntegrityError):
        h.run()
    assert not h.adapter.calls and not h.read_calls


@pytest.mark.parametrize("binding", ["subject", "causes", "settings", "limits", "initial"])
def test_restart_cannot_rebind_identity_or_refill_allowance(harness: Harness, binding: str) -> None:
    """SQL binds actor, provider, initial source identities and remaining limits."""
    h = harness
    h.responses(h.finish())
    h.run()
    h.restart()
    if binding == "subject":
        h.loop.subject = "other-actor"
    elif binding == "causes":
        h.loop.causes = frozenset({"other-cause"})
    elif binding == "settings":
        h.runtime.settings = h.runtime.settings.model_copy(update={"model": "different"})
    elif binding == "limits":
        h.loop.limits = h.limits.model_copy(update={"model_calls": 5})
    else:
        h.initial = h.additional
    with pytest.raises(BudgetConflict, match="binding"):
        h.run()
    assert not h.adapter.calls and not h.read_calls


@pytest.mark.parametrize("failure", [RuntimeError, PermissionError])
def test_failed_reads_are_explicit_and_denial_stops(
    harness: Harness, failure: type[Exception]
) -> None:
    """Transport failures feed typed status to the model; authorization denial stops the loop."""
    h = harness
    h.responses(read_decision(), h.finish())
    h.read_failure = failure("private backend error")
    result = h.run()
    assert result.stop_reason == ("DENIED" if failure is PermissionError else "FINISHED")
    assert result.evidence == (h.initial,)
    assert len(h.adapter.calls) == (1 if failure is PermissionError else 2)
    if failure is RuntimeError:
        content = h.adapter.calls[1][0][1].content
        assert isinstance(content, str) and "private backend error" not in content
        assert json.loads(content)["prior_results"] == [{"tool": "recent_logs", "status": "ERROR"}]


def test_initial_denial_prevents_context_and_model_work(harness: Harness) -> None:
    """Current authority is required even when replay would otherwise need no provider call."""
    h = harness
    h.adapter.allow = False
    assert h.run().stop_reason == "DENIED"
    assert not h.adapter.calls and not h.read_calls


def test_prepared_prompt_is_retained_before_model_invocation(harness: Harness) -> None:
    """The charge digest resolves to the exact prepared host roles, context and fixture count."""
    h = harness
    h.responses(h.finish())
    h.run()
    charged = h.ledger.get("incident").charges[0]
    assert isinstance(charged, ModelCharge)
    prepared = restore(h.store, charged.prompt_sha256, PreparedTurn)
    assert prepared.prompt.input_tokens == charged.input_tokens == 100
    assert prepared.prompt.langchain_messages() == h.adapter.calls[0][0]
    assert prepared.context.evidence_ids() == frozenset({h.initial.evidence_id})


def test_replay_reauthorizes_before_final_publication(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Revocation during saved receipt verification removes a previously valid final decision."""
    import payops.orchestrator.loop as module

    h = harness
    h.responses(h.finish())
    h.run()
    h.restart()
    original = module.restore

    def revoke(*args: Any, **kwargs: Any) -> Any:
        """A source read can take long enough for the initiating account grant to change."""
        result = original(*args, **kwargs)
        if isinstance(result, ModelReceipt):
            h.adapter.allow = False
        return result

    monkeypatch.setattr(module, "restore", revoke)
    result = h.run()
    assert result.stop_reason == "DENIED" and not result.hypotheses
    assert not h.adapter.calls and not h.read_calls


def replace_record(h: Harness, record: BudgetRecord) -> None:
    """Simulate valid-shaped SQL corruption without relying on malformed JSON or failed hashes."""
    validated = BudgetRecord.model_validate_json(record.model_dump_json())
    with Session(h.engine) as session:
        row = session.get(BudgetRow, "incident")
        assert row is not None
        row.payload = validated.model_dump_json()
        session.commit()


@pytest.mark.parametrize(
    "case", ["charge-kind", "prepared-binding", "model-binding", "read-charge", "read-binding"]
)
def test_valid_rehashed_receipts_must_agree_with_ledger(harness: Harness, case: str) -> None:
    """Valid JSON and artifact digests cannot conceal cross-operation or cross-run substitutions."""
    h = harness
    h.responses(read_decision(), h.finish(h.additional))
    h.run()
    record = h.ledger.get("incident")
    charges, completions = list(record.charges), list(record.completions)
    request = ReadRequest(tool="pod_events", service="payments-api", query=None)
    first = charges[0]
    assert isinstance(first, ModelCharge)
    if case == "charge-kind":
        charges[0] = ReadCharge(operation_id="model-01", requests=(request,), backend_read_count=2)
    elif case == "prepared-binding":
        prepared = restore(h.store, first.prompt_sha256, PreparedTurn)
        digest = retain(h.store, prepared.model_copy(update={"binding_sha256": "f" * 64}))
        charges[0] = first.model_copy(update={"prompt_sha256": digest})
    elif case == "model-binding":
        receipt = restore(h.store, completions[0].artifact_sha256, ModelReceipt)
        digest = retain(h.store, receipt.model_copy(update={"run_id": "foreign"}))
        completions[0] = completions[0].model_copy(update={"artifact_sha256": digest})
    elif case == "read-charge":
        charges[1] = ReadCharge(operation_id="reads-01", requests=(request,), backend_read_count=2)
    else:
        read_receipt = restore(h.store, completions[1].artifact_sha256, ReadReceipt)
        digest = retain(h.store, read_receipt.model_copy(update={"operation_id": "other"}))
        completions[1] = completions[1].model_copy(update={"artifact_sha256": digest})
    replace_record(
        h, record.model_copy(update={"charges": tuple(charges), "completions": tuple(completions)})
    )
    h.restart()
    with pytest.raises(EvidenceIntegrityError):
        h.run()
    assert not h.adapter.calls and not h.read_calls


@pytest.mark.parametrize(
    "case", ["failed-with-evidence", "foreign-service", "foreign-incident", "id-collision"]
)
def test_saved_read_evidence_revalidated_after_rehash(harness: Harness, case: str) -> None:
    """A forged but correctly hashed completed batch still cannot publish foreign evidence."""
    h = harness
    h.responses(read_decision(), h.finish(h.additional))
    h.run()
    record = h.ledger.get("incident")
    completion = record.completions[1]
    receipt = restore(h.store, completion.artifact_sha256, ReadReceipt)
    result = receipt.results[0]
    if case == "failed-with-evidence":
        result = result.model_copy(update={"status": "ERROR"})
    else:
        envelope = h.store.verify(h.additional)
        metadata = envelope["evidence"]
        assert isinstance(metadata, dict)
        key, value = {
            "foreign-service": ("resource", "risk-sim"),
            "foreign-incident": ("incident_id", "foreign"),
            "id-collision": ("evidence_id", h.initial.evidence_id),
        }[case]
        metadata[key] = value
        uri, digest = h.store.write(envelope)
        evidence = h.additional.model_copy(
            update={key: value, "artifact_uri": uri, "artifact_sha256": digest}
        )
        h.store.verify(evidence)
        result = result.model_copy(update={"evidence": (evidence,)})
    digest = retain(h.store, receipt.model_copy(update={"results": (result,)}))
    completions = list(record.completions)
    completions[1] = completion.model_copy(update={"artifact_sha256": digest})
    replace_record(h, record.model_copy(update={"completions": tuple(completions)}))
    h.restart()
    with pytest.raises(EvidenceIntegrityError):
        h.run()
    assert not h.adapter.calls and not h.read_calls


def test_evidence_merge_cap_does_not_publish_prefix(harness: Harness) -> None:
    """A whole batch that would exceed256 verified sources leaves prior session evidence intact."""
    h = harness
    initial = tuple(item(h.store) for _ in range(256))
    session = LoopSession(h.loop, h.incident, "a" * 64, initial)
    request = ReadRequest(tool="recent_logs", service="payments-api", query=None)
    with pytest.raises(LoopStopped) as caught:
        session.accept_reads((ReadResult(request=request, status="OK", evidence=(h.additional,)),))
    assert caught.value.reason == "BUDGET_EXHAUSTED" and session.evidence == initial


def test_prompt_token_limit_stops_before_charge(harness: Harness) -> None:
    """Tokenizer input above the configured model cap is a bounded stop with zero SQL charges."""
    h = harness
    h.adapter.tokens = 201
    assert h.run().stop_reason == "BUDGET_EXHAUSTED"
    assert not h.ledger.get("incident").charges and not h.adapter.calls


@pytest.mark.parametrize("limit", [100, 2000, 4096, 16000])
def test_local_context_omissions_keep_all_sources_verified(harness: Harness, limit: int) -> None:
    """Smaller prompts cannot admit omitted citations or hide corruption in dropped evidence."""
    h = harness
    h.runtime.settings = h.runtime.settings.model_copy(
        update={"provider": "local_llama", "input_token_limit": limit}
    )
    sources = tuple(item(h.store, text="observed " * 160) for _ in range(20))
    session = LoopSession(h.loop, h.incident, "a" * 64, sources)
    context = session.context()
    assert len(context.model_dump_json()) <= max(128, min(4096, limit))
    assert context.omitted_count == len(sources) - len(context.entries) > 0
    omitted = next(source for source in sources if source.evidence_id not in context.evidence_ids())
    with pytest.raises(ValueError, match="unresolved"):
        parse_decision(json.dumps(h.finish(omitted)), context.evidence_ids(), h.loop.causes)
    h.store.path_for(omitted.artifact_sha256).write_text("{}", encoding="utf-8")
    with pytest.raises(EvidenceIntegrityError):
        session.context()


def test_local_prompt_preserves_facts_and_replays_without_dispatch(harness: Harness) -> None:
    """Compact presentation keeps exact citation IDs/facts and durable original context."""
    h = harness
    settings = h.runtime.settings.model_copy(
        update={"provider": "local_llama", "input_token_limit": 4096}
    )
    h.runtime.settings = h.adapter.settings = settings
    h.adapter.chat = FakeMessagesListChatModel(responses=[reply(json.dumps(h.finish()))])
    first = h.run()
    assert first.stop_reason == "FINISHED"
    charge = h.ledger.get("incident").charges[0]
    assert isinstance(charge, ModelCharge)
    prepared = restore(h.store, charge.prompt_sha256, PreparedTurn)
    data = json.loads(str(h.adapter.calls[0][0][1].content))
    entry = data["context"]["entries"][0]
    assert entry["facts"] == prepared.context.entries[0].facts
    assert entry["evidence"]["evidence_id"] == h.initial.evidence_id
    assert "summary" not in entry["evidence"] and "artifact_sha256" not in entry["evidence"]
    assert prompt_context(prepared.context, local=False) == prepared.context.model_dump(mode="json")
    assert h.run() == first and len(h.adapter.calls) == 1


def test_local_metadata_only_entry_keeps_summary(harness: Harness) -> None:
    """When bounded source facts are omitted, their original summary must remain visible."""
    h = harness
    h.runtime.settings = h.runtime.settings.model_copy(
        update={"provider": "local_llama", "input_token_limit": 4096}
    )
    source = item(h.store, text="x" * 30000)
    context = LoopSession(h.loop, h.incident, "a" * 64, (source,)).context()
    data = json.loads(json.dumps(prompt_context(context, local=True)))
    entry = data["entries"][0]
    assert entry["facts"] is None and entry["facts_omitted"] is True
    assert entry["evidence"]["summary"] == source.summary


def test_local_read_budget_denial_retains_one_final_decision(harness: Harness) -> None:
    """No backend effect follows exhausted reads; a remaining model turn may honestly refuse."""
    h = harness
    h.runtime.settings = h.adapter.settings = h.runtime.settings.model_copy(
        update={"provider": "local_llama", "input_token_limit": 4096}
    )
    h.loop.limits = h.limits.model_copy(update={"tool_calls": 0, "backend_reads": 0})
    refusal: dict[str, Any] = {
        "decision": "refuse", "summary": "Insufficient evidence", "reads": [], "hypotheses": []
    }
    h.adapter.chat = FakeMessagesListChatModel(
        responses=[reply(json.dumps(read_decision())), reply(json.dumps(refusal))]
    )
    result = h.run()
    assert result.stop_reason == "REFUSED" and not result.hypotheses
    assert not h.read_calls and len(h.adapter.calls) == 2
    data = json.loads(str(h.adapter.calls[-1][0][1].content))
    assert data["allowed_decisions"] == ["finish", "refuse"]
    assert data["prior_results"][-1]["status"] == "READ_BUDGET_EXHAUSTED"
    before = h.ledger.get("incident")
    assert all(isinstance(charge, ModelCharge) for charge in before.charges)
    assert h.run() == result and h.ledger.get("incident") == before
    assert len(h.adapter.calls) == 2 and not h.read_calls


@pytest.mark.parametrize(
    "changes",
    [
        {"subject": ""},
        {"causes": frozenset[str]()},
        {"causes": frozenset(str(i) for i in range(65))},
    ],
)
def test_unbounded_or_missing_host_binding_rejected(
    harness: Harness, changes: dict[str, Any]
) -> None:
    """Host configuration must identify an actor and supply a bounded nonempty cause vocabulary."""
    h = harness
    with pytest.raises(ValueError, match="actor binding"):
        ReasoningLoop(
            h.root / "other-run",
            h.store,
            h.ledger,
            h.runtime,
            h.factory,
            subject=changes.get("subject", "actor"),
            causes=changes.get("causes", frozenset({"cause"})),
            limits=h.limits,
        )


def test_zero_turn_budget_still_rejects_corrupt_initial_source(harness: Harness) -> None:
    """Exhausted model allowance cannot bypass initial source integrity verification."""
    h = harness
    h.loop.limits = h.limits.model_copy(update={"model_calls": 0})
    h.store.path_for(h.initial.artifact_sha256).write_bytes(b"corrupt")
    with pytest.raises(EvidenceIntegrityError):
        h.run()
    assert not h.adapter.calls and not h.read_calls


def test_read_timeout_reentry_never_creates_another_reader(harness: Harness) -> None:
    """Saved timeout stops new factories while a closed registry still owns active transport."""
    h = harness
    h.responses(read_decision(), h.finish(h.additional))
    release = Event()

    def held(request: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Keep the real worker active after registry acceptance has timed out."""
        h.read_calls.append(request)
        assert release.wait(3)
        return (h.additional,)

    def factory(reserve: Reserve) -> ReadRegistry:
        """Create one actual timed registry and retain it for bounded fixture cleanup."""
        registry = ReadRegistry(
            {name: held for name in CATALOG},
            lambda: True,
            reserve,
            lambda evidence: verify_evidence(evidence, h.store),
            "incident",
            deadline_ceiling=0.03,
        )
        h.registries.append(registry)
        return registry

    h.loop.registry_factory = factory
    try:
        first = h.run()
        assert first.stop_reason == "TIMEOUT" and not first.hypotheses
        assert h.run().stop_reason == "TIMEOUT"
        assert len(h.adapter.calls) == len(h.read_calls) == len(h.registries) == 1
        assert len(h.ledger.get("incident").charges) == 2
    finally:
        release.set()


def test_persistent_auth_outage_stops_without_queued_work(harness: Harness) -> None:
    """The loop cannot dispatch or enqueue another auth request behind a stalled grant."""
    h = harness
    release = Event()
    calls: list[str] = []
    h.runtime.settings = h.runtime.settings.model_copy(update={"timeout_seconds": 0.03})
    h.adapter.settings = h.runtime.settings

    def blocked() -> bool:
        """Represent one unavailable identity provider under a bounded fixture wait."""
        calls.append("identity")
        assert release.wait(3)
        return True

    h.runtime.authorize = blocked
    try:
        assert h.run().stop_reason == "TIMEOUT"
        assert h.run().stop_reason == "BUSY"
        assert calls == ["identity"] and not h.adapter.calls and not h.read_calls
        assert not h.ledger.get("incident").charges
    finally:
        release.set()
