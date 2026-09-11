"""Independent valid-artifact controls exercise guards without a second masking failure."""

import json

import pytest
from test_reasoning_loop import Harness, read_decision, replace_record
from test_reasoning_loop import harness as harness

from payops.contracts import Contract, EvidenceItem, Incident
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.orchestrator.budget import ModelCharge
from payops.orchestrator.loop import LoopResult, LoopSession, ReasoningLoop
from payops.orchestrator.loop_records import ModelReceipt, PreparedTurn, restore, retain
from payops.orchestrator.reasoning import ReadRequest
from payops.orchestrator.state import InvestigationState
from payops.tools.registry import ReadResult


@pytest.mark.parametrize("kind", ["model", "reads"])
def test_denied_reservation_has_no_backend_effect(harness: Harness, kind: str) -> None:
    """An incidental later exception cannot hide an effect dispatched without a NEW charge."""
    h = harness
    limits = {"tokens": 119} if kind == "model" else {"tool_calls": 0}
    h.loop.limits = h.limits.model_copy(update=limits)
    h.responses(read_decision())
    entered: list[ReadRequest] = []
    original_read = h.read

    def tracked(request: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Observe dispatch before the fixture reader's separate SQL invariant can raise."""
        entered.append(request)
        return original_read(request)

    h.read = tracked
    try:
        h.run()
    except Exception:
        pass
    assert len(h.adapter.calls) == (0 if kind == "model" else 1)
    assert not entered


def test_valid_json_receipt_tamper_requires_anchored_digest(harness: Harness) -> None:
    """Valid schema with a different output digest must still fail the anchored byte check."""
    h = harness
    h.responses(h.finish())
    h.run()
    digest = h.ledger.get("incident").completions[0].artifact_sha256
    path = h.store.path_for(digest)
    payload = json.loads(path.read_text())
    payload["observation"]["output_sha256"] = "f" * 64
    ModelReceipt.model_validate_json(json.dumps(payload))
    path.write_text(json.dumps(payload), encoding="utf-8")
    h.restart()
    with pytest.raises(EvidenceIntegrityError):
        h.run()


def test_coherently_reanchored_prepared_binding_is_rejected(harness: Harness) -> None:
    """The response points to the changed prompt, isolating the prepared-turn binding check."""
    h = harness
    h.responses(h.finish())
    h.run()
    record = h.ledger.get("incident")
    charged, completed = record.charges[0], record.completions[0]
    assert isinstance(charged, ModelCharge)
    prepared = restore(h.store, charged.prompt_sha256, PreparedTurn)
    prompt_digest = retain(h.store, prepared.model_copy(update={"binding_sha256": "f" * 64}))
    receipt = restore(h.store, completed.artifact_sha256, ModelReceipt)
    receipt_digest = retain(h.store, receipt.model_copy(update={"prompt_sha256": prompt_digest}))
    replace_record(
        h,
        record.model_copy(
            update={
                "charges": (charged.model_copy(update={"prompt_sha256": prompt_digest}),),
                "completions": (completed.model_copy(update={"artifact_sha256": receipt_digest}),),
            }
        ),
    )
    h.restart()
    with pytest.raises(EvidenceIntegrityError, match="prompt binding"):
        h.run()
    assert not h.adapter.calls


def test_independently_valid_foreign_service_rejected_before_merge(harness: Harness) -> None:
    """No later model-prompt comparison masks the read-service scope predicate."""
    h = harness
    envelope = h.store.verify(h.additional)
    metadata = envelope["evidence"]
    assert isinstance(metadata, dict)
    metadata["resource"] = "risk-sim"
    uri, digest = h.store.write(envelope)
    foreign = h.additional.model_copy(
        update={
            "resource": "risk-sim",
            "artifact_uri": uri,
            "artifact_sha256": digest,
        }
    )
    h.store.verify(foreign)
    session = LoopSession(h.loop, h.incident, "a" * 64, (h.initial,))
    result = ReadResult(
        request=ReadRequest(tool="recent_logs", service="payments-api", query=None),
        status="OK",
        evidence=(foreign,),
    )
    with pytest.raises(EvidenceIntegrityError, match="scope"):
        session.accept_reads((result,))
    assert session.evidence == (h.initial,)


def test_foreign_result_has_valid_separate_ledger(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing-row exception cannot stand in for rejecting a returned foreign incident."""
    from test_graph_reasoning import GraphHarness, allowance

    from payops.orchestrator.graph_reasoning import reason

    h, graph = harness, GraphHarness(harness)
    state = graph.worker(pause=True).start(h.incident, allowance())
    loop = graph.factory(state, h.store)
    loop.ledger.open("foreign", "b" * 64, loop.limits)

    def foreign(incident: Incident, initial: tuple[EvidenceItem, ...]) -> LoopResult:
        """Return a valid foreign run whose separate budget row also exists."""
        return LoopResult(
            run_id="foreign",
            mode="fixture",
            stop_reason="FINISHED",
            evidence=(),
            receipt_sha256s=(),
        )

    def factory(state: InvestigationState, store: ArtifactStore) -> ReasoningLoop:
        """Preserve all other host bindings while substituting only the returned run."""
        return loop

    monkeypatch.setattr(loop, "run", foreign)
    with pytest.raises(EvidenceIntegrityError, match="another incident"):
        reason(state, h.store, factory)


def test_foreign_store_with_valid_identical_sources_rejected(harness: Harness) -> None:
    """Copy source bytes first so missing artifacts cannot mask root-store binding removal."""
    import shutil

    from test_graph_reasoning import GraphHarness, allowance

    from payops.orchestrator.graph_reasoning import reason

    h, graph = harness, GraphHarness(harness)
    state = graph.worker(pause=True).start(h.incident, allowance())
    loop = graph.factory(state, h.store)
    other = ArtifactStore(h.root / "other-valid-artifacts")
    shutil.copytree(h.store.root, other.root, dirs_exist_ok=True)
    other.verify(h.initial)
    other.verify(h.additional)
    loop.store = other

    def factory(state: InvestigationState, store: ArtifactStore) -> ReasoningLoop:
        """The foreign root contains valid source bytes, isolating the root binding check."""
        return loop

    with pytest.raises(EvidenceIntegrityError, match="store"):
        reason(state, h.store, factory)
    assert not h.adapter.calls


def test_source_changed_during_result_restore_cannot_publish(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Publication rechecks source bytes after the saved model decision has been verified."""
    import payops.orchestrator.loop as module

    h = harness
    h.responses(h.finish())
    original = module.restore

    def altered(store: ArtifactStore, digest: str, schema: type[Contract]) -> Contract:
        """Modify source bytes only after the actual saved model receipt has been verified."""
        result = original(store, digest, schema)
        if isinstance(result, ModelReceipt):
            path = h.store.path_for(h.initial.artifact_sha256)
            envelope = json.loads(path.read_text())
            envelope["payload"]["text"] = "changed after model completion"
            path.write_text(json.dumps(envelope), encoding="utf-8")
        return result

    monkeypatch.setattr(module, "restore", altered)
    with pytest.raises(EvidenceIntegrityError):
        h.run()
