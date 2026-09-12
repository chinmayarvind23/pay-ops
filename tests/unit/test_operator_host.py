"""Exercise the real local graph, SQL and client boundaries with explicit wire fixtures."""

import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Any

import certifi
import httpx
import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver
from sqlalchemy.orm import Session
from test_data_clients import evidence
from test_local_llama import Wire
from test_local_llama import settings as local_settings
from test_openai_adapter import generation, response
from test_payment_window import interval, raw_snapshot

from payops.contracts import EvidenceItem, Incident, IncidentCreate, utc_now
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.memory.data_clients import INDEXES
from payops.operator_config import LocalGrant, OperatorConfig, SecretReference
from payops.operator_host import (
    DeferredClose,
    HostFixture,
    KnowledgeBundle,
    NoDispatchAdapter,
    OperatorHost,
    ReadOnlyLedger,
    main,
)
from payops.orchestrator.budget import (
    BudgetConflict,
    BudgetRecord,
    BudgetRow,
    ModelCharge,
    ReadCharge,
)
from payops.orchestrator.graph import compile_graph
from payops.orchestrator.loop_records import ModelReceipt, restore, retain
from payops.orchestrator.nodes import incident_directory
from payops.orchestrator.reasoning import ReadRequest
from payops.orchestrator.state import InvestigationState, pack
from payops.tools.kubernetes import SERVICES
from payops.tools.registry import CATALOG


class HostHarness:
    """Every source is a synthetic wire response, while orchestration and persistence are real."""

    def __init__(self, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Make an explicit grant, credential references and original artifact-backed knowledge."""
        self.root = root
        self.account = "test-os-account"
        self.kube_calls: list[tuple[str, ...]] = []
        self.prom_calls: list[httpx.Request] = []
        self.es_calls: list[httpx.Request] = []
        self.model_calls: list[httpx.Request] = []
        self.model_data: list[dict[str, Any]] = []
        self.finish_first = False
        self.grant_path = root / "grant.json"
        self.write_grant()
        kubeconfig = root / "kubeconfig"
        kubeconfig.write_text("synthetic kubeconfig", encoding="utf-8")

        def executable(name: str) -> str:
            """Only the command builder is real; no subprocess is executed by this fixture."""
            return "fixture-kubectl"

        monkeypatch.setattr("payops.tools.kubernetes.shutil.which", executable)
        monkeypatch.setenv("PAYOPS_TEST_OPENAI", "synthetic-provider-key")
        monkeypatch.setenv("PAYOPS_TEST_ELASTIC", "synthetic-elastic-password")
        knowledge = ArtifactStore(root / "knowledge")
        self.documents = {
            kind: evidence(knowledge, source=kind, resource="payments-api")
            for kind in ("RUNBOOK", "MEMORY")
        }
        bundle = root / "knowledge.json"
        bundle.write_text(
            json.dumps(
                {
                    "artifacts_root": str(root / "knowledge"),
                    "items": [item.model_dump(mode="json") for item in self.documents.values()],
                }
            ),
            encoding="utf-8",
        )
        self.config = OperatorConfig(
            runtime=root / "runtime",
            kubeconfig=kubeconfig,
            grant_file=self.grant_path,
            release_labels=Path(__file__).resolve().parents[2] / "evals/golden/release-v2.json",
            knowledge_bundle=bundle,
            elastic_ca=Path(certifi.where()),
            provider_key=SecretReference(environment="PAYOPS_TEST_OPENAI"),
            elastic_key=SecretReference(environment="PAYOPS_TEST_ELASTIC"),
        )
        self.host: OperatorHost | None = None
        self.fixture = HostFixture(
            kubernetes=self.kubernetes,
            prometheus=httpx.MockTransport(self.prometheus),
            elasticsearch=httpx.MockTransport(self.elasticsearch),
            openai=httpx.MockTransport(self.openai),
            account=lambda: self.account,
        )
        self.incident = Incident(
            incident_id="operator-incident",
            request=IncidentCreate(title="Synthetic availability investigation"),
        )

    def write_grant(self, **changes: Any) -> None:
        """Refresh or revoke the explicit grant without rebuilding the host."""
        now = utc_now()
        grant = LocalGrant.model_validate(
            {
                "account": self.account,
                "enabled": True,
                "issued_at": now - timedelta(seconds=1),
                "expires_at": now + timedelta(hours=1),
                **changes,
            }
        )
        self.grant_path.write_text(grant.model_dump_json(), encoding="utf-8")

    def bind(self) -> OperatorHost:
        """A new host instance shares only saved files and explicit fixture transports."""
        self.host = OperatorHost(self.config, fixture=self.fixture)
        return self.host

    def kubernetes(self, args: tuple[str, ...]) -> str:
        """Initial and selected reads use the same actual fixed Kubernetes parser."""
        self.kube_calls.append(args)
        assert args[args.index("--context") + 1] == "kind-payops-dev"
        assert args[args.index("--namespace") + 1] == "payops-sandbox"
        if "logs" in args:
            return "Synthetic bounded service log"
        kind = args[args.index("get") + 1]
        if kind == "deployment":
            service = args[args.index("get") + 2]
            assert service in SERVICES
            return json.dumps(
                {
                    "metadata": {"name": service, "namespace": "payops-sandbox", "uid": service},
                    "spec": {"replicas": 1},
                    "status": {"readyReplicas": 1},
                }
            )
        assert kind in {"pods", "events"}
        return '{"items":[]}'

    def prometheus(self, request: httpx.Request) -> httpx.Response:
        """Return initial vectors and selected snapshots in their actual query formats."""
        self.prom_calls.append(request)
        if "time" not in request.url.params:
            return response({"status": "success", "data": {"resultType": "vector", "result": []}})
        at = float(request.url.params["time"])
        snapshot: Any = raw_snapshot(interval())
        snapshot["evaluated_at"] = at
        for row in snapshot["metrics"]["data"]["result"]:
            row["value"][0] = at
        snapshot["watermark"]["data"]["result"][0]["value"] = [at, str(at - 0.5)]
        part = "watermark" if "timestamp(" in request.url.params["query"] else "metrics"
        return response(snapshot[part])

    def elasticsearch(self, request: httpx.Request) -> httpx.Response:
        """Actual source hashes and copied artifacts verify the fixed scoped Elasticsearch hits."""
        self.es_calls.append(request)
        body = json.loads(request.content)
        filters = body["query"]["bool"]["filter"]
        assert filters[:2] == [
            {"term": {"namespace": "payops-sandbox"}},
            {"term": {"service": "payments-api"}},
        ]
        kind = filters[2]["term"]["kind"]
        item = self.documents[kind]
        return response(
            {
                "timed_out": False,
                "_shards": {"failed": 0},
                "hits": {
                    "hits": [
                        {
                            "_index": INDEXES[kind],
                            "_id": item.artifact_sha256,
                            "_source": {
                                "namespace": "payops-sandbox",
                                "service": "payments-api",
                                "kind": kind,
                                "evidence": item.model_dump(mode="json"),
                            },
                        }
                    ]
                },
            }
        )

    def openai(self, request: httpx.Request) -> httpx.Response:
        """A scripted provider selects all six reads before citing one actually submitted source."""
        self.model_calls.append(request)
        assert self.host is not None
        charge = self.host.ledger.get(self.incident.incident_id).charges[-1]
        assert isinstance(charge, ModelCharge) and charge.provider_requests == 2
        if request.url.path.endswith("/input_tokens"):
            return response({"object": "response.input_tokens", "input_tokens": 100})
        payload = json.loads(request.content)
        data = json.loads(payload["input"][1]["content"])
        self.model_data.append(data)
        assert set(data["cause_codes"]) == self.host.causes
        assert set(entry["name"] for entry in data["catalog"]["tools"]) == set(CATALOG)
        assert "primary_causes" not in data and "observation_conditions" not in data
        assert set(data).isdisjoint({"case_id", "scenario", "gold"})
        groups: list[tuple[str, str]] = [
            ("workload_status", "pod_events"),
            ("recent_logs", "payment_snapshot"),
            ("runbook_search", "incident_search"),
        ]
        turn = int(data["turn"])
        decision: dict[str, Any]
        if turn <= 3 and not self.finish_first:
            decision = {
                "decision": "read",
                "summary": "Need scoped evidence",
                "hypotheses": [],
                "reads": [
                    {
                        "tool": tool,
                        "service": "payments-api",
                        "query": "availability" if tool.endswith("search") else None,
                    }
                    for tool in groups[turn - 1]
                ],
            }
        else:
            decision = {
                "decision": "finish",
                "summary": "Synthetic fixture conclusion",
                "reads": [],
                "hypotheses": [
                    {
                        "cause_code": "PROCESSOR_UNAVAILABLE",
                        "confidence": 0.5,
                        "supporting_evidence_ids": [
                            data["context"]["entries"][0]["evidence"]["evidence_id"]
                        ],
                        "refuting_evidence_ids": [],
                        "missing_evidence": [],
                    }
                ],
            }
        result = generation()
        result["output"][0]["content"][0]["text"] = json.dumps(decision)
        return response(result)

    def close(self) -> None:
        """Release only this fixture's host and transport resources."""
        if self.host is not None:
            self.host.close()

    def census(self) -> tuple[int, int, int, int]:
        """Use concrete backend/provider request counts to detect unintended replay effects."""
        return len(self.kube_calls), len(self.prom_calls), len(self.es_calls), len(self.model_calls)

    def finished(self) -> tuple[OperatorHost, InvestigationState, ArtifactStore]:
        """A one-turn completed run keeps adversarial publication controls inexpensive."""
        self.finish_first = True
        host = self.bind()
        state = host.start(self.incident)
        assert state.reasoning_stop_reason == "FINISHED" and state.hypotheses
        root = incident_directory(host.worker.nodes.root, self.incident.incident_id)
        return host, state, ArtifactStore(root / "artifacts")


def checkpoint(host: OperatorHost, state: InvestigationState) -> None:
    """Inject a typed counterfactual through native checkpoint persistence, not a resume mock."""
    config: RunnableConfig = {"configurable": {"thread_id": state.incident.incident_id}}
    with SqliteSaver.from_conn_string(str(host.worker.database)) as saver:
        graph = compile_graph(host.worker.nodes, saver, False)
        graph.update_state(config, pack(state), as_node="finish")


def replace_record(host: OperatorHost, record: BudgetRecord) -> None:
    """Represent valid-shaped durable tampering while preserving actual SQL read boundaries."""
    validated = BudgetRecord.model_validate_json(record.model_dump_json())
    with Session(host.engine) as session:
        row = session.get(BudgetRow, record.run_id)
        assert row is not None
        row.payload = validated.model_dump_json()
        session.commit()


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[HostHarness]:
    """Each integration attempt owns isolated runtime/knowledge/grant files."""
    value = HostHarness(tmp_path, monkeypatch)
    try:
        yield value
    finally:
        value.close()


def test_full_host_catalog_native_graph_sql_and_restart_zero_dispatch(harness: HostHarness) -> None:
    """Connect all six reads and independently revalidate completed replay."""
    h = harness
    host = h.bind()
    state = host.start(h.incident)
    assert state.mode == "fixture_replay" and state.ranking_method == "model_provider"
    assert state.reasoning_stop_reason == "FINISHED" and state.terminal == "ESCALATED"
    assert state.phase == "FINISHED" and state.report is not None
    assert len(h.model_calls) == 8 and len(h.model_data) == 4
    assert len(h.kube_calls) == 30 and len(h.prom_calls) == 7 and len(h.es_calls) == 2
    assert state.tool_calls_reserved == 26 and state.backend_reads_reserved == 39
    record = host.ledger.get(h.incident.incident_id)
    assert len(record.charges) == len(record.completions) == 7
    reads = [charge for charge in record.charges if isinstance(charge, ReadCharge)]
    assert {request.tool for charge in reads for request in charge.requests} == set(CATALOG)
    assert sum(charge.backend_read_count for charge in reads) == 9
    assert {item.source for item in state.evidence} >= {"RUNBOOK", "MEMORY", "PROMETHEUS"}
    host.close()
    replay_host = h.bind()
    assert replay_host.resume(h.incident.incident_id) == state
    assert replay_host.ledger.get(h.incident.incident_id) == record
    assert (len(h.kube_calls), len(h.prom_calls), len(h.es_calls), len(h.model_calls)) == (
        30,
        7,
        2,
        8,
    )


def test_free_local_host_needs_no_provider_key_and_replays_without_calls(
    harness: HostHarness,
) -> None:
    """The same durable graph accepts a zero-price local model without opening remote transport."""
    h = harness
    wire = Wire()
    h.config = OperatorConfig.model_validate(
        {
            **h.config.model_dump(),
            "provider_key": None,
            "model": local_settings(),
            "reasoning": {**h.config.reasoning.model_dump(), "cost_nano_usd": 0},
        }
    )
    h.fixture = replace(h.fixture, openai=httpx.MockTransport(wire))
    host = h.bind()
    state = host.start(h.incident)
    assert state.reasoning_stop_reason == "REFUSED" and state.phase == "FINISHED"
    assert len(wire.calls) == 2 and not h.model_calls
    assert host.ledger.get(h.incident.incident_id).limits.cost_nano_usd == 0
    host.close()
    assert h.bind().resume(h.incident.incident_id) == state
    assert len(wire.calls) == 2


def test_free_local_host_rejects_key_or_positive_budget(harness: HostHarness) -> None:
    """A local profile cannot silently retain a paid provider credential or spending allowance."""
    base = {
        **harness.config.model_dump(),
        "model": local_settings(),
        "provider_key": None,
        "reasoning": {**harness.config.reasoning.model_dump(), "cost_nano_usd": 0},
    }
    for changes in (
        {"provider_key": harness.config.provider_key},
        {"reasoning": harness.config.reasoning},
    ):
        with pytest.raises(ValueError, match="reviewed bounds"):
            OperatorConfig.model_validate({**base, **changes})


def test_revoked_grant_prevents_credential_loading(
    harness: HostHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A denied host cannot use even explicitly configured credentials to construct clients."""
    harness.write_grant(enabled=False)

    def forbidden(reference: SecretReference) -> None:
        """Credential access itself is forbidden before current local authority."""
        pytest.fail("credential loaded before grant")

    monkeypatch.setattr(SecretReference, "load", forbidden)
    with pytest.raises(PermissionError):
        harness.bind()
    assert harness.census() == (0, 0, 0, 0) and not harness.config.runtime.exists()


@pytest.mark.parametrize("stage", ["kubernetes", "prometheus", "provider-count"])
def test_revocation_discards_inflight_result_and_stops_next_effect(
    harness: HostHarness, stage: str
) -> None:
    """Every initial command and the staged provider grant refresh has a real negative control."""
    h = harness
    original = h.fixture

    def kubernetes(args: tuple[str, ...]) -> str:
        """Revoke after the first actual command response was produced."""
        result = h.kubernetes(args)
        if stage == "kubernetes":
            h.write_grant(enabled=False)
        return result

    def prometheus(request: httpx.Request) -> httpx.Response:
        """Revoke after the first metrics response, before publication or another request."""
        result = h.prometheus(request)
        if stage == "prometheus":
            h.write_grant(enabled=False)
        return result

    def provider(request: httpx.Request) -> httpx.Response:
        """Counting is charged, but an expired interstage grant cannot authorize generation."""
        result = h.openai(request)
        if stage == "provider-count":
            h.write_grant(enabled=False)
        return result

    h.fixture = HostFixture(
        kubernetes,
        httpx.MockTransport(prometheus),
        original.elasticsearch,
        httpx.MockTransport(provider),
        original.account,
    )
    host = h.bind()
    with pytest.raises(PermissionError):
        host.start(h.incident)
    assert not h.es_calls and not h.model_data
    if stage == "kubernetes":
        assert h.census() == (1, 0, 0, 0)
    elif stage == "prometheus":
        assert len(h.prom_calls) == 1 and not h.model_calls
    else:
        assert len(h.model_calls) == 1
        assert len(host.ledger.get(h.incident.incident_id).charges) == 1


def test_completed_resume_requires_current_original_account(harness: HostHarness) -> None:
    """Neither a saved success nor a replacement account grant inherits earlier authority."""
    h = harness
    host, state, _ = h.finished()
    census = h.census()
    h.write_grant(enabled=False)
    with pytest.raises(PermissionError):
        host.resume(h.incident.incident_id)
    h.write_grant()
    assert host.resume(h.incident.incident_id) == state
    h.account = "other-os-account"
    h.write_grant()
    with pytest.raises(PermissionError):
        host.resume(h.incident.incident_id)
    host.close()
    replacement = h.bind()
    with pytest.raises(ValueError, match="ranking profile"):
        replacement.resume(h.incident.incident_id)
    assert h.census() == census


@pytest.mark.parametrize("target", ["source", "prompt", "receipt", "collection"])
def test_completed_replay_rechecks_all_saved_layers(harness: HostHarness, target: str) -> None:
    """Finished native graph state does not bypass source, prompt, result or collection hashes."""
    h = harness
    host, state, store = h.finished()
    record = host.ledger.get(h.incident.incident_id)
    charge = record.charges[0]
    assert isinstance(charge, ModelCharge)
    if target == "collection":
        path = incident_directory(host.worker.nodes.root, h.incident.incident_id)
        path = path / "graph-collection.json"
    else:
        digest = {
            "source": state.evidence[0].artifact_sha256,
            "prompt": charge.prompt_sha256,
            "receipt": record.completions[0].artifact_sha256,
        }[target]
        path = store.path_for(digest)
    path.write_bytes(b"corrupted")
    census = h.census()
    with pytest.raises((EvidenceIntegrityError, ValueError)):
        host.resume(h.incident.incident_id)
    assert h.census() == census and host.ledger.get(record.run_id) == record


@pytest.mark.parametrize("hidden", [False, True])
def test_unreasoned_report_cannot_hide_supported_result_or_journal(
    harness: HostHarness, hidden: bool
) -> None:
    """Matching typed report/checkpoint corruption cannot bypass SQL derivation verification."""
    h = harness
    host, state, _ = h.finished()
    assert state.report is not None
    updates: dict[str, Any] = {"reasoning_stop_reason": None}
    report_updates: dict[str, Any] = {"reasoning_stop_reason": None}
    if hidden:
        updates.update(hypotheses=(), reasoning_receipts=(), terminal="BUDGET_EXHAUSTED")
        report_updates.update(
            ranked_root_causes=(), reasoning_receipts=(), terminal_state="BUDGET_EXHAUSTED"
        )
    forged = state.model_copy(
        update={**updates, "report": state.report.model_copy(update=report_updates)}
    )
    checkpoint(host, forged)
    census = h.census()
    with pytest.raises(EvidenceIntegrityError, match="hides|unsupported"):
        host.resume(h.incident.incident_id)
    assert h.census() == census


@pytest.mark.parametrize("field", ["hypotheses", "reason", "mode", "incident", "terminal"])
def test_public_report_must_match_native_checkpoint(harness: HostHarness, field: str) -> None:
    """A stored report cannot selectively replace checkpoint fields while keeping valid JSON."""
    h = harness
    host, state, _ = h.finished()
    assert state.report is not None
    changes = {
        "hypotheses": {"ranked_root_causes": ()},
        "reason": {"reasoning_stop_reason": "BUDGET_EXHAUSTED"},
        "mode": {"mode": "local_kind"},
        "incident": {"incident_id": "foreign"},
        "terminal": {"terminal_state": "BUDGET_EXHAUSTED"},
    }[field]
    checkpoint(host, state.model_copy(update={"report": state.report.model_copy(update=changes)}))
    census = h.census()
    expected = ValueError if field == "incident" else EvidenceIntegrityError
    with pytest.raises(expected, match="cross-incident|checkpoint differ"):
        host.resume(h.incident.incident_id)
    assert h.census() == census


@pytest.mark.parametrize("case", ["missing-completion", "foreign-receipt", "new-read"])
def test_complete_journal_reconstruction_never_reserves_new_work(
    harness: HostHarness, case: str
) -> None:
    """Even valid rehashed and SQL-anchored receipts cannot create fresh replay dispatch."""
    h = harness
    host, state, store = h.finished()
    assert state.report is not None
    record = host.ledger.get(h.incident.incident_id)
    receipt = restore(store, record.completions[0].artifact_sha256, ModelReceipt)
    if case == "missing-completion":
        changed = record.model_copy(update={"completions": ()})
    else:
        if case == "foreign-receipt":
            receipt = receipt.model_copy(update={"run_id": "foreign-incident"})
        else:
            assert receipt.observation.decision is not None
            decision = receipt.observation.decision.model_copy(
                update={
                    "decision": "read",
                    "hypotheses": (),
                    "reads": (ReadRequest(tool="recent_logs", service="payments-api", query=None),),
                }
            )
            observation = receipt.observation.model_copy(update={"decision": decision})
            receipt = receipt.model_copy(update={"observation": observation})
        digest = retain(store, receipt)
        changed = record.model_copy(
            update={
                "completions": (
                    record.completions[0].model_copy(update={"artifact_sha256": digest}),
                )
            }
        )
        state = state.model_copy(
            update={
                "reasoning_receipts": (digest,),
                "report": state.report.model_copy(update={"reasoning_receipts": (digest,)}),
            }
        )
        checkpoint(host, state)
    replace_record(host, changed)
    census = h.census()
    with pytest.raises((EvidenceIntegrityError, BudgetConflict)):
        host.resume(h.incident.incident_id)
    assert h.census() == census and host.ledger.get(record.run_id) == changed


def test_readonly_ledger_replays_existing_and_denied_without_sql_writes(
    harness: HostHarness,
) -> None:
    """The replay guard independently rejects affordable NEW work and all completion writes."""
    h = harness
    host, _, _ = h.finished()
    record = host.ledger.get(h.incident.incident_id)
    ledger = ReadOnlyLedger(host.engine)
    assert ledger.open(record.run_id, record.binding_sha256, record.limits) == record
    with pytest.raises(BudgetConflict):
        ledger.open(record.run_id, "f" * 64, record.limits)
    first = record.charges[0]
    assert isinstance(first, ModelCharge)
    assert ledger.reserve(record, first) == "EXISTING"
    with pytest.raises(BudgetConflict, match="operation differs"):
        ledger.reserve(record, first.model_copy(update={"prompt_sha256": "f" * 64}))
    new = first.model_copy(update={"operation_id": "new-model"})
    with pytest.raises(EvidenceIntegrityError, match="new work"):
        ledger.reserve(record, new)
    oversized = new.model_copy(update={"input_tokens": 100000})
    assert ledger.reserve(record, oversized) == "DENIED"
    with pytest.raises(EvidenceIntegrityError, match="cannot publish"):
        ledger.complete(record, first.operation_id, "f" * 64)
    stale = record.model_copy(update={"completions": ()})
    with pytest.raises(BudgetConflict, match="changed"):
        ledger.reserve(stale, first)
    assert host.ledger.get(record.run_id) == record
    adapter = NoDispatchAdapter(h.config.model)
    messages = (SystemMessage("host"), HumanMessage("synthetic"))
    assert adapter.count_tokens(messages) == h.config.model.input_token_limit
    with pytest.raises(EvidenceIntegrityError):
        adapter.invoke(messages, 16)
    with pytest.raises(EvidenceIntegrityError):
        adapter.usage(AIMessage("unused"))


def test_deferred_close_waits_for_active_method_and_closes_once() -> None:
    """A timed-out thread retains its client lease without blocking host admission shutdown."""
    lifetime = DeferredClose()
    entered, release = Event(), Event()
    closes: list[str] = []
    lifetime.add(lambda: closes.append("client"))

    def operation() -> None:
        """Keep one concrete method alive independently of its caller's timeout."""
        with lifetime.operation():
            entered.set()
            assert release.wait(3)

    thread = Thread(target=operation)
    thread.start()
    try:
        assert entered.wait(2)
        lifetime.close()
        assert not closes
        with pytest.raises(PermissionError):
            with lifetime.operation():
                pytest.fail("new operation admitted after close")
    finally:
        release.set()
        thread.join(3)
    assert not thread.is_alive() and closes == ["client"]
    lifetime.close()
    assert closes == ["client"]
    with pytest.raises(PermissionError):
        lifetime.add(lambda: closes.append("late"))
    assert closes == ["client", "late"]


def test_deferred_cleanup_failure_still_releases_all_resources_once() -> None:
    """A client failure cannot skip other closes or leak its private exception text."""
    lifetime = DeferredClose()
    closed: list[str] = []
    lifetime.add(lambda: closed.append("first"))

    def failed() -> None:
        """Represent a transport cleanup failure containing private diagnostic information."""
        closed.append("failed")
        raise RuntimeError("private synthetic credential")

    lifetime.add(failed)
    lifetime.add(lambda: closed.append("last"))
    with pytest.raises(RuntimeError, match="^operator resource cleanup failed$"):
        lifetime.close()
    lifetime.close()
    assert closed == ["last", "failed", "first"]


@pytest.mark.parametrize("fault", ["duplicate", "foreign-scope", "derived", "corrupt", "missing"])
def test_knowledge_bundle_verifies_all_originals_before_copy(
    harness: HostHarness, fault: str
) -> None:
    """Host-supplied lookup indexes do not make foreign or modified source artifacts trusted."""
    h = harness
    source = ArtifactStore(h.root / "knowledge")
    items = tuple(h.documents.values())
    root = source.root
    if fault == "duplicate":
        items = (items[0], items[0])
    elif fault in {"foreign-scope", "derived"}:
        item = items[1]
        envelope = source.verify(item)
        payload, metadata = envelope["payload"], envelope["evidence"]
        assert isinstance(payload, dict) and isinstance(metadata, dict)
        if fault == "foreign-scope":
            payload["namespace"] = "other-namespace"
        else:
            metadata["query"] = "elasticsearch://derived"
        uri, digest = source.write(envelope)
        changes = {"query": metadata["query"]} if fault == "derived" else {}
        replacement = item.model_copy(
            update={
                **changes,
                "artifact_uri": uri,
                "artifact_sha256": digest,
            }
        )
        source.verify(replacement)
        items = (items[0], replacement)
    elif fault == "corrupt":
        source.path_for(items[1].artifact_sha256).write_bytes(b"corrupt")
    else:
        root = h.root / "missing"
    destination = ArtifactStore(h.root / "destination")
    with pytest.raises(EvidenceIntegrityError):
        KnowledgeBundle(artifacts_root=root, items=items).import_into(destination)
    assert not tuple(destination.root.rglob("*.json"))


def test_cli_plan_and_identity_never_load_secrets_or_construct_host(
    harness: HostHarness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The operator can review exact budgets and native identity without provider access."""
    config = harness.root / "operator.json"
    config.write_text(harness.config.model_dump_json(), encoding="utf-8")

    def forbidden(*args: Any, **kwargs: Any) -> None:
        """Plan is configuration inspection, not a credential or network readiness probe."""
        pytest.fail("plan attempted execution")

    monkeypatch.setattr("payops.operator_host.OperatorHost", forbidden)
    monkeypatch.setattr(SecretReference, "load", forbidden)
    assert main(["plan", "--config", str(config)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["provider_requests_sent"] == 0 and plan["count_endpoint_fee"] == "unestablished"
    assert (plan["logical_reads"], plan["backend_reads"], plan["provider_requests"]) == (26, 39, 8)
    monkeypatch.setattr("payops.operator_host.native_account", lambda: "fixture-os")
    assert main(["identity"]) == 0
    assert json.loads(capsys.readouterr().out)["account"] == "fixture-os"


@pytest.mark.parametrize("command", ["start", "resume", "plan"])
def test_cli_missing_config_and_inputs_are_sanitized(
    harness: HostHarness, command: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Incomplete explicit input never starts a host or exposes validation details."""
    assert main([command]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "operator-host-failed"
    if command != "plan":
        config = harness.root / "operator.json"
        config.write_text(harness.config.model_dump_json(), encoding="utf-8")
        assert main([command, "--config", str(config)]) == 1
        assert json.loads(capsys.readouterr().out)["status"] == "operator-host-failed"
    assert harness.census() == (0, 0, 0, 0)


@pytest.mark.parametrize("execute_failure", [False, True])
def test_cli_cleanup_failure_cannot_print_success_or_private_traceback(
    harness: HostHarness,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    execute_failure: bool,
) -> None:
    """Cleanup belongs inside the sanitized result boundary, including a prior execution error."""
    host, state, _ = harness.finished()
    host.close()
    config = harness.root / "operator.json"
    config.write_text(harness.config.model_dump_json(), encoding="utf-8")
    closes: list[int] = []

    class FailedCloseHost:
        """Only CLI error rendering is substituted; its success state is an actual completed run."""

        def __init__(self, config: OperatorConfig) -> None:
            """Do not reconstruct any clients for this cleanup-control fixture."""

        def resume(self, incident_id: str) -> InvestigationState:
            """A genuine success state or a private error reaches the same cleanup boundary."""
            if execute_failure:
                raise RuntimeError("private execution secret")
            return state

        def close(self) -> None:
            """A failing close must not replace the sanitized CLI outcome with a traceback."""
            closes.append(1)
            raise RuntimeError("private cleanup secret")

    monkeypatch.setattr("payops.operator_host.OperatorHost", FailedCloseHost)
    assert main(["resume", "--config", str(config), "--incident-id", "operator-incident"]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "status": "operator-host-failed",
        "provider_completion": "unestablished",
    }
    assert not output.err and "private" not in output.out and closes


def test_actual_cli_start_and_resume_return_verified_fixture_summary(
    harness: HostHarness, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """CLI argument parsing, execution, publication and cleanup wrap the actual native host."""
    h = harness
    h.finish_first = True
    config_path, incident_path = h.root / "operator.json", h.root / "incident.json"
    config_path.write_text(h.config.model_dump_json(), encoding="utf-8")
    incident_path.write_text(h.incident.model_dump_json(), encoding="utf-8")

    def factory(config: OperatorConfig) -> OperatorHost:
        """Only the explicit transport fixture is injected; host orchestration is unchanged."""
        h.host = OperatorHost(config, fixture=h.fixture)
        return h.host

    monkeypatch.setattr("payops.operator_host.OperatorHost", factory)
    assert main(["start", "--config", str(config_path), "--incident", str(incident_path)]) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["mode"] == "fixture_replay" and first["verified_receipts"] == 1
    assert first["reasoning_stop"] == "FINISHED" and first["ranking_method"] == "model_provider"
    assert h.host is not None and h.host.authority.closed
    census = h.census()
    assert (
        main(
            [
                "resume",
                "--config",
                str(config_path),
                "--incident-id",
                h.incident.incident_id,
            ]
        )
        == 0
    )
    second = json.loads(capsys.readouterr().out)
    first.pop("recorded_at")
    second.pop("recorded_at")
    assert first == second and h.census() == census


def test_initialization_failure_closes_already_created_provider(
    harness: HostHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Late constructor failure cannot leak the provider/client ownership created earlier."""
    from payops.orchestrator.openai_adapter import OpenAIResponsesAdapter
    from payops.tools.kubernetes import KubernetesRead

    original = OpenAIResponsesAdapter.close
    closed: list[int] = []

    def close(adapter: OpenAIResponsesAdapter) -> None:
        """Record actual adapter cleanup while retaining its normal client shutdown."""
        closed.append(1)
        original(adapter)

    def fail(reader: KubernetesRead, *args: Any, **kwargs: Any) -> None:
        """Fail after SQL/provider setup, before exposing any operational reader."""
        raise RuntimeError("synthetic unavailable kubectl")

    monkeypatch.setattr(OpenAIResponsesAdapter, "close", close)
    monkeypatch.setattr(KubernetesRead, "__init__", fail)
    with pytest.raises(RuntimeError, match="unavailable kubectl"):
        harness.bind()
    assert closed == [1] and harness.census() == (0, 0, 0, 0)


@pytest.mark.parametrize("changed", ["endpoint", "budget", "vocabulary"])
def test_resume_binds_all_trusted_host_configuration(harness: HostHarness, changed: str) -> None:
    """A new process cannot reinterpret saved evidence under different source or model policy."""
    h = harness
    host, _, _ = h.finished()
    host.close()
    census = h.census()
    if changed == "endpoint":
        h.config = h.config.model_copy(update={"prometheus_port": 19091})
    elif changed == "budget":
        h.config = h.config.model_copy(
            update={
                "reasoning": h.config.reasoning.model_copy(update={"model_calls": 3}),
            }
        )
    else:
        labels = json.loads(h.config.release_labels.read_bytes())
        labels["primary_causes"]["OOM-01"] = ["CPU_THROTTLING"]
        # Preserve a valid full manifest while changing its complete vocabulary.
        path = h.root / "changed-labels.json"
        path.write_text(json.dumps(labels), encoding="utf-8")
        h.config = h.config.model_copy(update={"release_labels": path})
    replacement = h.bind()
    with pytest.raises(ValueError, match="ranking profile"):
        replacement.resume(h.incident.incident_id)
    assert h.census() == census


@pytest.mark.parametrize(
    "key,value",
    [
        ("provider", "foreign"),
        ("model", "foreign"),
        ("mode", "fixture"),
        ("token_accounting", "fixture_exact"),
        ("input_token_limit", 16001),
        ("output_token_limit", 2049),
        ("output_token_limit", 15),
    ],
)
def test_operator_profile_rejects_unreviewed_model_settings(
    harness: HostHarness, key: str, value: Any
) -> None:
    """A syntactically valid operator file cannot silently select an unreviewed provider profile."""
    data = harness.config.model_dump()
    data["model"][key] = value
    with pytest.raises(ValueError):
        OperatorConfig.model_validate(data)
    assert harness.census() == (0, 0, 0, 0)


@pytest.mark.parametrize(
    "updates",
    [
        {"tool_calls": 21},
        {"backend_reads": 35},
    ],
)
def test_operator_profile_preserves_reviewed_read_bounds(
    harness: HostHarness, updates: dict[str, int]
) -> None:
    """Graph headroom cannot expand beyond the fixed reviewed collection plus loop ceilings."""
    data = harness.config.model_dump()
    data["reasoning"].update(updates)
    with pytest.raises(ValueError):
        OperatorConfig.model_validate(data)


def test_actual_timed_out_provider_keeps_client_until_method_returns(
    harness: HostHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Host close is prompt, retains an active wire lease and never starts late generation."""
    from payops.orchestrator.openai_adapter import OpenAIResponsesAdapter

    h = harness
    h.config = h.config.model_copy(
        update={
            "model": h.config.model.model_copy(update={"timeout_seconds": 0.2}),
        }
    )
    entered, release, closed = Event(), Event(), Event()
    original_close = OpenAIResponsesAdapter.close

    def close(adapter: OpenAIResponsesAdapter) -> None:
        """Record actual HTTP client closure, not merely the caller's runtime timeout."""
        original_close(adapter)
        closed.set()

    def count(request: httpx.Request) -> httpx.Response:
        """Hold the count response beyond the caller deadline without external network work."""
        assert request.url.path.endswith("/input_tokens")
        entered.set()
        assert release.wait(5)
        return h.openai(request)

    monkeypatch.setattr(OpenAIResponsesAdapter, "close", close)
    original = h.fixture
    h.fixture = HostFixture(
        original.kubernetes,
        original.prometheus,
        original.elasticsearch,
        httpx.MockTransport(count),
        original.account,
    )
    host = h.bind()
    try:
        state = host.start(h.incident)
        assert entered.is_set() and state.reasoning_stop_reason == "TIMEOUT"
        assert not state.hypotheses and len(state.reasoning_receipts) == 1
        host.close()
        assert not closed.is_set()
    finally:
        release.set()
        assert closed.wait(3)
    assert len(h.model_calls) == 1 and not h.model_data


def test_safe_empty_scope_stop_has_no_hidden_reasoning_journal(harness: HostHarness) -> None:
    """A legitimately rejected incident remains reportable without fabricating model results."""
    h = harness
    incident = h.incident.model_copy(
        update={
            "request": h.incident.request.model_copy(update={"namespace": "foreign"}),
        }
    )
    host = h.bind()
    state = host.start(incident)
    assert state.terminal == "SECURITY_BLOCK" and state.phase == "FINISHED"
    assert state.reasoning_stop_reason is None and not state.hypotheses
    with pytest.raises(KeyError):
        host.ledger.get(incident.incident_id)
    assert host.resume(incident.incident_id) == state
    assert h.census() == (0, 0, 0, 0)


def test_budget_denied_read_reconstructs_without_dispatch(harness: HostHarness) -> None:
    """A complete model receipt may request a read that the original budget correctly denied."""
    h = harness
    h.config = h.config.model_copy(
        update={
            "reasoning": h.config.reasoning.model_copy(
                update={"tool_calls": 0, "backend_reads": 0}
            ),
        }
    )
    host = h.bind()
    state = host.start(h.incident)
    assert state.reasoning_stop_reason == "BUDGET_EXHAUSTED"
    assert len(state.reasoning_receipts) == 1 and len(h.model_calls) == 2
    census = h.census()
    assert host.resume(h.incident.incident_id) == state and h.census() == census
    assert len(host.ledger.get(h.incident.incident_id).charges) == 1


def test_knowledge_bundle_aggregate_byte_cap_precedes_any_copy(harness: HostHarness) -> None:
    """Individually valid originals cannot exceed the whole import budget in aggregate."""
    h = harness
    source = ArtifactStore(h.root / "knowledge")
    items: list[EvidenceItem] = []
    for item in h.documents.values():
        envelope = source.verify(item)
        payload = envelope["payload"]
        assert isinstance(payload, dict)
        payload["body"] = "x" * 600000
        uri, digest = source.write(envelope)
        replacement = item.model_copy(update={"artifact_uri": uri, "artifact_sha256": digest})
        source.verify(replacement)
        items.append(replacement)
    destination = ArtifactStore(h.root / "destination")
    with pytest.raises(EvidenceIntegrityError, match="byte budget"):
        KnowledgeBundle(artifacts_root=source.root, items=tuple(items)).import_into(destination)
    assert not tuple(destination.root.rglob("*.json"))
