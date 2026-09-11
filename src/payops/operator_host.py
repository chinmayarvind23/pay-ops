"""Run a trusted local operator investigation; this module exposes no public authentication API."""

import argparse
import json
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Literal, NoReturn

import httpx
from langchain_core.messages import AIMessage, BaseMessage
from pydantic import Field
from sqlalchemy import Engine, create_engine

from payops.contracts import Contract, EvidenceItem, Incident, utc_now
from payops.evaluation.labels import load_labels
from payops.evidence.artifacts import MAX_ARTIFACT_BYTES, ArtifactStore, EvidenceIntegrityError
from payops.evidence.verification import verify_evidence
from payops.memory.data_clients import ElasticsearchConfig, ElasticsearchRetrieval, SearchRequest
from payops.operator_config import (
    LocalAuthority,
    OperatorConfig,
    load_config,
    local_path,
    native_account,
    read_bounded,
)
from payops.orchestrator.budget import (
    CHARGE,
    BudgetConflict,
    BudgetLedger,
    BudgetRecord,
    Charge,
    ReasoningBudget,
)
from payops.orchestrator.graph import InvestigationWorker
from payops.orchestrator.graph_reasoning import terminal
from payops.orchestrator.loop import ReasoningLoop
from payops.orchestrator.model_runtime import ModelRuntime, ModelSettings, ProviderDetails
from payops.orchestrator.nodes import incident_directory
from payops.orchestrator.openai_adapter import OpenAIResponsesAdapter
from payops.orchestrator.openai_wire import decode
from payops.orchestrator.reasoning import ProviderUsage, ReadRequest
from payops.orchestrator.state import InvestigationBudget, InvestigationState
from payops.tools.collect import Collection, collect_local
from payops.tools.kubernetes import SERVICES, KubernetesRead, run_read
from payops.tools.operational_reads import OperationalReads, ReadBinding
from payops.tools.payment import PaymentRead
from payops.tools.prometheus import PrometheusRead
from payops.tools.registry import CATALOG, ReadRegistry, Reserve


class KnowledgeBundle(Contract):
    """Original source artifacts are explicit; retrieval cannot select arbitrary local paths."""

    artifacts_root: Path
    items: tuple[EvidenceItem, ...] = Field(max_length=16)

    def import_into(self, destination: ArtifactStore) -> None:
        """Verify complete originals before copying them into the incident's lineage authority."""
        source_root = local_path(self.artifacts_root)
        if not source_root.is_dir():
            raise EvidenceIntegrityError("knowledge source directory unavailable")
        source = ArtifactStore(source_root)
        total = 0
        seen: set[str] = set()
        verified: list[tuple[EvidenceItem, bytes]] = []
        for item in self.items:
            if (
                item.source not in {"RUNBOOK", "MEMORY"}
                or item.resource not in SERVICES
                or item.query.startswith("elasticsearch://")
                or item.evidence_id in seen
            ):
                raise EvidenceIntegrityError("invalid original knowledge item")
            seen.add(item.evidence_id)
            payload = source.verify(item).get("payload")
            if not isinstance(payload, dict) or (
                payload.get("namespace"),
                payload.get("service"),
            ) != ("payops-sandbox", item.resource):
                raise EvidenceIntegrityError("knowledge source scope differs")
            raw = read_bounded(source.path_for(item.artifact_sha256), MAX_ARTIFACT_BYTES)
            total += len(raw)
            if total > 1048576:
                raise EvidenceIntegrityError("knowledge import byte budget exceeded")
            verified.append((item, raw))
        for item, raw in verified:
            digest = destination.write(decode(raw, MAX_ARTIFACT_BYTES))[1]
            if digest != item.artifact_sha256:
                raise EvidenceIntegrityError("knowledge copy digest differs")
            destination.verify(item)


class DeferredClose:
    """Close admission now and owned clients only after active provider/retrieval methods return."""

    def __init__(self) -> None:
        """This counter does not queue work, cancel threads or change shared runtime APIs."""
        self._lock = Lock()
        self._active = 0
        self._closed = False
        self._closers: list[Callable[[], None]] = []

    def add(self, close: Callable[[], None]) -> None:
        """Register resources before exposing them to any worker thread."""
        with self._lock:
            if self._closed:
                close()
                raise PermissionError("operator host closed")
            self._closers.append(close)

    def _ready(self) -> list[Callable[[], None]]:
        """Called only while locked; each client close callback is extracted once."""
        if self._closed and self._active == 0:
            closers, self._closers = self._closers, []
            return list(reversed(closers))
        return []

    @staticmethod
    def _finish(closers: list[Callable[[], None]]) -> None:
        """One close failure cannot prevent the remaining owned clients from being released."""
        failed = False
        for close in closers:
            try:
                close()
            except Exception:
                failed = True
        if failed:
            raise RuntimeError("operator resource cleanup failed")

    @contextmanager
    def operation(self) -> Generator[None]:
        """Timed-out work retains its resource lease until its concrete transport finishes."""
        with self._lock:
            if self._closed:
                raise PermissionError("operator host closed")
            self._active += 1
        try:
            yield
        finally:
            with self._lock:
                self._active -= 1
                closers = self._ready()
            self._finish(closers)

    def close(self) -> None:
        """Return promptly without closing an HTTP client underneath an active read."""
        with self._lock:
            self._closed = True
            closers = self._ready()
        self._finish(closers)


class ManagedProvider:
    """The host lease adds lifecycle ownership without changing the reviewed provider wire path."""

    def __init__(self, provider: OpenAIResponsesAdapter, lifetime: DeferredClose) -> None:
        """Borrow a fixed adapter and register exactly one owner-side close."""
        self.provider, self.lifetime = provider, lifetime
        self.settings = provider.settings
        lifetime.add(provider.close)

    def count_tokens(self, messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Preparation is local and cannot acquire provider network authority."""
        return self.provider.count_tokens(messages)

    def invoke(self, messages: tuple[BaseMessage, BaseMessage], output_limit: int) -> AIMessage:
        """Legacy provider invocation remains denied by the concrete adapter."""
        return self.provider.invoke(messages, output_limit)

    def invoke_staged(
        self,
        messages: tuple[BaseMessage, BaseMessage],
        output_limit: int,
        before_generation: Callable[[int], bool],
    ) -> AIMessage:
        """A lease includes counting, interstage authorization and generation."""
        with self.lifetime.operation():
            return self.provider.invoke_staged(messages, output_limit, before_generation)

    def usage(self, message: AIMessage) -> ProviderUsage | None:
        """Delegate only already raw-validated per-response usage."""
        return self.provider.usage(message)

    def details(self, message: AIMessage) -> ProviderDetails:
        """Preserve count/generation timing and normalization provenance."""
        return self.provider.details(message)


class ManagedRetrieval(ElasticsearchRetrieval):
    """Each incident's retrieval client remains alive while a registry worker is using it."""

    def __init__(
        self,
        config: ElasticsearchConfig,
        store: ArtifactStore,
        lifetime: DeferredClose,
        transport: httpx.MockTransport | None,
    ) -> None:
        """The host binds source storage and transport before any model-selected read."""
        super().__init__(config, store, transport=transport)
        self.lifetime = lifetime
        lifetime.add(self.close)

    def search(self, request: SearchRequest) -> tuple[EvidenceItem, ...]:
        """Late read completion cannot race its own client's shutdown."""
        with self.lifetime.operation():
            return super().search(request)


class AuthorizedPrometheus(PrometheusRead):
    """Initial collection refreshes authority around every individual fixed metrics request."""

    def __init__(
        self,
        origin: str,
        authority: LocalAuthority,
        transport: httpx.MockTransport | None,
    ) -> None:
        """The same fixed loopback source remains in use for initial evidence collection."""
        super().__init__(origin, transport)
        self.authority = authority

    def query(self, signal: str):
        """A revoked local grant stops later initial reads and discards an in-flight result."""
        self.authority.require()
        result = super().query(signal)
        self.authority.require()
        return result


class ReadOnlyLedger(BudgetLedger):
    """Finished-report reconstruction cannot create or complete a SQL reservation."""

    def __init__(self, engine: Engine) -> None:
        """Borrow an existing ledger without running schema creation."""
        self.engine = engine

    def open(self, run_id: str, binding_sha256: str, limits: ReasoningBudget) -> BudgetRecord:
        """Recompute the complete loop binding and compare it without attempting INSERT."""
        record = self.get(run_id)
        if record.binding_sha256 != binding_sha256 or record.limits != limits:
            raise BudgetConflict("finished reasoning binding differs")
        return record

    def reserve(
        self,
        expected: BudgetRecord,
        charge: Charge,
    ) -> Literal["NEW", "EXISTING", "DENIED"]:
        """Known budget denials replay; a newly affordable operation is an integrity failure."""
        expected = BudgetRecord.model_validate_json(expected.model_dump_json())
        charge = CHARGE.validate_json(charge.model_dump_json())
        if self.get(expected.run_id) != expected:
            raise BudgetConflict("finished ledger changed")
        for item in expected.charges:
            if item.operation_id == charge.operation_id:
                if item != charge:
                    raise BudgetConflict("finished operation differs")
                return "EXISTING"
        try:
            BudgetRecord.model_validate(
                {
                    **expected.model_dump(),
                    "charges": (*expected.charges, charge),
                }
            )
        except ValueError:
            return "DENIED"
        raise EvidenceIntegrityError("finished reconstruction requested new work")

    def complete(
        self,
        expected: BudgetRecord,
        operation_id: str,
        artifact_sha256: str,
    ) -> NoReturn:
        """Even accidental publication through this ledger cannot mutate the SQL journal."""
        raise EvidenceIntegrityError("finished reconstruction cannot publish a receipt")


class NoDispatchAdapter:
    """A second independent guard makes report reconstruction incapable of provider dispatch."""

    def __init__(self, settings: ModelSettings) -> None:
        """Only local preparation and authority checks are needed for replay."""
        self.settings = settings

    def count_tokens(self, messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Ceiling preparation has no network effects, even for a replayed budget denial."""
        return self.settings.input_token_limit

    def invoke(self, messages: tuple[BaseMessage, BaseMessage], output_limit: int) -> NoReturn:
        """Never dispatch through the verification runtime."""
        raise EvidenceIntegrityError("finished reconstruction cannot invoke provider")

    def usage(self, message: AIMessage) -> NoReturn:
        """No new provider message can exist in this runtime."""
        raise EvidenceIntegrityError("finished reconstruction cannot read new usage")


@dataclass(frozen=True)
class HostFixture:
    """Explicit test-only transports identify the worker as fixture replay; the CLI has no seam."""

    kubernetes: Callable[[tuple[str, ...]], str]
    prometheus: httpx.MockTransport
    elasticsearch: httpx.MockTransport
    openai: httpx.MockTransport
    account: Callable[[], str]


class OperatorHost:
    """One local process owns provider, bounded readers, SQLite durability and current authority."""

    def __init__(self, config: OperatorConfig, *, fixture: HostFixture | None = None) -> None:
        """Construction loads only explicitly referenced credentials after current authorization."""
        self.config = OperatorConfig.model_validate_json(config.model_dump_json())
        self.fixture = fixture
        self.authority = LocalAuthority(
            config.grant_file, account=fixture.account if fixture else native_account
        )
        self.authority.require()
        self.causes = load_labels(read_bounded(config.release_labels, 16384)).cause_vocabulary()
        self.knowledge = KnowledgeBundle.model_validate(
            decode(read_bounded(config.knowledge_bundle, 32768), 32768)
        )
        self.lifetime = DeferredClose()
        self._retrieval: dict[Path, ManagedRetrieval] = {}
        try:
            self._initialize()
        except BaseException:
            self.lifetime.close()
            raise

    def _initialize(self) -> None:
        """Build the concrete clients with explicit local endpoints and owned lifetime."""
        config, fixture = self.config, self.fixture
        self.elastic = ElasticsearchConfig(
            ca_file=config.elastic_ca,
            elastic_password=config.elastic_key.load(),
            elastic_port=config.elastic_port,
        )
        provider = OpenAIResponsesAdapter(
            config.model,
            config.provider_key.load(),
            test_transport=fixture.openai if fixture else None,
        )
        self.runtime = ModelRuntime(
            ManagedProvider(provider, self.lifetime), self.authority.allowed
        )
        self.lifetime.add(self.runtime.close)
        root = local_path(config.runtime)
        root.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            f"sqlite:///{(root / 'reasoning.sqlite').as_posix()}",
            connect_args={"timeout": 3},
            pool_size=4,
            max_overflow=0,
            pool_timeout=3,
            hide_parameters=True,
        )
        self.lifetime.add(self.engine.dispose)
        self.ledger = BudgetLedger(self.engine)
        invoke = fixture.kubernetes if fixture else run_read

        def authorized_command(args: tuple[str, ...]) -> str:
            """Every fixed Kubernetes command rechecks the local grant before and after its read."""
            self.authority.require()
            result = invoke(args)
            self.authority.require()
            return result

        self.kubernetes = KubernetesRead(config.kubeconfig, authorized_command)
        origin = f"http://127.0.0.1:{config.prometheus_port}"
        self.prometheus = AuthorizedPrometheus(
            origin, self.authority, fixture.prometheus if fixture else None
        )
        self.payment = PaymentRead(origin, fixture.prometheus if fixture else None)
        profile = sha256(
            json.dumps(
                {
                    "config": config.model_dump(mode="json"),
                    "subject": self.authority.subject,
                    "causes": sorted(self.causes),
                    "knowledge": self.knowledge.model_dump(mode="json"),
                    "fixture": fixture is not None,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        self.worker = InvestigationWorker(
            root / "graph",
            self._collect,
            mode="fixture_replay" if fixture else "local_kind",
            reasoner_factory=self._reasoner,
            ranking_profile="operator-openai-v1-" + profile,
            ranking_method="model_provider",
        )

    def _collect(self, incident: Incident, output: Path) -> Collection:
        """Current local authority also gates the initial fixed collector's reserved reads."""
        self.authority.require()
        self.knowledge.import_into(ArtifactStore(output / "artifacts"))
        result = collect_local(self.kubernetes, self.prometheus, output, incident.incident_id)
        self.authority.require()
        return result

    def _reasoner(self, state: InvestigationState, store: ArtifactStore) -> ReasoningLoop:
        """All six handlers retain scoped clients and the current responder grant callback."""
        self.authority.require()
        if store.root not in self._retrieval:
            self._retrieval[store.root] = ManagedRetrieval(
                self.elastic,
                store,
                self.lifetime,
                self.fixture.elasticsearch if self.fixture else None,
            )
        reads = OperationalReads(
            ReadBinding(incident_id=state.incident.incident_id, subject=self.authority.subject),
            store,
            self.kubernetes,
            self.payment,
            self._retrieval[store.root],
            self.authority.principal,
        )
        return self._loop(store, self.ledger, self.runtime, reads.registry)

    def _loop(
        self,
        store: ArtifactStore,
        ledger: BudgetLedger,
        runtime: ModelRuntime,
        registry: Callable[[Reserve], ReadRegistry],
    ) -> ReasoningLoop:
        """Initial and verification loops share exact actor, vocabulary, settings and limits."""
        return ReasoningLoop(
            self.config.runtime / "reasoning-runs",
            store,
            ledger,
            runtime,
            registry,
            subject=self.authority.subject,
            causes=self.causes,
            limits=self.config.reasoning,
        )

    def start(self, incident: Incident) -> InvestigationState:
        """An explicit start may issue paid requests; callers must coordinate execution first."""
        self.authority.require()
        state = self.worker.start(
            incident,
            InvestigationBudget(
                max_tool_calls=20 + self.config.reasoning.tool_calls,
                max_backend_reads=30 + self.config.reasoning.backend_reads,
            ),
        )
        return self._publish(state)

    def resume(self, incident_id: str) -> InvestigationState:
        """Preserve the saved profile and journal, then separately verify finished publication."""
        self.authority.require()
        return self._publish(self.worker.resume(incident_id))

    def _publish(self, state: InvestigationState) -> InvestigationState:
        """No stored finished report inherits an old grant or unchecked artifact/receipt trust."""
        self.authority.require()
        if state.report is None or (
            state.phase != "FINISHED"
            or state.report.incident_id != state.incident.incident_id
            or state.report.evidence != state.evidence
            or state.report.ranked_root_causes != state.hypotheses
            or state.report.mode != self.worker.mode
            or state.mode != self.worker.mode
            or state.report.terminal_state != state.terminal
            or state.report.ranking_method != state.ranking_method
            or state.report.reasoning_stop_reason != state.reasoning_stop_reason
            or state.report.reasoning_receipts != state.reasoning_receipts
        ):
            raise EvidenceIntegrityError("finished report and checkpoint differ")
        root = incident_directory(self.worker.nodes.root, state.incident.incident_id)
        store = ArtifactStore(root / "artifacts")
        for item in state.evidence:
            if item.incident_id != state.incident.incident_id:
                raise EvidenceIntegrityError("finished source incident differs")
            verify_evidence(item, store)
        if state.reasoning_stop_reason in {None, "SECURITY_BLOCK"}:
            if (
                state.hypotheses
                or state.reasoning_receipts
                or state.terminal
                not in {
                    "SECURITY_BLOCK",
                    "DEPENDENCY_UNAVAILABLE",
                    "BUDGET_EXHAUSTED",
                }
            ):
                raise EvidenceIntegrityError("unreasoned report contains unsupported results")
            try:
                self.ledger.get(state.incident.incident_id)
            except KeyError:
                pass
            else:
                raise EvidenceIntegrityError("unreasoned report hides a reasoning journal")
        else:
            self._reconstruct(state, store, root)
        self.authority.require()
        return state

    def _reconstruct(self, state: InvestigationState, store: ArtifactStore, root: Path) -> None:
        """Replay a complete SQL journal through independent ledger and adapter dispatch guards."""
        record = self.ledger.get(state.incident.incident_id)
        if (
            len(record.charges) != len(record.completions)
            or tuple(x.artifact_sha256 for x in record.completions) != state.reasoning_receipts
        ):
            raise EvidenceIntegrityError("finished journal incomplete or receipt census differs")
        initial = Collection.model_validate_json(
            read_bounded(root / "graph-collection.json", 1048576)
        )
        if initial.incident_id != state.incident.incident_id:
            raise EvidenceIntegrityError("finished collection incident differs")
        runtime = ModelRuntime(NoDispatchAdapter(self.config.model), self.authority.allowed)

        def registry(reserve: Reserve) -> ReadRegistry:
            """Budget-denied attempts can replay, but every unexpected backend handler is inert."""

            def forbidden(request: ReadRequest) -> NoReturn:
                """No operational clients are reachable from the finished-report verifier."""
                raise EvidenceIntegrityError("finished reconstruction cannot dispatch read")

            return ReadRegistry(
                {name: forbidden for name in CATALOG},
                self.authority.allowed,
                reserve,
                lambda item: verify_evidence(item, store),
                state.incident.incident_id,
            )

        try:
            replay = self._loop(store, ReadOnlyLedger(self.engine), runtime, registry).run(
                state.incident, initial.evidence
            )
        finally:
            runtime.close()
        if (
            replay.receipt_sha256s != state.reasoning_receipts
            or replay.evidence != state.evidence
            or replay.hypotheses != state.hypotheses
            or replay.stop_reason != state.reasoning_stop_reason
            or terminal(replay) != state.terminal
            or self.ledger.get(record.run_id) != record
        ):
            raise EvidenceIntegrityError("finished report differs from complete journal")
        if state.report is None or (
            state.report.evidence != replay.evidence
            or state.report.ranked_root_causes != replay.hypotheses
            or state.report.reasoning_receipts != replay.receipt_sha256s
            or state.report.ranking_method != "model_provider"
            or state.report.terminal_state != state.terminal
        ):
            raise EvidenceIntegrityError("finished public report differs from journal")

    def close(self) -> None:
        """Stop admission immediately; active concrete client methods retain their own leases."""
        self.authority.closed = True
        self.runtime.close()
        self.lifetime.close()


def main(argv: list[str] | None = None) -> int:
    """Plan is read-only configuration inspection; start/resume are explicit operator execution."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["identity", "plan", "start", "resume"])
    parser.add_argument("--config", type=Path)
    parser.add_argument("--incident", type=Path)
    parser.add_argument("--incident-id")
    args = parser.parse_args(argv)
    host: OperatorHost | None = None
    try:
        if args.command == "identity":
            print(json.dumps({"authority": "local-operating-system", "account": native_account()}))
            return 0
        if args.config is None:
            raise ValueError("explicit operator config required")
        config = load_config(args.config)
        if args.command == "plan":
            print(
                json.dumps(
                    {
                        "authority": "local-os-account-and-grant-file",
                        "persistence": "local-sqlite",
                        "model": config.model.model,
                        "model_calls": config.reasoning.model_calls,
                        "provider_requests": config.reasoning.provider_requests,
                        "logical_reads": 20 + config.reasoning.tool_calls,
                        "backend_reads": 30 + config.reasoning.backend_reads,
                        "generation_cost_reservation_nano_usd": config.reasoning.cost_nano_usd,
                        "count_endpoint_fee": "unestablished",
                        "provider_requests_sent": 0,
                    }
                )
            )
            return 0
        if args.command == "start" and args.incident is None:
            raise ValueError("explicit incident file required")
        if args.command == "resume" and args.incident_id is None:
            raise ValueError("explicit incident ID required")
        host = OperatorHost(config)
        state = (
            host.start(Incident.model_validate_json(read_bounded(args.incident, 16384)))
            if args.incident is not None and args.command == "start"
            else host.resume(args.incident_id)
        )
        host.close()
        host = None
        print(
            json.dumps(
                {
                    "incident_id": state.incident.incident_id,
                    "mode": state.mode,
                    "phase": state.phase,
                    "terminal": state.terminal,
                    "ranking_method": state.ranking_method,
                    "reasoning_stop": state.reasoning_stop_reason,
                    "verified_receipts": len(state.reasoning_receipts),
                    "recorded_at": utc_now().isoformat(),
                }
            )
        )
        return 0
    except Exception:
        print(
            json.dumps({"status": "operator-host-failed", "provider_completion": "unestablished"})
        )
        return 1
    finally:
        if host is not None:
            try:
                host.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
