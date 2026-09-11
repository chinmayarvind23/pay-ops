"""Real artifacts and registry dispatch exercise the trusted adapter's scope boundaries."""

import json
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
from test_data_clients import config
from test_payment_read import Stream
from test_payment_window import interval, raw_snapshot

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore
from payops.evidence.normalize import Observation, normalize
from payops.evidence.payment_window import snapshot_observation
from payops.memory.data_clients import (
    INDEXES,
    ElasticsearchRetrieval,
    EvidenceScope,
    RetrievalLineage,
)
from payops.orchestrator.reasoning import ReadRequest, ToolName
from payops.policy.contracts import Principal
from payops.tools.kubernetes import KubernetesRead
from payops.tools.operational_reads import OperationalReads, ReadBinding
from payops.tools.payment import PaymentRead
from payops.tools.registry import CATALOG, ReadResult


class Harness:
    """Borrow spec-constrained clients; no cloud, network, environment or credential discovery."""

    def __init__(self, root: Path) -> None:
        """Keep a fixed completion clock and independently inspect real retained artifacts."""
        self.now = utc_now()
        self.store = ArtifactStore(root)
        self.kube = Mock(spec=KubernetesRead)
        self.payment = Mock(spec=PaymentRead)
        self.search = Mock(spec=ElasticsearchRetrieval)
        self.actor: Principal | None = Principal(
            subject="actor",
            roles=("responder",),
            namespaces=("payops-sandbox",),
            verified_at=self.now,
            expires_at=self.now + timedelta(seconds=60),
        )
        self.costs: list[int] = []
        self.binding = OperationalReads(
            ReadBinding(incident_id="incident", subject="actor"),
            self.store,
            self.kube,
            self.payment,
            self.search,
            lambda: self.actor,
            clock=lambda: self.now,
        )

    def observation(self, **updates: Any) -> Observation:
        """Defaults model the real bounded log reader's exact source/query contract."""
        return Observation.model_validate(
            {
                "source": "LOG",
                "resource": "payments-api",
                "observed_at": self.now,
                "query": "logs.5m.100",
                "summary": "synthetic",
                "payload": {},
                **updates,
            }
        )

    def run(self, tool: ToolName = "recent_logs") -> ReadResult:
        """Dispatch through the actual registry, including its fresh publication grant."""
        registry = self.binding.registry(lambda requests, cost: self.charge(cost))
        try:
            return registry.dispatch(
                (
                    ReadRequest(
                        tool=tool,
                        service="payments-api",
                        query="timeout" if tool.endswith("search") else None,
                    ),
                )
            )[0]
        finally:
            registry.close()

    def charge(self, cost: int) -> bool:
        """Record the externally owned reservation without asserting durable storage."""
        self.costs.append(cost)
        return True

    def retrieval(self, kind: str = "RUNBOOK", **updates: Any) -> EvidenceItem:
        """Create a valid complete lineage with an old source and a fresh retrieval timestamp."""
        scope = EvidenceScope(
            incident_id="incident", namespace="payops-sandbox", service="payments-api"
        )
        scope = EvidenceScope.model_validate({**scope.model_dump(), **updates})
        old = self.now - timedelta(days=30)
        source = normalize(
            self.observation(
                source=kind,
                resource=scope.service,
                observed_at=old,
                query="original-guidance",
                payload={"namespace": scope.namespace, "service": scope.service},
            ),
            "historical-incident",
            old,
            old,
            self.store,
        )
        retrieved = utc_now()
        index = INDEXES["RUNBOOK" if kind == "RUNBOOK" else "MEMORY"]
        lineage = RetrievalLineage(scope=scope, index=index, source=source, retrieved_at=retrieved)
        item = normalize(
            self.observation(
                source=kind,
                resource=scope.service,
                observed_at=old,
                query=f"elasticsearch://{index}/fixed-scope-match",
                payload=JSON_OBJECT.validate_python(lineage.model_dump(mode="json")),
            ),
            scope.incident_id,
            old,
            retrieved,
            self.store,
        )
        self.now = utc_now()
        return item


@pytest.mark.parametrize("tool", list(CATALOG))
def test_exact_handlers_and_costs(tmp_path: Path, tool: ToolName) -> None:
    """Each tool calls only its actual client method and preserves the declared fixed cost."""
    h = Harness(tmp_path)
    h.kube.collect.return_value = (
        h.observation(source="DEPLOYMENT", query="kubernetes.status-snapshot"),
    )
    h.kube.events.return_value = (
        h.observation(source="KUBERNETES", query="events.current-pod-uid"),
    )
    h.kube.logs.return_value = h.observation()
    h.payment.snapshot.return_value = snapshot_observation(raw_snapshot(interval()), "payments-api")
    h.search.search.return_value = (
        h.retrieval("RUNBOOK" if tool == "runbook_search" else "MEMORY"),
    )
    result = h.run(tool)
    assert result.status == "OK" and len(result.evidence) == 1
    assert h.costs == [CATALOG[tool].backend_reads]
    calls = h.kube.method_calls + h.payment.method_calls + h.search.method_calls
    assert len(calls) == 1
    if tool.endswith("search"):
        search = h.search.search.call_args.args[0]
        assert search.size == 3 and search.text == "timeout"
        assert search.scope == EvidenceScope(
            incident_id="incident", namespace="payops-sandbox", service="payments-api"
        )
        assert result.evidence[0].observed_at < h.now - timedelta(days=29)
    if tool == "payment_snapshot":
        assert result.evidence[0].source == "PROMETHEUS"


def test_pod_resource_projection_overrides_collision(tmp_path: Path) -> None:
    """Host assignment preserves the genuine pod name, never an untrusted payload replacement."""
    h = Harness(tmp_path)
    h.kube.collect.return_value = (
        h.observation(
            source="KUBERNETES",
            query="kubernetes.status-snapshot",
            resource="payments-api-abc-123",
            payload={"kind": "Pod", "original_resource": "forged"},
        ),
    )
    result = h.run("workload_status")
    assert result.status == "OK" and result.evidence[0].resource == "payments-api"
    payload = h.store.verify(result.evidence[0])["payload"]
    assert isinstance(payload, dict) and payload["original_resource"] == "payments-api-abc-123"


@pytest.mark.parametrize(
    "updates",
    [
        {"source": "TRACE"},
        {"query": "arbitrary"},
        {"resource": "risk-sim"},
        {"payload": {"namespace": "foreign"}},
        {"payload": {"service": "risk-sim"}},
        {"source": "PAYMENT"},
    ],
)
def test_wrong_raw_source_or_scope_fails(tmp_path: Path, updates: dict[str, Any]) -> None:
    """Valid observations cannot cross service, namespace, query or source-kind boundaries."""
    h = Harness(tmp_path)
    h.kube.logs.return_value = h.observation(**updates)
    result = h.run()
    assert result.status == "ERROR" and not result.evidence
    assert not list(tmp_path.glob("*.json"))


@pytest.mark.parametrize("seconds", [-301, 1])
def test_stale_or_future_fails_whole_result_before_normalization(
    tmp_path: Path, seconds: int
) -> None:
    """A valid prefix cannot survive an out-of-window sibling or become a persisted prefix."""
    h = Harness(tmp_path)
    current = h.observation(source="KUBERNETES", query="events.current-pod-uid")
    h.kube.events.return_value = (
        current,
        current.model_copy(update={"observed_at": h.now + timedelta(seconds=seconds)}),
    )
    result = h.run("pod_events")
    assert result.status == "ERROR" and not result.evidence
    assert not list(tmp_path.glob("*.json"))


@pytest.mark.parametrize(
    "updates",
    [
        {"subject": "foreign"},
        {"roles": ("viewer",)},
        {"namespaces": ("foreign",)},
        {"enabled": False},
        {"verified_at": utc_now() - timedelta(seconds=61)},
        {"expires_at": utc_now() - timedelta(seconds=1)},
    ],
)
def test_current_responder_required_before_io(tmp_path: Path, updates: dict[str, Any]) -> None:
    """Stored-report view grants cannot start investigation reads, even with a valid subject."""
    h = Harness(tmp_path)
    assert h.actor is not None
    h.actor = h.actor.model_copy(update=updates)
    assert h.run().status == "DENIED"
    assert not h.kube.method_calls


def test_revocation_after_io_discards_result(tmp_path: Path) -> None:
    """A completed valid read is withheld when the next current-account lookup is denied."""
    h = Harness(tmp_path)

    def read(service: str) -> Observation:
        h.actor = None
        return h.observation()

    h.kube.logs.side_effect = read
    result = h.run()
    assert result.status == "DENIED" and not result.evidence


@pytest.mark.parametrize(
    "updates", [{"namespace": "foreign"}, {"service": "risk-sim"}, {"incident_id": "foreign"}]
)
def test_valid_foreign_retrieval_lineage_rejected(tmp_path: Path, updates: dict[str, Any]) -> None:
    """Rejection uses valid artifacts and nested scope, not an accidental digest mismatch."""
    h = Harness(tmp_path)
    h.search.search.return_value = (h.retrieval(**updates),)
    result = h.run("runbook_search")
    assert result.status == "ERROR" and not result.evidence


def test_retrieval_source_kind_and_count_rejected(tmp_path: Path) -> None:
    """An incident-search result cannot satisfy a runbook request or exceed host result size."""
    h = Harness(tmp_path)
    item = h.retrieval("MEMORY")
    h.search.search.return_value = (item,)
    assert h.run("runbook_search").status == "ERROR"
    h.search.search.return_value = (item,) * 4
    assert h.run("incident_search").status == "ERROR"


@pytest.mark.parametrize("seconds", [-1, 301])
def test_retrieval_freshness_uses_retrieval_time(tmp_path: Path, seconds: int) -> None:
    """Old guidance is valid; future or expired retrieval receipts are rejected."""
    h = Harness(tmp_path)
    h.search.search.return_value = (h.retrieval(),)
    h.now += timedelta(seconds=seconds)
    assert h.actor is not None
    h.actor = h.actor.model_copy(
        update={"verified_at": h.now, "expires_at": h.now + timedelta(seconds=60)}
    )
    assert h.run("runbook_search").status == "ERROR"


@pytest.mark.parametrize(
    "updates", [{"tool": "read_secret"}, {"query": "arbitrary"}, {"service": "foreign"}]
)
def test_constructed_request_revalidated_before_io(tmp_path: Path, updates: dict[str, Any]) -> None:
    """Even a model_construct bypass cannot reach a client through direct adapter dispatch."""
    h = Harness(tmp_path)
    request = ReadRequest(tool="recent_logs", service="payments-api", query=None).model_copy(
        update=updates
    )
    with pytest.raises(ValueError):
        h.binding._read(request)  # pyright: ignore[reportPrivateUsage]
    assert not h.kube.method_calls and not h.search.method_calls and not h.payment.method_calls


def test_identity_failure_and_raw_count_fail_closed(tmp_path: Path) -> None:
    """Unavailable identity or oversized backend output returns a complete error, never evidence."""
    h = Harness(tmp_path)
    h.binding._principal = Mock(side_effect=RuntimeError("private provider error"))  # pyright: ignore[reportPrivateUsage]
    assert h.run().status == "ERROR" and not h.kube.method_calls
    h.binding._principal = lambda: h.actor  # pyright: ignore[reportPrivateUsage]
    h.kube.collect.return_value = (h.observation(),) * 65
    assert h.run("workload_status").status == "ERROR"


def test_raw_artifact_scope_rechecked(tmp_path: Path) -> None:
    """A valid hash is insufficient when normalized payload binding is foreign."""
    h = Harness(tmp_path)
    item = normalize(
        h.observation(payload={"namespace": "foreign", "service": "payments-api"}),
        "incident",
        h.now,
        h.now,
        h.store,
    )
    with pytest.raises(ValueError, match="raw scope"):
        h.binding._verify(item)  # pyright: ignore[reportPrivateUsage]


def test_snapshot_projection_preserves_derivation_and_checks_time(tmp_path: Path) -> None:
    """Raw payment bytes keep their exact schema and source timestamp across host normalization."""
    h = Harness(tmp_path)
    observation = snapshot_observation(raw_snapshot(interval()), "payments-api")
    h.payment.snapshot.return_value = observation
    result = h.run("payment_snapshot")
    assert result.status == "OK"
    item = result.evidence[0]
    assert h.store.verify(item)["payload"] == observation.payload
    h.payment.snapshot.return_value = observation.model_copy(
        update={"observed_at": observation.observed_at + timedelta(seconds=1)}
    )
    assert h.run("payment_snapshot").status == "ERROR"
    changed = normalize(
        observation.model_copy(update={"query": "wrong"}),
        "incident",
        h.now - timedelta(minutes=5),
        h.now,
        h.store,
    )
    with pytest.raises(ValueError, match="metadata mismatch"):
        h.binding._verify(changed)  # pyright: ignore[reportPrivateUsage]
    envelope = h.store.verify(item)
    envelope["payload"] = []
    uri, digest = h.store.write(envelope)
    changed = item.model_copy(update={"artifact_uri": uri, "artifact_sha256": digest})
    with pytest.raises(ValueError, match="invalid payment snapshot payload"):
        h.binding._verify(changed)  # pyright: ignore[reportPrivateUsage]


def test_actual_clients_fixed_transport_costs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Concrete clients execute nine declared provider operations through real parsers."""
    h = Harness(tmp_path / "artifacts")
    commands: list[tuple[str, ...]] = []
    http_calls: list[httpx.Request] = []
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("synthetic config")

    def executable(name: str) -> str:
        return "trusted-kubectl"

    monkeypatch.setattr("payops.tools.kubernetes.shutil.which", executable)
    meta = {
        "namespace": "payops-sandbox",
        "name": "payments-api-abc-123",
        "uid": "pod-uid",
        "labels": {"app.kubernetes.io/name": "payments-api"},
    }

    def invoke(args: tuple[str, ...]) -> str:
        """Return narrow real Kubernetes object shapes selected by the client's fixed argv."""
        commands.append(args)
        if "logs" in args:
            return "synthetic bounded log"
        kind = args[args.index("get") + 1]
        if kind == "deployment":
            return json.dumps(
                {"metadata": {**meta, "name": "payments-api"}, "spec": {}, "status": {}}
            )
        if kind == "pods":
            return json.dumps({"items": [{"metadata": meta, "status": {}}]})
        assert kind == "events"
        return json.dumps(
            {
                "items": [
                    {
                        "metadata": {"namespace": "payops-sandbox"},
                        "involvedObject": {"uid": "pod-uid", "namespace": "payops-sandbox"},
                        "lastTimestamp": utc_now().isoformat(),
                        "message": "synthetic event",
                    }
                ]
            }
        )

    source_item = h.retrieval()
    lineage = RetrievalLineage.model_validate(h.store.verify(source_item)["payload"])
    memory_item = h.retrieval("MEMORY")
    memory = RetrievalLineage.model_validate(h.store.verify(memory_item)["payload"])

    def respond(request: httpx.Request) -> httpx.Response:
        """Exercise fixed Prometheus and Elasticsearch requests through actual HTTPX streams."""
        http_calls.append(request)
        if request.url.path == "/api/v1/query":
            at = float(request.url.params["time"])
            raw: Any = raw_snapshot(interval())
            raw["evaluated_at"] = at
            for row in raw["metrics"]["data"]["result"]:
                row["value"][0] = at
            mark = raw["watermark"]["data"]["result"][0]
            mark["value"] = [at, str(at - 0.5)]
            key = "watermark" if "timestamp(" in request.url.params["query"] else "metrics"
            result = raw[key]
        else:
            body = json.loads(request.content)
            assert body["size"] == 3
            assert request.url.params["allow_partial_search_results"] == "false"
            selected = memory if request.url.path.startswith("/" + INDEXES["MEMORY"]) else lineage
            result = {
                "timed_out": False,
                "_shards": {"failed": 0},
                "hits": {
                    "hits": [
                        {
                            "_index": selected.index,
                            "_id": selected.source.artifact_sha256,
                            "_source": {
                                "namespace": "payops-sandbox",
                                "service": "payments-api",
                                "kind": selected.source.source,
                                "evidence": selected.source.model_dump(mode="json"),
                            },
                        }
                    ]
                },
            }
        return httpx.Response(200, stream=Stream((json.dumps(result).encode(),)))

    transport = httpx.MockTransport(respond)
    retrieval = ElasticsearchRetrieval(config(), h.store, transport=transport)
    binding = OperationalReads(
        ReadBinding(incident_id="incident", subject="actor"),
        h.store,
        KubernetesRead(kubeconfig, invoke),
        PaymentRead(transport=transport),
        retrieval,
        lambda: h.actor,
    )
    registry = binding.registry(lambda requests, cost: h.charge(cost))
    try:
        for tool in (
            "workload_status",
            "pod_events",
            "recent_logs",
            "payment_snapshot",
            "runbook_search",
            "incident_search",
        ):
            request = ReadRequest.model_validate(
                {
                    "tool": tool,
                    "service": "payments-api",
                    "query": "timeout" if tool.endswith("search") else None,
                }
            )
            assert registry.dispatch((request,))[0].status == "OK"
        assert len(commands) == 5 and len(http_calls) == 4
        assert sum(h.costs) == 9
        assert all("--namespace" in command and "payops-sandbox" in command for command in commands)
    finally:
        registry.close()
        retrieval.close()
