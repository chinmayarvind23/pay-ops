"""Derived data must stay scoped, bounded and artifact-verified across backend failures."""

import json
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import certifi
import httpx
import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy import create_engine

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize
from payops.memory.data_clients import (
    INDEXES,
    MAX_CACHE_BYTES,
    MAX_SEARCH_BYTES,
    CacheEntry,
    ElasticsearchRetrieval,
    EvidenceScope,
    LocalDataConfig,
    RedisDerivedCache,
    RedisWire,
    RetrievalDocument,
    SearchRequest,
    SqlStores,
    verify_retrieval_evidence,
)


def config() -> LocalDataConfig:
    """Use a public CA bundle and synthetic credentials; no runtime files are discovered."""
    return LocalDataConfig(
        ca_file=Path(certifi.where()),
        postgres_password=SecretStr("synthetic-postgres-secret"),
        redis_password=SecretStr("synthetic-redis-secret"),
        elastic_password=SecretStr("synthetic-elastic-secret"),
    )


def scope() -> EvidenceScope:
    """Represent host-validated scope from an authoritative incident."""
    return EvidenceScope(incident_id="incident-one", namespace="payops-sandbox", service="payments")


def evidence(
    store: ArtifactStore,
    *,
    source: str = "RUNBOOK",
    namespace: str = "payops-sandbox",
    incident_id: str = "incident-one",
    resource: str = "payments",
) -> EvidenceItem:
    """Write real normalized artifacts so tampering checks exercise the actual verifier."""
    now = utc_now()
    observation = Observation.model_validate(
        {
            "source": source,
            "resource": resource,
            "observed_at": now,
            "query": "trusted-fixture",
            "summary": "Check upstream availability",
            "payload": {
                "namespace": namespace,
                "service": resource,
                "text": "upstream availability",
            },
        }
    )
    return normalize(observation, incident_id, now, now, store)


class Wire:
    """An untrusted cache can return tampered bytes and tracks pre-dispatch rejections."""

    def __init__(self) -> None:
        """Keep scope keys and write TTL visible without real network effects."""
        self.data: dict[str, bytes] = {}
        self.ttl: int | None = None

    def put(self, key: str, value: bytes, ttl: int) -> None:
        """Record the exact bounded envelope and backend TTL."""
        self.data[key], self.ttl = value, ttl

    def read(self, key: str) -> bytes:
        """Model GETRANGE's empty-key response."""
        return self.data.get(key, b"")


def test_config_redacts_secrets_and_rejects_foreign_urls(tmp_path: Path) -> None:
    """Endpoint and file authority come only from explicit validated host configuration."""
    settings = config()
    assert "synthetic" not in repr(settings) and "synthetic" not in settings.model_dump_json()
    for change in (
        {"postgres_host": "evil.example"},
        {"redis_host": "https://localhost"},
        {"elastic_port": True},
        {"ca_file": tmp_path / "absent"},
        {"redis_password": "short"},
    ):
        with pytest.raises(ValidationError) as error:
            LocalDataConfig.model_validate({**settings.model_dump(), **change})
        assert "synthetic-postgres-secret" not in str(error.value)


def test_factory_binds_tls_pool_and_owns_disposal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both real SQL stores borrow the same bounded factory pool and no password is printed."""
    engine = create_engine(f"sqlite:///{tmp_path / 'factory.db'}")
    observed: dict[str, Any] = {}

    def build(url: Any, **options: Any) -> Any:
        """Capture SQLAlchemy construction while using a real local engine for schema setup."""
        observed.update(url=url, **options)
        return engine

    monkeypatch.setattr("payops.memory.data_clients.create_engine", build)
    stores = SqlStores(config())
    assert stores.actions.engine is stores.incidents.engine is engine
    assert observed["pool_size"] == 4 and observed["max_overflow"] == 0
    assert observed["pool_timeout"] == 3 and observed["hide_parameters"] is True
    assert observed["connect_args"]["sslmode"] == "verify-full"
    assert "-csearch_path=payops,pg_catalog" in observed["connect_args"]["options"]
    assert observed["url"].username == "payops_app"
    assert "synthetic-postgres-secret" not in repr(observed["url"])
    stores.incidents.close()
    stores.actions.close()
    stores.close()


def test_factory_failed_bootstrap_disposes_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """A partially initialized factory cannot leak the connection pool it owns."""
    engine = create_engine("sqlite://")
    disposed: list[bool] = []

    def build(*args: object, **options: object) -> Any:
        """Return the real fixture pool while schema failure is injected separately."""
        return engine

    monkeypatch.setattr("payops.memory.data_clients.create_engine", build)
    monkeypatch.setattr(engine, "dispose", lambda: disposed.append(True))

    def fail(*args: object) -> None:
        """Force the second schema setup to fail after the first borrowed store exists."""
        raise RuntimeError("bootstrap failure")

    monkeypatch.setattr("payops.memory.data_clients.ActionStore", fail)
    with pytest.raises(RuntimeError, match="bootstrap failure"):
        SqlStores(config())
    assert disposed == [True]


def test_cache_roundtrip_expiry_and_scope_miss(tmp_path: Path) -> None:
    """A real verified artifact remains derived context and naturally expires."""
    store, wire, now = ArtifactStore(tmp_path), Wire(), utc_now()
    clock = [now]
    cache = RedisDerivedCache(wire, store, lambda: clock[0])
    item = evidence(store)
    assert cache.get(scope()) is None
    cache.put(scope(), (item,), 10)
    assert wire.ttl == 10 and cache.get(scope()) == (item,)
    assert cache.get(scope().model_copy(update={"incident_id": "other"})) is None
    clock[0] = now + timedelta(seconds=10)
    assert cache.get(scope()) is None


@pytest.mark.parametrize("ttl", [0, -1, 301, True, 1.2])
def test_cache_bad_ttl_never_dispatches(tmp_path: Path, ttl: Any) -> None:
    """TTL coercions and missing expiry cannot enter the backend."""
    wire = Wire()
    with pytest.raises(ValueError):
        RedisDerivedCache(wire, ArtifactStore(tmp_path)).put(scope(), (), ttl)
    assert not wire.data


@pytest.mark.parametrize("mutation", ["scope", "incident", "service", "duplicate", "digest"])
def test_cache_foreign_or_corrupt_bundle_fails_closed(tmp_path: Path, mutation: str) -> None:
    """A poisoned cache cannot reuse foreign evidence or silently drop integrity errors."""
    store, wire = ArtifactStore(tmp_path), Wire()
    cache = RedisDerivedCache(wire, store)
    item = evidence(store)
    cache.put(scope(), (item,))
    key = next(iter(wire.data))
    data = json.loads(wire.data[key])
    if mutation == "scope":
        data["scope"]["namespace"] = "foreign"
    elif mutation == "incident":
        data["evidence"][0]["incident_id"] = "foreign"
    elif mutation == "service":
        data["evidence"][0]["resource"] = "foreign"
    elif mutation == "duplicate":
        data["evidence"].append(data["evidence"][0])
    else:
        store.path_for(item.artifact_sha256).write_text("tampered")
    wire.data[key] = json.dumps(data).encode()
    with pytest.raises(EvidenceIntegrityError):
        cache.get(scope())


def test_cache_bytes_and_lifetime_are_bounded(tmp_path: Path) -> None:
    """Malformed oversize entries and unbounded envelope lifetimes cannot be returned."""
    store, wire = ArtifactStore(tmp_path), Wire()
    cache = RedisDerivedCache(wire, store)
    cache.put(scope(), ())
    key = next(iter(wire.data))
    wire.data[key] = b"x" * (MAX_CACHE_BYTES + 1)
    with pytest.raises(EvidenceIntegrityError, match="byte budget"):
        cache.get(scope())
    now = utc_now()
    with pytest.raises(ValidationError):
        CacheEntry(
            scope=scope(), evidence=(), created_at=now, expires_at=now + timedelta(seconds=301)
        )


def test_cache_oversize_write_has_no_dispatch(tmp_path: Path) -> None:
    """A valid collection of large citations still cannot exceed the cache envelope budget."""
    store, wire = ArtifactStore(tmp_path), Wire()
    now = utc_now()
    items = tuple(
        normalize(
            Observation(
                source="LOG",
                resource="payments",
                observed_at=now,
                query="q" * 4000,
                summary="s" * 4000,
            ),
            "incident-one",
            now,
            now,
            store,
        )
        for _ in range(9)
    )
    with pytest.raises(EvidenceIntegrityError, match="byte budget"):
        RedisDerivedCache(wire, store).put(scope(), items)
    assert not wire.data


@pytest.mark.parametrize(
    "change",
    [
        {"size": 0},
        {"size": 6},
        {"size": True},
        {"kind": "secrets"},
        {"text": "x" * 257},
        {"text": " "},
        {"index": "foreign"},
        {"query": {"match_all": {}}},
        {"url": "https://foreign"},
    ],
)
def test_search_arguments_cannot_add_authority(change: dict[str, Any]) -> None:
    """Caller-supplied URL, DSL and index fields fail before any transport exists."""
    with pytest.raises(ValidationError):
        SearchRequest.model_validate(
            {"scope": scope(), "kind": "RUNBOOK", "text": "upstream", **change}
        )


class RedisStub:
    """Expose concrete Redis adapter options and exact GETRANGE arguments."""

    def __init__(self, **options: Any) -> None:
        """Capture configuration and defer connection effects."""
        self.options = options
        self.args: tuple[object, ...] = ()
        self.value: object = b"bounded"
        self.written: object = True
        self.closed = False

    def set(self, key: str, value: bytes, ex: int) -> object:
        """Return an explicit write acknowledgment."""
        self.args = key, value, ex
        return self.written

    def getrange(self, *args: object) -> object:
        """Record the server-side substring cap."""
        self.args = args
        return self.value

    def close(self) -> None:
        """Make pool disposal observable."""
        self.closed = True


def test_redis_wire_requires_tls_and_server_byte_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """No unbounded GET or application credential escalation is hidden in the wire adapter."""
    backend = RedisStub()

    def factory(**options: Any) -> RedisStub:
        """Capture the SDK options without making a network request."""
        backend.options = options
        return backend

    monkeypatch.setattr("payops.memory.data_clients.redis.Redis", factory)
    wire = RedisWire(config())
    assert backend.options["ssl_check_hostname"] is True
    assert backend.options["ssl_cert_reqs"] == "required"
    assert backend.options["username"] == "payops_cache"
    assert backend.options["max_connections"] == 4
    wire.put("payops:key", b"v", 5)
    assert backend.args == ("payops:key", b"v", 5)
    assert wire.read("payops:key") == b"bounded"
    assert backend.args == ("payops:key", 0, MAX_CACHE_BYTES)
    backend.value = "bad"
    with pytest.raises(EvidenceIntegrityError):
        wire.read("payops:key")
    backend.written = False
    with pytest.raises(RuntimeError):
        wire.put("payops:key", b"v", 5)
    wire.close()
    assert backend.closed


class Stream(httpx.SyncByteStream):
    """Feed the same streaming code path used by real HTTP responses."""

    def __init__(self, data: bytes) -> None:
        """Keep test bytes explicit and deterministic."""
        self.data = data

    def __iter__(self) -> Iterator[bytes]:
        """Yield small pieces so the response limit is checked incrementally."""
        for start in range(0, len(self.data), 1024):
            yield self.data[start : start + 1024]


def search_payload(store: ArtifactStore) -> dict[str, Any]:
    """Index only references to a genuinely verified source artifact."""
    item = evidence(store, incident_id="source-runbook")
    doc = RetrievalDocument(
        namespace="payops-sandbox", service="payments", kind="RUNBOOK", evidence=item
    )
    return {
        "timed_out": False,
        "_shards": {"failed": 0},
        "hits": {
            "hits": [
                {
                    "_index": INDEXES["RUNBOOK"],
                    "_id": item.artifact_sha256,
                    "_source": doc.model_dump(mode="json"),
                }
            ]
        },
    }


def test_search_fixes_dsl_scope_and_rebinds_provenance(tmp_path: Path) -> None:
    """Plain hostile text stays match data and verified source lineage binds a new incident."""
    store = ArtifactStore(tmp_path)
    payload = search_payload(store)
    seen: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Return the fake server's artifact reference through an actual HTTPX stream."""
        seen.append(request)
        return httpx.Response(200, stream=Stream(json.dumps(payload).encode()))

    client = ElasticsearchRetrieval(config(), store, transport=httpx.MockTransport(respond))
    request = SearchRequest(scope=scope(), kind="RUNBOOK", text='* OR secret; {"script":"x"}')
    result = client.search(request)
    client.close()
    assert len(result) == 1 and result[0].incident_id == "incident-one"
    artifact = store.verify(result[0])
    lineage = verify_retrieval_evidence(result[0], store)
    assert lineage.source.incident_id == "source-runbook"
    assert result[0].observed_at == lineage.source.observed_at
    assert lineage.source.collected_at <= lineage.retrieved_at <= result[0].collected_at
    assert artifact["payload"] == lineage.model_dump(mode="json")
    wire = json.loads(seen[0].content)
    assert seen[0].url.path == "/payops-runbooks-v1/_search"
    assert seen[0].url.params["allow_partial_search_results"] == "false"
    assert wire["query"]["bool"]["must"][0]["match"]["text"]["query"] == request.text
    assert wire["query"]["bool"]["filter"][0] == {"term": {"namespace": "payops-sandbox"}}
    assert wire["query"]["bool"]["filter"][1] == {"term": {"service": "payments"}}
    assert wire["query"]["bool"]["filter"][2] == {"term": {"kind": "RUNBOOK"}}
    assert wire["size"] == 3 and wire["timeout"] == "2s"
    assert "text" not in wire["_source"]


def test_duplicate_search_documents_cannot_amplify_evidence(tmp_path: Path) -> None:
    """Repeating one artifact cannot manufacture two independent incident citations."""
    store = ArtifactStore(tmp_path)
    payload = search_payload(store)
    payload["hits"]["hits"] *= 2
    client = ElasticsearchRetrieval(
        config(),
        store,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Stream(json.dumps(payload).encode()))
        ),
    )
    try:
        with pytest.raises(EvidenceIntegrityError, match="duplicate retrieval"):
            client.search(SearchRequest(scope=scope(), kind="RUNBOOK", text="upstream"))
    finally:
        client.close()


@pytest.mark.parametrize(
    "mutation",
    [
        "timeout",
        "partial",
        "hits",
        "oversize_hits",
        "namespace",
        "index",
        "id",
        "kind",
        "resource",
        "artifact_namespace",
        "digest",
    ],
)
def test_search_rejects_partial_foreign_or_unverifiable_results(
    tmp_path: Path, mutation: str
) -> None:
    """No successful result survives incomplete search or contradictory artifact lineage."""
    store = ArtifactStore(tmp_path)
    payload = search_payload(store)
    row = payload["hits"]["hits"][0]
    if mutation == "timeout":
        payload["timed_out"] = True
    elif mutation == "partial":
        payload["_shards"]["failed"] = 1
    elif mutation == "hits":
        payload["hits"] = []
    elif mutation == "oversize_hits":
        payload["hits"]["hits"] *= 4
    elif mutation in {"index", "id"}:
        row["_" + mutation] = "foreign"
    elif mutation == "namespace":
        row["_source"]["namespace"] = "foreign"
    elif mutation in {"kind", "resource"}:
        row["_source"]["evidence"]["source" if mutation == "kind" else mutation] = (
            "MEMORY" if mutation == "kind" else "foreign"
        )
    elif mutation == "artifact_namespace":
        foreign = evidence(store, namespace="foreign", incident_id="source-runbook")
        row["_source"]["evidence"] = foreign.model_dump(mode="json")
        row["_id"] = foreign.artifact_sha256
    else:
        store.path_for(row["_id"]).write_text("tampered")
    client = ElasticsearchRetrieval(
        config(),
        store,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Stream(json.dumps(payload).encode()))
        ),
    )
    try:
        with pytest.raises(EvidenceIntegrityError):
            client.search(SearchRequest(scope=scope(), kind="RUNBOOK", text="availability"))
    finally:
        client.close()


@pytest.mark.parametrize("mode", ["status", "compression", "oversize", "invalid_json"])
def test_search_wire_rejections_are_bounded(tmp_path: Path, mode: str) -> None:
    """Errors, compressed bodies and excess bytes are rejected without exposing server text."""

    def respond(request: httpx.Request) -> httpx.Response:
        """Supply a deliberately invalid server response."""
        return httpx.Response(
            503 if mode == "status" else 200,
            headers={"content-encoding": "gzip"} if mode == "compression" else {},
            stream=Stream(b"x" * (MAX_SEARCH_BYTES + 1) if mode == "oversize" else b"not json"),
        )

    client = ElasticsearchRetrieval(
        config(), ArtifactStore(tmp_path), transport=httpx.MockTransport(respond)
    )
    try:
        with pytest.raises((EvidenceIntegrityError, ValidationError)):
            client.search(SearchRequest(scope=scope(), kind="RUNBOOK", text="availability"))
    finally:
        client.close()


@pytest.mark.parametrize("data", [b"{}", b""])
def test_search_trickling_or_empty_response_cannot_reset_elapsed_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: bytes
) -> None:
    """A peer cannot prolong retrieval indefinitely by sending another small chunk."""
    ticks = iter((0.0, 4.0))
    monkeypatch.setattr("payops.memory.data_clients.monotonic", lambda: next(ticks))
    client = ElasticsearchRetrieval(
        config(),
        ArtifactStore(tmp_path),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Stream(data))),
    )
    try:
        with pytest.raises(EvidenceIntegrityError, match="elapsed budget"):
            client.search(SearchRequest(scope=scope(), kind="RUNBOOK", text="upstream"))
    finally:
        client.close()
