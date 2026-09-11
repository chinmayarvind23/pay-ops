"""Operator-bound SQL, derived evidence caching and read-only retrieval adapters."""

import ssl
from collections.abc import Callable
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Annotated, Literal, Protocol, Self

import httpx
import redis
from pydantic import AwareDatetime, ConfigDict, Field, SecretStr, StringConstraints, model_validator
from redis.backoff import NoBackoff
from redis.retry import Retry
from sqlalchemy import URL, create_engine

from payops.contracts import Contract, EvidenceItem, Identifier, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize
from payops.memory.store import IncidentStore
from payops.remediation.store import ActionStore

MAX_CACHE_BYTES = 65_536
MAX_SEARCH_BYTES = 131_072
Port = Annotated[int, Field(strict=True, ge=1, le=65535)]
Kind = Literal["RUNBOOK", "MEMORY"]
Index = Literal["payops-runbooks-v1", "payops-memory-v1"]
INDEXES: dict[Kind, Index] = {"RUNBOOK": "payops-runbooks-v1", "MEMORY": "payops-memory-v1"}


class ElasticsearchConfig(Contract):
    """A retrieval-only host needs no PostgreSQL or Redis credential to read Elasticsearch."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    ca_file: Path
    elastic_password: SecretStr = Field(repr=False)
    elastic_host: Literal["localhost", "elasticsearch.payops-data.svc.cluster.local"] = "localhost"
    elastic_port: Port = 29200

    @model_validator(mode="after")
    def valid_credentials(self) -> Self:
        """Keep the existing explicit CA and bounded secret requirements on the smaller config."""
        if not self.ca_file.is_absolute() or not self.ca_file.is_file():
            raise ValueError("explicit CA file required")
        if not 16 <= len(self.elastic_password.get_secret_value()) <= 256:
            raise ValueError("credential length outside bounds")
        return self


class LocalDataConfig(Contract):
    """Only the trusted host constructs this object; credentials never come from model arguments."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    ca_file: Path
    postgres_password: SecretStr = Field(repr=False)
    redis_password: SecretStr = Field(repr=False)
    elastic_password: SecretStr = Field(repr=False)
    postgres_host: Literal["localhost", "postgres.payops-data.svc.cluster.local"] = "localhost"
    redis_host: Literal["localhost", "redis.payops-data.svc.cluster.local"] = "localhost"
    elastic_host: Literal["localhost", "elasticsearch.payops-data.svc.cluster.local"] = "localhost"
    postgres_port: Port = 25432
    redis_port: Port = 26379
    elastic_port: Port = 29200

    @model_validator(mode="after")
    def valid_credentials(self) -> Self:
        """Require an explicit CA file and bounded secrets without displaying their values."""
        if not self.ca_file.is_absolute() or not self.ca_file.is_file():
            raise ValueError("explicit CA file required")
        for secret in (self.postgres_password, self.redis_password, self.elastic_password):
            if not 16 <= len(secret.get_secret_value()) <= 256:
                raise ValueError("credential length outside bounds")
        return self


class SqlStores:
    """Own one shared bounded pool; both SQL stores borrow it and remain authoritative."""

    def __init__(self, config: LocalDataConfig) -> None:
        """Verify certificates and bind the application role, schema and server query deadlines."""
        url = URL.create(
            "postgresql+psycopg",
            username="payops_app",
            password=config.postgres_password.get_secret_value(),
            host=config.postgres_host,
            port=config.postgres_port,
            database="payops",
        )
        self.engine = create_engine(
            url,
            pool_size=4,
            max_overflow=0,
            pool_timeout=3,
            pool_pre_ping=True,
            hide_parameters=True,
            connect_args={
                "sslmode": "verify-full",
                "sslrootcert": str(config.ca_file),
                "connect_timeout": 3,
                "options": "-csearch_path=payops,pg_catalog -cstatement_timeout=5000 "
                "-clock_timeout=1000 -cidle_in_transaction_session_timeout=5000",
            },
        )
        try:
            self.incidents = IncidentStore(self.engine)
            self.actions = ActionStore(self.engine)
        except Exception:
            self.engine.dispose()
            raise

    def close(self) -> None:
        """Dispose the shared pool only through its owning factory."""
        self.engine.dispose()


class EvidenceScope(Contract):
    """The host derives this scope from the SQL incident and its authorized namespace/service."""

    incident_id: Identifier
    namespace: Identifier
    service: Identifier


class CacheEntry(Contract):
    """Cache bytes contain derived evidence only, never approvals or incident state."""

    scope: EvidenceScope
    created_at: AwareDatetime
    expires_at: AwareDatetime
    evidence: tuple[EvidenceItem, ...] = Field(max_length=16)

    @model_validator(mode="after")
    def valid_lifetime(self) -> Self:
        """Redis TTL is supplemented by a bounded lifetime in the untrusted stored envelope."""
        if not 0 < (self.expires_at - self.created_at).total_seconds() <= 300:
            raise ValueError("cache lifetime outside bounds")
        return self


class CacheWire(Protocol):
    """The cache wrapper can only write an expiring value or request a bounded substring."""

    def put(self, key: str, value: bytes, ttl: int) -> None:
        """Write one namespaced expiring derived value."""
        ...

    def read(self, key: str) -> bytes:
        """Read at most the cache byte cap plus one overflow sentinel."""
        ...


class RedisWire:
    """Concrete redis-py transport with verified TLS and no retries or arbitrary commands."""

    def __init__(self, config: LocalDataConfig) -> None:
        """Bind only the restricted cache identity and a four-connection pool."""
        self._client = redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            username="payops_cache",
            password=config.redis_password.get_secret_value(),
            ssl=True,
            ssl_ca_certs=str(config.ca_file),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0),
            max_connections=4,
            decode_responses=False,
        )

    def put(self, key: str, value: bytes, ttl: int) -> None:
        """Redis expires the same envelope lifetime checked on retrieval."""
        if self._client.set(key, value, ex=ttl) is not True:
            raise RuntimeError("cache write failed")

    def read(self, key: str) -> bytes:
        """GETRANGE bounds the reply on the server before the client allocates it."""
        value = self._client.getrange(key, 0, MAX_CACHE_BYTES)
        if not isinstance(value, bytes):
            raise EvidenceIntegrityError("invalid cache response")
        return value

    def close(self) -> None:
        """Release the owned Redis pool."""
        self._client.close()


class RedisDerivedCache:
    """A miss or transport failure leaves SQL authority unchanged; corruption is explicit."""

    def __init__(
        self, wire: CacheWire, artifacts: ArtifactStore, clock: Callable[[], datetime] = utc_now
    ) -> None:
        """Inject only host-owned transport, artifact authority and wall clock."""
        self._wire, self._artifacts, self._clock = wire, artifacts, clock

    @staticmethod
    def _key(scope: EvidenceScope) -> str:
        """Hash a validated complete scope so separators cannot alias another incident."""
        return "payops:evidence:v1:" + sha256(scope.model_dump_json().encode()).hexdigest()

    def _verify(self, entry: CacheEntry, scope: EvidenceScope) -> None:
        """Reverify every citation and incident/service ownership before any cached hit is used."""
        if entry.scope != scope:
            raise EvidenceIntegrityError("cache scope mismatch")
        if len({item.evidence_id for item in entry.evidence}) != len(entry.evidence):
            raise EvidenceIntegrityError("duplicate cached evidence")
        for item in entry.evidence:
            if item.incident_id != scope.incident_id or item.resource != scope.service:
                raise EvidenceIntegrityError("cached evidence scope mismatch")
            if item.query.startswith("elasticsearch://"):
                if verify_retrieval_evidence(item, self._artifacts).scope != scope:
                    raise EvidenceIntegrityError("cached retrieval namespace scope mismatch")
            else:
                self._artifacts.verify(item)

    def put(self, scope: EvidenceScope, evidence: tuple[EvidenceItem, ...], ttl: int = 60) -> None:
        """Publish only a bounded artifact-verified bundle with a short explicit TTL."""
        if type(ttl) is not int or not 1 <= ttl <= 300:
            raise ValueError("cache TTL outside bounds")
        now = self._clock()
        entry = CacheEntry(
            scope=scope, evidence=evidence, created_at=now, expires_at=now + timedelta(seconds=ttl)
        )
        self._verify(entry, scope)
        data = entry.model_dump_json().encode()
        if len(data) > MAX_CACHE_BYTES:
            raise EvidenceIntegrityError("cache entry exceeds byte budget")
        self._wire.put(self._key(scope), data, ttl)

    def get(self, scope: EvidenceScope) -> tuple[EvidenceItem, ...] | None:
        """Expired entries miss; corrupt or foreign entries never become evidence."""
        data = self._wire.read(self._key(scope))
        if not data:
            return None
        if len(data) > MAX_CACHE_BYTES:
            raise EvidenceIntegrityError("cache entry exceeds byte budget")
        entry = CacheEntry.model_validate_json(data)
        self._verify(entry, scope)
        if not entry.created_at <= self._clock() < entry.expires_at:
            return None
        return entry.evidence


class SearchRequest(Contract):
    """Models may supply plain text; the host binds scope, kind, limits and index choice."""

    scope: EvidenceScope
    kind: Kind
    text: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=256)]
    size: int = Field(default=3, strict=True, ge=1, le=5)


class RetrievalDocument(Contract):
    """An index entry points to separately verified evidence, rather than asserting authority."""

    namespace: Identifier
    service: Identifier
    kind: Kind
    evidence: EvidenceItem


class RetrievalLineage(Contract):
    """Keep the full source citation and its timestamps when binding context to another incident."""

    transformation: Literal["payops-retrieval-v1"] = "payops-retrieval-v1"
    scope: EvidenceScope
    index: Index
    source: EvidenceItem
    retrieved_at: AwareDatetime
    retrieval_only: Literal[True] = True


def verify_retrieval_evidence(item: EvidenceItem, store: ArtifactStore) -> RetrievalLineage:
    """Verify the derived artifact and its complete bounded source chain before using context."""
    return _verify_retrieval(item, store, set())


def _verify_retrieval(item: EvidenceItem, store: ArtifactStore, seen: set[str]) -> RetrievalLineage:
    """Eight retrieval links bound traversal; each original observation time remains unchanged."""
    if item.artifact_sha256 in seen or len(seen) >= 8:
        raise EvidenceIntegrityError("retrieval lineage cycle or depth exceeded")
    seen.add(item.artifact_sha256)
    lineage = RetrievalLineage.model_validate(store.verify(item).get("payload"))
    source = lineage.source
    kind = item.source
    if kind != "RUNBOOK" and kind != "MEMORY":
        raise EvidenceIntegrityError("retrieval evidence kind mismatch")
    if (
        lineage.index != INDEXES[kind]
        or item.query != f"elasticsearch://{lineage.index}/fixed-scope-match"
        or item.incident_id != lineage.scope.incident_id
        or item.resource != lineage.scope.service
        or source.source != item.source
        or source.resource != item.resource
        or item.summary != source.summary
        or item.observed_at != source.observed_at
        or not (
            source.observed_at <= source.collected_at <= lineage.retrieved_at <= item.collected_at
        )
    ):
        raise EvidenceIntegrityError("retrieval metadata disagrees with source lineage")
    actual_scope = _source_scope(source, store, seen)
    if actual_scope != (lineage.scope.namespace, lineage.scope.service):
        raise EvidenceIntegrityError("retrieval source scope mismatch")
    return lineage


def _source_scope(
    item: EvidenceItem, store: ArtifactStore, seen: set[str]
) -> tuple[object, object]:
    """A source is either an original scoped artifact or another fully verified retrieval link."""
    raw = store.verify(item).get("payload")
    if item.query.startswith("elasticsearch://"):
        previous = _verify_retrieval(item, store, seen)
        return previous.scope.namespace, previous.scope.service
    elif isinstance(raw, dict):
        return raw.get("namespace"), raw.get("service")
    raise EvidenceIntegrityError("retrieval source payload is invalid")


class ElasticsearchRetrieval:
    """Fixed REST searches use streaming byte limits and artifact-bound sanitized provenance."""

    def __init__(
        self,
        config: LocalDataConfig | ElasticsearchConfig,
        artifacts: ArtifactStore,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        """Only trusted tests may inject transport; no model controls endpoints or credentials."""
        self._artifacts = artifacts
        self._client = httpx.Client(
            base_url=f"https://{config.elastic_host}:{config.elastic_port}",
            auth=("payops_reader", config.elastic_password.get_secret_value()),
            verify=ssl.create_default_context(cafile=str(config.ca_file)),
            timeout=3,
            follow_redirects=False,
            trust_env=False,
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=4),
            transport=transport,
            headers={"Accept-Encoding": "identity"},
        )

    def _response(self, request: SearchRequest) -> bytes:
        """The JSON query has exact filters and plain match text, never a caller-provided DSL."""
        query = {
            "size": request.size,
            "timeout": "2s",
            "track_total_hits": False,
            "_source": ["namespace", "service", "kind", "evidence"],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"namespace": request.scope.namespace}},
                        {"term": {"service": request.scope.service}},
                        {"term": {"kind": request.kind}},
                    ],
                    "must": [{"match": {"text": {"query": request.text, "operator": "and"}}}],
                }
            },
        }
        content = bytearray()
        started = monotonic()
        with self._client.stream(
            "POST",
            f"/{INDEXES[request.kind]}/_search",
            params={"allow_partial_search_results": "false"},
            json=query,
        ) as response:
            if (
                response.status_code != 200
                or response.headers.get("content-encoding", "identity") != "identity"
            ):
                raise EvidenceIntegrityError("retrieval response rejected")
            for part in response.iter_raw():
                if monotonic() - started > 3:
                    raise EvidenceIntegrityError("retrieval elapsed budget exceeded")
                if len(content) + len(part) > MAX_SEARCH_BYTES:
                    raise EvidenceIntegrityError("retrieval exceeds byte budget")
                content.extend(part)
        if monotonic() - started > 3:
            raise EvidenceIntegrityError("retrieval elapsed budget exceeded")
        return bytes(content)

    def search(self, request: SearchRequest) -> tuple[EvidenceItem, ...]:
        """Reject incomplete results and normalize verified material for this incident."""
        payload = JSON_OBJECT.validate_json(self._response(request))
        shards, hits = payload.get("_shards"), payload.get("hits")
        if payload.get("timed_out") is not False or not isinstance(shards, dict):
            raise EvidenceIntegrityError("incomplete retrieval response")
        if type(shards.get("failed")) is not int or shards["failed"] != 0:
            raise EvidenceIntegrityError("partial retrieval response")
        if not isinstance(hits, dict) or not isinstance(hits.get("hits"), list):
            raise EvidenceIntegrityError("invalid retrieval hits")
        rows = hits["hits"]
        if not isinstance(rows, list) or len(rows) > request.size:
            raise EvidenceIntegrityError("retrieval hit budget exceeded")
        seen: set[str] = set()
        return tuple(self._item(row, request, seen) for row in rows)

    def _item(self, row: object, request: SearchRequest, seen: set[str]) -> EvidenceItem:
        """The returned index, document digest, scope and artifact payload must all agree."""
        data = JSON_OBJECT.validate_python(row)
        document = RetrievalDocument.model_validate(data.get("_source"))
        expected = (request.scope.namespace, request.scope.service, request.kind)
        if (document.namespace, document.service, document.kind) != expected:
            raise EvidenceIntegrityError("retrieval scope mismatch")
        item = document.evidence
        if item.artifact_sha256 in seen:
            raise EvidenceIntegrityError("duplicate retrieval artifact")
        seen.add(item.artifact_sha256)
        if data.get("_index") != INDEXES[request.kind] or data.get("_id") != item.artifact_sha256:
            raise EvidenceIntegrityError("retrieval identity mismatch")
        if item.source != request.kind or item.resource != request.scope.service:
            raise EvidenceIntegrityError("retrieval evidence mismatch")
        if _source_scope(item, self._artifacts, set()) != expected[:2]:
            raise EvidenceIntegrityError("retrieval artifact scope mismatch")
        now = utc_now()
        lineage = RetrievalLineage(
            scope=request.scope, index=INDEXES[request.kind], source=item, retrieved_at=now
        )
        result = normalize(
            Observation(
                source=request.kind,
                resource=request.scope.service,
                observed_at=item.observed_at,
                query=f"elasticsearch://{INDEXES[request.kind]}/fixed-scope-match",
                summary=item.summary,
                payload=JSON_OBJECT.validate_python(lineage.model_dump(mode="json")),
            ),
            request.scope.incident_id,
            item.observed_at,
            now,
            self._artifacts,
        )
        verify_retrieval_evidence(result, self._artifacts)
        return result

    def close(self) -> None:
        """Close the owned HTTP connection pool."""
        self._client.close()
