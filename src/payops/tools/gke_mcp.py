"""Source-pinned GKE MCP read boundary; transport verification is not live GKE evidence."""

import asyncio
import json
import re
from hashlib import sha256
from typing import Annotated, Literal, Protocol

from pydantic import AwareDatetime, ConfigDict, Field, JsonValue, StringConstraints, TypeAdapter

from payops.contracts import Contract, Identifier, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.redact import redact, redact_text

SOURCE_PIN = "a97e99d852c70cf9eb5b85ddf6504a8f0ee8d9d4"
# Full inputSchema hashes came from actual tools/list using the pinned k8s.Install.
SCHEMA_HASHES = {
    "get_k8s_resource": "2d8c5ea756bc0ae80174c9c015dc286fb6687ba041e4ce59ce339454e7ba8b0f",
    "list_k8s_events": "57c87d46599955c23824d78383fc97721ec185f70f541ff5f61d2ae77462f072",
    "get_k8s_logs": "0a27f0bcd5d704d00286e2cf1d05c9f380b278bd627e1eea0b7a694afc00943c",
}
RESOURCE_KINDS = {
    "pods": ("Pod", "v1"),
    "services": ("Service", "v1"),
    "events": ("Event", "v1"),
    "deployments": ("Deployment", "apps/v1"),
    "replicasets": ("ReplicaSet", "apps/v1"),
}
ResourceType = Literal["pods", "services", "events", "deployments", "replicasets"]
Name = Annotated[str, StringConstraints(max_length=253, pattern=r"^[a-z0-9][a-z0-9.-]*$")]
type JsonObject = dict[str, JsonValue]
Status = Literal[
    "OK",
    "EMPTY",
    "PARTIAL",
    "UNAVAILABLE",
    "UPSTREAM_ERROR",
    "INVALID_RESPONSE",
    "OVERSIZED",
    "TIMEOUT",
]
MAX_TEXT_BYTES = 65536
JSON_OBJECT = TypeAdapter(JsonObject)


class StrictContract(Contract):
    """No coercion may turn an invalid numeric bound or extra field into a capability."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class GkeScope(StrictContract):
    """Trusted configuration fixes the complete cloud and namespace boundary."""

    project_id: Name
    location: Name
    cluster_name: Name
    namespace: Name


class ResourceGrant(StrictContract):
    """Exact names come from trusted workload inventory, never model-selected prefixes."""

    resource_type: ResourceType
    name: Name
    containers: tuple[Name, ...] = ()


class ReadBase(StrictContract):
    """Each request repeats its binding so cross-incident reuse fails before dispatch."""

    incident_id: Identifier
    scope: GkeScope
    name: Name


class ResourceRead(ReadBase):
    """Only named resources are supported because the upstream LIST has no item budget."""

    operation: Literal["get_resource"]
    resource_type: ResourceType


class EventRead(ReadBase):
    """Event tables stay bounded and scoped to one approved involved object."""

    operation: Literal["get_events"]
    resource_type: ResourceType
    limit: int = Field(default=50, ge=1, le=100)


class LogRead(ReadBase):
    """One named container and positive limits avoid upstream unbounded-default behavior."""

    operation: Literal["get_container_logs"]
    container: Name
    tail: int = Field(default=100, ge=1, le=200)
    since_seconds: int = Field(default=300, ge=1, le=900)
    previous: bool = False


class McpSessionInfo(StrictContract):
    """An SDK binding copies validated initialize metadata into this internal record."""

    server_name: str = Field(min_length=1, max_length=256)
    server_version: str = Field(min_length=1, max_length=128)
    protocol_version: Literal["2025-11-25"]


Request = Annotated[ResourceRead | EventRead | LogRead, Field(discriminator="operation")]
REQUEST = TypeAdapter(Request)


class McpTransport(Protocol):
    """Internal async seam; an SDK binding owns initialized sessions and bounded wire reads."""

    @property
    def session_info(self) -> McpSessionInfo:
        """Expose initialized-session provenance without importing upstream instructions."""
        ...

    async def list_tools(self) -> list[JsonObject]:
        """Return complete paginated tools/list results, bounded to 256 tools/256 KiB."""
        ...

    async def call_tool(self, name: str, arguments: JsonObject) -> JsonObject:
        """Return the standard MCP result object with a cancellable bounded response read."""
        ...


class McpReadResult(Contract):
    """Retained sanitized evidence records uncertainty without fabricating event timestamps."""

    status: Status
    payload: JsonObject
    collected_at: AwareDatetime
    observed_at: None = None
    artifact_uri: str
    artifact_sha256: str
    untrusted_text: Literal[True] = True


def canonical(value: JsonValue) -> bytes:
    """Use identical UTF-8 serialization for observed schema pins and artifact provenance."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def check_capabilities(catalog: list[JsonObject]) -> None:
    """Hash all approved schemas exactly; future additions never become exposed wrappers."""
    if len(catalog) > 256 or len(canonical(list(catalog))) > 262144:
        raise ValueError("capability catalog exceeds budget")
    catalog = TypeAdapter(list[JsonObject]).validate_python(catalog, strict=True)
    observed: dict[str, str] = {}
    names: set[str] = set()
    for tool in catalog:
        name = tool.get("name")
        if not isinstance(name, str) or name in names:
            raise ValueError("invalid or duplicate upstream capability")
        names.add(name)
        if name in SCHEMA_HASHES:
            schema = tool.get("inputSchema")
            if not isinstance(schema, dict):
                raise ValueError("missing upstream input schema")
            observed[name] = sha256(canonical(schema)).hexdigest()
    if observed != SCHEMA_HASHES:
        raise ValueError("approved upstream schemas differ from pinned registration")


def translate(request: Request) -> tuple[str, JsonObject]:
    """Map closed PayOps wrappers to source-verified flat upstream keys only."""
    args: JsonObject = {**request.scope.model_dump(), "name": request.name}
    if isinstance(request, LogRead):
        args.update(
            {
                "container": request.container,
                "tail": request.tail,
                "since": f"{request.since_seconds}s",
                "previous": request.previous,
                "timestamps": True,
                "allContainers": False,
            }
        )
        return "get_k8s_logs", args
    args["resourceType"] = request.resource_type
    if isinstance(request, EventRead):
        args.update({"limit": request.limit, "allNamespaces": False})
        return "list_k8s_events", args
    args["outputFormat"] = "json"
    return "get_k8s_resource", args


def text_content(reply: JsonObject) -> str:
    """Only bounded text blocks are accepted; embedded resources are never dereferenced."""
    content = reply.get("content")
    if not isinstance(content, list) or not 1 <= len(content) <= 8:
        raise ValueError("invalid MCP content")
    if not isinstance(reply.get("isError", False), bool) or "structuredContent" in reply:
        raise ValueError("unexpected MCP result shape")
    texts: list[str] = []
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "text":
            raise ValueError("non-text MCP content")
        text = block.get("text")
        if not isinstance(text, str):
            raise ValueError("invalid MCP text")
        texts.append(text)
    combined = "\n".join(texts)
    if len(combined.encode()) > MAX_TEXT_BYTES:
        raise OverflowError("MCP text exceeds budget")
    return combined


def scalars(value: JsonValue, fields: tuple[str, ...]) -> JsonObject:
    """Project scalar leaves so an unexpected nested object cannot smuggle extra fields."""
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return {
        key: item
        for key, item in value.items()
        if key in fields and not isinstance(item, (list, dict))
    }


def conditions(value: JsonValue) -> list[JsonValue]:
    """Diagnostic conditions retain reasons and times while omitting arbitrary message text."""
    if not isinstance(value, list) or len(value) > 32:
        raise ValueError("invalid conditions")
    return [scalars(item, ("type", "status", "reason", "lastTransitionTime")) for item in value]


def resource_payload(text: str, request: ResourceRead) -> JsonObject:
    """Verify GVR/name/namespace before retaining minimal status and metadata fields."""
    value = JSON_OBJECT.validate_json(text)
    metadata = scalars(value.get("metadata"), ("name", "namespace", "uid", "resourceVersion"))
    kind, version = RESOURCE_KINDS[request.resource_type]
    if (value.get("kind"), value.get("apiVersion")) != (kind, version):
        raise ValueError("unexpected resource kind or version")
    if metadata.get("name") != request.name or metadata.get("namespace") != request.scope.namespace:
        raise ValueError("returned resource escapes request scope")
    raw_status = value.get("status", {})
    status = scalars(
        raw_status,
        (
            "phase",
            "reason",
            "replicas",
            "readyReplicas",
            "availableReplicas",
            "updatedReplicas",
            "observedGeneration",
            "unavailableReplicas",
        ),
    )
    if isinstance(raw_status, dict) and "conditions" in raw_status:
        status["conditions"] = conditions(raw_status["conditions"])
    projected: JsonObject = {
        "kind": kind,
        "apiVersion": version,
        "metadata": metadata,
        "status": status,
    }
    if kind == "Event":
        projected["event"] = scalars(
            value,
            (
                "type",
                "reason",
                "message",
                "count",
                "firstTimestamp",
                "lastTimestamp",
                "eventTime",
            ),
        )
    return JSON_OBJECT.validate_python(redact(projected))


def log_payload(text: str) -> tuple[Status, JsonObject]:
    """Pinned handler markers indicate partial/unavailable output independently of isError."""
    if text == "(no logs found)" or not text.strip():
        return "EMPTY", {"text": "", "time_semantics": "no observations"}
    lines = text.splitlines()
    failures = [line for line in lines if re.match(r"^Error: failed to (stream|read) logs:", line)]
    truncated = "... (logs truncated due to size limit)" in lines
    status: Status = "OK"
    if failures:
        status = "UNAVAILABLE" if len(failures) == len(lines) else "PARTIAL"
    elif truncated:
        status = "PARTIAL"
    return status, {
        "text": redact_text(text),
        "truncated": truncated,
        "time_semantics": "upstream timestamped lines; collection time separate",
    }


def decode(reply: JsonObject, request: Request) -> tuple[Status, JsonObject, str | None]:
    """Every failure keeps an explicit status instead of an empty healthy observation."""
    try:
        reply = JSON_OBJECT.validate_python(reply, strict=True)
        encoded = canonical(reply)
        if len(encoded) > 262144:
            return "OVERSIZED", {}, None
        digest = sha256(encoded).hexdigest()
        text = text_content(reply)
        if reply.get("isError", False):
            return "UPSTREAM_ERROR", {"reason": "upstream reported tool failure"}, digest
        if isinstance(request, ResourceRead):
            return "OK", resource_payload(text, request), digest
        if isinstance(request, LogRead):
            status, payload = log_payload(text)
            return status, payload, digest
        return "OK", {"text": redact_text(text), "time_semantics": "relative event table"}, digest
    except OverflowError:
        return "OVERSIZED", {}, None
    except (ValueError, TypeError, RecursionError):
        return "INVALID_RESPONSE", {}, None


class GkeMcpAdapter:
    """Reasoning receives three closed wrappers; the upstream catalog never grants authority."""

    def __init__(
        self,
        *,
        scope: GkeScope,
        incident_id: str,
        grants: tuple[ResourceGrant, ...],
        transport: McpTransport,
        store: ArtifactStore,
        timeout_seconds: float = 10,
    ) -> None:
        """Trusted construction binds exact resources and a cancellable total request deadline."""
        if not 0 < timeout_seconds <= 30 or not 1 <= len(grants) <= 64:
            raise ValueError("invalid MCP adapter budget")
        self._scope = scope
        self._incident_id = TypeAdapter(Identifier).validate_python(incident_id)
        self._grants = {(grant.resource_type, grant.name): grant for grant in grants}
        if len(self._grants) != len(grants):
            raise ValueError("duplicate resource grants")
        self._transport, self._store = transport, store
        self._session = transport.session_info
        self._timeout = timeout_seconds

    def _authorize(self, request: Request) -> None:
        """Invalid input and foreign scope are rejected before any transport method is invoked."""
        if request.scope != self._scope or request.incident_id != self._incident_id:
            raise ValueError("request outside bound incident scope")
        resource = "pods" if isinstance(request, LogRead) else request.resource_type
        grant = self._grants.get((resource, request.name))
        if grant is None:
            raise ValueError("resource has no exact named grant")
        if isinstance(request, LogRead) and request.container not in grant.containers:
            raise ValueError("container has no exact named grant")

    def _retain(
        self,
        tool: str,
        arguments: JsonObject,
        status: Status,
        payload: JsonObject,
        response_digest: str | None = None,
    ) -> McpReadResult:
        """Persist only sanitized output with scope, hashes and separate collection time."""
        collected = utc_now()
        envelope: JsonObject = {
            "incident_id": self._incident_id,
            "scope": self._scope.model_dump(),
            "contract_source_pin": SOURCE_PIN,
            "session": self._session.model_dump(),
            "input_schema_sha256": SCHEMA_HASHES[tool],
            "tool": tool,
            "arguments": arguments,
            "collected_at": collected.isoformat(),
            "observed_at": None,
            "response_sha256": response_digest,
            "untrusted_text": True,
            "status": status,
            "payload": payload,
        }
        uri, digest = self._store.write(envelope)
        return McpReadResult(
            status=status,
            payload=payload,
            collected_at=collected,
            artifact_uri=uri,
            artifact_sha256=digest,
        )

    async def read(self, value: JsonObject) -> McpReadResult:
        """Refresh pinned capabilities per call, then dispatch one bounded authorized read."""
        request = REQUEST.validate_python(value)
        self._authorize(request)
        tool, arguments = translate(request)
        try:
            async with asyncio.timeout(self._timeout):
                check_capabilities(await self._transport.list_tools())
                reply = await self._transport.call_tool(tool, arguments)
        except TimeoutError:
            return self._retain(tool, arguments, "TIMEOUT", {})
        except OSError:
            return self._retain(tool, arguments, "UNAVAILABLE", {})
        status, payload, digest = decode(reply, request)
        return self._retain(tool, arguments, status, payload, digest)
