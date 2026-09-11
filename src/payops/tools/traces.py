"""Identity-pinned local trace reads with an explicit independent command budget."""

import os
import shutil
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import BinaryIO, Literal, Protocol

from pydantic import Field, JsonValue

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, EvidenceIntegrityError
from payops.evidence.trace_span import (
    MAX_LOG_BYTES,
    MAX_SPANS,
    Immutable,
    PodIdentity,
    Service,
    SourceSpan,
    TraceLog,
    TraceScope,
    derive_trace_span,
    parse_console_log,
    publish_trace_log,
)

MAX_COMMANDS_PER_SERVICE = 8
MAX_COMMANDS = 40
MAX_SECONDS = 30.0
MAX_METADATA_BYTES = 1048576
MAX_SOURCE_BYTES = 10 * MAX_LOG_BYTES
type Object = dict[str, JsonValue]


# Join the job before launching the command, so no descendant can race job assignment.
# OS exit closes its noninherited job handle; os._exit preserves the command's exit code.
WINDOWS_JOB_GUARDIAN = """import os,subprocess,sys,win32api,win32job
job=win32job.CreateJobObject(None,"")
info=win32job.QueryInformationJobObject(job,win32job.JobObjectExtendedLimitInformation)
info["BasicLimitInformation"]["LimitFlags"] |= win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
win32job.SetInformationJobObject(job,win32job.JobObjectExtendedLimitInformation,info)
win32job.AssignProcessToJobObject(job,win32api.GetCurrentProcess())
code=subprocess.call(sys.argv[1:],stdin=subprocess.DEVNULL,close_fds=True)
os._exit(code)
"""


class ReadCommand(Protocol):
    """Only trusted adapter code constructs the complete subprocess argv and read limits."""

    def __call__(self, args: tuple[str, ...], maximum: int, timeout: float) -> bytes:
        """Return complete bounded stdout or fail without exposing process stderr."""
        ...


def drain_pipe(stream: BinaryIO, output: bytearray, limit: int, exceeded: threading.Event) -> None:
    """Each pipe retains at most its limit and one 4096-byte in-flight chunk."""
    try:
        while chunk := stream.read(4096):
            if len(output) + len(chunk) > limit:
                exceeded.set()
                return
            output.extend(chunk)
    except OSError:
        exceeded.set()


def stop_owned_process(child: subprocess.Popen[bytes]) -> None:
    """Terminate only the owned Windows guardian job or the newly created POSIX process group."""
    if sys.platform == "win32":
        if child.poll() is None:
            child.kill()
    else:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def bounded_read(args: tuple[str, ...], maximum: int, timeout: float) -> bytes:
    """Drain small chunks and terminate excess output or deadline expiration without a shell."""
    if not 0 < maximum <= 262144 or not 0 < timeout <= 12:
        raise ValueError("invalid trace subprocess budget")
    buffers = [bytearray(), bytearray()]
    exceeded = threading.Event()
    readers: list[threading.Thread] = []
    failure = False

    command = (
        (sys.executable, "-c", WINDOWS_JOB_GUARDIAN, *args) if sys.platform == "win32" else args
    )
    with subprocess.Popen(
        command,
        shell=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if sys.platform == "win32" else 0,
        start_new_session=sys.platform != "win32",
    ) as child:
        try:
            assert child.stdout is not None and child.stderr is not None
            for stream, output, limit in (
                (child.stdout, buffers[0], maximum),
                (child.stderr, buffers[1], 8192),
            ):
                reader = threading.Thread(
                    target=drain_pipe, args=(stream, output, limit, exceeded), daemon=True
                )
                readers.append(reader)
                reader.start()
            deadline = monotonic() + timeout
            while child.poll() is None or any(reader.is_alive() for reader in readers):
                if exceeded.is_set() or monotonic() >= deadline:
                    failure = True
                    break
                exceeded.wait(min(0.02, max(0, deadline - monotonic())))
        finally:
            stop_owned_process(child)
            child.wait(timeout=2)
            for reader in readers:
                if reader.ident is not None:
                    reader.join(timeout=2)
        if (
            failure
            or exceeded.is_set()
            or child.returncode
            or any(reader.is_alive() for reader in readers)
        ):
            raise ValueError("trace subprocess failed or exceeded budget")
    return bytes(buffers[0])


def _object(value: JsonValue) -> Object:
    """Malformed Kubernetes objects cannot be interpreted as missing healthy telemetry."""
    if not isinstance(value, dict):
        raise ValueError("Kubernetes object required")
    return value


def _items(value: JsonValue, maximum: int) -> list[Object]:
    """Every selected collection has a small fixed cardinality bound."""
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError("Kubernetes list exceeds trace reader scope")
    return [_object(item) for item in value]


def _text(value: JsonValue) -> str:
    """Resource identifiers are validated before they can become a named get argument."""
    if not isinstance(value, str) or not value or len(value) > 128:
        raise ValueError("Kubernetes identity string required")
    return value


def _metadata(raw: Object, scope: TraceScope, name: str | None = None) -> Object:
    """Namespace, service label and exact resource name must agree with trusted scope."""
    metadata = _object(raw.get("metadata"))
    labels = _object(metadata.get("labels"))
    if (
        metadata.get("namespace") != scope.namespace
        or labels.get("app.kubernetes.io/name") != scope.service
        or (name is not None and metadata.get("name") != name)
    ):
        raise ValueError("trace resource crosses namespace or service ownership")
    return metadata


def _owner(metadata: Object, kind: str) -> Object:
    """Require one controlling owner; unrelated references cannot establish deployment ownership."""
    owners = [
        owner
        for owner in _items(metadata.get("ownerReferences"), 8)
        if owner.get("controller") is True
    ]
    if (
        len(owners) != 1
        or owners[0].get("kind") != kind
        or owners[0].get("apiVersion") != "apps/v1"
    ):
        raise ValueError("trace resource controller ownership is invalid")
    return owners[0]


def _identity(raw: Object, scope: TraceScope, deployment_uid: str, replica_uid: str) -> PodIdentity:
    """Pin current container identity, restart count and the verified controller UID."""
    metadata = _metadata(raw, scope)
    owner = _owner(metadata, "ReplicaSet")
    if owner.get("uid") != replica_uid:
        raise ValueError("trace pod controller changed")
    statuses = _items(_object(raw.get("status")).get("containerStatuses"), 8)
    selected = [entry for entry in statuses if entry.get("name") == "sandbox"]
    if len(selected) != 1:
        raise ValueError("exact sandbox container status required")
    return PodIdentity.model_validate(
        {
            "pod_name": metadata.get("name"),
            "pod_uid": metadata.get("uid"),
            "deployment_uid": deployment_uid,
            "replica_set_uid": replica_uid,
            "container_id": selected[0].get("containerID"),
            "restart_count": selected[0].get("restartCount"),
        }
    )


class GraphSummary(Immutable):
    """Missing parents and duplicate records remain explicit; no graph certifies payment success."""

    sampling: Literal["bounded_sample"] = "bounded_sample"
    span_count: int = Field(ge=0, le=MAX_SPANS)
    trace_count: int = Field(ge=0, le=32)
    resolved_parent_edges: int = Field(ge=0)
    unresolved_parent_edges: int = Field(ge=0)
    null_parents: int = Field(ge=0)
    duplicate_records: int = Field(ge=0)


def summarize_graph(spans: tuple[SourceSpan, ...]) -> GraphSummary:
    """Validate bounded duplicates and cycles without fabricating unobserved parent spans."""
    if len(spans) > MAX_SPANS:
        raise ValueError("aggregate span budget exceeded")
    nodes: dict[tuple[str, str], SourceSpan] = {}
    for span in spans:
        key = (span.trace_id, span.span_id)
        if key in nodes and nodes[key] != span:
            raise EvidenceIntegrityError("conflicting duplicate trace span")
        nodes[key] = span
    for key, span in nodes.items():
        seen = {key}
        current = span
        while current.parent_id is not None:
            parent = (current.trace_id, current.parent_id)
            if parent in seen:
                raise EvidenceIntegrityError("trace parent cycle")
            seen.add(parent)
            if parent not in nodes:
                break
            current = nodes[parent]
    resolved = sum((span.trace_id, span.parent_id) in nodes for span in nodes.values())
    roots = sum(span.parent_id is None for span in nodes.values())
    return GraphSummary(
        span_count=len(nodes),
        trace_count=len({key[0] for key in nodes}),
        resolved_parent_edges=resolved,
        unresolved_parent_edges=len(nodes) - resolved - roots,
        null_parents=roots,
        duplicate_records=len(spans) - len(nodes),
    )


class TraceCollection(Immutable):
    """Expose actual command cost separately from the fixed reservation."""

    sources: tuple[EvidenceItem, ...] = Field(max_length=10)
    spans: tuple[EvidenceItem, ...] = Field(max_length=MAX_SPANS)
    graph: GraphSummary
    commands_used: int = Field(ge=0, le=MAX_COMMANDS)
    reserved_commands: int = Field(ge=0, le=MAX_COMMANDS)
    services_without_pods: tuple[Service, ...] = Field(max_length=5)
    partial_candidates: int = Field(ge=0)
    malformed_candidates: int = Field(ge=0)
    excluded_spans: int = Field(ge=0)
    capped_sources: int = Field(ge=0, le=10)


@dataclass
class _Budget:
    """One collection shares call, source-byte, metadata-byte and monotonic deadline limits."""

    deadline: float
    clock: Callable[[], float]
    commands: int = 0
    metadata_bytes: int = 0
    source_bytes: int = 0

    def remaining(self) -> float:
        """Check elapsed time before dispatch and after bounded parsing or artifact work."""
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise ValueError("trace collection deadline exceeded")
        return min(12.0, remaining)


class TraceRead:
    """Keep the trace command reservation independent of other collector budgets."""

    maximum_commands_per_service = MAX_COMMANDS_PER_SERVICE
    maximum_commands = MAX_COMMANDS

    def __init__(
        self,
        kubeconfig: Path,
        invoke: ReadCommand = bounded_read,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        """Bind trusted host configuration and an explicit local context."""
        executable = shutil.which("kubectl")
        if executable is None or not kubeconfig.is_file():
            raise ValueError("kubectl and explicit kubeconfig required")
        self._prefix = (
            executable,
            "--kubeconfig",
            str(kubeconfig.resolve()),
            "--context",
            "kind-payops-dev",
            "--namespace",
            "payops-sandbox",
            "--request-timeout=8s",
        )
        self._invoke, self._clock = invoke, clock

    def _read(self, args: tuple[str, ...], budget: _Budget, logs: bool = False) -> bytes:
        """Reserve one command before dispatch and account for exact returned source bytes."""
        timeout = budget.remaining()
        if budget.commands >= MAX_COMMANDS:
            raise ValueError("trace command budget exceeded")
        budget.commands += 1
        maximum = MAX_LOG_BYTES if logs else 262144
        raw = self._invoke((*self._prefix, *args), maximum, timeout)
        budget.remaining()
        if len(raw) > maximum:
            raise ValueError("trace response exceeds byte budget")
        if logs:
            budget.source_bytes += len(raw)
        else:
            budget.metadata_bytes += len(raw)
        if budget.source_bytes > MAX_SOURCE_BYTES or budget.metadata_bytes > MAX_METADATA_BYTES:
            raise ValueError("aggregate trace bytes exceeded")
        return raw

    def _get(self, args: tuple[str, ...], budget: _Budget) -> Object:
        """All get kinds and option shapes originate from fixed internal call sites."""
        return JSON_OBJECT.validate_json(self._read(("get", *args, "-o", "json"), budget))

    def _pod(
        self, pod: Object, scope: TraceScope, deployment_uid: str, budget: _Budget
    ) -> TraceLog:
        """Verify ownership and reread exact pod/container identity after reading logs."""
        metadata = _metadata(pod, scope)
        controller = _owner(metadata, "ReplicaSet")
        replica_uid = _text(controller.get("uid"))
        identity = _identity(pod, scope, deployment_uid, replica_uid)
        replica_name = _text(controller.get("name"))
        if not replica_name.startswith(f"{scope.service}-") or not all(
            char.isalnum() or char == "-" for char in replica_name
        ):
            raise ValueError("ReplicaSet name outside service scope")
        replica = self._get(("replicaset", replica_name), budget)
        replica_meta = _metadata(replica, scope, replica_name)
        owner = _owner(replica_meta, "Deployment")
        if (
            replica_meta.get("uid") != replica_uid
            or owner.get("uid") != deployment_uid
            or owner.get("name") != scope.service
        ):
            raise ValueError("ReplicaSet does not belong to selected deployment")
        started = utc_now()
        raw = self._read(
            (
                "logs",
                identity.pod_name,
                "--container=sandbox",
                "--tail=2000",
                f"--since-time={scope.start.isoformat()}",
                "--timestamps=true",
                f"--limit-bytes={MAX_LOG_BYTES}",
            ),
            budget,
            True,
        )
        after = self._get(("pod", identity.pod_name), budget)
        if _identity(after, scope, deployment_uid, replica_uid) != identity:
            raise ValueError("pod or current container changed during trace read")
        return TraceLog(
            scope=scope,
            identity=identity,
            captured_start=started,
            captured_end=utc_now(),
            parsed=parse_console_log(raw, scope),
        )

    def collect(self, scopes: tuple[TraceScope, ...], store: ArtifactStore) -> TraceCollection:
        """Collect up to five service windows under a forty-command reservation."""
        if not 1 <= len(scopes) <= 5 or len({scope.service for scope in scopes}) != len(scopes):
            raise ValueError("unique bounded trace service scopes required")
        if (
            len({(scope.incident_id, scope.namespace, scope.start, scope.end) for scope in scopes})
            != 1
        ):
            raise ValueError("trace collection crosses incident or time scope")
        if scopes[0].end > utc_now():
            raise ValueError("trace incident window ends in the future")
        budget = _Budget(self._clock() + MAX_SECONDS, self._clock)
        captures: list[TraceLog] = []
        for scope in scopes:
            deployment = self._get(("deployment", scope.service), budget)
            deployment_uid = _text(_metadata(deployment, scope, scope.service).get("uid"))
            pods = self._get(("pods", "-l", f"app.kubernetes.io/name={scope.service}"), budget)
            for pod in _items(pods.get("items"), 2):
                captures.append(self._pod(pod, scope, deployment_uid, budget))
        graph = summarize_graph(
            tuple(record.span for capture in captures for record in capture.parsed.spans)
        )
        sources: list[EvidenceItem] = []
        spans: list[EvidenceItem] = []
        for capture in captures:
            budget.remaining()
            source = publish_trace_log(capture, store)
            sources.append(source)
            for ordinal in range(len(capture.parsed.spans)):
                budget.remaining()
                spans.append(derive_trace_span(source, ordinal, store))
        budget.remaining()
        return TraceCollection(
            sources=tuple(sources),
            spans=tuple(spans),
            graph=graph,
            commands_used=budget.commands,
            reserved_commands=len(scopes) * MAX_COMMANDS_PER_SERVICE,
            services_without_pods=tuple(
                scope.service
                for scope in scopes
                if not any(capture.scope.service == scope.service for capture in captures)
            ),
            partial_candidates=sum(capture.parsed.partial_candidates for capture in captures),
            malformed_candidates=sum(capture.parsed.malformed_candidates for capture in captures),
            excluded_spans=sum(capture.parsed.excluded_spans for capture in captures),
            capped_sources=sum(capture.parsed.limit_reached for capture in captures),
        )
