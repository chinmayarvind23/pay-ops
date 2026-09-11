"""Scope, ownership, command budgets and real bounded subprocess behavior remain independent."""

import io
import json
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.trace_span import ROLES, SourceSpan, TraceScope, verify_trace_span
from payops.tools.traces import (
    WINDOWS_JOB_GUARDIAN,
    TraceRead,
    bounded_read,
    drain_pipe,
    stop_owned_process,
    summarize_graph,
)

AT = datetime(2024, 1, 1, tzinfo=UTC)


def scope() -> TraceScope:
    """An old bounded interval is a valid diagnostic request without becoming fresh evidence."""
    return TraceScope(
        incident_id="incident", service="payments-api", start=AT, end=AT + timedelta(seconds=10)
    )


def source_span(identifier: str = "2", parent: str | None = None) -> SourceSpan:
    """Graph inputs already passed the bounded source parser and emitting-service check."""
    return SourceSpan(
        name="sandbox.payments",
        trace_id="0x" + "1" * 32,
        span_id="0x" + identifier * 16,
        parent_id="0x" + parent * 16 if parent else None,
        kind="SpanKind.SERVER",
        start_time=AT,
        end_time=AT + timedelta(seconds=1),
        status_code="UNSET",
        service_name="payops-sandbox-payments",
    )


class Backend:
    """A strict fake accepts only the adapter's fixed sequence of named resource reads."""

    def __init__(self, replicas: int = 1) -> None:
        """Retain calls so denials and fixed reservation cost can be checked directly."""
        self.calls: list[tuple[str, ...]] = []
        self.replicas = replicas
        self.change = ""
        self.elapsed = 0.0

    def metadata(self, name: str, uid: str, owner: str | None = None) -> dict[str, Any]:
        """Every workload object carries the same namespace/service labels and controlling chain."""
        value: dict[str, Any] = {
            "name": name,
            "uid": uid,
            "namespace": "payops-sandbox",
            "labels": {"app.kubernetes.io/name": "payments-api"},
        }
        if owner:
            value["ownerReferences"] = [
                {
                    "apiVersion": "apps/v1",
                    "kind": owner,
                    "controller": True,
                    "name": "payments-api-rs" if owner == "ReplicaSet" else "payments-api",
                    "uid": "rs-uid" if owner == "ReplicaSet" else "deployment-uid",
                }
            ]
        return value

    def pod(self, ordinal: int) -> dict[str, Any]:
        """Distinct pod/container identities allow a real two-pod rollout-shaped sample."""
        return {
            "metadata": self.metadata(f"payments-api-rs-{ordinal}", f"pod-{ordinal}", "ReplicaSet"),
            "status": {
                "containerStatuses": [
                    {"name": "sandbox", "restartCount": 0, "containerID": f"containerd://{ordinal}"}
                ]
            },
        }

    def __call__(self, args: tuple[str, ...], maximum: int, timeout: float) -> bytes:
        """Return only synthetic response bytes; no Kubernetes command or effect is executed."""
        assert args[3:8] == (
            "--context",
            "kind-payops-dev",
            "--namespace",
            "payops-sandbox",
            "--request-timeout=8s",
        )
        assert 0 < timeout <= 12
        command = args[8:]
        self.calls.append(command)
        self.elapsed += 10 if self.change == "slow" else 0
        if self.change == "oversize":
            return b"x" * (maximum + 1)
        if command[0] == "logs":
            assert command[2:] == (
                "--container=sandbox",
                "--tail=2000",
                "--since-time=2024-01-01T00:00:00+00:00",
                "--timestamps=true",
                "--limit-bytes=131072",
            )
            if self.change == "malformed_log":
                return b"2024-01-01T00:00:12Z {\n"
            span = source_span()
            value = {
                "name": span.name,
                "context": {"trace_id": span.trace_id, "span_id": span.span_id},
                "parent_id": None,
                "kind": span.kind,
                "start_time": span.start_time.isoformat(),
                "end_time": span.end_time.isoformat(),
                "status": {"status_code": "UNSET"},
                "resource": {"attributes": {"service.name": span.service_name}},
            }
            return "".join(
                f"2024-01-01T00:00:12Z {line}\n"
                for line in json.dumps(value, indent=4).splitlines()
            ).encode()
        assert command[0] == "get" and command[-2:] == ("-o", "json")
        kind = command[1]
        value = self.response(kind, command)
        changes: dict[str, dict[str, tuple[tuple[str | int, ...], Any]]] = {
            "deployment": {
                "namespace": (("metadata", "namespace"), "foreign"),
                "missing_uid": (("metadata", "uid"), None),
            },
            "pods": {
                "pod_label": (("items", 0, "metadata", "labels"), {}),
                "bad_owner": (("items", 0, "metadata", "ownerReferences", 0, "controller"), False),
                "owner_injection": (
                    ("items", 0, "metadata", "ownerReferences", 0, "name"),
                    "--all-namespaces",
                ),
                "container": (("items", 0, "status", "containerStatuses"), []),
                "malformed_items": (("items",), None),
            },
            "replicaset": {
                "foreign_deployment": (("metadata", "ownerReferences", 0, "uid"), "foreign")
            },
            "pod": {
                "restart": (("status", "containerStatuses", 0, "restartCount"), 1),
                "uid": (("metadata", "uid"), "replacement"),
                "controller": (("metadata", "ownerReferences", 0, "uid"), "replacement"),
            },
        }
        if self.change in changes[kind]:
            path, replacement = changes[kind][self.change]
            target: Any = value
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = replacement
        if self.change == "metadata_budget":
            value["ignored_padding"] = "x" * 250000
        return json.dumps(value).encode()

    def response(self, kind: str, command: tuple[str, ...]) -> dict[str, Any]:
        """Create an untouched response before applying the selected adversarial variation."""
        if kind == "deployment":
            return {"metadata": self.metadata("payments-api", "deployment-uid")}
        if kind == "pods":
            return {"items": [self.pod(i) for i in range(self.replicas)]}
        if kind == "replicaset":
            return {"metadata": self.metadata("payments-api-rs", "rs-uid", "Deployment")}
        assert kind == "pod"
        return self.pod(int(command[2].split("-")[-1]))


def installed(name: str) -> str:
    """Use an explicit fixture executable instead of discovering real Kubernetes tools."""
    return "kubectl-fixture"


def missing(name: str) -> None:
    """Simulate an operator setup with no configured Kubernetes executable."""
    return None


def reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backend: Backend) -> TraceRead:
    """Bind a private fixture config and callable without modifying a real kubeconfig."""
    config = tmp_path / "kubeconfig"
    config.write_text("fixture config")
    monkeypatch.setattr("payops.tools.traces.shutil.which", installed)
    return TraceRead(config, backend, lambda: backend.elapsed)


@pytest.mark.parametrize("replicas,calls", [(0, 2), (1, 5), (2, 8)])
def test_exact_read_cost_and_reverified_source_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replicas: int, calls: int
) -> None:
    """A registry can reserve8 calls per service while receipts expose the actual complete cost."""
    backend = Backend(replicas)
    store = ArtifactStore(tmp_path / "artifacts")
    result = reader(tmp_path, monkeypatch, backend).collect((scope(),), store)
    assert len(backend.calls) == result.commands_used == calls
    assert result.reserved_commands == TraceRead.maximum_commands_per_service == 8
    assert TraceRead.maximum_commands == 40
    assert len(result.sources) == len(result.spans) == replicas
    assert result.services_without_pods == (("payments-api",) if replicas == 0 else ())
    assert result.graph.duplicate_records == max(0, replicas - 1)
    for item in result.spans:
        assert verify_trace_span(item, store).duration_microseconds == 1000000


@pytest.mark.parametrize(
    "change",
    [
        "namespace",
        "missing_uid",
        "pod_label",
        "bad_owner",
        "owner_injection",
        "container",
        "malformed_items",
        "foreign_deployment",
        "restart",
        "uid",
        "controller",
        "oversize",
        "slow",
    ],
)
def test_scope_races_and_source_errors_never_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """Changed ownership or incomplete read work invalidates the whole unpublished collection."""
    backend = Backend()
    backend.change = change
    store = ArtifactStore(tmp_path / "artifacts")
    with pytest.raises(ValueError):
        reader(tmp_path, monkeypatch, backend).collect((scope(),), store)
    assert not list(store.root.glob("*.json"))
    if change == "owner_injection":
        assert all("replicaset" not in command for command in backend.calls)


def test_excess_pods_and_duplicate_scopes_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The model cannot multiply backend work by repeating a service or selecting rollout fanout."""
    backend = Backend(3)
    read = reader(tmp_path, monkeypatch, backend)
    store = ArtifactStore(tmp_path / "artifacts")
    for scopes in (
        (),
        (scope(), scope()),
        (scope(), scope().model_copy(update={"incident_id": "other", "service": "risk-sim"})),
    ):
        with pytest.raises(ValueError):
            read.collect(scopes, store)
        assert not backend.calls
    with pytest.raises(ValueError):
        read.collect((scope(),), store)
    assert len(backend.calls) == 2


def test_partial_log_does_not_become_empty_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clipped object produces source incompleteness and no successful trace span."""
    backend = Backend()
    backend.change = "malformed_log"
    store = ArtifactStore(tmp_path / "artifacts")
    result = reader(tmp_path, monkeypatch, backend).collect((scope(),), store)
    assert not result.spans
    payload: Any = store.verify(result.sources[0])["payload"]
    assert payload["parsed"]["partial_candidates"] == 1
    assert payload["parsed"]["sampling"] == "bounded_sample"


def test_configuration_is_explicit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing operator configuration cannot fall back to the user's default cluster context."""
    monkeypatch.setattr("payops.tools.traces.shutil.which", missing)
    with pytest.raises(ValueError):
        TraceRead(tmp_path / "absent")
    monkeypatch.setattr("payops.tools.traces.shutil.which", installed)
    with pytest.raises(ValueError):
        TraceRead(tmp_path / "absent")


def test_graph_preserves_missing_parents_and_rejects_conflicts_and_cycles() -> None:
    """A bounded graph may have external parents; conflicting IDs and cycles are invalid."""
    root, child, orphan = source_span(), source_span("3", "2"), source_span("4", "5")
    summary = summarize_graph((root, child, orphan, root))
    assert (
        summary.span_count,
        summary.resolved_parent_edges,
        summary.unresolved_parent_edges,
        summary.null_parents,
        summary.duplicate_records,
    ) == (3, 1, 1, 1, 1)
    with pytest.raises(EvidenceIntegrityError, match="duplicate"):
        summarize_graph((root, root.model_copy(update={"status_code": "ERROR"})))
    with pytest.raises(EvidenceIntegrityError, match="cycle"):
        summarize_graph((source_span("2", "3"), source_span("3", "2")))
    with pytest.raises(ValueError, match="budget"):
        summarize_graph((root,) * 129)
    with pytest.raises(ValueError):
        summarize_graph(
            tuple(root.model_copy(update={"trace_id": f"0x{i:032x}"}) for i in range(1, 34))
        )


@pytest.mark.parametrize("maximum,timeout", [(0, 1), (262145, 1), (10, 0), (10, 13)])
def test_process_budget_validation(maximum: int, timeout: float) -> None:
    """Invalid transport limits fail before any subprocess is started."""
    with pytest.raises(ValueError, match="budget"):
        bounded_read(("missing",), maximum, timeout)


def test_real_subprocess_reads_stdout_and_bounds_failures() -> None:
    """Actual local Python pipes exercise EOF, stderr, large output and deadline termination."""
    expected = b"fixture\r\n" if sys.platform == "win32" else b"fixture\n"
    assert bounded_read((sys.executable, "-c", "print('fixture')"), 100, 3) == expected
    for code, limit, timeout in (
        ("print('x'*20000)", 10, 3),
        ("import sys; sys.stderr.write('x'*20000)", 100, 3),
        ("import sys; sys.stderr.write('sensitive fixture'); sys.exit(1)", 100, 3),
        ("import time; time.sleep(30)", 100, 0.2),
    ):
        with pytest.raises(ValueError, match="trace subprocess failed") as error:
            bounded_read((sys.executable, "-c", code), limit, timeout)
        assert "sensitive" not in str(error.value)


def test_aggregate_metadata_and_reserved_command_budgets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Individually valid responses cannot consume unbounded cumulative bytes or command slots."""
    backend = Backend(2)
    backend.change = "metadata_budget"
    store = ArtifactStore(tmp_path / "artifacts")
    with pytest.raises(ValueError, match="aggregate trace bytes"):
        reader(tmp_path, monkeypatch, backend).collect((scope(),), store)
    assert not list(store.root.glob("*.json"))
    backend = Backend()
    monkeypatch.setattr("payops.tools.traces.MAX_COMMANDS", 4)
    with pytest.raises(ValueError, match="command budget"):
        reader(tmp_path, monkeypatch, backend).collect((scope(),), store)
    assert len(backend.calls) == 4


def test_future_window_is_rejected_without_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The time scope must already be observable before any source query starts."""
    backend = Backend()
    future = datetime.now(UTC) + timedelta(days=1)
    request = TraceScope(
        incident_id="incident",
        service="payments-api",
        start=future,
        end=future + timedelta(seconds=10),
    )
    with pytest.raises(ValueError, match="future"):
        reader(tmp_path, monkeypatch, backend).collect(
            (request,), ArtifactStore(tmp_path / "artifacts")
        )
    assert not backend.calls


def test_nonobject_source_is_not_empty_telemetry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An invalid object must fail before absent labels could be mistaken for an empty result."""

    class Broken(Backend):
        """Return a syntactically valid JSON envelope with a malformed metadata member."""

        def response(self, kind: str, command: tuple[str, ...]) -> dict[str, Any]:
            """The top-level decoder accepts JSON, but scoped object validation must reject it."""
            return {"metadata": []}

    with pytest.raises(ValueError, match="object"):
        reader(tmp_path, monkeypatch, Broken()).collect(
            (scope(),), ArtifactStore(tmp_path / "artifacts")
        )


def test_five_services_and_two_pods_use_exact_forty_command_ceiling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The published maximum includes every ownership lookup and after-read identity check."""

    class Fleet(Backend):
        """Reuse response shapes across services and retain exact command receipts."""

        active_service = "payments-api"

        def __call__(self, args: tuple[str, ...], maximum: int, timeout: float) -> bytes:
            """Map fixture names to the selected service and return explicit empty logs."""
            if args[8:10] == ("get", "deployment"):
                self.active_service = args[10]
            result = super().__call__(args, maximum, timeout)
            if args[8] == "logs":
                return b""
            return result.replace(b"payments-api", self.active_service.encode())

    backend = Fleet(2)
    scopes = tuple(
        TraceScope(
            incident_id="incident", service=service, start=AT, end=AT + timedelta(seconds=10)
        )
        for service in ROLES
    )
    result = reader(tmp_path, monkeypatch, backend).collect(
        scopes, ArtifactStore(tmp_path / "artifacts")
    )
    assert result.commands_used == result.reserved_commands == len(backend.calls) == 40
    assert len(result.sources) == 10 and not result.spans
    assert not result.services_without_pods
    assert result.graph.sampling == "bounded_sample"


def test_pipe_io_failure_sets_collection_failure() -> None:
    """An OS read failure cannot become a complete short or empty log response."""

    class Broken(io.BytesIO):
        """Simulate a native pipe failure before any complete source bytes are returned."""

        def read(self, size: int | None = -1) -> bytes:
            """Report transport failure rather than clean EOF."""
            raise OSError("synthetic pipe failure")

    failed = threading.Event()
    output = bytearray()
    drain_pipe(Broken(), output, 10, failed)
    assert failed.is_set() and not output


@pytest.mark.parametrize("interruption_at", [1, 2])
def test_owner_interruption_reaps_the_real_child(
    monkeypatch: pytest.MonkeyPatch, interruption_at: int
) -> None:
    """Host interruption must terminate the owned process before propagating cancellation."""
    original = subprocess.Popen
    children: list[subprocess.Popen[bytes]] = []

    def tracked(args: tuple[str, ...], **kwargs: Any) -> subprocess.Popen[bytes]:
        """Retain the OS process handle for an actual post-cancellation termination check."""
        child = cast("subprocess.Popen[bytes]", original(args, **kwargs))
        children.append(child)
        return child

    calls = 0

    def interrupted_clock() -> float:
        """Interrupt after the process and its pipe readers have started."""
        nonlocal calls
        calls += 1
        if calls == interruption_at:
            raise KeyboardInterrupt
        return 0.0

    monkeypatch.setattr("payops.tools.traces.subprocess.Popen", tracked)
    monkeypatch.setattr("payops.tools.traces.monotonic", interrupted_clock)
    with pytest.raises(KeyboardInterrupt):
        bounded_read((sys.executable, "-c", "import time; time.sleep(30)"), 100, 3)
    assert len(children) == 1 and children[0].poll() is not None


DESCENDANT_PROBE = r"""
import csv,json,subprocess,sys,time
from pathlib import Path
from payops.tools.traces import bounded_read
from payops.tools import traces
if len(sys.argv)>3:
    traces.WINDOWS_JOB_GUARDIAN=sys.argv[3]
program=("import subprocess,sys;from pathlib import Path;"
         "c=subprocess.Popen([sys.executable,'-c','import time;time.sleep(8)']);"
         "Path(sys.argv[1]).write_text(str(c.pid))")
if len(sys.argv)>2 and sys.argv[2]=="timeout":
    program += ";import time;time.sleep(8)"
started=time.monotonic()
try:
    bounded_read((sys.executable,"-c",program,sys.argv[1]),100,2)
    outcome="complete"
except ValueError:
    outcome="bounded-failure"
elapsed=time.monotonic()-started
pid=int(Path(sys.argv[1]).read_text())
status=subprocess.run(["tasklist","/FI",f"PID eq {pid}","/FO","CSV","/NH"],
                      capture_output=True,text=True,timeout=3,check=True)
alive=any(len(row)>1 and row[1]==str(pid) for row in csv.reader(status.stdout.splitlines()))
print(json.dumps({"elapsed":elapsed,"descendant_pid":pid,
                  "descendant_reaped":not alive,"outcome":outcome}))
assert elapsed<5 and not alive
"""


@pytest.mark.skipif(sys.platform != "win32", reason="Windows Job Object process-tree acceptance")
@pytest.mark.parametrize("mode", ["early_exit", "timeout"])
def test_real_descendant_inherited_pipes_are_reaped_with_outer_watchdog(
    tmp_path: Path,
    mode: str,
) -> None:
    """An actual eight-second descendant cannot retain inherited pipes after its parent exits."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            DESCENDANT_PROBE,
            str(tmp_path / "descendant.pid"),
            mode,
            WINDOWS_JOB_GUARDIAN,
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads(result.stdout)
    assert receipt["descendant_reaped"] and receipt["elapsed"] < 5


@pytest.mark.skipif(sys.platform != "win32", reason="Windows Job Object setup failure")
def test_job_setup_failure_prevents_command_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Job setup failure must not fall back to an unowned child."""
    failed = WINDOWS_JOB_GUARDIAN.replace(
        "win32job.AssignProcessToJobObject(job,win32api.GetCurrentProcess())",
        "raise OSError('synthetic job assignment failure')",
    )
    monkeypatch.setattr("payops.tools.traces.WINDOWS_JOB_GUARDIAN", failed)
    marker = tmp_path / "command-started"
    program = "import sys;from pathlib import Path;Path(sys.argv[1]).write_text('started')"
    with pytest.raises(ValueError, match="subprocess failed"):
        bounded_read((sys.executable, "-c", program, str(marker)), 100, 3)
    assert not marker.exists()


@pytest.mark.parametrize("missing_group", [False, True])
def test_posix_cleanup_targets_only_the_owned_process_group(
    monkeypatch: pytest.MonkeyPatch,
    missing_group: bool,
) -> None:
    """Verify POSIX group selection with mocks, without claiming live POSIX teardown."""

    class Child:
        """Only the new-session leader PID is used by the POSIX cleanup branch."""

        pid = 23456

    calls: list[tuple[int, int]] = []

    def kill_group(pid: int, number: int) -> None:
        """Record the exact group and signal without touching any actual process."""
        calls.append((pid, number))
        if missing_group:
            raise ProcessLookupError

    with monkeypatch.context() as patch:
        patch.setattr("payops.tools.traces.sys.platform", "linux")
        patch.setattr("payops.tools.traces.os.killpg", kill_group, raising=False)
        patch.setattr("payops.tools.traces.signal.SIGKILL", 9, raising=False)
        stop_owned_process(cast(subprocess.Popen[bytes], Child()))
    assert calls == [(23456, 9)]


def test_second_pipe_thread_start_failure_reaps_child(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failure partway through startup must reap the child without joining an unstarted thread."""
    original_spawn, original_start = subprocess.Popen, threading.Thread.start
    children: list[subprocess.Popen[bytes]] = []
    starts = 0

    def tracked(args: tuple[str, ...], **kwargs: Any) -> subprocess.Popen[bytes]:
        """Retain a real process handle for termination verification after thread startup fails."""
        child = cast("subprocess.Popen[bytes]", original_spawn(args, **kwargs))
        children.append(child)
        return child

    def fail_second(reader: threading.Thread) -> None:
        """Allow the first pipe reader to run, then emulate a thread resource failure."""
        nonlocal starts
        starts += 1
        if starts == 2:
            raise RuntimeError("synthetic thread start failure")
        original_start(reader)

    monkeypatch.setattr("payops.tools.traces.subprocess.Popen", tracked)
    monkeypatch.setattr("payops.tools.traces.threading.Thread.start", fail_second)
    with pytest.raises(RuntimeError, match="thread start"):
        bounded_read((sys.executable, "-c", "import time; time.sleep(30)"), 100, 3)
    assert starts == 2 and len(children) == 1 and children[0].poll() is not None
