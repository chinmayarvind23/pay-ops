"""Real local stdio fixtures verify SDK transport behavior, never live GKE access."""

import asyncio
import csv
import json
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

from payops.tools.mcp_transport import OperatorStdioConfig, connect_mcp

# A deliberately small protocol peer makes malformed frames and pagination reproducible.
# It has no cloud client, credentials, network listener, or operational Kubernetes handler.
SERVER = r'''
import json
import os
import sys
import time
from pathlib import Path
import subprocess

mode, journal = sys.argv[1:]
Path(journal + ".pid").write_text(str(os.getpid()))
def send(identifier, result):
    """Emit one MCP JSON-RPC result using standard newline framing."""
    print(json.dumps({"jsonrpc": "2.0", "id": identifier, "result": result}), flush=True)

for line in sys.stdin:
    request = json.loads(line)
    with Path(journal).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(request) + "\n")
    method = request.get("method")
    if "id" not in request:
        continue
    identifier = request["id"]
    if method == "initialize":
        if mode == "init_timeout":
            time.sleep(60)
        version = "2025-03-26" if mode == "old_protocol" else "2025-11-25"
        send(identifier, {"protocolVersion": version, "capabilities": {"tools": {}},
                          "serverInfo": {"name": "payops-stdio-fixture", "version": "1"}})
    elif method == "tools/list":
        if mode == "list_rpc_error":
            print(json.dumps({"jsonrpc": "2.0", "id": identifier,
                              "error": {"code": -32603, "message": "fixture failure"}}), flush=True)
            continue
        cursor = request.get("params", {}).get("cursor")
        name = "get_k8s_logs" if cursor else "get_k8s_resource"
        tool = {"name": name, "description": "Local fixture", "inputSchema": {"type": "object"}}
        result = {"tools": [tool]}
        if mode == "pages" and not cursor:
            result["nextCursor"] = "page-two"
        if mode == "cycle":
            result["nextCursor"] = "same"
        if mode == "many_pages":
            result["nextCursor"] = str(int(cursor or "0") + 1)
        if mode == "many_tools":
            result["tools"] = [dict(tool, name=f"tool-{i}") for i in range(257)]
        if mode == "catalog_bytes":
            result["tools"][0]["description"] = "x" * 150000
            if not cursor:
                result["nextCursor"] = "page-two"
        if mode == "long_cursor":
            result["nextCursor"] = "x" * 1025
        if mode == "bad_catalog":
            result["tools"] = "invalid"
        send(identifier, result)
    elif method == "tools/call":
        if mode == "callbacks":
            requests = [
                ("sampling/createMessage", {"messages": [{"role": "user", "content": {
                    "type": "text", "text": "synthetic request"}}], "maxTokens": 1}),
                ("elicitation/create", {"message": "synthetic request", "requestedSchema": {
                    "type": "object", "properties": {"answer": {"type": "string"}}}}),
                ("roots/list", {}),
            ]
            for callback_id, (callback, parameters) in enumerate(requests, start=900):
                print(json.dumps({"jsonrpc": "2.0", "id": callback_id,
                                  "method": callback, "params": parameters}), flush=True)
            for _ in requests:
                reply = sys.stdin.readline()
                with Path(journal).open("a", encoding="utf-8") as stream:
                    stream.write(reply)
        if mode == "child":
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            Path(journal + ".child.pid").write_text(str(child.pid))
        if mode == "timeout":
            time.sleep(60)
        if mode == "oversize":
            sys.stdout.write("x" * 300000)
            sys.stdout.flush()
            time.sleep(60)
        if mode == "invalid":
            print("not json", flush=True)
            continue
        if mode == "duplicate":
            print('{"jsonrpc":"2.0","id":' + str(identifier) +
                  ',"result":{"content":[],"isError":false,"isError":true}}', flush=True)
            continue
        if mode == "nan":
            print('{"jsonrpc":"2.0","id":' + str(identifier) +
                  ',"result":{"content":[],"unexpected":NaN}}', flush=True)
            continue
        if mode == "partial":
            sys.stdout.write('{"jsonrpc":')
            sys.stdout.flush()
            break
        if mode == "many_messages":
            for _ in range(1025):
                print('{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}', flush=True)
        if mode == "bad_result":
            send(identifier, {"content": 3})
            continue
        if mode == "coercion":
            send(identifier, {"content": [{"type": "text", "text": "fixture output"}],
                              "isError": "false", "structuredContent": None})
            continue
        if mode == "rpc_error":
            print(json.dumps({"jsonrpc": "2.0", "id": identifier,
                              "error": {"code": -32603, "message": "fixture failure"}}), flush=True)
            continue
        text = "fixture output"
        if mode == "result_bytes":
            text = "x" * 263000
        if mode == "session_bytes":
            text = "x" * 250000
        if mode == "env":
            text = json.dumps({"sentinel_present": "PAYOPS_PARENT_SECRET" in os.environ,
                               "explicit_present": "PAYOPS_FIXTURE_ONLY" in os.environ})
        send(identifier, {"content": [{"type": "text", "text": text}],
                          "isError": mode == "tool_error"})
'''

SDK_SERVER = '''
from mcp.server import MCPServer

server = MCPServer("payops-official-sdk-fixture", version="1")

@server.tool()
def get_k8s_logs() -> str:
    """Return synthetic text without any Kubernetes or cloud dependency."""
    return "official SDK fixture output"

server.run(transport="stdio")
'''


def fixture_config(tmp_path: Path, mode: str = "normal") -> OperatorStdioConfig:
    """Bind every subprocess input before the adapter receives model requests."""
    server = tmp_path / "server.py"
    server.write_text(SERVER, encoding="utf-8")
    environment = (("SystemRoot", os.environ.get("SystemRoot", "")),) if os.name == "nt" else ()
    return OperatorStdioConfig(
        executable=Path(sys.executable),
        arguments=("-I", str(server), mode, str(tmp_path / "journal.jsonl")),
        cwd=tmp_path,
        environment=(*environment, ("PAYOPS_FIXTURE_ONLY", "yes")),
        timeout_seconds=0.5 if "timeout" in mode else 2.0,
    )


def journal(tmp_path: Path) -> list[str]:
    """Read only fixture method names to prove which requests reached the subprocess."""
    lines = (tmp_path / "journal.jsonl").read_text().splitlines()
    return [json.loads(line)["method"] for line in lines]


def assert_reaped(tmp_path: Path, suffix: str = ".pid") -> None:
    """Check the recorded fixture PID after shutdown rather than assuming context exit killed it."""
    pid = int((tmp_path / ("journal.jsonl" + suffix)).read_text())
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW,
            check=True,
        )
        assert all(
            len(row) < 2 or row[1] != str(pid) for row in csv.reader(result.stdout.splitlines())
        )
    else:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_real_stdio_handshake_pages_and_aliases(tmp_path: Path) -> None:
    """The current SDK negotiates a real peer and preserves upstream wire field names."""

    async def run() -> None:
        """Keep SDK lifecycle and its structured concurrency scopes on one task."""
        async with connect_mcp(fixture_config(tmp_path, "pages")) as transport:
            assert transport.session_info.server_name == "payops-stdio-fixture"
            assert transport.session_info.protocol_version == "2025-11-25"
            catalog = await transport.list_tools()
            assert [tool["name"] for tool in catalog] == ["get_k8s_resource", "get_k8s_logs"]
            assert "inputSchema" in catalog[0]
            reply = await transport.call_tool("get_k8s_resource", {"name": "payments-api"})
            assert reply == {
                "content": [{"type": "text", "text": "fixture output"}],
                "isError": False,
            }
            assert "structuredContent" not in reply
        with pytest.raises(OSError, match="closed"):
            await transport.list_tools()

    asyncio.run(run())
    assert journal(tmp_path) == [
        "initialize",
        "notifications/initialized",
        "tools/list",
        "tools/list",
        "tools/call",
    ]


def test_official_sdk_stdio_server(tmp_path: Path) -> None:
    """The transport also interoperates with the actual SDK server, beyond the adversarial peer."""
    config = fixture_config(tmp_path)
    server = tmp_path / "official_server.py"
    server.write_text(SDK_SERVER, encoding="utf-8")
    config = replace(config, arguments=("-I", str(server)), timeout_seconds=5)

    async def run() -> None:
        """Use the current SDK's MCPServer and Client over real process pipes."""
        async with connect_mcp(config) as transport:
            assert transport.session_info.server_name == "payops-official-sdk-fixture"
            assert (await transport.list_tools())[0]["name"] == "get_k8s_logs"
            result = await transport.call_tool("get_k8s_logs", {})
            assert "official SDK fixture output" in str(result["content"])

    asyncio.run(run())


def test_server_initiated_capabilities_are_denied(tmp_path: Path) -> None:
    """A peer cannot trigger sampling, elicitation or filesystem-root discovery through defaults."""

    async def run() -> None:
        """The adversarial peer sends three valid reverse requests while a tool call is pending."""
        async with connect_mcp(fixture_config(tmp_path, "callbacks")) as transport:
            await transport.call_tool("get_k8s_logs", {})

    asyncio.run(run())
    frames = [json.loads(line) for line in (tmp_path / "journal.jsonl").read_text().splitlines()]
    assert frames[0]["params"]["capabilities"] == {}
    replies = {frame["id"]: frame for frame in frames if frame.get("id") in (900, 901, 902)}
    assert set(replies) == {900, 901, 902}
    for response in replies.values():
        assert "result" not in response and response["error"]["code"] == -32600
    assert_reaped(tmp_path)


@pytest.mark.parametrize(
    "mode",
    [
        "cycle",
        "many_pages",
        "many_tools",
        "catalog_bytes",
        "long_cursor",
        "bad_catalog",
        "list_rpc_error",
    ],
)
def test_catalog_bounds(tmp_path: Path, mode: str) -> None:
    """Repeated cursors, excessive pages and excessive tools fail without operational dispatch."""

    async def run() -> None:
        """Exercise pagination through actual SDK requests."""
        async with connect_mcp(fixture_config(tmp_path, mode)) as transport:
            with pytest.raises(OSError):
                await transport.list_tools()

    asyncio.run(run())
    assert "tools/call" not in journal(tmp_path)
    assert journal(tmp_path).count("tools/list") <= 16


@pytest.mark.parametrize(
    "mode",
    [
        "oversize",
        "invalid",
        "partial",
        "rpc_error",
        "result_bytes",
        "many_messages",
        "bad_result",
        "duplicate",
        "nan",
    ],
)
def test_bad_wire_is_unavailable(tmp_path: Path, mode: str) -> None:
    """A nonterminated oversize frame is stopped before the SDK JSON parser receives it."""

    async def run() -> None:
        """Read each adversarial response through the real child pipe."""
        async with connect_mcp(fixture_config(tmp_path, mode)) as transport:
            with pytest.raises(OSError) as failure:
                await transport.call_tool("get_k8s_logs", {})
            assert not isinstance(failure.value, TimeoutError)

    asyncio.run(run())
    assert_reaped(tmp_path)


@pytest.mark.parametrize("mode", ["timeout", "init_timeout"])
def test_deadlines(tmp_path: Path, mode: str) -> None:
    """Handshake and operation waits are bounded even when a server stops reading stdin."""

    async def run() -> None:
        """Assert a timeout survives context teardown with its original meaning."""
        with pytest.raises(TimeoutError):
            async with connect_mcp(fixture_config(tmp_path, mode)) as transport:
                await transport.call_tool("get_k8s_logs", {})

    start = time.monotonic()
    asyncio.run(run())
    assert time.monotonic() - start < 5
    assert_reaped(tmp_path)


def test_environment_and_unapproved_tool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The child gets only operator-specified environment and cannot dispatch raw mutation names."""
    monkeypatch.setenv("PAYOPS_PARENT_SECRET", "synthetic-never-inherit")

    async def run() -> None:
        """Pass only a synthetic variable and inspect booleans returned by the child."""
        async with connect_mcp(fixture_config(tmp_path, "env")) as transport:
            with pytest.raises(ValueError, match="approved"):
                await transport.call_tool("apply_k8s_manifest", {})
            reply = await transport.call_tool("get_k8s_logs", {})
            assert "false" in str(reply) and "true" in str(reply)

    asyncio.run(run())
    assert_reaped(tmp_path)
    assert journal(tmp_path).count("tools/call") == 1


def test_tool_error_preserved(tmp_path: Path) -> None:
    """MCP tool errors stay typed results for the adapter's UPSTREAM_ERROR classification."""

    async def run() -> None:
        """Keep isError distinct from JSON-RPC transport failure."""
        async with connect_mcp(fixture_config(tmp_path, "tool_error")) as transport:
            assert (await transport.call_tool("get_k8s_logs", {}))["isError"] is True

    asyncio.run(run())


def test_raw_result_contract_preserved(tmp_path: Path) -> None:
    """The SDK does not coerce malformed tool fields before the strict adapter sees them."""

    async def run() -> None:
        """Retain explicit nulls and wrong scalar types as inert JSON for adapter validation."""
        async with connect_mcp(fixture_config(tmp_path, "coercion")) as transport:
            result = await transport.call_tool("get_k8s_logs", {})
            assert result["isError"] == "false" and "structuredContent" in result

    asyncio.run(run())
    assert journal(tmp_path).count("tools/list") == 0


def test_cancelled_call_propagates(tmp_path: Path) -> None:
    """Caller cancellation is never turned into an apparently successful empty response."""

    async def run() -> None:
        """Cancel only an operation task while the owning task controls context teardown."""
        async with connect_mcp(fixture_config(tmp_path, "timeout")) as transport:
            task = asyncio.create_task(transport.call_tool("get_k8s_logs", {}))
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_config_rejects_path_lookup(tmp_path: Path) -> None:
    """A trusted configuration still requires explicit files, never PATH or shell resolution."""
    with pytest.raises(ValueError, match="absolute"):
        OperatorStdioConfig(executable=Path("python"), cwd=tmp_path)


def test_descendant_shutdown(tmp_path: Path) -> None:
    """A child that inherits stdout cannot survive a normally completed host session."""

    async def run() -> None:
        """Spawn descendants only after initialization, once SDK process-group binding exists."""
        async with connect_mcp(fixture_config(tmp_path, "child")) as transport:
            await transport.call_tool("get_k8s_logs", {})

    asyncio.run(run())
    assert_reaped(tmp_path)
    assert_reaped(tmp_path, ".child.pid")


def test_owner_cancellation_reaps_server(tmp_path: Path) -> None:
    """Cancelling the complete session task still closes the child process tree."""

    async def run() -> None:
        """Cancel after initialization so the test exercises lifecycle cleanup, not spawn timing."""
        ready = asyncio.Event()

        async def owner() -> None:
            """Keep the structured SDK session and its close operation in the same task."""
            async with connect_mcp(fixture_config(tmp_path, "timeout")) as transport:
                ready.set()
                await transport.call_tool("get_k8s_logs", {})

        task = asyncio.create_task(owner())
        await ready.wait()
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert_reaped(tmp_path)


@pytest.mark.skipif(os.name != "nt", reason="Windows script suffix execution boundary")
def test_windows_shell_wrapper_rejected(tmp_path: Path) -> None:
    """An existing batch wrapper must not reach Windows command resolution."""
    wrapper = tmp_path / "server.cmd"
    wrapper.write_text("@echo synthetic wrapper", encoding="utf-8")
    with pytest.raises(ValueError, match="exe"):
        replace(fixture_config(tmp_path), executable=wrapper)
    assert not (tmp_path / "journal.jsonl.pid").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("executable", Path("C:/missing.exe") if os.name == "nt" else Path("/missing")),
        ("timeout_seconds", 0),
        ("timeout_seconds", 31),
        ("arguments", ("x",) * 33),
        ("arguments", ("bad\0arg",)),
        ("arguments", ("x" * 4097,)),
        ("environment", (("DUPLICATE", "a"), ("DUPLICATE", "b"))),
        ("environment", (("bad=name", "a"),)),
        ("environment", (("NAME", "bad\0value"),)),
        ("environment", (("NAME", "x" * 8193),)),
    ],
)
def test_invalid_operator_config(tmp_path: Path, field: str, value: object) -> None:
    """Malformed configuration fails at construction, before the server's PID file exists."""
    with pytest.raises(ValueError):
        replace(fixture_config(tmp_path), **{field: value})
    assert not (tmp_path / "journal.jsonl.pid").exists()


def test_protocol_downgrade_rejected(tmp_path: Path) -> None:
    """An otherwise valid handshake cannot silently change the verified protocol contract."""

    async def run() -> None:
        """Reject initialization metadata before any catalog or operational call."""
        with pytest.raises(OSError, match="unsupported"):
            async with connect_mcp(fixture_config(tmp_path, "old_protocol")):
                pytest.fail("downgraded session was exposed")

    asyncio.run(run())
    assert "tools/call" not in journal(tmp_path)
    assert_reaped(tmp_path)


def test_session_budget(tmp_path: Path) -> None:
    """Many individually valid responses cannot consume an unbounded session byte budget."""

    async def run() -> None:
        """A finite session requires a new trusted host lifecycle after its budget expires."""
        async with connect_mcp(fixture_config(tmp_path, "session_bytes")) as transport:
            with pytest.raises(OSError):
                for _ in range(35):
                    await transport.call_tool("get_k8s_logs", {})
            with pytest.raises(OSError):
                await transport.call_tool("get_k8s_logs", {})

    asyncio.run(run())
    assert_reaped(tmp_path)


def test_large_arguments_never_dispatch(tmp_path: Path) -> None:
    """Arguments remain bounded before SDK serialization or pipe writes."""

    async def run() -> None:
        """Request rejection leaves the fixture without any tool invocation."""
        async with connect_mcp(fixture_config(tmp_path)) as transport:
            with pytest.raises(ValueError, match="arguments"):
                await transport.call_tool("get_k8s_logs", {"name": "x" * 16385})

    asyncio.run(run())
    assert "tools/call" not in journal(tmp_path)
