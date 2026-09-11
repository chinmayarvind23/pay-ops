"""Bounded operator-configured stdio for MCP SDK 2.2; never a model-facing process tool."""

import json
import os
import re
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Never, TypeGuard, cast

import anyio
import mcp_types
from anyio.abc import Process
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp import Client
from mcp.os.posix.utilities import terminate_posix_process_tree
from mcp.os.win32.utilities import (
    ServerProcess,
    close_process_job,
    create_windows_process,
    terminate_windows_process_tree,
)
from mcp.shared.exceptions import MCPError
from mcp.shared.message import SessionMessage
from pydantic import BaseModel, ConfigDict

from payops.tools.gke_mcp import JSON_OBJECT, SCHEMA_HASHES, JsonObject, McpSessionInfo, canonical

MAX_FRAME = 270336  # A 256 KiB result plus its JSON-RPC envelope fits this pre-parse ceiling.
MAX_RESULT = 262144
MAX_SESSION_BYTES = 8388608
MAX_SESSION_MESSAGES = 1024
type Streams = tuple[
    MemoryObjectReceiveStream[SessionMessage | Exception], MemoryObjectSendStream[SessionMessage]
]


class _RawToolResult(mcp_types.Result):
    """Keep tool payload fields untouched until the adapter applies its strict wire contract."""

    model_config = ConfigDict(extra="allow")


@dataclass(frozen=True)
class OperatorStdioConfig:
    """Construct only in trusted host wiring; no request schema accepts process configuration."""

    executable: Path
    cwd: Path
    arguments: tuple[str, ...] = ()
    environment: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        """Fix direct argv and explicit environment without PATH lookup or shell expansion."""
        if not self.executable.is_absolute() or not self.cwd.is_absolute():
            raise ValueError("operator executable and cwd must be absolute")
        if not self.executable.is_file() or not self.cwd.is_dir():
            raise ValueError("operator executable or cwd does not exist")
        if os.name == "nt" and self.executable.suffix.lower() != ".exe":
            raise ValueError("Windows operator executable must be an exe")
        if not 0 < self.timeout_seconds <= 30 or len(self.arguments) > 32:
            raise ValueError("invalid operator process budget")
        if any("\0" in arg or len(arg) > 4096 for arg in self.arguments):
            raise ValueError("invalid operator argument")
        _validate_environment(self.environment)


def _validate_environment(environment: tuple[tuple[str, str], ...]) -> None:
    """Prevent ambiguous environment keys and cap retained operator configuration."""
    keys = [key.upper() if os.name == "nt" else key for key, _ in environment]
    if len(keys) > 64 or len(set(keys)) != len(keys):
        raise ValueError("duplicate or excessive operator environment")
    for key, value in environment:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", key):
            raise ValueError("invalid operator environment name")
        if "\0" in value or len(value) > 8192:
            raise ValueError("invalid operator environment value")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Ambiguous duplicate JSON keys are rejected before SDK parsing can select a winner."""
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate MCP JSON key")
        result[name] = value
    return result


def _reject_constant(value: str) -> Never:
    """The JSON-RPC wire accepts finite JSON numbers, never NaN or infinity extensions."""
    raise ValueError("non-JSON numeric constant")


async def _spawn(config: OperatorStdioConfig) -> ServerProcess:
    """Use pinned SDK process helpers for hidden Windows jobs and POSIX process groups."""
    with open(os.devnull, "w", encoding="utf-8") as stderr:
        if sys.platform == "win32":
            return await create_windows_process(
                str(config.executable),
                list(config.arguments),
                dict(config.environment),
                stderr,
                config.cwd,
            )
        return await anyio.open_process(
            [str(config.executable), *config.arguments],
            env=dict(config.environment),
            stderr=stderr,
            cwd=config.cwd,
            start_new_session=True,
        )


async def _stop(process: ServerProcess) -> None:
    """Close stdin, then terminate the owned process tree with bounded shielded waits."""
    with anyio.CancelScope(shield=True):
        if process.stdin:
            with suppress(OSError, anyio.BrokenResourceError, anyio.ClosedResourceError):
                await process.stdin.aclose()
        with anyio.move_on_after(0.3):
            while process.returncode is None:
                await anyio.sleep(0.01)
        if sys.platform == "win32":
            await terminate_windows_process_tree(process)
            close_process_job(process)
        else:
            assert isinstance(process, Process)
            await terminate_posix_process_tree(process, 0.3)
        with anyio.move_on_after(2):
            while process.returncode is None:
                await anyio.sleep(0.01)
        if process.stdout:
            with suppress(OSError, anyio.BrokenResourceError, anyio.ClosedResourceError):
                await process.stdout.aclose()
        if _is_async_process(process):
            with anyio.move_on_after(1):
                await process.aclose()


def _is_async_process(process: object) -> TypeGuard[Process]:
    """Narrow SDK platform unions without assuming Windows job wrappers expose aclose."""
    return isinstance(process, Process)


class _BoundedPipe:
    """A raw byte guard prevents unlimited line accumulation in the SDK's stock stdio reader."""

    def __init__(self, process: ServerProcess) -> None:
        """Use zero-buffer message channels so the pipe cannot outrun the SDK consumer."""
        self.process = process
        self.output, self.read = anyio.create_memory_object_stream[SessionMessage | Exception](0)
        self.write, self.input = anyio.create_memory_object_stream[SessionMessage](0)
        self.failure: OSError | None = None
        self.total_bytes = 0
        self.messages = 0
        self.stopped = False

    async def reader(self) -> None:
        """Count bytes before parsing and stop on malformed, truncated or oversized framing."""
        assert self.process.stdout
        buffer = bytearray()
        try:
            async with self.output:
                while True:
                    chunk = await self.process.stdout.receive(4096)
                    self.total_bytes += len(chunk)
                    if self.total_bytes > MAX_SESSION_BYTES:
                        raise OSError("MCP session byte budget exceeded")
                    buffer.extend(chunk)
                    await self._deliver_lines(buffer)
                    if len(buffer) > MAX_FRAME:
                        raise OSError("MCP frame byte budget exceeded")
        except anyio.EndOfStream:
            self.failure = OSError("MCP truncated frame" if buffer else "MCP server closed")
        except (OSError, ValueError, RecursionError):
            self.failure = OSError("MCP invalid or oversized wire data")
        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
            pass  # Normal context teardown closes channels under pending pipe tasks.

    async def _deliver_lines(self, buffer: bytearray) -> None:
        """Only a complete bounded JSON-RPC frame may reach the SDK dispatcher."""
        while (newline := buffer.find(b"\n")) >= 0:
            self.messages += 1
            if newline > MAX_FRAME or self.messages > MAX_SESSION_MESSAGES:
                raise OSError("MCP wire frame budget exceeded")
            frame = bytes(buffer[:newline])
            del buffer[: newline + 1]
            value = json.loads(
                frame, object_pairs_hook=_unique_object, parse_constant=_reject_constant
            )
            message = mcp_types.jsonrpc_message_adapter.validate_python(value, by_name=False)
            await self.output.send(SessionMessage(message))

    async def writer(self) -> None:
        """Serialize SDK messages with wire aliases; never interpret arguments as process input."""
        assert self.process.stdin
        try:
            async with self.input:
                async for message in self.input:
                    encoded = message.message.model_dump_json(by_alias=True, exclude_unset=True)
                    payload = encoded.encode("utf-8") + b"\n"
                    if len(payload) > MAX_FRAME:
                        raise OSError("MCP outgoing frame exceeds budget")
                    await self.process.stdin.send(payload)
        except (OSError, anyio.BrokenResourceError):
            self.failure = OSError("MCP server pipe unavailable")
            self.read.close()
        except anyio.ClosedResourceError:
            pass  # A completed or cancelled host context closes its owned streams.

    @asynccontextmanager
    async def streams(self) -> AsyncGenerator[Streams]:
        """Keep lifecycle in one task and ensure cancellation still reaps the owned child."""
        async with anyio.create_task_group() as group:
            group.start_soon(self.reader)
            group.start_soon(self.writer)
            try:
                yield self.read, self.write
            finally:
                self.read.close()
                self.write.close()
                await _stop(self.process)
                self.stopped = True
                group.cancel_scope.cancel()


def _wire_object(model: BaseModel) -> JsonObject:
    """Preserve standard MCP aliases and omit absent optional structuredContent."""
    result = JSON_OBJECT.validate_python(
        model.model_dump(mode="json", by_alias=True, exclude_unset=True),
        strict=True,
    )
    if len(canonical(result)) > MAX_RESULT:
        raise OSError("MCP result exceeds byte budget")
    return result


class SdkMcpTransport:
    """Internal adapter transport; authorization remains the GkeMcpAdapter's responsibility."""

    def __init__(self, client: Client, pipe: _BoundedPipe, timeout_seconds: float) -> None:
        """Bind actual initialized metadata without exposing server instructions or callbacks."""
        info = client.server_info
        if info is None or client.protocol_version != "2025-11-25":
            raise OSError("MCP session has an unsupported initialization contract")
        self._session_info = McpSessionInfo(
            server_name=info.name, server_version=info.version, protocol_version="2025-11-25"
        )
        self._client, self._pipe, self._timeout = client, pipe, timeout_seconds
        self._closed = False
        self._lock = anyio.Lock()

    @property
    def session_info(self) -> McpSessionInfo:
        """Expose only the metadata needed for retained evidence provenance."""
        return self._session_info

    def _check_open(self) -> None:
        """A failed or closed session cannot silently reconnect or reuse a different process."""
        if self._closed:
            raise OSError("MCP transport is closed")
        if self._pipe.failure:
            raise self._pipe.failure

    def mark_closed(self) -> None:
        """Invalidate the host-owned transport after SDK context teardown."""
        self._closed = True

    async def list_tools(self) -> list[JsonObject]:
        """Collect a complete catalog with a total deadline and independent pagination bounds."""
        self._check_open()
        try:
            with anyio.fail_after(self._timeout):
                async with self._lock:
                    return await self._catalog()
        except MCPError as exc:
            raise _sdk_error(exc, self._pipe) from None
        except (ValueError, RecursionError):
            raise OSError("MCP catalog has an invalid response contract") from None

    async def _catalog(self) -> list[JsonObject]:
        """Never cache capabilities; cursor cycles and incomplete catalogs are hard failures."""
        catalog: list[JsonObject] = []
        cursors: set[str] = set()
        cursor: str | None = None
        for _ in range(16):
            page = await self._client.list_tools(cursor=cursor, cache_mode="bypass")
            _wire_object(page)
            catalog.extend(_wire_object(tool) for tool in page.tools)
            if len(catalog) > 256 or len(canonical(list(catalog))) > MAX_RESULT:
                raise OSError("MCP catalog exceeds budget")
            cursor = page.next_cursor
            if cursor is None:
                return catalog
            if len(cursor.encode()) > 1024 or cursor in cursors:
                raise OSError("MCP cursor invalid or repeated")
            cursors.add(cursor)
        raise OSError("MCP catalog exceeds page budget")

    async def call_tool(self, name: str, arguments: JsonObject) -> JsonObject:
        """Expose only approved upstream names; resource authorization occurs before this seam."""
        self._check_open()
        if name not in SCHEMA_HASHES:
            raise ValueError("MCP tool name is not approved")
        arguments = JSON_OBJECT.validate_python(arguments, strict=True)
        if len(canonical(arguments)) > 16384:
            raise ValueError("MCP arguments exceed budget")
        try:
            with anyio.fail_after(self._timeout):
                async with self._lock:
                    # Generic Result preserves content/isError types for the strict adapter.
                    # The high-level call would also execute untrusted outputSchema validation.
                    result = await self._client.session.send_request(
                        mcp_types.CallToolRequest(
                            params=mcp_types.CallToolRequestParams(name=name, arguments=arguments)
                        ),
                        _RawToolResult,
                    )
                    return _wire_object(result)
        except MCPError as exc:
            raise _sdk_error(exc, self._pipe) from None
        except (ValueError, RecursionError):
            raise OSError("MCP tool has an invalid response contract") from None


def _sdk_error(error: MCPError, pipe: _BoundedPipe) -> OSError:
    """Remove untrusted server error text while preserving transport timeout classification."""
    if error.code == mcp_types.REQUEST_TIMEOUT:
        return TimeoutError("MCP response deadline exceeded")
    if pipe.failure:
        return pipe.failure
    return OSError("MCP request failed")


def _ungroup(error: BaseExceptionGroup[BaseException]) -> BaseException:
    """Undo SDK single-error task-group wrapping without discarding concurrent failures."""
    if len(error.exceptions) != 1:
        return error
    result = error.exceptions[0]
    if isinstance(result, BaseExceptionGroup):
        return _ungroup(cast(BaseExceptionGroup[BaseException], result))
    return result


@asynccontextmanager
async def connect_mcp(config: OperatorStdioConfig) -> AsyncGenerator[SdkMcpTransport]:
    """Own one SDK session in the caller task; this factory is absent from model tools."""
    process = await _spawn(config)
    pipe = _BoundedPipe(process)
    transport: SdkMcpTransport | None = None
    try:
        async with Client(
            pipe.streams(), mode="legacy", cache=None, read_timeout_seconds=config.timeout_seconds
        ) as client:
            transport = SdkMcpTransport(client, pipe, config.timeout_seconds)
            yield transport
    except MCPError as exc:
        raise _sdk_error(exc, pipe) from None
    except BaseExceptionGroup as exc:
        cause = _ungroup(exc)
        if isinstance(cause, MCPError):
            raise _sdk_error(cause, pipe) from None
        raise cause from None
    finally:
        if transport is not None:
            transport.mark_closed()
        if not pipe.stopped:
            await _stop(process)
