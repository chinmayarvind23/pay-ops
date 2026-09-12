"""Dependency failures must affect real payment HTTP results without leaking credentials."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import psycopg
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from payops.sandbox.dependencies import DependencyGate
from payops.sandbox.models import Sample, SandboxConfig
from payops.sandbox.service import create_service


@pytest.fixture
def credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Only a temporary fixed fixture credential is ever read by these transport tests."""
    (tmp_path / "password").write_text("fixture-password-not-live")
    (tmp_path / "ca.crt").write_text("fixture")
    monkeypatch.setattr("payops.sandbox.dependencies.CREDENTIALS", tmp_path)
    return tmp_path


def test_disabled_gate_never_reads_credentials(tmp_path: Path) -> None:
    """The normal sandbox remains usable without any new credential files."""
    with patch("payops.sandbox.dependencies.CREDENTIALS", tmp_path / "missing"):
        asyncio.run(DependencyGate("none").check("synthetic-disabled"))


def test_postgres_uses_fixed_read_only_query(credentials: Path) -> None:
    """A closed destination, verified TLS and constant SQL remain outside request control."""
    cursor = MagicMock()
    cursor.__aenter__ = AsyncMock(return_value=cursor)
    cursor.__aexit__ = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(1,))
    connection = MagicMock()
    connection.__aenter__ = AsyncMock(return_value=connection)
    connection.__aexit__ = AsyncMock()
    connection.cursor.return_value = cursor
    with patch(
        "payops.sandbox.dependencies.psycopg.AsyncConnection.connect",
        new_callable=AsyncMock,
        return_value=connection,
    ) as connect:
        asyncio.run(DependencyGate("postgres").check("synthetic-postgres"))
    settings = connect.call_args.kwargs
    assert settings["user"] == "payops_synthetic"
    assert settings["sslmode"] == "verify-full" and settings["sslrootcert"] == str(
        credentials / "ca.crt"
    )
    assert "default_transaction_read_only=on" in settings["options"]
    cursor.execute.assert_awaited_once_with("SELECT 1")
    connection.__aexit__.assert_awaited_once()


def test_cache_is_verified_probe_only(credentials: Path) -> None:
    """The cache path has one connection, no retries, and only PING authority."""
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock()
    client.ping = AsyncMock(return_value=True)
    with patch("payops.sandbox.dependencies.redis.Redis", return_value=client) as factory:
        asyncio.run(DependencyGate("redis").check("synthetic-cache"))
    options = factory.call_args.kwargs
    assert options["username"] == "payops_probe" and options["max_connections"] == 1
    assert options["ssl_check_hostname"] and options["ssl_cert_reqs"] == "required"
    client.ping.assert_awaited_once()
    client.__aexit__.assert_awaited_once()


def test_exhaustion_fails_http_then_recovery_accepts(
    credentials: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A failed reservation is released so recovery can process the same synthetic sample."""

    def peer(request: httpx.Request) -> httpx.Response:
        """The four downstream HTTP responses are fixture-only; payment execution is real."""
        role = {8101: "risk", 8102: "processor", 8103: "ledger", 8104: "webhook"}[
            request.url.port or 0
        ]
        return httpx.Response(
            200,
            json={
                "sample_id": "synthetic-dependency",
                "role": role,
                "status": "accepted",
                "synthetic": True,
            },
        )

    error = psycopg.OperationalError(
        'too many connections for role "payops_synthetic" fixture-password-not-live'
    )
    with patch.object(
        DependencyGate, "postgres", new_callable=AsyncMock, side_effect=[error, None]
    ) as read:
        with TestClient(
            create_service(
                "payments", SandboxConfig(dependency="postgres"), httpx.MockTransport(peer)
            )
        ) as client:
            payload = Sample(sample_id="synthetic-dependency").model_dump()
            assert client.post("/simulate", json=payload).status_code == 503
            assert client.post("/simulate", json=payload).json()["status"] == "accepted"
            assert client.post("/simulate", json=payload).json()["status"] == "accepted"
        assert read.await_count == 2
    raw = capsys.readouterr().out
    assert "fixture-password-not-live" not in raw
    events = [json.loads(line) for line in raw.splitlines() if "synthetic.dependency" in line]
    assert [e["outcome"] for e in events] == ["connection_exhausted", "ok"]
    assert events[0]["sqlstate"] == "53300"


def test_nonpayment_dependency_profile_rejects(credentials: Path) -> None:
    """The optional profile cannot silently broaden all five service roles."""
    with pytest.raises(ValueError, match="payments role"):
        create_service("risk", SandboxConfig(dependency="postgres"))


@pytest.mark.parametrize("invalid", ["short", "oversized", "missing_ca"])
def test_invalid_dependency_credentials_fail_before_connection(
    credentials: Path, invalid: str
) -> None:
    """A dependency cannot silently use an unverified TLS endpoint or unbounded credential."""
    if invalid == "missing_ca":
        (credentials / "ca.crt").unlink()
    else:
        (credentials / "password").write_text("x" * (257 if invalid == "oversized" else 15))
    with pytest.raises(ValueError, match="bounded credentials and explicit CA"):
        DependencyGate("postgres")


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("secret-fixture"),
        ValueError("secret-fixture"),
        psycopg.OperationalError("secret-fixture"),
    ],
)
def test_unavailable_check_releases_admission_and_redacts_errors(
    credentials: Path, capsys: pytest.CaptureFixture[str], error: Exception
) -> None:
    """Transport and response failures remain generic and a subsequent request can recover."""
    gate = DependencyGate("postgres")

    async def exercise() -> None:
        """Both checks share one semaphore and event loop, including after the failed await."""
        with pytest.raises(HTTPException) as caught:
            await gate.check("synthetic-failed")
        assert caught.value.status_code == 503
        assert caught.value.detail == "synthetic dependency unavailable"
        await gate.check("synthetic-recovered")

    with patch.object(gate, "postgres", new_callable=AsyncMock, side_effect=[error, None]):
        asyncio.run(exercise())
    raw = capsys.readouterr().out
    assert "secret-fixture" not in raw
    assert [json.loads(line)["outcome"] for line in raw.splitlines()] == ["unavailable", "ok"]


def test_cancelled_check_propagates_and_records_cancellation(
    credentials: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Cancellation must not become an ordinary 503 or leave the admission slot occupied."""
    gate = DependencyGate("redis")

    async def exercise() -> None:
        """Reuse the gate after cancellation to expose leaked admission state."""
        with pytest.raises(asyncio.CancelledError):
            await gate.check("synthetic-cancelled")
        assert gate.slots._value == 4
        await gate.check("synthetic-recovered")

    with patch.object(
        gate, "cache", new_callable=AsyncMock, side_effect=[asyncio.CancelledError(), None]
    ):
        asyncio.run(exercise())
    assert [json.loads(line)["outcome"] for line in capsys.readouterr().out.splitlines()] == [
        "cancelled",
        "ok",
    ]
