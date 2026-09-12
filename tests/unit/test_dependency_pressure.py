"""Failed pressure experiments must release every acquired session and helper process."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from payops.scenarios.dependency_gateway import DependencyGateway
from payops.scenarios.dependency_pressure import connect_pressure, postgres_pressure

RUN = "a" * 32
MODULE = "payops.scenarios.dependency_pressure"


@pytest.mark.parametrize("failure", ["none", "connect", "probe", "body", "terminate"])
def test_pressure_releases_partial_and_complete_acquisitions(tmp_path: Path, failure: str) -> None:
    """Cleanup covers setup failure, caller failure, and a forward requiring forced shutdown."""
    (tmp_path / "scenario-postgres-password").write_text("fixture-password")
    gateway = MagicMock(spec=DependencyGateway)
    gateway.postgres_sessions.return_value = {"limit": 4, "total": 0}
    gateway.postgres_forward_argv.return_value = ("kubectl", "port-forward")
    connections = [MagicMock() for _ in range(4)]
    for connection in connections:
        connection.execute.return_value.fetchone.return_value = (1,)
    if failure == "probe":
        connections[1].execute.return_value.fetchone.return_value = (0,)
    attempts = (
        [connections[0], RuntimeError("connection failed")] if failure == "connect" else connections
    )
    process = MagicMock()
    if failure == "terminate":
        process.wait.side_effect = [subprocess.TimeoutExpired("kubectl", 5), 0]
    with (
        patch(MODULE + ".socket.socket"),
        patch(MODULE + ".subprocess.Popen", return_value=process),
        patch(MODULE + ".connect_pressure", side_effect=attempts),
    ):
        if failure in {"connect", "probe", "body"}:
            with pytest.raises((RuntimeError, ValueError)):
                with postgres_pressure(gateway, tmp_path, RUN):
                    raise RuntimeError("caller failed")
        else:
            with postgres_pressure(gateway, tmp_path, RUN):
                for connection in connections:
                    connection.close.assert_not_called()
    acquired = 1 if failure == "connect" else 2 if failure == "probe" else 4
    for connection in connections[:acquired]:
        connection.close.assert_called_once()
    for connection in connections[acquired:]:
        connection.close.assert_not_called()
    process.terminate.assert_called_once()
    assert process.wait.call_count == (2 if failure == "terminate" else 1)
    assert process.kill.call_count == (1 if failure == "terminate" else 0)


@pytest.mark.parametrize("run,total,limit", [("../other", 0, 4), (RUN, 1, 4), (RUN, 0, 5)])
def test_pressure_preflight_never_starts_on_foreign_or_busy_role(
    tmp_path: Path, run: str, total: int, limit: int
) -> None:
    """Rejected ownership or role state must fail before reading credentials or starting I/O."""
    gateway = MagicMock(spec=DependencyGateway)
    gateway.postgres_sessions.return_value = {"limit": limit, "total": total}
    with patch(MODULE + ".subprocess.Popen") as spawn, pytest.raises(ValueError):
        with postgres_pressure(gateway, tmp_path, run):
            pytest.fail("preflight accepted")
    spawn.assert_not_called()


def test_connection_startup_retry_preserves_verified_read_only_scope(tmp_path: Path) -> None:
    """A not-yet-ready forward can retry without changing the destination or role authority."""
    connection = MagicMock()
    with (
        patch(MODULE + ".time.monotonic", side_effect=[0, 0, 1]),
        patch(MODULE + ".time.sleep") as sleep,
        patch(
            MODULE + ".psycopg.connect", side_effect=[psycopg.OperationalError(), connection]
        ) as connect,
    ):
        assert connect_pressure(tmp_path, "fixture-password", RUN) is connection
    sleep.assert_called_once_with(0.1)
    settings = connect.call_args.kwargs
    assert settings["hostaddr"] == "127.0.0.1" and settings["port"] == 35532
    assert settings["sslmode"] == "verify-full"
    assert settings["user"] == "payops_synthetic"
    assert "default_transaction_read_only=on" in settings["options"]
    assert settings["application_name"] == "payops-scenario-" + RUN


def test_connection_deadline_does_not_disclose_driver_error(tmp_path: Path) -> None:
    """Repeated startup failures end at the deadline with a credential-free exception."""
    with (
        patch(MODULE + ".time.monotonic", side_effect=[0, 0, 11]),
        patch(MODULE + ".time.sleep"),
        patch(MODULE + ".psycopg.connect", side_effect=psycopg.OperationalError("secret-fixture")),
        pytest.raises(TimeoutError, match="^dedicated pressure connection unavailable$"),
    ):
        connect_pressure(tmp_path, "secret-fixture", RUN)
