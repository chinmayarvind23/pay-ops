"""Own four read-only sessions and a private local forward for bounded role exhaustion."""

import re
import socket
import subprocess
import time
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from pathlib import Path

import psycopg

from payops.scenarios.dependency_gateway import DependencyGateway


@contextmanager
def postgres_pressure(
    gateway: DependencyGateway, credentials: Path, run_id: str
) -> Generator[None]:
    """Close only owned sessions; server idle timeout provides a second cleanup bound."""
    if re.fullmatch("[0-9a-f]{32}", run_id) is None:
        raise ValueError("invalid pressure run identity")
    before = gateway.postgres_sessions(run_id)
    if before.get("limit") != 4 or before.get("total") != 0:
        raise ValueError("dedicated role is not idle at its reviewed limit")
    password = (credentials / "scenario-postgres-password").read_text(encoding="utf-8")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 35532))
    process = subprocess.Popen(
        gateway.postgres_forward_argv(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        shell=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        with ExitStack() as stack:
            for _ in range(4):
                connection = connect_pressure(credentials, password, run_id)
                stack.callback(connection.close)
                if connection.execute("SELECT 1").fetchone() != (1,):
                    raise ValueError("pressure session read-only probe failed")
            yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def connect_pressure(
    credentials: Path, password: str, run_id: str
) -> psycopg.Connection[tuple[object, ...]]:
    """Retry only startup connection establishment without logging credential-bearing errors."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            return psycopg.connect(
                host="localhost",
                hostaddr="127.0.0.1",
                port=35532,
                dbname="payops",
                user="payops_synthetic",
                password=password,
                sslmode="verify-full",
                sslrootcert=str(credentials / "ca.crt"),
                connect_timeout=2,
                autocommit=True,
                application_name="payops-scenario-" + run_id,
                options="-c default_transaction_read_only=on -c statement_timeout=2000",
            )
        except psycopg.OperationalError:
            time.sleep(0.1)
    raise TimeoutError("dedicated pressure connection unavailable")
