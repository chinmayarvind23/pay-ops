"""Real read-only dependency checks for the synthetic payment path."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import psycopg
import redis.asyncio as redis
from fastapi import HTTPException
from opentelemetry import trace
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

Dependency = Literal["none", "postgres", "redis"]
CREDENTIALS = Path("/run/payops-dependency")


class DependencyGate:
    """A fixed TLS endpoint and read-only identity are selected by deployment, never HTTP input."""

    def __init__(self, kind: Dependency) -> None:
        """The normal sandbox requires no credentials and creates no dependency connections."""
        if kind not in {"none", "postgres", "redis"}:
            raise ValueError("unknown synthetic dependency")
        self.kind = kind
        self.password = ""
        self.slots = asyncio.Semaphore(4)
        if kind != "none":
            with (CREDENTIALS / "password").open("rb") as stream:
                raw = stream.read(257)
            if not 16 <= len(raw) <= 256 or not (CREDENTIALS / "ca.crt").is_file():
                raise ValueError("dependency requires bounded credentials and explicit CA")
            self.password = raw.decode("utf-8")

    async def postgres(self) -> None:
        """Open one verified, read-only session and run only a constant scalar query."""
        async with await psycopg.AsyncConnection.connect(
            host="postgres.payops-data.svc.cluster.local",
            port=5432,
            dbname="payops",
            user="payops_synthetic",
            password=self.password,
            sslmode="verify-full",
            sslrootcert=str(CREDENTIALS / "ca.crt"),
            connect_timeout=3,
            autocommit=True,
            application_name="payops-synthetic-request",
            options="-cdefault_transaction_read_only=on -cstatement_timeout=2000",
        ) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
                if await cursor.fetchone() != (1,):
                    raise ValueError("unexpected synthetic database response")

    async def cache(self) -> None:
        """PING uses the existing probe-only identity and releases its single connection."""
        async with redis.Redis(
            host="redis.payops-data.svc.cluster.local",
            port=6379,
            username="payops_probe",
            password=self.password,
            ssl=True,
            ssl_ca_certs=str(CREDENTIALS / "ca.crt"),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0),
            max_connections=1,
        ) as client:
            if await client.ping() is not True:  # pyright: ignore[reportUnknownMemberType]
                raise ValueError("unexpected synthetic cache response")

    async def check(self, sample_id: str) -> None:
        """Bound admission and I/O without logging credential-bearing exception text."""
        if self.kind == "none":
            return
        started = datetime.now(UTC).isoformat()
        outcome = "unavailable"
        sqlstate: str | None = None
        try:
            async with asyncio.timeout(4), self.slots:
                if self.kind == "postgres":
                    await self.postgres()
                else:
                    await self.cache()
            outcome = "ok"
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except psycopg.OperationalError as error:
            if (
                error.sqlstate == "53300"
                or 'too many connections for role "payops_synthetic"' in str(error)
            ):
                outcome, sqlstate = "connection_exhausted", "53300"
            raise HTTPException(503, "synthetic dependency unavailable") from None
        except (TimeoutError, redis.RedisError, ValueError):
            raise HTTPException(503, "synthetic dependency unavailable") from None
        finally:
            print(
                json.dumps(
                    {
                        "event": "synthetic.dependency",
                        "sample_id": sample_id,
                        "dependency": self.kind,
                        "started_at": started,
                        "completed_at": datetime.now(UTC).isoformat(),
                        "outcome": outcome,
                        "sqlstate": sqlstate,
                        "trace_id": format(
                            trace.get_current_span().get_span_context().trace_id, "032x"
                        ),
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )
