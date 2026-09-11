"""Bounded synthetic inputs and deployment-owned network destinations."""

import ipaddress
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

Role = Literal["payments", "risk", "ledger", "processor", "webhook"]
Processor = Literal["A", "B"]
Region = Literal["us", "eu"]
Method = Literal["credit", "debit"]
RiskProtocol = Literal["v1", "v2"]


class Sample(BaseModel):
    """Samples deliberately exclude money, account details and arbitrary metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    sample_id: str = Field(pattern=r"^synthetic-[a-zA-Z0-9-]{1,64}$")
    processor: Processor = "A"
    region: Region = "us"
    payment_method: Method = "credit"


class SimulationResult(BaseModel):
    """Every response remains explicitly distinguishable from a real authorization."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    sample_id: str
    role: Role
    status: Literal["accepted", "declined"]
    synthetic: Literal[True] = True


class RiskSampleV2(BaseModel):
    """A versioned wire envelope makes rollout incompatibility a real schema failure."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    protocol: Literal["payops-risk-v2"]
    sample: Sample


class FaultConfig(BaseModel):
    """Trusted harness state changes reproducible behavior without a control endpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    unavailable: bool = False
    delay_ms: int = Field(default=0, ge=0, le=5000)
    rate_limit_every: int = Field(default=0, ge=0, le=1000)
    decline_every: int = Field(default=0, ge=0, le=1000)
    processor: Processor | None = None
    region: Region | None = None
    payment_method: Method | None = None
    seed: int = Field(default=0, ge=0, le=2**31 - 1)

    def matches(self, sample: Sample) -> bool:
        """Unchanged control slices isolate processor, region and method regressions."""
        return all(
            selected is None or selected == actual
            for selected, actual in (
                (self.processor, sample.processor),
                (self.region, sample.region),
                (self.payment_method, sample.payment_method),
            )
        )


def allowed_destination(url: str, explicit_hosts: tuple[str, ...]) -> bool:
    """Only deployment-owned origins pass; user requests cannot supply a destination."""
    parsed = urlsplit(url)
    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        return False
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return False
    if parsed.path not in {"", "/"} or "\\" in url:
        return False
    host = parsed.hostname
    if host == "localhost" or host.endswith(".svc.cluster.local"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return parsed.scheme == "https" and host in explicit_hosts


class SandboxConfig(BaseModel):
    """External simulator allowlisting is an explicit operator deployment decision."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    risk_url: str = "http://127.0.0.1:8101"
    processor_url: str = "http://127.0.0.1:8102"
    ledger_url: str = "http://127.0.0.1:8103"
    webhook_url: str = "http://127.0.0.1:8104"
    explicit_synthetic_hosts: tuple[str, ...] = ()
    timeout_seconds: float = Field(default=2.0, gt=0, le=10)
    idempotency_capacity: int = Field(default=10000, ge=1, le=100000)
    risk_protocol: RiskProtocol = "v1"

    @model_validator(mode="after")
    def validate_origins(self) -> Self:
        """Validate all peers at startup so later requests use immutable safe origins."""
        for url in (self.risk_url, self.processor_url, self.ledger_url, self.webhook_url):
            if not allowed_destination(url, self.explicit_synthetic_hosts):
                raise ValueError("destination must be an approved synthetic service origin")
        return self

    def destination(self, role: Role) -> str:
        """A closed role map prevents arbitrary outbound endpoint selection."""
        destinations = {
            "risk": self.risk_url,
            "processor": self.processor_url,
            "ledger": self.ledger_url,
            "webhook": self.webhook_url,
        }
        return destinations[role]
