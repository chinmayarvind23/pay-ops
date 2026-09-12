"""Explicit local operator configuration is separate from public or cloud authentication."""

import ctypes
import os
import sys
from collections.abc import Callable
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal, Self, cast

from pydantic import AwareDatetime, ConfigDict, Field, SecretStr, model_validator

from payops.contracts import Contract, utc_now
from payops.orchestrator.budget import ReasoningBudget
from payops.orchestrator.local_llama import LOCAL_MODELS, ZERO_PRICE
from payops.orchestrator.model_runtime import ModelSettings
from payops.orchestrator.openai_adapter import STANDARD_PRICE
from payops.orchestrator.openai_wire import MODEL, decode
from payops.policy.contracts import Principal


def local_path(path: Path) -> Path:
    """Only explicit absolute local paths are accepted; UNC locations are not local authority."""
    raw = str(path)
    if raw.startswith("\\\\?\\") and not raw.upper().startswith("\\\\?\\UNC\\"):
        path = Path(raw[4:])
    if not path.is_absolute() or str(path).startswith(("\\\\", "//")):
        raise ValueError("absolute local path required")
    resolved = path.resolve()
    resolved_text = str(resolved)
    if resolved_text.upper().startswith("\\\\?\\UNC\\") or (
        resolved_text.startswith(("\\\\", "//")) and not resolved_text.startswith("\\\\?\\")
    ):
        raise ValueError("resolved path must remain local")
    return resolved


def read_bounded(path: Path, limit: int) -> bytes:
    """Read one regular local file with an overflow sentinel, never a discovered credential file."""
    path = local_path(path)
    if not path.is_file():
        raise ValueError("explicit local file unavailable")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("local configuration exceeds byte bound")
    return data


class SecretReference(Contract):
    """A caller names exactly one source; no dotenv, keychain or default variable is searched."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    environment: str | None = Field(default=None, pattern=r"^[A-Z_][A-Z0-9_]{0,127}$")
    file: Path | None = None

    @model_validator(mode="after")
    def one_source(self) -> Self:
        """Ambiguous or relative references cannot silently select another credential."""
        if (self.environment is None) == (self.file is None):
            raise ValueError("exactly one explicit secret reference required")
        if self.file is not None:
            local_path(self.file)
        return self

    def load(self) -> SecretStr:
        """Only an execution path loads the chosen bounded secret; plans use the reference only."""
        value = (
            os.environ.get(self.environment, "")
            if self.environment is not None
            else read_bounded(cast(Path, self.file), 4096).decode("utf-8").rstrip("\r\n")
        )
        if not value or len(value.encode("utf-8")) > 4096:
            raise ValueError("explicit credential unavailable or oversized")
        return SecretStr(value)


class LocalGrant(Contract):
    """An operator-maintained local grant is not a Firebase token or remote account assertion."""

    account: str = Field(min_length=1, max_length=256)
    namespace: Literal["payops-sandbox"] = "payops-sandbox"
    role: Literal["responder"] = "responder"
    enabled: bool = Field(strict=True)
    issued_at: AwareDatetime
    expires_at: AwareDatetime

    @model_validator(mode="after")
    def lifetime(self) -> Self:
        """Local grants have an explicit lifetime of at most eight hours, never sliding expiry."""
        if not 0 < (self.expires_at - self.issued_at).total_seconds() <= 8 * 3600:
            raise ValueError("local grant lifetime outside bounds")
        return self


def native_account() -> str:
    """Read process identity from the OS API rather than USER or USERNAME environment strings."""
    if sys.platform == "win32":
        library = ctypes.WinDLL("secur32", use_last_error=True)
        function = library.GetUserNameExW
        function.argtypes = [ctypes.c_int, ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_ulong)]
        function.restype = ctypes.c_ubyte
        buffer, size = ctypes.create_unicode_buffer(1024), ctypes.c_ulong(1024)
        if not function(2, buffer, ctypes.byref(size)):
            raise PermissionError("local operating-system identity unavailable")
        return "windows:" + buffer.value.casefold()
    uid = os.getuid()
    return "posix-uid:" + str(uid)


class LocalAuthority:
    """Retain the initiating OS account and reread its current local namespace grant."""

    def __init__(
        self,
        grant_file: Path,
        *,
        account: Callable[[], str] = native_account,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """Only trusted host/test wiring supplies the identity function and local file path."""
        self.path = local_path(grant_file)
        self._account, self._clock = account, clock
        self.account = account()
        self.subject = "local-operator-" + sha256(self.account.encode()).hexdigest()
        self.closed = False

    def principal(self) -> Principal | None:
        """Any changed account, revoked/expired grant or malformed local file denies authority."""
        try:
            started = self._clock()
            grant = LocalGrant.model_validate(decode(read_bounded(self.path, 4096), 4096))
            account = self._account()
            now = self._clock()
            if (
                self.closed
                or account != self.account
                or grant.account != self.account
                or not grant.enabled
                or not grant.issued_at <= now < grant.expires_at
                or not 0 <= (now - started).total_seconds() <= 8
            ):
                return None
            return Principal(
                subject=self.subject,
                roles=("responder",),
                namespaces=(grant.namespace,),
                verified_at=started,
                expires_at=grant.expires_at,
            )
        except (OSError, ValueError):
            return None

    def allowed(self) -> bool:
        """The only local capability this host needs is current namespace-scoped investigation."""
        return self.principal() is not None

    def require(self) -> None:
        """Failures carry no credential, OS account or grant-file contents into CLI errors."""
        if not self.allowed():
            raise PermissionError("current local operator grant required")


def model_settings() -> ModelSettings:
    """Pin the reviewed first provider profile and its supported execution bounds."""
    return ModelSettings(
        provider="openai",
        model=MODEL,
        mode="provider",
        token_accounting="provider_ceiling",
        input_token_limit=16000,
        output_token_limit=2048,
        timeout_seconds=25,
        price=STANDARD_PRICE,
    )


def reasoning_budget() -> ReasoningBudget:
    """Four model turns can select the full six-read catalog and then finish."""
    return ReasoningBudget(
        model_calls=4,
        tokens=72192,
        cost_nano_usd=84864000,
        tool_calls=6,
        backend_reads=9,
        provider_requests=8,
    )


def reviewed_model(model: ModelSettings, key: SecretReference | None) -> bool:
    """Local inference has no credential or charged rate; remote configuration remains explicit."""
    if model.mode != "provider" or model.token_accounting != "provider_ceiling":
        return False
    if model.provider == "local_llama":
        return (
            model.model in LOCAL_MODELS
            and model.price == ZERO_PRICE
            and key is None
            and model.input_token_limit <= 4096
            and 16 <= model.output_token_limit <= 512
        )
    return (
        model.provider == "openai"
        and model.model == MODEL
        and model.price == STANDARD_PRICE
        and key is not None
        and model.input_token_limit <= 16000
        and 16 <= model.output_token_limit <= 2048
    )


class OperatorConfig(Contract):
    """Paths, endpoints, prices and budgets are trusted operator input, never model arguments."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    runtime: Path
    kubeconfig: Path
    grant_file: Path
    release_labels: Path
    knowledge_bundle: Path
    provider_key: SecretReference | None = None
    elastic_key: SecretReference
    elastic_ca: Path
    elastic_port: int = Field(default=29200, strict=True, ge=1, le=65535)
    prometheus_port: int = Field(default=19090, strict=True, ge=1, le=65535)
    model: ModelSettings = Field(default_factory=model_settings)
    reasoning: ReasoningBudget = Field(default_factory=reasoning_budget)

    @model_validator(mode="after")
    def bounds(self) -> Self:
        """Reject unsupported configurations before constructing a client or loading secrets."""
        for path in (
            self.runtime,
            self.kubeconfig,
            self.grant_file,
            self.release_labels,
            self.knowledge_bundle,
            self.elastic_ca,
        ):
            local_path(path)
        model = self.model
        if (
            not reviewed_model(model, self.provider_key)
            or (model.provider == "local_llama" and self.reasoning.cost_nano_usd != 0)
            or self.reasoning.tool_calls > 20
            or self.reasoning.backend_reads > 34
        ):
            raise ValueError("operator profile outside reviewed bounds")
        return self


def load_config(path: Path) -> OperatorConfig:
    """Plans and execution share strict bounded JSON parsing without implicit credential reads."""
    return OperatorConfig.model_validate(decode(read_bounded(path, 32768), 32768))
