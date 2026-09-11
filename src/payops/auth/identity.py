"""Identity Platform claims require both SDK verification and current account authorization."""

import os
from datetime import UTC, datetime, timedelta
from typing import Annotated, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StringConstraints

from payops.contracts import Contract, Identifier, utc_now
from payops.policy.contracts import Principal, Role


class IdentitySettings(Contract):
    """Only operator configuration selects the Identity Platform project and optional tenant."""

    project_id: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")]
    tenant_id: Identifier | None = None


class Account(Contract):
    """A narrow account projection excludes emails, provider profiles and other personal fields."""

    subject: Identifier
    disabled: bool = Field(strict=True)
    tenant_id: Identifier | None
    tokens_valid_after: AwareDatetime
    grants: dict[str, JsonValue]


class Grants(Contract):
    """The current account's payops custom claim has a closed role and namespace schema."""

    roles: tuple[Role, ...] = Field(min_length=1, max_length=4)
    namespaces: tuple[Identifier, ...] = Field(min_length=1, max_length=8)


class TokenClaims(BaseModel):
    """Ignore unrelated JWT profile fields; authorization never uses token-carried role claims."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    sub: Identifier
    aud: str
    iss: str
    iat: int = Field(ge=0, le=4102444800, strict=True)
    exp: int = Field(ge=0, le=4102444800, strict=True)
    auth_time: int = Field(ge=0, le=4102444800, strict=True)
    firebase: dict[str, JsonValue]


class IdentitySource(Protocol):
    """An operator-owned source must verify signatures and query authoritative current accounts."""

    def verify(self, token: str) -> dict[str, JsonValue]:
        """Use the provider SDK with revocation checking and no emulator or unsigned fallback."""
        ...

    def lookup(self, subject: str) -> Account:
        """Fetch current enabled state, revocation epoch, tenant and custom grants."""
        ...


class AuthenticationDenied(PermissionError):
    """A stable public error hides tokens and provider exception details."""


def reject_emulator() -> None:
    """The SDK checks this environment switch dynamically and otherwise skips signatures."""
    if os.environ.get("FIREBASE_AUTH_EMULATOR_HOST") is not None:
        raise ValueError("Identity Platform emulator is not an operational identity source")


class IdentityService:
    """Authenticated request identity and later action reauthorization share current grant reads."""

    def __init__(self, settings: IdentitySettings, source: IdentitySource) -> None:
        """Trusted startup wiring supplies the source; neither token nor request selects it."""
        reject_emulator()
        self.settings, self.source = settings, source

    def _account(self, subject: str) -> tuple[Account, datetime]:
        """A missing, disabled, foreign or excessively delayed account lookup grants nothing."""
        reject_emulator()
        started = utc_now()
        account = self.source.lookup(subject)
        if (
            account.subject != subject
            or account.disabled
            or account.tenant_id != self.settings.tenant_id
            or not 0 <= (utc_now() - started).total_seconds() <= 60
        ):
            raise ValueError("account unavailable")
        return account, started

    def _claims(self, token: str) -> TokenClaims:
        """Defend explicit project, tenant and time scope even if upstream wiring is mistaken."""
        reject_emulator()
        if not 0 < len(token) <= 8192 or any(ord(char) < 33 or ord(char) > 126 for char in token):
            raise ValueError("malformed bearer")
        claims = TokenClaims.model_validate(self.source.verify(token))
        now = utc_now().timestamp()
        if (
            claims.aud != self.settings.project_id
            or claims.iss != f"https://securetoken.google.com/{self.settings.project_id}"
            or claims.firebase.get("tenant") != self.settings.tenant_id
            or not claims.auth_time <= claims.iat <= now < claims.exp
            or claims.exp - claims.iat > 3600
        ):
            raise ValueError("invalid token scope")
        return claims

    @staticmethod
    def _principal(
        account: Account, observed_at: datetime, expiry: datetime | None = None
    ) -> Principal:
        """Grant snapshots expire in sixty seconds and never outlive the authenticated token."""
        grants = Grants.model_validate(account.grants)
        now = utc_now()
        cutoff = observed_at + timedelta(seconds=60)
        expires = min(expiry, cutoff) if expiry else cutoff
        if expires <= now:
            raise ValueError("identity expired")
        return Principal(
            subject=account.subject,
            roles=grants.roles,
            namespaces=grants.namespaces,
            verified_at=observed_at,
            expires_at=expires,
        )

    def authenticate(self, token: str) -> Principal:
        """A valid signature cannot preserve roles or sessions revoked since token issuance."""
        try:
            claims = self._claims(token)
            account, observed_at = self._account(claims.sub)
            if claims.auth_time < account.tokens_valid_after.timestamp():
                raise ValueError("session revoked")
            return self._principal(account, observed_at, datetime.fromtimestamp(claims.exp, UTC))
        except Exception:
            raise AuthenticationDenied("AUTHENTICATION_DENIED") from None

    def principal(self, subject: str) -> Principal | None:
        """Only authenticated backend callers may request a fresh action-actor grant snapshot."""
        try:
            return self._principal(*self._account(subject))
        except Exception:
            return None
