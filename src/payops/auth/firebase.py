"""Pinned Firebase Admin SDK binding for Google Identity Platform, with no emulator fallback."""

from datetime import UTC, datetime
from typing import Protocol, cast

import firebase_admin  # pyright: ignore[reportMissingTypeStubs]
from firebase_admin import auth  # pyright: ignore[reportMissingTypeStubs]
from pydantic import JsonValue

from payops.auth.identity import Account, IdentitySettings, reject_emulator
from payops.evidence.artifacts import JSON_OBJECT

type JsonObject = dict[str, JsonValue]


class SdkOptions(Protocol):
    """The SDK option getter is untyped; validate the returned value instead of assuming it."""

    def get(self, name: str) -> object:
        """Read only an operator-owned app option."""
        ...


class SdkApp(Protocol):
    """Typed view of the two documented app properties used for configuration binding."""

    @property
    def project_id(self) -> object:
        """Return the configured project value for exact comparison."""
        ...

    @property
    def options(self) -> SdkOptions:
        """Return the configured app options getter."""
        ...


class SdkUser(Protocol):
    """Only these documented UserRecord properties cross the untyped SDK boundary."""

    @property
    def uid(self) -> str:
        """Return the provider user identifier, excluding profile data."""
        ...

    @property
    def disabled(self) -> bool:
        """Return the current account-disabled flag."""
        ...

    @property
    def tenant_id(self) -> str | None:
        """Return the account's current tenant scope."""
        ...

    @property
    def tokens_valid_after_timestamp(self) -> int:
        """Return the SDK's documented revocation timestamp in milliseconds."""
        ...

    @property
    def custom_claims(self) -> object:
        """Return untrusted-shaped grant data for strict local validation."""
        ...


class SdkClient(Protocol):
    """A narrow interface prevents application code reaching SDK account-mutation methods."""

    def verify_id_token(
        self, id_token: str, check_revoked: bool = False, clock_skew_seconds: int = 0
    ) -> object:
        """Verify Google signature, project, issuer, lifetime, tenant and revocation."""
        ...

    def get_user(self, uid: str) -> SdkUser:
        """Read the authoritative current account; never set custom claims here."""
        ...


class FirebaseIdentitySource:
    """Trusted startup supplies a configured SDK app; the model sees neither app nor credentials."""

    def __init__(self, app: firebase_admin.App, settings: IdentitySettings) -> None:
        """Require the exact project and SDK request timeout before constructing a client."""
        reject_emulator()
        configured = cast(SdkApp, app)
        if (
            configured.project_id != settings.project_id
            or configured.options.get("httpTimeout") != 5
        ):
            raise ValueError("SDK app must bind the configured project and five-second timeout")
        # SDK 7.5.0 has no typing marker; all results are revalidated through our closed contracts.
        self._client = cast(SdkClient, auth.Client(app, tenant_id=settings.tenant_id))
        self.settings = settings

    def verify(self, token: str) -> JsonObject:
        """Require revocation checking and propagate provider failures to the deny gate."""
        reject_emulator()
        return JSON_OBJECT.validate_python(
            self._client.verify_id_token(token, check_revoked=True, clock_skew_seconds=0)
        )

    def lookup(self, subject: str) -> Account:
        """Project only current payops grants, enabled state, tenant and revocation time."""
        reject_emulator()
        user = self._client.get_user(subject)
        claims = JSON_OBJECT.validate_python(user.custom_claims)
        grants = JSON_OBJECT.validate_python(claims.get("payops"))
        return Account(
            subject=user.uid,
            disabled=user.disabled,
            tenant_id=user.tenant_id,
            tokens_valid_after=datetime.fromtimestamp(
                user.tokens_valid_after_timestamp / 1000, UTC
            ),
            grants=grants,
        )
