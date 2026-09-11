"""Backend identity comes from verified tokens and current account grants, never token role text."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import JsonValue

from payops.auth.identity import Account, AuthenticationDenied, IdentityService, IdentitySettings
from payops.contracts import utc_now
from payops.policy.engine import identity_valid


class Source:
    """A trusted-source fixture exposes each verification and fresh account lookup explicitly."""

    def __init__(self) -> None:
        """Token-carried admin text intentionally disagrees with authoritative viewer grants."""
        now = int(utc_now().timestamp())
        self.claims: dict[str, JsonValue] = {
            "sub": "alice",
            "aud": "payops-project",
            "iss": "https://securetoken.google.com/payops-project",
            "iat": now - 10,
            "exp": now + 3590,
            "auth_time": now - 20,
            "firebase": {},
            "payops": {"roles": ["approver"]},
        }
        self.account = Account(
            subject="alice",
            disabled=False,
            tenant_id=None,
            tokens_valid_after=utc_now() - timedelta(days=1),
            grants={"roles": ["viewer"], "namespaces": ["payops-sandbox"]},
        )
        self.verifications: list[str] = []
        self.lookups: list[str] = []
        self.failure = False

    def verify(self, token: str) -> dict[str, JsonValue]:
        """A fixture failure represents SDK signature, expiry, revocation or transport failure."""
        self.verifications.append(token)
        if self.failure:
            raise ValueError("private token detail")
        return self.claims

    def lookup(self, subject: str) -> Account:
        """Every call returns the current account rather than cached token custom claims."""
        self.lookups.append(subject)
        if self.failure:
            raise OSError("private backend detail")
        return self.account


def service() -> tuple[Source, IdentityService]:
    """Use a fixed configured project and no tenant unless explicitly selected by the operator."""
    source = Source()
    return source, IdentityService(IdentitySettings(project_id="payops-project"), source)


def test_roles_are_current_account_grants() -> None:
    """Signed token roles are ignored in favor of separately retrieved current grants."""
    source, identity = service()
    principal = identity.authenticate("signed-token")
    assert principal.subject == "alice" and principal.roles == ("viewer",)
    assert source.verifications == ["signed-token"] and source.lookups == ["alice"]
    source.account = source.account.model_copy(
        update={"grants": {"roles": ["responder"], "namespaces": ["payops-sandbox"]}}
    )
    assert identity.principal("alice").roles == ("responder",)  # type: ignore[union-attr]
    assert source.lookups == ["alice", "alice"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("aud", "foreign-project"),
        ("iss", "https://attacker.invalid"),
        ("sub", ""),
        ("exp", 1),
        ("iat", 9999999999),
        ("auth_time", 9999999999),
        ("exp", True),
        ("firebase", {"tenant": "foreign"}),
        ("sub", "../../alice"),
    ],
)
def test_invalid_verified_claims_fail_closed(field: str, value: JsonValue) -> None:
    """Even a misconfigured upstream verifier cannot bypass local project/time/tenant bindings."""
    source, identity = service()
    source.claims[field] = value
    with pytest.raises(AuthenticationDenied):
        identity.authenticate("signed-token")


@pytest.mark.parametrize("change", ["disabled", "revoked", "subject", "tenant", "grants"])
def test_current_account_denies_revoked_or_misbound_sessions(change: str) -> None:
    """A cryptographically valid token is insufficient after account or grant changes."""
    source, identity = service()
    changes: dict[str, dict[str, object]] = {
        "disabled": {"disabled": True},
        "revoked": {"tokens_valid_after": utc_now()},
        "subject": {"subject": "mallory"},
        "tenant": {"tenant_id": "other"},
        "grants": {"grants": {"roles": ["superadmin"], "namespaces": ["payops-sandbox"]}},
    }
    source.account = Account.model_validate({**source.account.model_dump(), **changes[change]})
    with pytest.raises(AuthenticationDenied):
        identity.authenticate("signed-token")


def test_backend_failure_has_no_sensitive_error_text() -> None:
    """Identity outages deny both API authentication and execution-time principal refresh."""
    source, identity = service()
    source.failure = True
    with pytest.raises(AuthenticationDenied, match="AUTHENTICATION_DENIED") as caught:
        identity.authenticate("secret-token-value")
    assert "secret" not in str(caught.value) and "private" not in str(caught.value)
    assert identity.principal("alice") is None


@pytest.mark.parametrize("token", ["", "x" * 8193, "contains\nnewline"])
def test_malformed_bearer_is_rejected_before_network(token: str) -> None:
    """Oversized or control-character credentials consume no identity-provider calls."""
    source, identity = service()
    with pytest.raises(AuthenticationDenied):
        identity.authenticate(token)
    assert source.verifications == []


def test_emulator_configuration_never_disables_signature_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Firebase's emulator environment switch is forbidden before construction and every use."""
    source, identity = service()
    monkeypatch.setenv("FIREBASE_AUTH_EMULATOR_HOST", "127.0.0.1:9099")
    with pytest.raises(AuthenticationDenied):
        identity.authenticate("unsigned-token")
    with pytest.raises(ValueError):
        IdentityService(IdentitySettings(project_id="payops-project"), source)
    assert identity.principal("alice") is None and source.verifications == []


def test_valid_tenant_requires_both_token_and_account_scope() -> None:
    """An explicitly configured tenant works only when independently read token/account agree."""
    source = Source()
    source.claims["firebase"] = {"tenant": "tenant-one"}
    source.account = source.account.model_copy(update={"tenant_id": "tenant-one"})
    identity = IdentityService(
        IdentitySettings(project_id="payops-project", tenant_id="tenant-one"), source
    )
    assert identity.authenticate("signed-token").subject == "alice"


def test_token_expiring_during_account_lookup_is_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    """Token lifetime is checked after the current-account read, not only before it begins."""
    source, identity = service()
    source.claims["exp"] = int(utc_now().timestamp()) + 2
    future = utc_now() + timedelta(seconds=3)

    def delayed(subject: str) -> Account:
        """Advance authentication wall time while the provider lookup is in flight."""
        monkeypatch.setattr("payops.auth.identity.utc_now", lambda: future)
        return source.account

    monkeypatch.setattr(source, "lookup", delayed)
    with pytest.raises(AuthenticationDenied):
        identity.authenticate("signed-token")


def test_principal_expiry_is_exactly_bounded_by_token_and_refresh_window() -> None:
    """API principals cannot outlive their token and backend refresh grants last sixty seconds."""
    source, identity = service()
    token_expiry = int(utc_now().timestamp()) + 30
    source.claims["exp"] = token_expiry
    actor = identity.authenticate("signed-token")
    assert actor.expires_at == datetime.fromtimestamp(token_expiry, UTC)
    refreshed = identity.principal("alice")
    assert refreshed is not None
    assert (refreshed.expires_at - refreshed.verified_at).total_seconds() == 60


def test_excessively_delayed_account_lookup_grants_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful provider response after the freshness allowance is not current authority."""
    source, identity = service()
    future = utc_now() + timedelta(seconds=61)

    def delayed(subject: str) -> Account:
        """Return an otherwise valid account after more than sixty seconds of modeled delay."""
        monkeypatch.setattr("payops.auth.identity.utc_now", lambda: future)
        return source.account

    monkeypatch.setattr(source, "lookup", delayed)
    with pytest.raises(AuthenticationDenied):
        identity.authenticate("signed-token")


def test_slow_lookup_cannot_renew_grant_freshness(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fifty-nine-second provider read leaves one second of the original freshness allowance."""
    source, identity = service()
    started = utc_now()
    monkeypatch.setattr("payops.auth.identity.utc_now", lambda: started)

    def delayed(subject: str) -> Account:
        """The account may reflect source state at lookup start rather than response arrival."""
        completed = started + timedelta(seconds=59)
        monkeypatch.setattr("payops.auth.identity.utc_now", lambda: completed)
        return source.account

    monkeypatch.setattr(source, "lookup", delayed)
    principal = identity.authenticate("signed-token")
    assert principal.verified_at == started
    assert principal.expires_at == started + timedelta(seconds=60)
    assert not identity_valid(
        principal, "viewer", "payops-sandbox", started + timedelta(seconds=61)
    )
