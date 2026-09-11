"""Real Firebase SDK signature/claim verification with local RSA keys and mocked HTTPS responses."""

import base64
import json
from collections.abc import Iterator
from datetime import timedelta
from io import BytesIO
from typing import Any
from uuid import uuid4

import firebase_admin  # pyright: ignore[reportMissingTypeStubs]
import pytest
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.x509.oid import NameOID
from firebase_admin import credentials  # pyright: ignore[reportMissingTypeStubs]
from google.auth.credentials import AnonymousCredentials

from payops.auth.firebase import FirebaseIdentitySource
from payops.auth.identity import AuthenticationDenied, IdentityService, IdentitySettings
from payops.contracts import utc_now


class OfflineCredential(credentials.Base):
    """A credential with no secret and no network refresh is used only in this offline SDK test."""

    def get_credential(self) -> AnonymousCredentials:
        """Construct the SDK transport without loading user credentials."""
        return AnonymousCredentials()


class Fixture:
    """Real token signatures and provider-shaped responses exercise the actual pinned SDK."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, tenant: str | None = None) -> None:
        """Generate a transient signing key and intercept only the two expected Google endpoints."""
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        now = utc_now()
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "offline-identity-test")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(self.key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=1))
            .sign(self.key, hashes.SHA256())
        )
        self.certificate = cert.public_bytes(serialization.Encoding.PEM).decode()
        self.account: dict[str, Any] = {
            "localId": "alice",
            "disabled": False,
            "validSince": str(int(now.timestamp()) - 300),
            "customAttributes": json.dumps(
                {"payops": {"roles": ["responder"], "namespaces": ["payops-sandbox"]}}
            ),
        }
        self.claims: dict[str, Any] = {
            "sub": "alice",
            "aud": "payops-project",
            "iss": "https://securetoken.google.com/payops-project",
            "iat": int(now.timestamp()) - 10,
            "auth_time": int(now.timestamp()) - 20,
            "exp": int(now.timestamp()) + 3590,
            "firebase": {},
        }
        if tenant is not None:
            self.claims["firebase"] = {"tenant": tenant}
            self.account["tenantId"] = tenant
        account_url = "https://identitytoolkit.googleapis.com/v1/projects/payops-project"
        account_url += f"/tenants/{tenant}" if tenant else ""
        account_url += "/accounts:lookup"
        self.calls: list[str] = []
        fixture = self

        def request(
            session: requests.Session, method: str, url: str, **kwargs: Any
        ) -> requests.Response:
            """Unknown requests fail the test instead of leaking to the network."""
            fixture.calls.append(url)
            if (
                url
                == "https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com"
            ):
                body: dict[str, Any] = {"fixture-key": fixture.certificate}
            elif url == account_url:
                assert kwargs["json"] == {"localId": ["alice"]}
                body = {"users": [fixture.account]}
            else:
                raise AssertionError("unexpected identity SDK endpoint")
            response = requests.Response()
            response.status_code, response.url = 200, url
            response.raw = BytesIO(json.dumps(body).encode())
            return response

        monkeypatch.setattr(requests.Session, "request", request)
        self.app = firebase_admin.initialize_app(  # pyright: ignore[reportUnknownMemberType]
            OfflineCredential(),
            {"projectId": "payops-project", "httpTimeout": 5},
            name=f"payops-offline-{uuid4()}",
        )
        settings = IdentitySettings(project_id="payops-project", tenant_id=tenant)
        self.source = FirebaseIdentitySource(self.app, settings)
        self.identity = IdentityService(settings, self.source)

    def token(self, *, wrong_key: bool = False, algorithm: str = "RS256") -> str:
        """Encode a genuine RS256 JWT; altered signing keys must fail actual SDK verification."""
        header = {"alg": algorithm, "kid": "fixture-key"}
        chunks = [
            base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=")
            for value in (header, self.claims)
        ]
        signed = b".".join(chunks)
        key = (
            rsa.generate_private_key(public_exponent=65537, key_size=2048)
            if wrong_key
            else self.key
        )
        signature = key.sign(signed, padding.PKCS1v15(), hashes.SHA256())
        return (signed + b"." + base64.urlsafe_b64encode(signature).rstrip(b"=")).decode()


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> Iterator[Fixture]:
    """Delete each test app even when a signature or claim assertion fails."""
    fixture = Fixture(monkeypatch)
    try:
        yield fixture
    finally:
        firebase_admin.delete_app(fixture.app)  # pyright: ignore[reportUnknownMemberType]


def test_real_sdk_verifies_signature_and_refreshes_account(sdk: Fixture) -> None:
    """The SDK validates RSA and queries revocation before a second current-grant lookup."""
    principal = sdk.identity.authenticate(sdk.token())
    assert principal.subject == "alice" and principal.roles == ("responder",)
    assert sum(url.endswith("accounts:lookup") for url in sdk.calls) == 2
    assert any("securetoken@" in url for url in sdk.calls)


@pytest.mark.parametrize(
    "case", ["signature", "algorithm", "issuer", "audience", "expired", "revoked", "disabled"]
)
def test_real_sdk_rejects_invalid_tokens_or_accounts(sdk: Fixture, case: str) -> None:
    """Cryptographic and provider revocation failures pass through the stable denial boundary."""
    if case == "issuer":
        sdk.claims["iss"] = "https://attacker.invalid"
    elif case == "audience":
        sdk.claims["aud"] = "foreign-project"
    elif case == "expired":
        sdk.claims["exp"] = int(utc_now().timestamp()) - 60
    elif case == "revoked":
        sdk.account["validSince"] = str(int(utc_now().timestamp()))
    elif case == "disabled":
        sdk.account["disabled"] = True
    token = sdk.token(
        wrong_key=case == "signature", algorithm="none" if case == "algorithm" else "RS256"
    )
    with pytest.raises(AuthenticationDenied):
        sdk.identity.authenticate(token)


@pytest.mark.parametrize("project,timeout", [("foreign-project", 5), ("payops-project", 120)])
def test_sdk_configuration_must_match_operator_scope(project: str, timeout: int) -> None:
    """The host cannot silently select a different app project or the SDK's long default timeout."""
    app = firebase_admin.initialize_app(  # pyright: ignore[reportUnknownMemberType]
        OfflineCredential(),
        {"projectId": project, "httpTimeout": timeout},
        name=f"payops-misconfigured-{uuid4()}",
    )
    try:
        with pytest.raises(ValueError, match="configured project"):
            FirebaseIdentitySource(app, IdentitySettings(project_id="payops-project"))
    finally:
        firebase_admin.delete_app(app)  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.parametrize("mismatch", [None, "token", "account"])
def test_real_sdk_tenant_is_bound_to_token_and_account(
    monkeypatch: pytest.MonkeyPatch, mismatch: str | None
) -> None:
    """Tenant client URLs and signed claims must agree, as must the returned current account."""
    fixture = Fixture(monkeypatch, tenant="tenant-one")
    try:
        if mismatch == "token":
            fixture.claims["firebase"] = {"tenant": "tenant-two"}
        elif mismatch == "account":
            fixture.account["tenantId"] = "tenant-two"
        if mismatch is None:
            assert fixture.identity.authenticate(fixture.token()).subject == "alice"
            assert sum("/tenants/tenant-one/accounts:lookup" in url for url in fixture.calls) == 2
        else:
            with pytest.raises(AuthenticationDenied):
                fixture.identity.authenticate(fixture.token())
    finally:
        firebase_admin.delete_app(fixture.app)  # pyright: ignore[reportUnknownMemberType]


def test_direct_sdk_binding_rechecks_late_emulator_toggle(
    sdk: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Changing the environment after client creation cannot activate unsigned SDK verification."""
    token = sdk.token()
    monkeypatch.setenv("FIREBASE_AUTH_EMULATOR_HOST", "127.0.0.1:9099")
    with pytest.raises(ValueError, match="emulator"):
        sdk.source.verify(token)
    with pytest.raises(ValueError, match="emulator"):
        sdk.source.lookup("alice")
    assert sdk.calls == []


def test_new_token_iat_cannot_resurrect_an_older_revoked_session(sdk: Fixture) -> None:
    """SDK iat revocation checking is supplemented by the original authentication-time check."""
    now = int(utc_now().timestamp())
    sdk.claims.update(iat=now - 1, auth_time=now - 20, exp=now + 3590)
    sdk.account["validSince"] = str(now - 10)
    token = sdk.token()
    # The pinned SDK compares iat. The service must additionally compare auth_time.
    assert sdk.source.verify(token)["sub"] == "alice"
    with pytest.raises(AuthenticationDenied):
        sdk.identity.authenticate(token)
