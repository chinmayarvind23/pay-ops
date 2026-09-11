"""Authenticated HTTP routes cannot derive subject, scope or report authority from request text."""

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from fastapi.testclient import TestClient

from payops.auth.identity import AuthenticationDenied
from payops.contracts import Incident, IncidentCreate, IncidentReport, utc_now
from payops.memory.store import IncidentStore
from payops.policy.contracts import Principal
from payops.protected_api import Mode, create_protected_app


class Identity:
    """Provider transport is tested separately; this fixture selects verified HTTP actor grants."""

    def __init__(self) -> None:
        """Keep revocation and token-use records outside request-controlled input."""
        self.revoked = False
        self.calls: list[str] = []

    def authenticate(self, token: str) -> Principal:
        """Only two predetermined credentials exist; request bodies cannot create an identity."""
        self.calls.append(token)
        if self.revoked or token not in {"responder-token", "viewer-token", "foreign-token"}:
            raise AuthenticationDenied("AUTHENTICATION_DENIED")
        now = utc_now()
        return Principal(
            subject="alice" if token == "responder-token" else "bob",
            roles=("responder",) if token == "responder-token" else ("viewer",),
            namespaces=("foreign",) if token == "foreign-token" else ("payops-sandbox",),
            verified_at=now,
            expires_at=now + timedelta(seconds=60),
        )


class Runtime:
    """The configured investigator records scope and can expose a downstream-boundary failure."""

    def __init__(self, root: Path) -> None:
        """Every test owns a separate database and no operational executor or credentials."""
        self.store = IncidentStore(f"sqlite:///{root / 'incidents.db'}")
        self.identity = Identity()
        self.calls: list[str] = []
        self.failure: str | None = None

    def investigate(self, incident: Incident) -> IncidentReport:
        """No credential or actor string is passed into the investigation callback."""
        self.calls.append(incident.incident_id)
        if self.failure == "revoke":
            self.identity.revoked = True
        return IncidentReport(
            incident_id="foreign" if self.failure == "incident" else incident.incident_id,
            terminal_state="EVIDENCE_INSUFFICIENT",
            mode="mock" if self.failure == "mode" else "fixture_replay",
            duration_seconds=0,
        )


@pytest.fixture
def runtime(tmp_path: Path) -> Iterator[Runtime]:
    """Release the owned engine even when an authorization assertion fails."""
    result = Runtime(tmp_path)
    try:
        yield result
    finally:
        result.store.close()


def client(runtime: Runtime) -> TestClient:
    """Construct a protected fixture app whose mode cannot be changed by an HTTP request."""
    return TestClient(
        create_protected_app(
            runtime.store, runtime.identity, runtime.investigate, mode="fixture_replay"
        )
    )


def headers(token: str = "responder-token") -> dict[str, str]:
    """Known fixture bearer values are not real credentials."""
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.parametrize("mode", ["mock", "", "production"])
def test_invalid_runtime_mode(runtime: Runtime, mode: str) -> None:
    """Untyped host configuration cannot expand the operational mode allowlist."""
    with pytest.raises(ValueError, match="Operational mode"):
        create_protected_app(
            runtime.store, runtime.identity, runtime.investigate, mode=cast(Mode, mode)
        )


def test_authenticated_incident_loop_and_read_only_role(runtime: Runtime) -> None:
    """A responder creates/investigates while a viewer can only read the same namespace."""
    with client(runtime) as http:
        assert http.get("/api/health").json()["mode"] == "fixture_replay"
        created = http.post("/api/incidents", json={"title": "Unavailable"}, headers=headers())
        assert created.status_code == 201
        incident_id = created.json()["incident_id"]
        path = f"/api/incidents/{incident_id}"
        assert http.get(path, headers=headers("viewer-token")).status_code == 200
        assert http.post(path + "/investigate", headers=headers("viewer-token")).status_code == 403
        result = http.post(path + "/investigate", headers=headers())
        assert result.status_code == 200 and result.json()["mode"] == "fixture_replay"
        assert http.post(path + "/investigate", headers=headers()).json() == result.json()
        assert runtime.calls == [incident_id]


@pytest.mark.parametrize("credential", [None, "Bearer invalid", "Basic responder-token"])
def test_missing_or_invalid_authentication_prevents_work(
    runtime: Runtime, credential: str | None
) -> None:
    """An actor field in JSON cannot compensate for an absent or invalid bearer credential."""
    with client(runtime) as http:
        response = http.post(
            "/api/incidents",
            json={"title": "Unavailable"},
            headers={"Authorization": credential} if credential else {},
        )
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == "Bearer"
        assert runtime.calls == []


@pytest.mark.parametrize("change", ["viewer", "namespace", "service", "actor"])
def test_request_cannot_expand_actor_authority(runtime: Runtime, change: str) -> None:
    """Body fields, a foreign namespace and broad service names cannot create operational grants."""
    body = {"title": "Unavailable"}
    if change == "namespace":
        body["namespace"] = "kube-system"
    elif change == "service":
        body["service"] = "other-service"
    elif change == "actor":
        body["subject"] = "admin"
    with client(runtime) as http:
        response = http.post(
            "/api/incidents",
            json=body,
            headers=headers("viewer-token" if change == "viewer" else "responder-token"),
        )
        assert response.status_code == (422 if change == "actor" else 403)


def test_cross_namespace_and_absent_incidents_share_not_found(runtime: Runtime) -> None:
    """A token cannot discover whether another namespace's incident identifier exists."""
    incident = runtime.store.create(IncidentCreate(title="Unavailable"), None)
    with client(runtime) as http:
        foreign = http.get(
            f"/api/incidents/{incident.incident_id}", headers=headers("foreign-token")
        )
        absent = http.get("/api/incidents/absent", headers=headers("foreign-token"))
        assert foreign.status_code == absent.status_code == 404
        assert foreign.json() == absent.json()


@pytest.mark.parametrize("failure,expected", [("revoke", 401), ("incident", 502), ("mode", 502)])
def test_late_revocation_or_wrong_report_cannot_publish(
    runtime: Runtime, failure: str, expected: int
) -> None:
    """The callback cannot publish across mode/incident boundaries or past actor revocation."""
    incident = runtime.store.create(IncidentCreate(title="Unavailable"), None)
    runtime.failure = failure
    with client(runtime) as http:
        response = http.post(
            f"/api/incidents/{incident.incident_id}/investigate", headers=headers()
        )
        assert response.status_code == expected
    saved = runtime.store.get(incident.incident_id)
    assert saved is not None and saved.report is None


def test_idempotency_conflict_has_stable_authenticated_response(runtime: Runtime) -> None:
    """Reusing an ingestion key cannot silently replace another incident's payload."""
    scoped_headers = headers() | {"Idempotency-Key": "alert"}
    with client(runtime) as http:
        assert (
            http.post("/api/incidents", json={"title": "A"}, headers=scoped_headers).status_code
            == 201
        )
        response = http.post("/api/incidents", json={"title": "B"}, headers=scoped_headers)
        assert response.status_code == 409 and response.json()["detail"] == "IDEMPOTENCY_CONFLICT"


def test_preexisting_foreign_service_cannot_reach_investigator(runtime: Runtime) -> None:
    """Imports or another API may create records that bypass this app's ingestion allowlist."""
    incident = runtime.store.create(
        IncidentCreate(title="Imported", service="foreign-service"), None
    )
    with client(runtime) as http:
        path = f"/api/incidents/{incident.incident_id}"
        assert http.get(path, headers=headers()).status_code == 403
        assert http.post(path + "/investigate", headers=headers()).status_code == 403
    assert runtime.calls == []
    saved = runtime.store.get(incident.incident_id)
    assert saved is not None and saved.report is None


def test_expired_principal_is_not_accepted_from_verifier(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The HTTP boundary does not assume a returned principal remains valid indefinitely."""
    expired = runtime.identity.authenticate("responder-token").model_copy(
        update={"expires_at": utc_now() - timedelta(seconds=1)}
    )

    def stale(token: str) -> Principal:
        """Simulate a verifier cache returning an expired grant snapshot."""
        return expired

    monkeypatch.setattr(runtime.identity, "authenticate", stale)
    with client(runtime) as http:
        assert (
            http.post("/api/incidents", json={"title": "Denied"}, headers=headers()).status_code
            == 401
        )


def test_concurrent_foreign_mode_winner_is_not_returned_as_local(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A canonical report won by another configured mode cannot be relabeled by this API."""
    incident = runtime.store.create(IncidentCreate(title="Concurrent"), None)

    def competing(incident: Incident) -> IncidentReport:
        """Commit another mode before returning the locally computed fixture report."""
        report = runtime.investigate(incident)
        runtime.store.save_report(report.model_copy(update={"mode": "mock"}))
        return report

    app = create_protected_app(runtime.store, runtime.identity, competing, mode="fixture_replay")
    with TestClient(app) as http:
        response = http.post(
            f"/api/incidents/{incident.incident_id}/investigate", headers=headers()
        )
        assert response.status_code == 409 and response.json()["detail"] == "REPORT_MODE_MISMATCH"
