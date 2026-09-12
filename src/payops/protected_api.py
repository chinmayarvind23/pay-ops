"""Authenticated incident endpoints bind every read and investigation to backend actor scope."""

from collections.abc import Callable
from typing import Annotated, Literal, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from payops.auth.identity import AuthenticationDenied
from payops.contracts import Incident, IncidentCreate, IncidentReport, utc_now
from payops.memory.store import IdempotencyConflict, IncidentStore
from payops.policy.contracts import Principal
from payops.policy.engine import identity_valid
from payops.remediation.broker import RemediationBroker
from payops.tools.kubernetes import SERVICES

Mode = Literal["local_kind", "cloud_gke", "fixture_replay"]
Bearer = Annotated[HTTPAuthorizationCredentials | None, Depends(HTTPBearer(auto_error=False))]
Investigator = Callable[[Incident], IncidentReport]


class Authenticator(Protocol):
    """The configured verifier authenticates bearer tokens; a request never supplies a subject."""

    def authenticate(self, token: str) -> Principal:
        """Return current verified grants or raise the stable authentication-denied error."""
        ...


def actor(identity: Authenticator, credential: HTTPAuthorizationCredentials | None) -> Principal:
    """Missing or invalid authentication reveals no provider details or token material."""
    if credential is None or credential.scheme.lower() != "bearer":
        raise HTTPException(
            401, detail="AUTHENTICATION_REQUIRED", headers={"WWW-Authenticate": "Bearer"}
        )
    try:
        principal = identity.authenticate(credential.credentials)
        if not principal.enabled or principal.expires_at <= utc_now():
            raise AuthenticationDenied("AUTHENTICATION_DENIED")
        return principal
    except AuthenticationDenied:
        raise HTTPException(
            401, detail="AUTHENTICATION_DENIED", headers={"WWW-Authenticate": "Bearer"}
        ) from None


def authorize(principal: Principal, namespace: str, *, write: bool = False) -> None:
    """Every request checks scope and freshness; broad roles never bypass the synthetic boundary."""
    roles = ("responder",) if write else ("viewer", "responder", "approver", "executor")
    if namespace != "payops-sandbox" or not any(
        identity_valid(principal, role, namespace, utc_now()) for role in roles
    ):
        raise HTTPException(403, detail="SCOPE_DENIED")


def scoped_incident(store: IncidentStore, incident_id: str, principal: Principal) -> Incident:
    """Absent and out-of-scope incidents share a not-found response to avoid existence leaks."""
    incident = store.get(incident_id)
    if incident is None or incident.request.namespace not in principal.namespaces:
        raise HTTPException(404, detail="INCIDENT_NOT_FOUND")
    authorize(principal, incident.request.namespace)
    if incident.request.service not in SERVICES:
        raise HTTPException(403, detail="SERVICE_DENIED")
    return incident


def attach_remediation(
    app: FastAPI,
    store: IncidentStore,
    identity: Authenticator,
    mode: Mode,
    remediation: RemediationBroker | None,
) -> None:
    """Require explicit same-mode broker wiring without enabling actions on the default server."""
    if remediation is None:
        return
    from payops.remediation.api import action_router

    if remediation.mode != mode:
        raise ValueError("Remediation mode must match operational mode")
    app.include_router(action_router(store, identity, remediation))


def create_protected_app(
    store: IncidentStore,
    identity: Authenticator,
    investigate: Investigator,
    *,
    mode: Mode,
    remediation: RemediationBroker | None = None,
) -> FastAPI:
    """Host wiring owns all dependency lifetimes; there is no default operational setup."""
    if mode not in {"local_kind", "cloud_gke", "fixture_replay"}:
        raise ValueError("Operational mode must be explicitly configured")
    app = FastAPI(title="PayOps authenticated incidents", version="0.1.0")
    attach_remediation(app, store, identity, mode, remediation)

    @app.get("/api/health")
    def health() -> dict[str, str]:
        """Liveness discloses the fixed runtime mode without reading incident or identity data."""
        return {"status": "ok", "mode": mode}

    @app.post("/api/incidents", status_code=201)
    def create(
        request: IncidentCreate,
        bearer: Bearer,
        idempotency_key: Annotated[str | None, Header(min_length=1, max_length=128)] = None,
    ) -> Incident:
        """A current responder may ingest only an allowlisted synthetic incident."""
        principal = actor(identity, bearer)
        authorize(principal, request.namespace, write=True)
        if request.service not in SERVICES:
            raise HTTPException(403, detail="SERVICE_DENIED")
        try:
            return store.create(request, idempotency_key)
        except IdempotencyConflict:
            raise HTTPException(409, detail="IDEMPOTENCY_CONFLICT") from None

    @app.get("/api/incidents/{incident_id}")
    def get(incident_id: str, bearer: Bearer) -> Incident:
        """Read permission never implies permission to start an operational investigation."""
        return scoped_incident(store, incident_id, actor(identity, bearer))

    @app.post("/api/incidents/{incident_id}/investigate")
    def run(incident_id: str, bearer: Bearer) -> IncidentReport:
        """Reauthenticate after investigation before publishing or returning its result."""
        principal = actor(identity, bearer)
        incident = scoped_incident(store, incident_id, principal)
        authorize(principal, incident.request.namespace, write=True)
        report = incident.report or investigate(incident)
        if report.incident_id != incident_id or report.mode != mode:
            raise HTTPException(502, detail="INVESTIGATOR_SCOPE_MISMATCH")
        authorize(actor(identity, bearer), incident.request.namespace, write=True)
        saved = store.save_report(report)
        if saved.report is None or saved.report.mode != mode:
            raise HTTPException(409, detail="REPORT_MODE_MISMATCH")
        return saved.report

    return app
