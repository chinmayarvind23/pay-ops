"""Local API walking skeleton; operational authentication is a later release gate."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException

from payops.contracts import Incident, IncidentCreate, IncidentReport
from payops.memory.store import IdempotencyConflict, IncidentStore
from payops.orchestrator.mock import investigate_mock


def create_app(database_url: str = "sqlite:///payops-local.db") -> FastAPI:
    """Use a factory so tests and multiple deployments cannot share global stores."""
    store = IncidentStore(database_url)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        """Close connections even when request handling exits exceptionally."""
        try:
            yield
        finally:
            store.close()

    app = FastAPI(title="PayOps", version="0.1.0", lifespan=lifespan)

    @app.get("/api/health")
    def health() -> dict[str, str]:
        """Expose mode so mock plumbing cannot be confused with live diagnostics."""
        return {"status": "ok", "mode": "mock"}

    @app.post("/api/incidents", status_code=201)
    def create_incident(
        request: IncidentCreate,
        idempotency_key: Annotated[str | None, Header(max_length=128, min_length=1)] = None,
    ) -> Incident:
        """Persist alert acceptance before returning an identifier."""
        try:
            return store.create(request, idempotency_key)
        except IdempotencyConflict as error:
            raise HTTPException(
                409,
                detail={"code": "IDEMPOTENCY_CONFLICT", "message": str(error), "retryable": False},
            ) from error

    @app.get("/api/incidents/{incident_id}")
    def get_incident(incident_id: str) -> Incident:
        """Missing identifiers return a stable error without revealing store internals."""
        incident = store.get(incident_id)
        if incident is None:
            raise HTTPException(
                404,
                detail={
                    "code": "NOT_FOUND",
                    "message": "Incident not found",
                    "retryable": False,
                    "incident_id": incident_id,
                },
            )
        return incident

    @app.post("/api/incidents/{incident_id}/investigate")
    def investigate(incident_id: str) -> IncidentReport:
        """Only mock reads run here; no operational executor is reachable."""
        incident = get_incident(incident_id)
        if incident.report is not None:
            return incident.report
        report = investigate_mock(incident)
        store.save_report(report)
        return report

    return app
