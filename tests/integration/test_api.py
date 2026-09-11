"""Exercise the complete local incident loop through HTTP contracts."""

from pathlib import Path

from fastapi.testclient import TestClient

from payops.api import create_app


def test_incident_survives_app_restart(tmp_path: Path) -> None:
    """A successful response must represent persisted, reloadable work."""
    url = f"sqlite:///{tmp_path / 'incidents.db'}"
    with TestClient(create_app(url)) as client:
        response = client.post(
            "/api/incidents",
            json={"title": "Payment failures"},
            headers={"Idempotency-Key": "alert-1"},
        )
        assert response.status_code == 201
        incident_id = response.json()["incident_id"]
        duplicate = client.post(
            "/api/incidents",
            json={"title": "Payment failures"},
            headers={"Idempotency-Key": "alert-1"},
        )
        assert duplicate.json()["incident_id"] == incident_id
        result = client.post(f"/api/incidents/{incident_id}/investigate")
        assert result.status_code == 200
        assert result.json()["mode"] == "mock"
        assert result.json()["evidence"][0]["incident_id"] == incident_id
        assert client.post(f"/api/incidents/{incident_id}/investigate").json() == result.json()
    with TestClient(create_app(url)) as restarted:
        assert restarted.get(f"/api/incidents/{incident_id}").json()["report"] == result.json()


def test_conflict_and_validation(tmp_path: Path) -> None:
    """Idempotency keys cannot silently replace a different incident payload."""
    with TestClient(create_app(f"sqlite:///{tmp_path / 'incidents.db'}")) as client:
        assert client.get("/api/health").json()["status"] == "ok"
        assert client.get("/api/incidents/absent").status_code == 404
        assert client.post("/api/incidents/absent/investigate").status_code == 404
        assert client.post("/api/incidents", json={"title": "", "shell": "x"}).status_code == 422
        client.post("/api/incidents", json={"title": "A"}, headers={"Idempotency-Key": "same"})
        conflict = client.post(
            "/api/incidents", json={"title": "B"}, headers={"Idempotency-Key": "same"}
        )
        assert conflict.status_code == 409
