"""GraphQL and Slack reuse backend identity; neither surface grants remediation authority."""

import json
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from test_protected_api import Runtime, headers

from payops.contracts import (
    EvidenceItem,
    IncidentCreate,
    IncidentReport,
    RootCauseHypothesis,
    utc_now,
)
from payops.protected_api import create_protected_app
from payops.slack_notifications import SlackNotifier

HOOK = "https://hooks.slack.com/services/Tfixture/Bfixture/secretfixture"


def test_graphql_nested_evidence_and_authorization(tmp_path: Path) -> None:
    """An authorized viewer can explore paged evidence but cannot read foreign incident scope."""
    runtime = Runtime(tmp_path)
    item = runtime.store.create(IncidentCreate(title="fixture"), None)
    now = utc_now()
    evidence = EvidenceItem(
        evidence_id="e1",
        incident_id=item.incident_id,
        source="KUBERNETES",
        observed_at=now,
        collected_at=now,
        query="fixture",
        resource="payments-api",
        artifact_uri="private/path",
        artifact_sha256="a" * 64,
        summary="observed unavailable",
    )
    runtime.store.save_report(
        IncidentReport(
            incident_id=item.incident_id,
            terminal_state="ESCALATED",
            mode="fixture_replay",
            duration_seconds=1,
            evidence=(evidence,),
            ranked_root_causes=(
                RootCauseHypothesis(
                    cause_code="PROCESSOR_UNAVAILABLE",
                    confidence=0.9,
                    supporting_evidence_ids=("e1",),
                ),
            ),
        )
    )
    query = (
        "query($id:ID!){incident(id:$id){id title evidence(limit:1){id source summary} "
        "report{mode causes{code supportingEvidenceIds}}}}"
    )
    try:
        with TestClient(
            create_protected_app(
                runtime.store, runtime.identity, runtime.investigate, mode="fixture_replay"
            )
        ) as client:
            payload = {"query": query, "variables": {"id": item.incident_id}}
            response = client.post("/api/graphql", json=payload, headers=headers("viewer-token"))
            assert response.status_code == 200
            result = response.json()["data"]["incident"]
            assert result["evidence"][0]["id"] == "e1"
            assert "private/path" not in response.text
            assert result["report"]["causes"][0]["supportingEvidenceIds"] == ["e1"]
            assert client.post("/api/graphql", json=payload).status_code == 401
            denied = client.post("/api/graphql", json=payload, headers=headers("foreign-token"))
            assert denied.json() == {"data": None, "errors": [{"message": "QUERY_REJECTED"}]}
            runtime.identity.revoked = True
            assert client.post("/api/graphql", json=payload, headers=headers()).status_code == 401
    finally:
        runtime.store.close()


@pytest.mark.parametrize(
    "query",
    [
        'mutation{incident(id:"x"){id}}',
        "{__schema{queryType{name}}}",
        'query A{incident(id:"x"){id}} query B{incident(id:"x"){id}}',
        '{incident(id:"x"){...I}} fragment I on Incident{id}',
        "{broken",
        "{" + " ".join(f'a{i}:incident(id:"x"){{id}}' for i in range(30)) + "}",
        '{incident(id:"x"){a{b{c{d{e}}}}}}',
    ],
)
def test_graphql_rejects_mutations_fragments_and_resource_exhaustion(
    tmp_path: Path, query: str
) -> None:
    """Invalid operations fail before any incident lookup or operational callback."""
    runtime = Runtime(tmp_path)
    try:
        with TestClient(
            create_protected_app(
                runtime.store, runtime.identity, runtime.investigate, mode="fixture_replay"
            )
        ) as client:
            response = client.post("/api/graphql", json={"query": query}, headers=headers())
            assert response.status_code == 400
            assert runtime.calls == []
    finally:
        runtime.store.close()


def test_slack_notification_requires_responder_and_contains_only_reference(tmp_path: Path) -> None:
    """Viewer and foreign callers never dispatch; notification cannot invoke an investigator."""
    requests: list[httpx.Request] = []

    def serve(request: httpx.Request) -> httpx.Response:
        """Capture the exact Slack wire payload without sending a real message."""
        requests.append(request)
        return httpx.Response(200, text="ok")

    runtime = Runtime(tmp_path)
    item = runtime.store.create(IncidentCreate(title="@channel secret untrusted title"), None)
    notifier = SlackNotifier(
        SecretStr(HOOK), "https://operator.example", httpx.MockTransport(serve)
    )
    try:
        with TestClient(
            create_protected_app(
                runtime.store,
                runtime.identity,
                runtime.investigate,
                mode="fixture_replay",
                slack=notifier,
            )
        ) as client:
            path = f"/api/incidents/{item.incident_id}/notifications/slack"
            assert client.post(path).status_code == 401
            assert client.post(path, headers=headers("viewer-token")).status_code == 403
            assert client.post(path, headers=headers("foreign-token")).status_code == 404
            assert requests == []
            assert client.post(path, headers=headers()).status_code == 202
            assert len(requests) == 1 and runtime.calls == []
            body = json.loads(requests[0].content)
            assert item.incident_id in body["text"] and "@channel" not in str(body)
            assert body["blocks"][0]["text"]["type"] == "plain_text"
    finally:
        runtime.store.close()


@pytest.mark.parametrize("result", ["timeout", "redirect", "rejected", "oversized", "wrong_body"])
def test_slack_failure_is_bounded_redacted_and_never_retried(result: str) -> None:
    """A possibly delivered message is not resent automatically and webhook secrets never escape."""
    calls = 0

    def serve(request: httpx.Request) -> httpx.Response:
        """Represent failures with actual HTTPX responses and exceptions."""
        nonlocal calls
        calls += 1
        if result == "timeout":
            raise httpx.ReadTimeout(HOOK, request=request)
        status = 302 if result == "redirect" else 429 if result == "rejected" else 200
        return httpx.Response(
            status,
            text="x" * 1025 if result == "oversized" else "no",
            headers={"location": "https://foreign.example"},
        )

    notifier = SlackNotifier(
        SecretStr(HOOK), "https://operator.example", httpx.MockTransport(serve)
    )
    with pytest.raises(RuntimeError, match="^SLACK_DELIVERY_UNCONFIRMED$"):
        notifier.send("incident1")
    assert calls == 1


@pytest.mark.parametrize(
    "hook,origin",
    [
        ("https://foreign.example/services/a/b/c", "https://operator.example"),
        (HOOK, "http://operator.example"),
        (HOOK, "https://user:secret@operator.example"),
        (HOOK, "https://operator.example/path"),
    ],
)
def test_slack_destination_is_host_configured_and_closed(hook: str, origin: str) -> None:
    """A configured URL cannot redirect secrets or embed caller-selected credentials."""
    with pytest.raises(ValueError):
        SlackNotifier(SecretStr(hook), origin)


def test_graphql_empty_report_and_invalid_pagination(tmp_path: Path) -> None:
    """New incidents expose no invented report, and invalid page bounds return a stable error."""
    runtime = Runtime(tmp_path)
    try:
        item = runtime.store.create(IncidentCreate(title="new incident"), None)
        with TestClient(
            create_protected_app(
                runtime.store, runtime.identity, runtime.investigate, mode="fixture_replay"
            )
        ) as client:
            query = "query($id:ID!){incident(id:$id){report{mode} evidence{summary}}}"
            result = client.post(
                "/api/graphql",
                json={"query": query, "variables": {"id": item.incident_id}},
                headers=headers(),
            )
            assert result.json()["data"]["incident"] == {"report": None, "evidence": []}
            query = "query($id:ID!){incident(id:$id){evidence(limit:51){summary}}}"
            result = client.post(
                "/api/graphql",
                json={"query": query, "variables": {"id": item.incident_id}},
                headers=headers(),
            )
            assert result.json()["data"] is None and result.json()["errors"]
    finally:
        runtime.store.close()


def test_slack_route_reports_unconfirmed_delivery_without_secret(tmp_path: Path) -> None:
    """The API reports delivery failure without exposing webhook credentials or provider content."""

    def serve(request: httpx.Request) -> httpx.Response:
        """Use a rejected fixture response without contacting Slack."""
        return httpx.Response(403, text=HOOK)

    runtime = Runtime(tmp_path)
    notifier = SlackNotifier(
        SecretStr(HOOK), "https://operator.example", httpx.MockTransport(serve)
    )
    try:
        item = runtime.store.create(IncidentCreate(title="fixture"), None)
        with TestClient(
            create_protected_app(
                runtime.store,
                runtime.identity,
                runtime.investigate,
                mode="fixture_replay",
                slack=notifier,
            )
        ) as client:
            result = client.post(
                f"/api/incidents/{item.incident_id}/notifications/slack", headers=headers()
            )
            assert result.status_code == 502 and "secretfixture" not in result.text
        with pytest.raises(ValueError, match="invalid incident reference"):
            notifier.send("../@channel")
    finally:
        runtime.store.close()
