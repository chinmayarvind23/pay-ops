"""Queue redelivery must preserve durable work and current backend authority."""

from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from test_graph import collector

from payops.contracts import IncidentCreate, IncidentReport, utc_now
from payops.memory.store import IncidentStore
from payops.orchestrator.graph import InvestigationWorker
from payops.orchestrator.state import InvestigationBudget, InvestigationState
from payops.policy.contracts import Principal
from payops.pubsub_worker import InvestigationRequest, PubSubIncidentWorker


def principal() -> Principal:
    """Return a fresh configured responder rather than trusting queue attributes."""
    now = utc_now()
    return Principal(
        subject="worker",
        roles=("responder",),
        namespaces=("payops-sandbox",),
        verified_at=now,
        expires_at=now + timedelta(minutes=5),
    )


def test_redelivery_reuses_native_graph(tmp_path: Path) -> None:
    """A real SQL store and LangGraph checkpoint survive an unacknowledged delivery."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'incidents.db'}")
    incident = store.create(IncidentCreate(title="Queue incident"), "alert-1")
    calls: list[str] = []
    graph = InvestigationWorker(tmp_path / "graph", collector(calls), pause_before_ranking=True)
    worker = PubSubIncidentWorker(store, graph, principal, mode="local_kind")
    message = MagicMock()
    message.data = InvestigationRequest(incident_id=incident.incident_id).model_dump_json().encode()
    worker.handle(message)
    worker.handle(message)
    assert calls == [incident.incident_id]
    assert message.ack.call_count == 2
    message.nack.assert_not_called()
    saved = store.get(incident.incident_id)
    assert saved is not None and saved.report is not None
    store.close()


@pytest.mark.parametrize(
    "data",
    [b"{", b"x" * 1025, b'{"incident_id":"x","action":"scale"}'],
    ids=["invalid-json", "oversize", "action-injection"],
)
def test_bad_messages_do_not_dispatch(tmp_path: Path, data: bytes) -> None:
    """Invalid envelopes are negatively acknowledged for configured dead-letter handling."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'incidents.db'}")
    investigator = MagicMock()
    worker = PubSubIncidentWorker(store, investigator, principal, mode="local_kind")
    message = MagicMock(data=data)
    worker.handle(message)
    message.nack.assert_called_once()
    message.ack.assert_not_called()
    investigator.start.assert_not_called()
    store.close()


def test_revocation_before_publication(tmp_path: Path) -> None:
    """A completed graph cannot publish under an expired or revoked worker grant."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'incidents.db'}")
    incident = store.create(IncidentCreate(title="Queue incident"), None)
    graph = InvestigationWorker(tmp_path / "graph", collector([]))
    authority = MagicMock(side_effect=[principal(), None])
    worker = PubSubIncidentWorker(store, graph, authority, mode="local_kind")
    message = MagicMock(
        data=InvestigationRequest(incident_id=incident.incident_id).model_dump_json().encode()
    )
    worker.handle(message)
    message.nack.assert_called_once()
    saved = store.get(incident.incident_id)
    assert saved is not None and saved.report is None
    store.close()


def test_wrong_report_mode_not_acknowledged(tmp_path: Path) -> None:
    """Already stored reports must match the worker's configured operational mode."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'incidents.db'}")
    incident = store.create(IncidentCreate(title="Queue incident"), None)
    store.save_report(
        IncidentReport(
            incident_id=incident.incident_id,
            terminal_state="EVIDENCE_INSUFFICIENT",
            mode="fixture_replay",
            duration_seconds=0,
        )
    )
    worker = PubSubIncidentWorker(store, MagicMock(), principal, mode="local_kind")
    with pytest.raises(ValueError, match="scope"):
        worker.process(
            InvestigationRequest(incident_id=incident.incident_id).model_dump_json().encode()
        )
    store.close()


def test_subscription_is_fixed_and_bounded(tmp_path: Path) -> None:
    """Host configuration controls the subscription and limits outstanding work to one."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'incidents.db'}")
    worker = PubSubIncidentWorker(store, MagicMock(), principal, mode="local_kind")
    client = MagicMock()
    with pytest.raises(ValueError):
        worker.subscribe(client, "untrusted-url")
    client.subscribe.assert_not_called()
    result = worker.subscribe(client, "projects/payops-test/subscriptions/incidents")
    assert result is client.subscribe.return_value
    options = client.subscribe.call_args.kwargs
    assert options["flow_control"].max_messages == 1
    assert options["flow_control"].max_bytes == 65536
    assert options["flow_control"].max_lease_duration == 600
    assert options["await_callbacks_on_shutdown"] is True
    store.close()


def test_unfinished_state_is_not_published(tmp_path: Path) -> None:
    """A host that still cannot finish after resume must not cause queue acknowledgement."""
    store = IncidentStore(f"sqlite:///{tmp_path / 'incidents.db'}")
    incident = store.create(IncidentCreate(title="Queue incident"), None)
    investigator = MagicMock()
    investigator.start.return_value = InvestigationState(
        incident=incident, budget=InvestigationBudget()
    )
    investigator.resume.return_value = InvestigationState(
        incident=incident, budget=InvestigationBudget()
    )
    worker = PubSubIncidentWorker(store, investigator, principal, mode="local_kind")
    with pytest.raises(ValueError, match="unfinished"):
        worker.process(
            InvestigationRequest(incident_id=incident.incident_id).model_dump_json().encode()
        )
    saved = store.get(incident.incident_id)
    assert saved is not None and saved.report is None
    store.close()
