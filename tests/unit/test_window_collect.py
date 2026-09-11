"""The window plan counts fixed backend reads and never obtains scenario labels or gold inputs."""

import json
from datetime import timedelta
from pathlib import Path

import pytest
from test_payment_window import interval, raw_snapshot

from payops.contracts import Incident, IncidentCreate, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import Observation
from payops.evidence.payment_window import Service, snapshot_observation, verify_payment_window
from payops.tools.kubernetes import KubernetesRead
from payops.tools.payment import PaymentRead
from payops.tools.window_collect import WindowCollector, window_plan


class Calls:
    """An independent transport counter verifies the reservation existed before dispatch."""

    def __init__(self, root: Path) -> None:
        """Record exact logical dispatch order and costs without any live Kubernetes calls."""
        self.root = root
        self.operations: list[str] = []
        self.reads = 0
        self.fail = False

    def record(self, operation: str, cost: int) -> None:
        """A fixed plan must already be durable when an adapter is entered."""
        plan = json.loads((self.root / "window-plan.json").read_text())
        assert plan["backend_reads"] == 34 and plan["logical_operations"] == 20
        self.operations.append(operation)
        self.reads += cost


class Kubernetes(KubernetesRead):
    """Only counting and source observations are simulated; no commands exist in this fixture."""

    def __init__(self, calls: Calls) -> None:
        """Bind an independent counter in place of a kubectl process."""
        self.calls = calls

    def collect(self, service: str) -> tuple[Observation, ...]:
        """Deployment plus pods consumes two reserved commands."""
        self.calls.record("status", 2)
        return ()

    def events(self, service: str) -> tuple[Observation, ...]:
        """Current pod ownership plus events consumes two reserved commands."""
        self.calls.record("events", 2)
        return ()

    def logs(self, service: str) -> Observation:
        """One log command can fail without deleting other retained artifacts."""
        self.calls.record("logs", 1)
        if self.calls.fail:
            raise ValueError("private provider failure")
        return Observation(
            source="LOG",
            resource=service,
            observed_at=utc_now(),
            query="logs",
            summary="Synthetic log",
        )


class Payment(PaymentRead):
    """Source timestamps deliberately remain old so the collector must publish missing windows."""

    def __init__(self, calls: Calls) -> None:
        """No Prometheus connection is configured for plan verification."""
        self.calls = calls
        self.interval = interval()

    def snapshot(self, service: Service, at: object = None) -> Observation:
        """Each snapshot has two charged reads even when its first read fails."""
        self.calls.record("snapshot", 2)
        if self.calls.fail:
            raise ValueError("private provider failure")
        return snapshot_observation(raw_snapshot(self.interval, service=service), service)

    def query(self, signal: str) -> Observation:
        """Only the fixed targets query belongs to this collection profile."""
        assert signal == "targets"
        self.calls.record("targets", 1)
        return Observation(
            source="PROMETHEUS",
            resource="sandbox-scrapes",
            observed_at=utc_now(),
            query="up",
            summary="Targets observed",
        )


@pytest.mark.parametrize(
    "service,pair",
    [
        ("payments-api", ["payments-api", "processor-adapter"]),
        ("processor-adapter", ["payments-api", "processor-adapter"]),
        ("webhook-sim", ["webhook-sim", "payments-api"]),
    ],
)
def test_fixed_plan_and_stale_scrapes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service: str, pair: list[str]
) -> None:
    """Twenty logical operations reserve34 commands/queries and never retry stale data."""
    sleeps: list[float] = []
    monkeypatch.setattr("payops.tools.window_collect.time.sleep", sleeps.append)
    calls = Calls(tmp_path)
    incident = Incident(request=IncidentCreate(title="Untrusted case-like text", service=service))
    collector = WindowCollector(Kubernetes(calls), Payment(calls))
    result = collector(incident, tmp_path)
    assert calls.reads == 34 and len(calls.operations) == 20
    assert calls.operations[:2] == calls.operations[-2:] == ["snapshot", "snapshot"]
    assert sleeps == [5.2] and not result.failures
    plan = json.loads((tmp_path / "window-plan.json").read_text())
    assert plan["services"] == pair
    windows = [item for item in result.evidence if item.source == "PAYMENT"]
    assert len(windows) == 2
    assert all(
        verify_payment_window(item, ArtifactStore(tmp_path / "artifacts")).status == "missing"
        for item in windows
    )
    previous = calls.reads
    with pytest.raises(FileExistsError):
        collector(incident, tmp_path)
    assert calls.reads == previous


def test_partial_failures_preserve_other_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Snapshot and log failures cannot masquerade as healthy zeros or erase target observations."""
    sleeps: list[float] = []
    monkeypatch.setattr("payops.tools.window_collect.time.sleep", sleeps.append)
    calls = Calls(tmp_path)
    calls.fail = True
    result = WindowCollector(Kubernetes(calls), Payment(calls))(
        Incident(request=IncidentCreate(title="Alert")), tmp_path
    )
    assert calls.reads == 34
    assert len(result.failures) == 11 and len(result.evidence) == 1
    assert "private" not in result.model_dump_json()


def test_stale_event_does_not_discard_later_current_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Source order cannot turn one old event into loss of valid current observations."""
    sleeps: list[float] = []
    monkeypatch.setattr("payops.tools.window_collect.time.sleep", sleeps.append)
    calls = Calls(tmp_path)

    def events(reader: Kubernetes, service: str) -> tuple[Observation, ...]:
        """Return old and current events in the same already charged source response."""
        reader.calls.record("events", 2)
        now = utc_now()
        return tuple(
            Observation(
                source="KUBERNETES",
                resource=service,
                observed_at=at,
                query="events",
                summary="Observed event",
            )
            for at in (now - timedelta(hours=1), now)
        )

    monkeypatch.setattr(Kubernetes, "events", events)
    result = WindowCollector(Kubernetes(calls), Payment(calls))(
        Incident(request=IncidentCreate(title="Alert")), tmp_path
    )
    assert calls.reads == 34 and len(result.failures) == 5
    assert len([item for item in result.evidence if item.query == "events"]) == 5


@pytest.mark.parametrize(
    "namespace,service", [("foreign", "payments-api"), ("payops-sandbox", "risk-sim")]
)
def test_unsupported_scope_has_no_plan(namespace: str, service: str) -> None:
    """Unsupported alerts require a separately selected collector profile."""
    with pytest.raises(ValueError, match="unsupported"):
        window_plan(
            Incident(request=IncidentCreate(title="Alert", namespace=namespace, service=service))
        )


def test_slow_collection_preserves_partial_observations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An excessive interval retains partial evidence and prevents final reads."""
    start = utc_now()
    instants = iter([start, start, start + timedelta(seconds=181)])
    monkeypatch.setattr("payops.tools.window_collect.utc_now", lambda: next(instants))
    calls = Calls(tmp_path)

    def unavailable(_reader: object, _selector: str) -> Observation:
        """Prevent successful observation normalization from consuming the simulated clock."""
        raise ValueError("source unavailable")

    monkeypatch.setattr(Kubernetes, "logs", unavailable)
    monkeypatch.setattr(Payment, "snapshot", unavailable)
    monkeypatch.setattr(Payment, "query", unavailable)
    result = WindowCollector(Kubernetes(calls), Payment(calls))(
        Incident(request=IncidentCreate(title="Alert")), tmp_path
    )
    assert result.failures[-1].error_type == "MeasurementIntervalExceeded"
    assert (tmp_path / "collection.json").exists()
