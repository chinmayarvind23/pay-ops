"""Misleading observations preserve their acquisition times and separate condition labels."""

import json
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from prometheus_client import CollectorRegistry, Counter
from pydantic import ValidationError

from payops.sandbox.models import SandboxConfig
from payops.sandbox.telemetry_conditions import MetricSnapshot, emit_archived_error


def test_delayed_export_keeps_old_bytes_then_refreshes() -> None:
    """A changed real counter remains hidden only until the fixed snapshot lifetime expires."""
    registry = CollectorRegistry()
    counter = Counter("fixture_events", "fixture", registry=registry)
    snapshot = MetricSnapshot(registry, True)
    with patch("payops.sandbox.telemetry_conditions.monotonic", side_effect=[10, 69, 70]):
        before = snapshot.read()
        counter.inc()
        assert snapshot.read() == before
        after = snapshot.read()
    assert before[0] != after[0] and b"fixture_events_total 1.0" in after[0]
    assert b"sandbox_metrics_snapshot_timestamp_seconds" in before[0]


def test_normal_export_is_fresh() -> None:
    """The default profile never delays existing service counters."""
    registry = CollectorRegistry()
    counter = Counter("fixture_events", "fixture", registry=registry)
    snapshot = MetricSnapshot(registry, False)
    before = snapshot.read()
    counter.inc()
    assert snapshot.read()[0] != before[0]


def test_archived_error_is_explicitly_old(capsys: pytest.CaptureFixture[str]) -> None:
    """The distractor includes the source event's age instead of posing as a current fault."""
    emit_archived_error()
    row = json.loads(capsys.readouterr().out)
    age = datetime.now(UTC) - datetime.fromisoformat(row["original_event_at"])
    assert 86399 <= age.total_seconds() <= 86401
    assert row["archived"] and row["dependency"] == "postgres"


@pytest.mark.parametrize("condition", ["archived_error", "delayed_metrics"])
def test_condition_requires_dependency(condition: str) -> None:
    """Unrelated fault profiles cannot accidentally inherit a misleading-telemetry treatment."""
    with pytest.raises(ValidationError):
        SandboxConfig.model_validate({"telemetry_condition": condition})
