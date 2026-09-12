"""Cloud read contracts enforce resource ownership, bounded pages and incident windows."""

from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.cloud.logging_v2.types import ListLogEntriesResponse
from google.cloud.monitoring_v3.types import ListTimeSeriesResponse

from payops.contracts import utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import normalize
from payops.tools.cloud_observability import (
    RESTART_METRIC,
    CloudScope,
    document,
    read_logs,
    read_restarts,
    scoped_resource,
    window,
)

SCOPE = CloudScope(project="payops-test", cluster="sandbox", location="us-central1")


def test_logs_are_scoped_normalized_and_partial(tmp_path: Path) -> None:
    """A protobuf response becomes hash-verified evidence without fetching the next page."""
    now = utc_now()
    response = ListLogEntriesResponse(
        entries=[
            {
                "resource": {"type": "k8s_container", "labels": SCOPE.labels("payments-api")},
                "timestamp": now,
                "text_payload": "processor unavailable",
            }
        ],
        next_page_token="more",
    )
    client = MagicMock()
    client.list_log_entries.return_value.pages = iter([response])
    observations = read_logs(client, SCOPE, "payments-api", now - timedelta(minutes=1), now)
    assert len(observations) == 1 and observations[0].payload["partial"] is True
    store = ArtifactStore(tmp_path)
    item = normalize(observations[0], "incident-1", now - timedelta(minutes=1), now, store)
    assert store.verify(item)["payload"] is not None
    args = client.list_log_entries.call_args.kwargs
    assert args["retry"] is None and args["timeout"] == 5
    assert args["request"]["page_size"] == 20
    assert 'resource.labels.project_id="payops-test"' in args["request"]["filter"]
    assert args["request"]["resource_names"] == ["projects/payops-test"]


@pytest.mark.parametrize("bad", ["none", "metric", "time", "resource", "empty"])
def test_restart_response_contract(bad: str) -> None:
    """Returned series must retain the configured metric, container and observation window."""
    now = utc_now()
    labels = SCOPE.labels("payments-api")
    if bad == "resource":
        labels["namespace_name"] = "production"
    response = ListTimeSeriesResponse(
        time_series=[
            {
                "resource": {"type": "k8s_container", "labels": labels},
                "metric": {"type": "wrong" if bad == "metric" else RESTART_METRIC},
                "points": [
                    {
                        "interval": {
                            "end_time": now + timedelta(hours=1) if bad == "time" else now
                        },
                        "value": {"int64_value": 2},
                    }
                ],
            }
        ]
    )
    if bad == "empty":
        response.time_series[0].points.clear()
    client = MagicMock()
    client.list_time_series.return_value.pages = iter([response])
    if bad != "none":
        with pytest.raises(ValueError):
            read_restarts(client, SCOPE, "payments-api", now - timedelta(minutes=1), now)
    else:
        result = read_restarts(client, SCOPE, "payments-api", now - timedelta(minutes=1), now)
        assert len(result) == 1 and result[0].source == "KUBERNETES"
        assert result[0].payload["partial"] is False
        args = client.list_time_series.call_args.kwargs
        assert args["retry"] is None and args["timeout"] == 5
        assert args["request"]["name"] == "projects/payops-test"
        assert RESTART_METRIC in args["request"]["filter"]


def test_log_outside_window_rejected() -> None:
    """A provider returning stale logs cannot pass them off as current incident evidence."""
    now = utc_now()
    response = ListLogEntriesResponse(
        entries=[
            {
                "resource": {"type": "k8s_container", "labels": SCOPE.labels("payments-api")},
                "timestamp": now - timedelta(hours=1),
                "text_payload": "old",
            }
        ]
    )
    client = MagicMock()
    client.list_log_entries.return_value.pages = iter([response])
    with pytest.raises(ValueError, match="window"):
        read_logs(client, SCOPE, "payments-api", now - timedelta(minutes=1), now)


def test_invalid_queries_and_oversized_pages() -> None:
    """Bad operator scope and response size fail before they can widen evidence acquisition."""
    now = utc_now()
    with pytest.raises(ValueError):
        SCOPE.filter('payments-api" OR true')
    with pytest.raises(ValueError):
        window(now, now + timedelta(hours=1))
    with pytest.raises(ValueError):
        window(now.replace(tzinfo=None), now)
    with pytest.raises(ValueError):
        document("x" * 131073)
    with pytest.raises(ValueError):
        scoped_resource({"type": "global", "labels": {}}, SCOPE.labels("payments-api"))
