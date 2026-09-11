"""Fresh CPU windows must belong to the same pods on both sides of acquisition."""

import json
from datetime import timedelta
from decimal import Decimal

import pytest
from test_concurrency_evidence import START
from test_concurrency_lifetime import evidence

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.hpa_metrics import cpu_cores, validate_cpu_metrics


@pytest.mark.parametrize(
    "raw,expected", [("50000000n", "0.05"), ("50000u", "0.05"), ("50m", "0.05"), ("0.05", "0.05")]
)
def test_cpu_quantity_units(raw: str, expected: str) -> None:
    """The actual Metrics API nanocore representation has the same units as CPU requests."""
    assert cpu_cores(raw) == Decimal(expected)


@pytest.mark.parametrize("value", [True, "NaN", "1Gi", "-1m", "2", "1e-2", "1" * 33])
def test_invalid_cpu_quantities_reject(value: object) -> None:
    """Reject booleans, unsupported suffixes and measurements outside the configured envelope."""
    with pytest.raises(ValueError):
        cpu_cores(value)


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "restart",
        "stale",
        "preload",
        "duplicate",
        "namespace",
        "missing",
        "container",
        "window",
    ],
)
def test_raw_sample_binding(fault: str | None) -> None:
    """An apparently high CPU value cannot qualify when its source or averaging window differs."""
    _, _, _, identity, _, _ = evidence()
    row: JsonObject = {
        "metadata": {"name": identity.pod_name, "namespace": "payops-sandbox"},
        "timestamp": (START + timedelta(seconds=30)).isoformat(),
        "window": "15.5s",
        "containers": [{"name": "sandbox", "usage": {"cpu": "100000000n"}}],
    }
    rows = [row]
    after = (identity,)
    if fault == "restart":
        after = (identity.model_copy(update={"restart_count": 1}),)
    elif fault == "stale":
        row["timestamp"] = (START - timedelta(seconds=60)).isoformat()
    elif fault == "preload":
        row["window"] = "40s"
    elif fault == "duplicate":
        rows.append(row)
    elif fault == "namespace":
        object_value(row["metadata"])["namespace"] = "foreign"
    elif fault == "missing":
        rows = []
    elif fault == "container":
        object_items(row["containers"])[0]["name"] = "sidecar"
    elif fault == "window":
        row["window"] = "NaN"
    raw = json.dumps(
        {"kind": "PodMetricsList", "apiVersion": "metrics.k8s.io/v1beta1", "items": rows}
    ).encode()
    if fault:
        with pytest.raises(ValueError):
            validate_cpu_metrics(raw, (identity,), after, START, START + timedelta(seconds=35))
    else:
        result = validate_cpu_metrics(raw, (identity,), after, START, START + timedelta(seconds=35))
        assert result.average_utilization == 200 and result.average_cores == Decimal("0.1")
