"""Closed PromQL templates read real infrastructure counters without query injection."""

import math
from urllib.parse import urlsplit

import httpx
from pydantic import TypeAdapter

from payops.contracts import Contract, utc_now
from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.normalize import Observation
from payops.tools.kubernetes import SERVICES

QUERIES = {
    "requests": 'payment_requests_total{job="payops-sandbox",service="payments-api"}',
    "latency": (
        'payment_authorization_latency_seconds_bucket{job="payops-sandbox",service="payments-api"}'
    ),
    "declines": 'payment_declines_total{job="payops-sandbox",service="payments-api"}',
    "conflicts": 'payment_idempotency_conflicts_total{job="payops-sandbox",service="payments-api"}',
    "targets": 'up{job="payops-sandbox"}',
}


class VectorSample(Contract):
    """Instant samples carry a timestamp and encoded scalar under bounded source labels."""

    metric: dict[str, str]
    value: tuple[float, str]


def validate_samples(results: object, signal: str) -> None:
    """Reject nonfinite and cross-service values instead of presenting them as healthy metrics."""
    samples = TypeAdapter(list[VectorSample]).validate_python(results)
    for sample in samples:
        service = sample.metric.get("service")
        if service not in SERVICES or (signal != "targets" and service != "payments-api"):
            raise ValueError("Prometheus sample has unexpected service scope")
        if not math.isfinite(sample.value[0]) or not math.isfinite(float(sample.value[1])):
            raise ValueError("Prometheus sample is nonfinite")


class PrometheusRead:
    """Endpoint configuration is operator-owned; tool callers select a closed signal name."""

    def __init__(
        self, origin: str = "http://127.0.0.1:19090", transport: httpx.BaseTransport | None = None
    ) -> None:
        """The first adapter is local-only and cannot become an arbitrary HTTP proxy."""
        parsed = urlsplit(origin)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.path:
            raise ValueError("Prometheus endpoint must be a loopback HTTP origin")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("invalid Prometheus origin")
        self.origin = origin
        self.transport = transport

    def query(self, signal: str) -> Observation:
        """Retain the exact query and server response; empty series are not healthy zeros."""
        if signal not in QUERIES:
            raise ValueError("signal outside Prometheus allowlist")
        with httpx.Client(
            transport=self.transport, timeout=5, trust_env=False, follow_redirects=False
        ) as client:
            with client.stream(
                "GET", self.origin + "/api/v1/query", params={"query": QUERIES[signal]}
            ) as response:
                response.raise_for_status()
                chunks = bytearray()
                for chunk in response.iter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > 131072:
                        raise ValueError("Prometheus response exceeds byte budget")
        payload = JSON_OBJECT.validate_json(bytes(chunks))
        if payload.get("status") != "success":
            raise ValueError("Prometheus query failed")
        data = payload.get("data")
        if not isinstance(data, dict) or data.get("resultType") != "vector":
            raise ValueError("invalid Prometheus instant-vector response")
        results = data.get("result")
        if not isinstance(results, list) or len(results) > 512:
            raise ValueError("invalid or oversized Prometheus series")
        validate_samples(results, signal)
        return Observation(
            source="PROMETHEUS",
            resource="sandbox-scrapes" if signal == "targets" else "payments-api",
            observed_at=utc_now(),
            query=QUERIES[signal],
            summary=f"Prometheus {signal}: {str(payload.get('data'))[:3300]}",
            payload=payload,
        )
