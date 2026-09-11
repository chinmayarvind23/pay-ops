"""Two fixed Prometheus reads capture independently verifiable payment snapshot evidence."""

import json
from datetime import datetime
from time import monotonic

import httpx
from pydantic import JsonValue

from payops.contracts import utc_now
from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.normalize import Observation
from payops.evidence.payment_window import Service, snapshot_observation, snapshot_queries
from payops.tools.prometheus import PrometheusRead


def unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    """Duplicate keys cannot silently replace provenance or numeric fields during decoding."""
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate provider JSON key")
        result[key] = value
    return result


class PaymentRead(PrometheusRead):
    """Reuse the local endpoint boundary while exposing only closed payment service selectors."""

    def _snapshot_query(self, query: str, at: float) -> dict[str, JsonValue]:
        """Each charged read has no retries, an explicit server timeout and bounded decoding."""
        with httpx.Client(
            transport=self.transport, timeout=5, trust_env=False, follow_redirects=False
        ) as client:
            started = monotonic()
            with client.stream(
                "GET",
                self.origin + "/api/v1/query",
                params={"query": query, "time": at, "timeout": "3s", "limit": 128},
                headers={"Accept-Encoding": "identity"},
            ) as response:
                response.raise_for_status()
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise ValueError("compressed payment response denied")
                content = bytearray()
                for chunk in response.iter_raw():
                    if monotonic() - started > 5:
                        raise ValueError("payment response elapsed budget exceeded")
                    if len(content) + len(chunk) > 131072:
                        raise ValueError("payment response exceeds byte budget")
                    content.extend(chunk)
                if monotonic() - started > 5:
                    raise ValueError("payment response elapsed budget exceeded")
        return JSON_OBJECT.validate_python(json.loads(content, object_pairs_hook=unique_object))

    def snapshot(self, service: Service, at: datetime | None = None) -> Observation:
        """One logical snapshot costs two provider reads pinned to the same evaluation instant."""
        query, watermark_query = snapshot_queries(service)
        instant = at or utc_now()
        if instant.tzinfo is None or instant > utc_now():
            raise ValueError("snapshot requires a nonfuture aware time")
        payload: dict[str, JsonValue] = {
            "evaluated_at": instant.timestamp(),
            "query": query,
            "watermark_query": watermark_query,
            "metrics": self._snapshot_query(query, instant.timestamp()),
            "watermark": self._snapshot_query(watermark_query, instant.timestamp()),
        }
        return snapshot_observation(payload, service)
