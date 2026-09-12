"""Explicit synthetic observation conditions keep misleading evidence separate from root causes."""

import json
from datetime import UTC, datetime, timedelta
from time import monotonic

from prometheus_client import CollectorRegistry, Gauge, generate_latest


class MetricSnapshot:
    """Serve a real earlier registry snapshot with its original timestamp."""

    def __init__(self, registry: CollectorRegistry, delayed: bool) -> None:
        """Keep one bounded response; the normal path remains fresh."""
        self.registry = registry
        self.delayed = delayed
        self.body = b""
        self.acquired = 0.0
        self.timestamp = ""
        self.epoch = Gauge(
            "sandbox_metrics_snapshot_timestamp_seconds",
            "Actual creation time of this metrics response snapshot",
            registry=registry,
        )

    def read(self) -> tuple[bytes, str]:
        """Keep the entire prior response unchanged for 60 seconds."""
        now = monotonic()
        if not self.delayed or not self.body or now - self.acquired >= 60:
            instant = datetime.now(UTC)
            self.epoch.set(instant.timestamp())
            body = generate_latest(self.registry)
            if len(body) > 131072:
                raise ValueError("synthetic metrics snapshot exceeds its byte bound")
            self.body, self.acquired, self.timestamp = body, now, instant.isoformat()
        return self.body, self.timestamp


def emit_archived_error() -> None:
    """A clearly dated synthetic old database error distracts from the current cache failure."""
    print(
        json.dumps(
            {
                "event": "synthetic.archived_error",
                "archived": True,
                "original_event_at": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
                "dependency": "postgres",
                "message": "database connection exhausted",
                "synthetic": True,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
