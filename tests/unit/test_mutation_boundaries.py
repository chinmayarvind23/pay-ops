"""Independent negative controls for late publication and durable reasoning reservations."""

from time import monotonic

import pytest
from test_registry import Harness
from test_registry import harness as harness

from payops.tools.registry import Status


@pytest.mark.parametrize("offset,expected", [(-60, "OK"), (60, "TIMEOUT")])
def test_publication_completion_time_controls_retained_grant(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, offset: float, expected: str
) -> None:
    """An already-complete future may still carry a grant completed after its admission cutoff."""
    registry = harness.registry()

    def completed() -> tuple[Status, float]:
        """Control recorded completion while preserving the actual worker slot lifecycle."""
        try:
            return "OK", monotonic() + offset
        finally:
            registry._slots.release()  # pyright: ignore[reportPrivateUsage]

    monkeypatch.setattr(registry, "_refresh", completed)
    assert registry._publication_status() == expected  # pyright: ignore[reportPrivateUsage]
