"""Explicit zero slices and process epochs make counter-delta denominators interpretable."""

from itertools import product

from payops.sandbox.models import Sample
from payops.sandbox.telemetry import SandboxMetrics


def test_declared_slices_start_at_zero_and_one_attempt_changes_only_its_slice() -> None:
    """A missing series must not be confused with an unobserved successful or failed request."""
    metrics = SandboxMetrics()
    for status, processor, region, method in product(
        ("accepted", "declined", "error"), ("A", "B"), ("us", "eu"), ("credit", "debit")
    ):
        value = metrics.registry.get_sample_value(
            "payment_requests_total",
            {"status": status, "processor": processor, "region": region, "payment_method": method},
        )
        assert value == 0
    epoch = metrics.registry.get_sample_value("sandbox_process_start_time_seconds")
    assert epoch is not None and epoch > 0
    metrics.observe(Sample(sample_id="synthetic-epoch", processor="B"), "declined", 0.1)
    assert (
        metrics.registry.get_sample_value(
            "payment_requests_total",
            {"status": "declined", "processor": "B", "region": "us", "payment_method": "credit"},
        )
        == 1
    )
    assert metrics.registry.get_sample_value("sandbox_process_start_time_seconds") == epoch
    assert (
        metrics.registry.get_sample_value(
            "payment_authorization_latency_seconds_count", {"processor": "B", "region": "eu"}
        )
        == 0
    )
