"""The per-request memory profile is deployment-owned and respects payment idempotency."""

from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from payops.sandbox.models import SandboxConfig, SimulationResult
from payops.sandbox.service import create_service


@pytest.mark.parametrize("fail_first", [False, True])
def test_payment_reservation_replay_and_worker_failure(fail_first: bool) -> None:
    """A failed allocation can retry; a successful replay performs no additional memory work."""
    sample = {"sample_id": "synthetic-memory-request"}
    result = SimulationResult(sample_id=sample["sample_id"], role="payments", status="accepted")
    with (
        patch("payops.sandbox.service.ConcurrentMemory", autospec=True) as worker,
        patch("payops.sandbox.service.payment_path", return_value=result) as payment,
    ):
        worker.return_value.run.side_effect = (
            [HTTPException(503, "full"), None] if fail_first else [None]
        )
        with TestClient(
            create_service("payments", SandboxConfig(concurrency_memory=True))
        ) as client:
            if fail_first:
                assert client.post("/simulate", json=sample).status_code == 503
                payment.assert_not_called()
            assert client.post("/simulate", json=sample).status_code == 200
            assert client.post("/simulate", json=sample).status_code == 200
            assert (
                client.post("/simulate", json={**sample, "concurrency_memory": True}).status_code
                == 422
            )
        assert worker.return_value.run.await_count == (2 if fail_first else 1)
        payment.assert_awaited_once()
        worker.return_value.close.assert_called_once()


def test_profile_cannot_mix_cpu_or_change_role() -> None:
    """A concurrency contrast must not silently alter CPU work or a different dependency."""
    with pytest.raises(ValueError):
        SandboxConfig(concurrency_memory=True, cpu_rounds=1)
    with pytest.raises(ValueError):
        create_service("risk", SandboxConfig(concurrency_memory=True))
