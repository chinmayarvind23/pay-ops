"""The in-cluster load cannot select another origin or reuse connections to pin a replica."""

import asyncio
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from payops.scenarios.hpa_load import ORIGIN, HpaLoadDriver, guard, load_workload


@pytest.mark.parametrize(
    "system,enabled", [("Windows", "kind-v1"), ("Linux", "no"), ("Linux", "kind-v1")]
)
def test_job_guard(system: str, enabled: str) -> None:
    """Accidental workstation execution must not generate load against a configured network."""
    with (
        patch("payops.scenarios.hpa_load.platform.system", return_value=system),
        patch.dict("os.environ", {"PAYOPS_HPA_LOAD": enabled}),
    ):
        if system == "Linux" and enabled == "kind-v1":
            guard()
        else:
            with pytest.raises(ValueError):
                guard()


def test_fixed_transport_disables_connection_reuse(tmp_path: Path) -> None:
    """Use the real driver with intercepted HTTP and check its cluster-local client settings."""

    def respond(request: httpx.Request) -> httpx.Response:
        """Only the health route is needed to inspect the client lifecycle."""
        assert request.url.path == "/health"
        return httpx.Response(200, json={"status": "ok", "role": "payments", "synthetic": True})

    async def exercise() -> None:
        """The inherited context closes its HTTP client even without executing a batch."""
        client = httpx.AsyncClient(base_url=ORIGIN, transport=httpx.MockTransport(respond))
        with (
            patch("payops.scenarios.hpa_load.guard"),
            patch("payops.scenarios.hpa_load.httpx.AsyncClient", return_value=client) as factory,
        ):
            driver = HpaLoadDriver(tmp_path / "unused", tmp_path)
            async with driver._client(load_workload()):  # pyright: ignore[reportPrivateUsage]
                assert not client.is_closed
            settings = factory.call_args.kwargs
            assert settings["base_url"] == ORIGIN and settings["trust_env"] is False
            assert settings["limits"].max_keepalive_connections == 0
            assert client.is_closed
            with pytest.raises(ValueError):
                async with driver._client(load_workload().model_copy(update={"concurrency": 8})):  # pyright: ignore[reportPrivateUsage]
                    raise AssertionError("invalid workload reached transport")

    asyncio.run(exercise())


def test_main_emits_original_plan_and_receipt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The job output retains driver evidence and requests the fixed 256-request batch."""
    import json
    from unittest.mock import AsyncMock

    from test_concurrency_traffic import exercise

    from payops.scenarios.hpa_load import main

    receipt, plan, _ = exercise(tmp_path, False)
    with (
        patch("payops.scenarios.hpa_load.guard"),
        patch("payops.scenarios.hpa_load.OUTPUT", tmp_path / "traffic"),
        patch("payops.scenarios.hpa_load.HpaLoadDriver") as factory,
    ):
        factory.return_value.run = AsyncMock(return_value=receipt)
        main()
        called = factory.return_value.run.call_args.args[0]
        assert called.distribution[0].count == 256 and called.concurrency == 4
    output = json.loads(capsys.readouterr().out)
    assert output["plan"] == plan and output["receipt"] == receipt.model_dump(mode="json")
