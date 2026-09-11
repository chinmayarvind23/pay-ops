"""Fixed in-cluster load reaches the Service VIP instead of one port-forward-selected pod."""

import asyncio
import json
import os
import platform
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from payops.scenarios.traffic import SliceCount, TrafficDriver, Workload, wait_forward_ready

ORIGIN = "http://payments-api.payops-sandbox.svc.cluster.local:8080"
OUTPUT = Path("/tmp/payops-hpa-load")


def load_workload() -> Workload:
    """Use the same finite request count, slice and concurrency on both sides of the cap change."""
    return Workload(
        role="payments",
        distribution=(SliceCount(processor="A", region="us", payment_method="credit", count=256),),
        concurrency=4,
        request_timeout_seconds=5.0,
        deadline_seconds=180.0,
        seed=0,
    )


def guard() -> None:
    """Only the explicitly enabled local Linux job may use the fixed cluster-local destination."""
    if platform.system() != "Linux" or os.environ.get("PAYOPS_HPA_LOAD") != "kind-v1":
        raise ValueError("HPA load requires the explicit local Linux job configuration")


class HpaLoadDriver(TrafficDriver):
    """Keep existing plan/attempt/receipt semantics while changing only trusted transport setup."""

    @asynccontextmanager
    async def _client(self, workload: Workload) -> AsyncGenerator[httpx.AsyncClient]:
        """New connections allow Service routing to distribute requests across replicas."""
        guard()
        if workload != load_workload():
            raise ValueError("HPA job workload differs from the fixed experiment")
        async with httpx.AsyncClient(
            base_url=ORIGIN,
            trust_env=False,
            follow_redirects=False,
            timeout=5.0,
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=0),
        ) as client:
            await wait_forward_ready(client, "payments")
            yield client


def main() -> None:
    """The Job emits the original plan and complete receipt; it never mutates cluster state."""
    guard()
    driver = HpaLoadDriver(Path("/unused-kubeconfig"), OUTPUT)
    receipt = asyncio.run(driver.run(load_workload()))
    plan = json.loads((OUTPUT / receipt.run_id / "plan.json").read_text())
    print(
        json.dumps(
            {
                "event": "synthetic.hpa_load",
                "plan": plan,
                "receipt": receipt.model_dump(mode="json"),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    if receipt.status != "completed":
        raise RuntimeError("HPA load did not complete its fixed batch")


if __name__ == "__main__":
    main()
