"""Fixed in-cluster load reaches the Service VIP instead of one port-forward-selected pod."""

import asyncio
import json
import os
import platform
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from pydantic import JsonValue

from payops.scenarios.traffic import (
    AttemptRecord,
    PlannedAttempt,
    SliceCount,
    TrafficDriver,
    Workload,
    record_attempt,
    wait_forward_ready,
)

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

    def plan_metadata(self) -> dict[str, JsonValue]:
        """Persist pacing before traffic so rejected requests cannot silently shorten the load."""
        return {"launch_interval_seconds": 0.5}

    async def _dispatch(
        self,
        workload: Workload,
        plan: tuple[PlannedAttempt, ...],
        records: dict[int, AttemptRecord],
        probe: bool,
    ) -> None:
        """Spread the fixed batch across 127.5 seconds, retaining the existing concurrency bound."""
        if probe or workload != load_workload():
            raise ValueError("HPA load requires its fixed paced workload")
        semaphore = asyncio.Semaphore(workload.concurrency)
        async with self._client(workload) as client, asyncio.TaskGroup() as tasks:
            for index, item in enumerate(plan):
                if index:
                    await asyncio.sleep(0.5)
                tasks.create_task(record_attempt(client, item, workload, semaphore, records))

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
