"""Reuse bounded Kubernetes acquisition with a fixed payments-only experiment target."""

from typing import Literal

from payops.scenarios.leak_gateway import LeakGateway


class ConcurrencyGateway(LeakGateway):
    """Inherit exact CAS, byte caps and controller snapshots without a second transport."""

    log_target: Literal["risk-sim", "payments-api"] = "payments-api"

    @staticmethod
    def validate_target(name: str) -> None:
        """Only the payments Deployment may change during the fixed concurrency contrast."""
        if name != "payments-api":
            raise ValueError("concurrency scenario only permits payments-api")
