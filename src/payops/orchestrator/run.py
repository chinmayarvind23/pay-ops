"""Operator-only local lifecycle command with explicit checkpoint and cluster locations."""

import argparse
from pathlib import Path

from payops.contracts import Incident, IncidentCreate
from payops.orchestrator.graph import InvestigationWorker
from payops.tools.collect import Collection, collect_local
from payops.tools.kubernetes import KubernetesRead
from payops.tools.payment import PaymentRead
from payops.tools.prometheus import PrometheusRead
from payops.tools.window_collect import WindowCollector


def main() -> None:
    """Start or resume a local investigation without exposing operational authority over HTTP."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--resume", help="Previously recorded incident ID")
    parser.add_argument("--title", default="Synthetic payment incident")
    parser.add_argument("--pause-before-ranking", action="store_true")
    parser.add_argument("--payment-windows", action="store_true")
    args = parser.parse_args()
    kubernetes = KubernetesRead(args.kubeconfig)
    prometheus = PrometheusRead()

    def collect(incident: Incident, output: Path) -> Collection:
        """The graph receives the fixed local collector, not CLI strings or executable recipes."""
        if args.payment_windows:
            return WindowCollector(kubernetes, PaymentRead())(incident, output)
        return collect_local(kubernetes, prometheus, output, incident.incident_id)

    worker = InvestigationWorker(
        args.runtime,
        collect,
        pause_before_ranking=args.pause_before_ranking,
        collection_profile="payment_windows_v1" if args.payment_windows else "instant_v1",
    )
    state = (
        worker.resume(args.resume)
        if args.resume
        else worker.start(Incident(request=IncidentCreate(title=args.title)))
    )
    print(state.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
