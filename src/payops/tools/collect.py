"""Operator collection command records failures and verified observations outside source."""

import argparse
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Literal

import httpx

from payops.contracts import Contract, EvidenceItem, new_id, utc_now
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import normalize
from payops.tools.kubernetes import SERVICES, KubernetesRead
from payops.tools.prometheus import QUERIES, PrometheusRead


class CollectionFailure(Contract):
    """Error classification contains no raw exception text or credentials."""

    tool: str
    resource: str
    error_type: str


class Collection(Contract):
    """A partial collection remains auditable and cannot masquerade as full source coverage."""

    incident_id: str
    mode: Literal["local_kind"] = "local_kind"
    evidence: tuple[EvidenceItem, ...]
    failures: tuple[CollectionFailure, ...]


def collect_local(
    kubernetes: KubernetesRead,
    prometheus: PrometheusRead,
    output: Path,
    incident_id: str | None = None,
) -> Collection:
    """Observe only current sandbox state; no case definitions or scorer labels enter here."""
    identity = incident_id or new_id()
    store = ArtifactStore(output / "artifacts")
    start = utc_now() - timedelta(minutes=5)
    evidence: list[EvidenceItem] = []
    failures: list[CollectionFailure] = []
    calls = [
        ("kubernetes", service, lambda service=service: kubernetes.collect(service))
        for service in sorted(SERVICES)
    ]
    calls += [
        ("events", service, lambda service=service: kubernetes.events(service))
        for service in sorted(SERVICES)
    ]
    calls += [
        ("logs", service, lambda service=service: (kubernetes.logs(service),))
        for service in sorted(SERVICES)
    ]
    calls += [
        ("prometheus", signal, lambda signal=signal: (prometheus.query(signal),))
        for signal in sorted(QUERIES)
    ]
    for tool, resource, call in calls:
        try:
            for observation in call():
                if observation.observed_at < start:
                    failures.append(
                        CollectionFailure(
                            tool=tool, resource=resource, error_type="OutsideQueryWindow"
                        )
                    )
                    continue
                item = normalize(
                    observation, identity, start, utc_now() + timedelta(seconds=1), store
                )
                store.verify(item)
                evidence.append(item)
        except (ValueError, OSError, subprocess.SubprocessError, httpx.HTTPError) as error:
            failures.append(
                CollectionFailure(tool=tool, resource=resource, error_type=type(error).__name__)
            )
    result = Collection(incident_id=identity, evidence=tuple(evidence), failures=tuple(failures))
    output.mkdir(parents=True, exist_ok=True)
    (output / "collection.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def main() -> None:
    """Require explicit evidence output and kubeconfig to avoid accidental global state."""
    parser = argparse.ArgumentParser(description="Collect bounded local PayOps evidence")
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = collect_local(KubernetesRead(args.kubeconfig), PrometheusRead(), args.output)
    print(
        f"Collected {len(result.evidence)} verified observations; {len(result.failures)} failures"
    )


if __name__ == "__main__":
    main()
