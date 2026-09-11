"""Operator entry point for the initial four-case local development benchmark."""

import argparse
from pathlib import Path

from payops.evaluation.local import run_suite


def main() -> None:
    """Separate operator configuration and frozen scorer labels from investigation inputs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--gold", type=Path, default=Path("evals/golden/local-initial.json"))
    args = parser.parse_args()
    print(run_suite(args.kubeconfig, args.output, args.gold))


if __name__ == "__main__":
    main()
