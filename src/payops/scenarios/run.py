"""Explicit operator entry point for the bounded local fault harness."""

import argparse
from pathlib import Path
from typing import get_args

from pydantic import TypeAdapter

from payops.scenarios.contracts import CaseId
from payops.scenarios.runner import LocalScenarioRunner


def main() -> int:
    """Require explicit cluster and evidence paths before starting one reviewed fault."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=get_args(CaseId), required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    case_id = TypeAdapter[CaseId](CaseId).validate_python(args.scenario)
    receipt = LocalScenarioRunner(args.kubeconfig, args.output).run(case_id)
    print(receipt.model_dump_json(indent=2))
    return 0 if receipt.activated and receipt.cleanup_verified and not receipt.failure else 1


if __name__ == "__main__":
    raise SystemExit(main())
