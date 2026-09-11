"""Enforce module coverage floors without letting large modules conceal small gaps."""

import argparse
import json
import math
from pathlib import Path
from typing import Any

# These entry points require live subprocess/cluster evidence, retained separately.
INTEGRATION_ONLY = frozenset(
    {
        "cli.py",
        "evaluation/run.py",
        "scenarios/run.py",
        "scenarios/startup_failure.py",
        "scenarios/kubectl.py",
    }
)
REQUIRED = frozenset({"contracts/__init__.py", "evaluation/metrics.py"})
CRITICAL = ("contracts/", "evidence/", "evaluation/", "policy/", "auth/", "actions/")


def evaluate(document: dict[str, Any]) -> dict[str, Any]:
    """Use coverage.py's combined statement/branch percentage and fail missing core inputs."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for filename, detail in document["files"].items():
        normalized = filename.replace("\\", "/")
        if "src/payops/" not in normalized:
            continue
        module = normalized.split("src/payops/", 1)[1]
        seen.add(module)
        summary = detail["summary"]
        threshold = 95 if module.startswith(CRITICAL) else 85
        excluded = module in INTEGRATION_ONLY
        measured = float(summary["percent_covered"])
        rows.append(
            {
                "module": module,
                "coverage": measured,
                "threshold": threshold,
                "integration_only": excluded,
                "passed": math.isfinite(measured)
                and 0 <= measured <= 100
                and (excluded or measured >= threshold),
            }
        )
    missing = sorted(REQUIRED - seen)
    branch_measured = document.get("meta", {}).get("branch_coverage") is True
    return {
        "passed": branch_measured and not missing and all(row["passed"] for row in rows),
        "branch_coverage": branch_measured,
        "missing_required_modules": missing,
        "modules": rows,
    }


def main() -> int:
    """Keep a machine-readable gate record even when coverage is below its required floor."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(json.loads(args.coverage.read_text(encoding="utf-8")))
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    for row in result["modules"]:
        if not row["passed"]:
            print(f"{row['module']}: {row['coverage']:.2f}% < {row['threshold']}%")
    print(f"Coverage gate: {'pass' if result['passed'] else 'fail'}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
