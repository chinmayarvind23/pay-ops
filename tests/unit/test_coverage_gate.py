"""The CI threshold gate rejects rounded near-misses and missing measurement data."""

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "critical,core,branches,omit,expected",
    [
        (95.0, 85.0, True, False, 0),
        (94.999, 85.0, True, False, 1),
        (95.0, 84.999, True, False, 1),
        (100.0, 100.0, False, False, 1),
        (100.0, 100.0, True, True, 1),
        (float("inf"), 100.0, True, False, 1),
    ],
)
def test_coverage_thresholds_fail_closed(
    tmp_path: Path, critical: float, core: float, branches: bool, omit: bool, expected: int
) -> None:
    """Exercise the command contract using Windows paths and exact gate boundary cases."""
    files = {
        "src\\payops\\contracts\\__init__.py": {"summary": {"percent_covered": critical}},
        "src\\payops\\evaluation\\metrics.py": {"summary": {"percent_covered": critical}},
        "src\\payops\\tools\\kubernetes.py": {"summary": {"percent_covered": core}},
        "src\\payops\\cli.py": {"summary": {"percent_covered": 0}},
    }
    if omit:
        del files["src\\payops\\evaluation\\metrics.py"]
    coverage = tmp_path / "coverage.json"
    output = tmp_path / "gate.json"
    coverage.write_text(json.dumps({"meta": {"branch_coverage": branches}, "files": files}))
    script = Path(__file__).resolve().parents[2] / "scripts" / "coverage_gate.py"
    result = subprocess.run(
        [sys.executable, str(script), str(coverage), "--output", str(output)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == expected, result.stdout + result.stderr
    assert json.loads(output.read_text())["passed"] is (expected == 0)


@pytest.mark.parametrize(
    "module",
    [
        "protected_api.py",
        "memory/data_clients.py",
        "orchestrator/reasoning.py",
        "orchestrator/budget.py",
        "orchestrator/model_runtime.py",
        "tools/registry.py",
        "tools/operational_reads.py",
    ],
)
@pytest.mark.parametrize("coverage_value,expected", [(94.999, 1), (95.0, 0)])
def test_authority_boundaries_require_critical_coverage(
    tmp_path: Path, module: str, coverage_value: float, expected: int
) -> None:
    """Authentication, data and model trust boundaries cannot pass the ordinary module floor."""
    files = {
        "src/payops/contracts/__init__.py": {"summary": {"percent_covered": 100}},
        "src/payops/evaluation/metrics.py": {"summary": {"percent_covered": 100}},
        f"src/payops/{module}": {"summary": {"percent_covered": coverage_value}},
    }
    coverage, output = tmp_path / "coverage.json", tmp_path / "gate.json"
    coverage.write_text(json.dumps({"meta": {"branch_coverage": True}, "files": files}))
    script = Path(__file__).resolve().parents[2] / "scripts" / "coverage_gate.py"
    result = subprocess.run(
        [sys.executable, str(script), str(coverage), "--output", str(output)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    document = json.loads(output.read_text())
    assert result.returncode == expected, result.stdout + result.stderr
    assert document["passed"] is (expected == 0)
    reviewed = next(row for row in document["modules"] if row["module"] == module)
    assert reviewed["threshold"] == 95 and reviewed["integration_only"] is False
