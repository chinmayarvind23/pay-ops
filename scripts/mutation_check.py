"""Run enumerated semantic mutants in isolated snapshots, never in the working source."""

import argparse
import ast
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Mutation:
    """Exact single replacements make each experiment independently reviewable."""

    name: str
    path: str
    before: str
    after: str
    test: str


CONTRACT = "src/payops/contracts/__init__.py"
ARTIFACT = "src/payops/evidence/artifacts.py"
NORMALIZE = "src/payops/evidence/normalize.py"
REDACT = "src/payops/evidence/redact.py"
METRIC = "src/payops/evaluation/metrics.py"
GRAPH = "src/payops/orchestrator/graph.py"
NODES = "src/payops/orchestrator/nodes.py"
SCHEMA_TESTS = "tests/unit/test_contracts.py tests/unit/test_lineage.py"
EVIDENCE_TESTS = "tests/unit/test_evidence.py"
METRIC_TESTS = "tests/unit/test_metrics.py"
GRAPH_TESTS = "tests/unit/test_graph.py"
MUTATIONS = (
    Mutation("schema_extra_fields", CONTRACT, 'extra="forbid"', 'extra="allow"', SCHEMA_TESTS),
    Mutation("schema_frozen", CONTRACT, "frozen=True", "frozen=False", SCHEMA_TESTS),
    Mutation(
        "schema_aware_observation",
        CONTRACT,
        "observed_at: AwareDatetime",
        "observed_at: datetime",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_report_owner",
        CONTRACT,
        "self.report is not None and self.report.incident_id != self.incident_id",
        "False",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_duplicate_evidence",
        CONTRACT,
        "if len(ids) != len(self.evidence):",
        "if False:",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_unresolved_citation",
        CONTRACT,
        "if not (supports | refutes) <= ids:",
        "if False:",
        SCHEMA_TESTS,
    ),
    Mutation(
        "schema_conflicting_citation", CONTRACT, "if supports & refutes:", "if False:", SCHEMA_TESTS
    ),
    Mutation(
        "schema_confidence_bound",
        CONTRACT,
        "ge=0, le=1, allow_inf_nan=False",
        "ge=0, le=2, allow_inf_nan=False",
        SCHEMA_TESTS,
    ),
    Mutation(
        "artifact_hash", ARTIFACT, "sha256(content).hexdigest() != digest", "False", EVIDENCE_TESTS
    ),
    Mutation(
        "artifact_metadata",
        ARTIFACT,
        'if payload.get("evidence") != expected:',
        "if False:",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "artifact_uri",
        ARTIFACT,
        'if evidence.artifact_uri != f"sha256://{digest}":',
        "if False:",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "artifact_no_overwrite",
        ARTIFACT,
        "os.link(temporary, path)",
        "os.replace(temporary, path)",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "context_verify", NORMALIZE, "        store.verify(item)", "        pass", EVIDENCE_TESTS
    ),
    Mutation(
        "context_incident",
        NORMALIZE,
        "if len({item.incident_id for item in evidence}) > 1:",
        "if False:",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "redaction_env",
        REDACT,
        'SENSITIVE_KEY.search(key) or (sensitive_name and key in {"value", "valueFrom"})',
        "SENSITIVE_KEY.search(key)",
        EVIDENCE_TESTS,
    ),
    Mutation(
        "recall_denominator", METRIC, "total=len(gold)", "total=len(predictions)", METRIC_TESTS
    ),
    Mutation(
        "recall_equal_gate",
        METRIC,
        "self.fraction >= Fraction(required_hits, required_total)",
        "self.fraction > Fraction(required_hits, required_total)",
        METRIC_TESTS,
    ),
    Mutation(
        "recall_threshold_validation",
        METRIC,
        "required_total <= 0 or not 0 <= required_hits <= required_total",
        "required_total <= 0",
        METRIC_TESTS,
    ),
    Mutation(
        "recall_top_k",
        METRIC,
        "predictions.get(case, ())[:k]",
        "predictions.get(case, ())[:k + 1]",
        METRIC_TESTS,
    ),
    Mutation("recall_unknown_case", METRIC, "or set(predictions) - set(gold)", "", METRIC_TESTS),
    Mutation(
        "recall_duplicate_ranks",
        METRIC,
        "if any(len(ranking) != len(set(ranking)) for ranking in predictions.values()):",
        "if False:",
        METRIC_TESTS,
    ),
    Mutation(
        "attribution_deduplicate",
        METRIC,
        "unique = frozenset(predicted)",
        "unique = predicted",
        METRIC_TESTS,
    ),
    Mutation(
        "attribution_relation",
        METRIC,
        "int(link in gold)",
        "int(any(label.evidence_id == link.evidence_id for label in gold))",
        METRIC_TESTS,
    ),
    Mutation(
        "attribution_owner", METRIC, "or item.incident_id != link.incident_id", "", METRIC_TESTS
    ),
    Mutation(
        "attribution_alias", METRIC, "or item.evidence_id != link.evidence_id", "", METRIC_TESTS
    ),
    Mutation(
        "attribution_artifact",
        METRIC,
        "            store.verify(item)",
        "            pass",
        METRIC_TESTS,
    ),
    Mutation(
        "percentile_estimator",
        METRIC,
        "math.ceil(quantile * len(samples))",
        "math.floor(quantile * len(samples))",
        METRIC_TESTS,
    ),
    Mutation("percentile_negative", METRIC, "or sample < 0", "", METRIC_TESTS),
    Mutation(
        "graph_attempt_limit", NODES, "used >= state.budget.max_steps", "False", GRAPH_TESTS
    ),
    Mutation(
        "graph_attempt_durability",
        NODES,
        '    with (directory / f"attempt-{used + 1:02d}.json").open("xb") as stream:\n'
        "        stream.write(record.model_dump_json().encode())\n"
        "        stream.flush()\n"
        "        os.fsync(stream.fileno())",
        "    # Mutation: lose reservation before a possible operation crash.\n    del record",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_node_start_deadline",
        NODES,
        "or utc_now() >= state.budget.node_start_deadline",
        "",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_read_budget",
        NODES,
        "if reserved > state.budget.max_tool_calls:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_dispatch_exclusive",
        NODES,
        'with (output / "collection-dispatched").open("xb") as marker:',
        'with (output / "collection-dispatched").open("wb") as marker:',
        GRAPH_TESTS,
    ),
    Mutation("graph_retained_batch", NODES, "if result_path.exists():", "if False:", GRAPH_TESTS),
    Mutation(
        "graph_batch_owner",
        NODES,
        "if result.incident_id != state.incident.incident_id:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_integrity_outcome",
        NODES,
        '            except EvidenceIntegrityError:\n'
        '                return updated(state, terminal="SECURITY_BLOCK")',
        '            except EvidenceIntegrityError:\n'
        '                return updated(state, terminal="EVIDENCE_INSUFFICIENT")',
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_namespace_scope",
        NODES,
        'request.namespace == "payops-sandbox" and request.service in SERVICES',
        "request.service in SERVICES",
        GRAPH_TESTS,
    ),
    Mutation("graph_report_mode", NODES, "mode=current.mode", 'mode="local_kind"', GRAPH_TESTS),
    Mutation(
        "graph_resume_mode", GRAPH, "if existing.mode != self.mode:", "if False:", GRAPH_TESTS
    ),
    Mutation(
        "graph_thread_identity",
        GRAPH,
        "if existing.incident != initial.incident:",
        "if False:",
        GRAPH_TESTS,
    ),
    Mutation(
        "graph_worker_lock",
        GRAPH,
        "FileLock(lock_path, timeout=0)",
        '__import__("contextlib").nullcontext()',
        GRAPH_TESTS,
    ),
)


def snapshot(repo: Path, destination: Path) -> dict[str, str]:
    """Copy only Python source/tests, excluding credentials, environments and cloud state."""
    hashes: dict[str, str] = {}
    for directory in ("src", "tests"):
        for source in sorted((repo / directory).rglob("*.py")):
            relative = source.relative_to(repo)
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            content = source.read_bytes()
            target.write_bytes(content)
            hashes[relative.as_posix()] = hashlib.sha256(content).hexdigest()
    (destination / "pytest.ini").write_text("[pytest]\naddopts = --strict-markers\n")
    return hashes


def classify(report: Path, returncode: int) -> tuple[str, list[dict[str, str]]]:
    """Only assertion failures or explicit pytest expectation failures count as killed."""
    if not report.exists():
        return "infrastructure_error", []
    try:
        root = ET.parse(report).getroot()
    except ET.ParseError:
        return "infrastructure_error", []
    errors = list(root.iter("error"))
    failures = list(root.iter("failure"))
    details = [
        {"type": node.get("type", ""), "message": node.get("message", "")}
        for node in errors + failures
    ]
    if errors:
        return "collection_or_setup_error", details
    if returncode == 0 and not failures:
        return "survived", details
    assertion_failures = [
        node
        for node in failures
        if node.get("message", "").startswith(
            ("AssertionError", "assert ", "Failed: DID NOT RAISE")
        )
    ]
    if returncode == 1 and assertion_failures:
        return "killed_by_assertion", details
    return "test_runtime_error", details


def run_tests(workspace: Path, selection: str) -> dict[str, Any]:
    """Fresh interpreters verify imports point into the snapshot before selected tests run."""
    keep = {"SYSTEMROOT", "WINDIR", "PATH", "PATHEXT", "TEMP", "TMP", "TMPDIR", "COMSPEC"}
    environment = {key: value for key, value in os.environ.items() if key.upper() in keep}
    environment.update(
        PYTHONPATH=str(workspace / "src"),
        PYTHONDONTWRITEBYTECODE="1",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
    )
    report = workspace / "junit.xml"
    probe = subprocess.run(
        [sys.executable, "-c", "import payops; print(payops.__file__)"],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if probe.returncode or str(workspace) not in probe.stdout:
        return {"status": "import_isolation_error", "import_probe": probe.stdout + probe.stderr}
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-c",
        "pytest.ini",
        "-q",
        "--tb=short",
        f"--junitxml={report}",
        *selection.split(),
    ]
    try:
        result = subprocess.run(
            command, cwd=workspace, env=environment, capture_output=True, text=True, timeout=60
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "command": command}
    (workspace / "pytest.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
    status, details = classify(report, result.returncode)
    return {
        "status": status,
        "returncode": result.returncode,
        "failures": details,
        "command": command,
        "import_probe": probe.stdout.strip(),
    }


def run_mutation(baseline: Path, output: Path, mutation: Mutation) -> dict[str, Any]:
    """Preserve each complete mutated snapshot and exact diff inputs for later review."""
    workspace = output / mutation.name
    shutil.copytree(baseline, workspace, ignore=shutil.ignore_patterns("junit.xml", "pytest.txt"))
    path = workspace / mutation.path
    original = path.read_text(encoding="utf-8")
    if original.count(mutation.before) != 1:
        return {"mutation": asdict(mutation), "status": "replacement_mismatch"}
    mutated = original.replace(mutation.before, mutation.after, 1)
    try:
        ast.parse(mutated)
    except SyntaxError as error:
        return {"mutation": asdict(mutation), "status": "invalid_syntax", "error": str(error)}
    path.write_text(mutated, encoding="utf-8")
    return {"mutation": asdict(mutation), **run_tests(workspace, mutation.test)}


def run_metadata(repo: Path, baseline: Path) -> dict[str, Any]:
    """Record actual source bytes and environment versions, including uncommitted inputs."""
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    return {
        "git_sha": revision,
        "lock_sha256": hashlib.sha256((repo / "uv.lock").read_bytes()).hexdigest(),
        "source_hashes": snapshot(repo, baseline),
        "started_at": datetime.now(UTC).isoformat(),
        "python": sys.version,
        "dependency_versions": {name: version(name) for name in ("pytest", "pydantic")},
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "results": [],
    }


def main() -> int:
    """Non-killed mutants return a failing gate while retaining every result for diagnosis."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo, parent = args.repo.resolve(), args.output.resolve()
    if parent == repo or repo in parent.parents:
        parser.error("mutation evidence must be outside the source repository")
    parent.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="semantic-", dir=parent))
    baseline = output / "baseline"
    manifest = run_metadata(repo, baseline)
    selection = f"{SCHEMA_TESTS} {EVIDENCE_TESTS} {METRIC_TESTS} {GRAPH_TESTS}"
    baseline_result = run_tests(baseline, selection)
    manifest["baseline"] = baseline_result
    if baseline_result["status"] != "survived":
        manifest["gate"] = "baseline_failed"
    else:
        for mutation in MUTATIONS:
            result = run_mutation(baseline, output, mutation)
            manifest["results"].append(result)
            print(f"{mutation.name}: {result['status']}", flush=True)
        manifest["gate"] = (
            "pass"
            if all(result["status"] == "killed_by_assertion" for result in manifest["results"])
            else "fail"
        )
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Evidence: {output}", flush=True)
    return 0 if manifest["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
