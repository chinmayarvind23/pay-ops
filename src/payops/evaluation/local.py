"""Freeze live predictions before scoring a declared four-case development suite."""

import hashlib
import json
import subprocess
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from payops.contracts import IncidentReport, new_id
from payops.evaluation.metrics import recall_at_k
from payops.evidence.artifacts import ArtifactStore
from payops.orchestrator.baseline import rank_evidence
from payops.scenarios.contracts import CaseId
from payops.scenarios.runner import LocalScenarioRunner
from payops.tools.collect import collect_local
from payops.tools.kubernetes import KubernetesRead
from payops.tools.prometheus import PrometheusRead

INITIAL_CASES: frozenset[CaseId] = frozenset({"ROLLOUT-01", "ROLLOUT-02", "ROLLOUT-03", "DEP-01"})


def write_once(path: Path, content: str) -> str:
    """An existing run cannot be silently rewritten after seeing its score."""
    payload = content.encode("utf-8")
    with path.open("xb") as stream:
        stream.write(payload)
    return hashlib.sha256(payload).hexdigest()


def investigate(kubeconfig: Path, output: Path) -> IncidentReport:
    """Only observed state enters diagnosis; no case ID, recipe, or gold is accepted."""
    started = time.perf_counter()
    collection = collect_local(KubernetesRead(kubeconfig), PrometheusRead(), output)
    hypotheses = rank_evidence(collection.evidence, ArtifactStore(output / "artifacts"))
    report = IncidentReport(
        incident_id=collection.incident_id,
        evidence=collection.evidence,
        ranked_root_causes=hypotheses,
        terminal_state="ESCALATED" if hypotheses else "EVIDENCE_INSUFFICIENT",
        mode="local_kind",
        duration_seconds=time.perf_counter() - started,
    )
    write_once(output / "prediction.json", report.model_dump_json(indent=2))
    return report


def frozen_predictions(paths: dict[str, Path]) -> dict[str, tuple[str, ...]]:
    """Scoring reloads saved predictions instead of trusting mutable callback memory."""
    return {
        case: tuple(
            cause.cause_code
            for cause in IncidentReport.model_validate_json(path.read_bytes()).ranked_root_causes
        )
        for case, path in paths.items()
    }


def investigation_callback(kubeconfig: Path, output: Path) -> Callable[[], None]:
    """Bind one opaque output location without giving diagnosis the scorer's case key."""

    def collect_and_rank() -> None:
        """The scenario runner needs completion, while predictions persist independently."""
        investigate(kubeconfig, output)

    return collect_and_rank


def source_manifest() -> dict[str, str]:
    """Record exact working sources as well as Git revision, including uncommitted changes."""
    root = Path(__file__).resolve().parents[3]
    files = sorted((root / "src" / "payops").rglob("*.py"))
    files.extend(root / name for name in ("pyproject.toml", "uv.lock"))
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files
    }


def unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    """JSON duplicate keys must fail before a parser can silently replace a frozen case."""
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate gold case key")
        result[key] = value
    return result


def load_gold(content: bytes) -> dict[CaseId, tuple[str, ...]]:
    """Reject missing, repeated, or ambiguous labels before any infrastructure mutation."""
    raw = json.loads(content, object_pairs_hook=unique_object)
    gold = TypeAdapter(dict[CaseId, tuple[str, ...]]).validate_python(raw)
    if frozenset(gold) != INITIAL_CASES or any(not causes for causes in gold.values()):
        raise ValueError("initial suite requires all four declared cases")
    recall_at_k({str(case): causes for case, causes in gold.items()}, {}, 1)
    return gold


def run_suite(kubeconfig: Path, output: Path, gold_path: Path) -> Path:
    """Failures remain in the declared denominator; unresolved cleanup stops further injection."""
    gold_bytes = gold_path.read_bytes()
    gold = load_gold(gold_bytes)
    directory = output / new_id()
    directory.mkdir(parents=True)
    write_once(directory / "gold.json", gold_bytes.decode("utf-8"))
    revision = subprocess.run(
        ("git", "rev-parse", "HEAD"), check=True, capture_output=True, text=True
    ).stdout.strip()
    write_once(directory / "revision.txt", revision)
    sources = source_manifest()
    write_once(directory / "source-manifest.json", json.dumps(sources, indent=2))
    runner = LocalScenarioRunner(kubeconfig, directory / "injections")
    predictions: dict[str, Path] = {}
    runs: dict[str, object] = {}
    for case in gold:
        evidence = directory / "investigations" / new_id()
        receipt = runner.run(case, investigation_callback(kubeconfig, evidence))
        runs[case] = receipt.model_dump(mode="json")
        if receipt.investigation_status == "completed":
            predictions[case] = evidence / "prediction.json"
        write_once(directory / f"{case}-receipt.json", receipt.model_dump_json(indent=2))
        if not receipt.cleanup_verified:
            break
    rankings = frozen_predictions(predictions)
    scoring_gold = {str(case): causes for case, causes in gold.items()}
    score = {
        "mode": "local_kind",
        "method": "deterministic-development-baseline",
        "gold_sha256": hashlib.sha256(gold_bytes).hexdigest(),
        "git_revision": revision,
        "source_manifest_unchanged": sources == source_manifest(),
        "recall_at_1": asdict(recall_at_k(scoring_gold, rankings, 1)),
        "recall_at_3": asdict(recall_at_k(scoring_gold, rankings, 3)),
        "predictions": {
            case: str(path.relative_to(directory)) for case, path in predictions.items()
        },
        "prediction_sha256": {
            case: hashlib.sha256(path.read_bytes()).hexdigest()
            for case, path in predictions.items()
        },
        "runs": runs,
        "limitations": [
            "Four known development cases; not the 24-case release benchmark or held-out accuracy",
            "No provider calls, human comparison, or independently labeled attribution score",
            "Investigation clock covers collection through report validation, excluding injection",
        ],
    }
    write_once(directory / "score.json", json.dumps(score, indent=2))
    return directory
