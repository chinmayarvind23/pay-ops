"""Project the frozen four-case development run into facts safe for a static public replay."""

import argparse
import json
from hashlib import sha256
from pathlib import Path
from typing import cast

from pydantic import JsonValue

from payops.contracts import IncidentReport
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, publish_once
from payops.evidence.verification import verify_evidence
from payops.scenarios.contracts import ScenarioReceipt, object_items, object_value

REVISION = "6aa083a548e659c5fb6ce916070a9f377927e64d"
SCORE_SHA256 = "9d5eba0fe45054cb57c747d7eb5cab9a79e988951e31ccb1310772ce9f46fe44"
SOURCE_MANIFEST_SHA256 = "2b009155d7ff41e15c02eb8f16cf63c04adbb34e0f52147fa0182ef882c0826b"
TITLES = {
    "ROLLOUT-01": "Application exits during startup",
    "ROLLOUT-02": "Invalid processor configuration",
    "ROLLOUT-03": "Readiness probe returns 404",
    "DEP-01": "Processor has no running replicas",
}
CAUSES = {
    "STARTUP_FAILURE",
    "INVALID_CONFIGURATION",
    "READINESS_PROBE_FAILURE",
    "PROCESSOR_UNAVAILABLE",
}
SERVICES = ("payments-api", "risk-sim", "processor-adapter", "ledger-sim", "webhook-sim")


def verify_receipt(root: Path, receipt: ScenarioReceipt, case: str) -> None:
    """Retained cleanup claims require every referenced injection artifact to remain intact."""
    if (
        receipt.case_id != case
        or receipt.mode != "local_kind"
        or not receipt.activated
        or not receipt.cleanup_verified
        or receipt.failure
        or receipt.cleanup_failure
        or receipt.investigation_status != "completed"
        or not 1 <= len(receipt.artifacts) <= 512
    ):
        raise ValueError("development case was not successfully restored")
    for artifact in receipt.artifacts:
        path = contained(root, f"injections/{receipt.run_id}/{artifact.name}")
        if sha256(read_bytes(path, 1048576)).hexdigest() != artifact.sha256:
            raise ValueError("injection artifact digest differs")


def read_bytes(path: Path, maximum: int = 262144) -> bytes:
    """Reject oversized files before allocating beyond the declared input allowance."""
    with path.open("rb") as stream:
        content = stream.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError("replay input exceeds byte bound")
    return content


def read_object(path: Path, maximum: int = 262144) -> dict[str, JsonValue]:
    """Operator inputs are bounded files; no URL or evidence URI is dereferenced."""
    return JSON_OBJECT.validate_json(read_bytes(path, maximum))


def contained(root: Path, relative: str) -> Path:
    """Normalize historical Windows separators while rejecting paths outside the run."""
    path = (root / relative.replace("\\", "/")).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("replay input escapes the frozen run")
    return path


def numeric_facts(value: dict[str, JsonValue], names: tuple[str, ...]) -> dict[str, JsonValue]:
    """Only nonnegative integer counters cross the public projection boundary."""
    return {
        name: value[name]
        for name in names
        if type(value.get(name)) is int and 0 <= cast(int, value[name]) <= 1000000
    }


def pod_facts(status: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Convert runtime states to fixed flags instead of exporting arbitrary status messages."""
    result: dict[str, JsonValue] = {"running": status.get("phase") == "Running"}
    rows = object_items(status.get("containerStatuses", []))
    if len(rows) == 1:
        row = rows[0]
        result.update(numeric_facts(row, ("restartCount",)))
        result["ready"] = row.get("ready") is True
        state = object_value(row.get("state", {}))
        result["crashLoopBackOff"] = (
            object_value(state.get("waiting", {})).get("reason") == "CrashLoopBackOff"
        )
        previous = object_value(object_value(row.get("lastState", {})).get("terminated", {}))
        result.update(numeric_facts(previous, ("exitCode",)))
    return result


def project_facts(payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Export selected numeric facts and fixed pattern indicators, never source free text."""
    result = numeric_facts(payload, ("replicas", "generation", "pod_count"))
    if payload.get("kind") == "Deployment":
        result.update(
            numeric_facts(
                object_value(payload.get("status", {})),
                ("readyReplicas", "availableReplicas", "updatedReplicas"),
            )
        )
    elif payload.get("kind") == "Pod":
        result.update(pod_facts(object_value(payload.get("status", {}))))
    if isinstance(payload.get("message"), str):
        message = str(payload["message"])
        result["readinessProbe404"] = "Readiness probe failed" in message and "404" in message
    if "lines" in payload:
        lines = payload["lines"]
        text = (
            lines
            if isinstance(lines, str)
            else "\n".join(str(line) for line in lines if isinstance(line, str))
            if isinstance(lines, list)
            else ""
        )
        result["configurationValidationError"] = (
            "ValidationError" in text and "SandboxConfig" in text
        )
        result["invalidSyntheticOrigin"] = "destination must be an approved synthetic" in text
        result["http503Observed"] = "503 Service Unavailable" in text
    return result


def project_report(report: IncidentReport, store: ArtifactStore) -> dict[str, JsonValue]:
    """Verify even omitted evidence, then remap citations to local public labels."""
    report = IncidentReport.model_validate_json(report.model_dump_json())
    if report.mode != "local_kind" or report.ranking_method != "deterministic":
        raise ValueError("development replay requires a local deterministic report")
    if len(report.evidence) > 256 or len(report.ranked_root_causes) > 3:
        raise ValueError("development report census exceeds bounds")
    selected = {
        key
        for cause in report.ranked_root_causes
        for key in (*cause.supporting_evidence_ids, *cause.refuting_evidence_ids)
    }
    exported: list[JsonValue] = []
    labels: dict[str, str] = {}
    for item in report.evidence:
        verify_evidence(item, store)
        if item.evidence_id not in selected:
            continue
        service = next(
            (
                name
                for name in SERVICES
                if item.resource == name or item.resource.startswith(name + "-")
            ),
            None,
        )
        if service is None:
            raise ValueError("development evidence has an unknown service")
        label = f"E{len(labels) + 1:02}"
        labels[item.evidence_id] = label
        exported.append(
            {
                "id": label,
                "source": item.source,
                "service": service,
                "observed_at": item.observed_at.isoformat(),
                "source_sha256": item.artifact_sha256,
                "facts": project_facts(object_value(store.verify(item)["payload"])),
            }
        )
    causes: list[JsonValue] = []
    for cause in report.ranked_root_causes:
        if cause.cause_code not in CAUSES:
            raise ValueError("development report has an unknown cause")
        causes.append(
            {
                "cause": cause.cause_code,
                "confidence": cause.confidence,
                "supports": [labels[key] for key in cause.supporting_evidence_ids],
                "refutes": [labels[key] for key in cause.refuting_evidence_ids],
                "missing_evidence_count": len(cause.missing_evidence),
            }
        )
    return {
        "evidence": exported,
        "causes": causes,
        "evidence_count": len(report.evidence),
        "terminal_state": report.terminal_state,
        "duration_seconds": report.duration_seconds,
        "ranking_method": "deterministic",
    }


def export_run(root: Path) -> dict[str, JsonValue]:
    """Only the declared four-case run can enter this first public data release."""
    score_bytes = read_bytes(root / "score.json")
    if sha256(score_bytes).hexdigest() != SCORE_SHA256 or sha256(
        read_bytes(root / "source-manifest.json")
    ).hexdigest() != SOURCE_MANIFEST_SHA256:
        raise ValueError("reviewed development capture digest differs")
    score = JSON_OBJECT.validate_json(score_bytes)
    if score.get("git_revision") != REVISION or score.get("source_manifest_unchanged") is not True:
        raise ValueError("unexpected development source provenance")
    predictions, hashes = (
        object_value(score["predictions"]),
        object_value(score["prediction_sha256"]),
    )
    if set(predictions) != set(TITLES) or set(hashes) != set(TITLES):
        raise ValueError("development replay requires all four cases")
    cases: list[JsonValue] = []
    for case, title in TITLES.items():
        path = contained(root, str(predictions[case]))
        raw = read_bytes(path)
        if len(raw) > 262144 or sha256(raw).hexdigest() != hashes[case]:
            raise ValueError("frozen prediction digest differs")
        receipt = ScenarioReceipt.model_validate(read_object(root / f"{case}-receipt.json"))
        verify_receipt(root, receipt, case)
        if receipt != ScenarioReceipt.model_validate(object_value(score["runs"])[case]):
            raise ValueError("case receipt differs from reviewed score capture")
        report = IncidentReport.model_validate_json(raw)
        cases.append(
            {
                "case_id": case,
                "title": title,
                "report_sha256": hashes[case],
                "cleanup_verified": True,
                **project_report(report, ArtifactStore(path.parent / "artifacts")),
            }
        )
    return {
        "version": 1,
        "source_revision": REVISION,
        "cases": cases,
        "scope": "Four known local development cases with deterministic ranking",
        "projection": "Selected facts only; raw logs, identifiers and local paths omitted",
        "provider_measurement": False,
        "human_timing_measurement": False,
    }


def main() -> None:
    """Create a reviewable static bundle without contacting any runtime or provider."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    content = json.dumps(export_run(args.run.resolve()), indent=2, allow_nan=False).encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    publish_once(args.output, content)


if __name__ == "__main__":
    main()
