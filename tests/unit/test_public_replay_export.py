"""The public projection retains verified facts without copying private free text or paths."""

import json
import sys
from datetime import timedelta
from hashlib import sha256
from pathlib import Path

import pytest
from pydantic import JsonValue

from payops.contracts import IncidentReport, RootCauseHypothesis, utc_now
from payops.evaluation import public_replay as replay
from payops.evaluation.public_replay import (
    REVISION,
    TITLES,
    contained,
    export_run,
    pod_facts,
    project_facts,
    project_report,
    read_object,
)
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize
from payops.scenarios.contracts import ScenarioReceipt, object_items, object_value


def report_fixture(
    root: Path, resource: str = "payments-api"
) -> tuple[IncidentReport, ArtifactStore]:
    """Include private-looking free text outside the closed public fact vocabulary."""
    store, now = ArtifactStore(root / "artifacts"), utc_now()
    item = normalize(
        Observation(
            source="LOG",
            resource=resource,
            observed_at=now,
            query="logs.recent",
            summary="C:\\private\\operator-notes customer-private-marker",
            payload={
                "lines": "ValidationError SandboxConfig 503 Service Unavailable",
                "unrecognized": "customer-private-marker",
            },
        ),
        "incident-private-marker",
        now - timedelta(seconds=1),
        now + timedelta(seconds=1),
        store,
    )
    report = IncidentReport(
        incident_id=item.incident_id,
        evidence=(item,),
        mode="local_kind",
        terminal_state="ESCALATED",
        duration_seconds=1.5,
        ranked_root_causes=(
            RootCauseHypothesis(
                cause_code="STARTUP_FAILURE",
                confidence=0.75,
                supporting_evidence_ids=(item.evidence_id,),
            ),
        ),
    )
    return report, store


def run_fixture(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Build four isolated fixture reports and hashed cleanup artifacts, never live measurements."""
    predictions: dict[str, JsonValue] = {}
    hashes: dict[str, JsonValue] = {}
    receipts: dict[str, JsonValue] = {}
    for case in TITLES:
        directory = root / "investigations" / case
        report, _ = report_fixture(directory)
        raw = report.model_dump_json().encode()
        (directory / "prediction.json").write_bytes(raw)
        predictions[case] = f"investigations\\{case}\\prediction.json"
        hashes[case] = sha256(raw).hexdigest()
        injection = root / "injections" / case
        injection.mkdir(parents=True)
        (injection / "cleanup.json").write_bytes(b"{}")
        receipt = ScenarioReceipt.model_validate(
            {
                "run_id": case,
                "case_id": case,
                "mode": "local_kind",
                "implementation_variant": "fixture",
                "activated": True,
                "cleanup_verified": True,
                "investigation_status": "completed",
                "artifacts": [{"name": "cleanup.json", "sha256": sha256(b"{}").hexdigest()}],
            }
        )
        (root / f"{case}-receipt.json").write_text(receipt.model_dump_json(), encoding="utf-8")
        receipts[case] = receipt.model_dump(mode="json")
    (root / "score.json").write_text(
        json.dumps(
            {
                "git_revision": REVISION,
                "source_manifest_unchanged": True,
                "predictions": predictions,
                "prediction_sha256": hashes,
                "runs": receipts,
            }
        ),
        encoding="utf-8",
    )
    (root / "source-manifest.json").write_bytes(b"{}")
    monkeypatch.setattr(
        replay, "SCORE_SHA256", sha256((root / "score.json").read_bytes()).hexdigest()
    )
    monkeypatch.setattr(replay, "SOURCE_MANIFEST_SHA256", sha256(b"{}").hexdigest())


def test_public_projection_excludes_free_text_and_preserves_citations(tmp_path: Path) -> None:
    """Public labels still resolve after original incident/evidence identifiers are omitted."""
    report, store = report_fixture(tmp_path)
    result = project_report(report, store)
    raw = json.dumps(result)
    assert "private-marker" not in raw and "operator-notes" not in raw
    assert report.evidence[0].evidence_id not in raw and "sha256://" not in raw
    rows = object_items(result["evidence"])
    assert rows[0]["id"] == "E01"
    assert rows[0]["source_sha256"] == report.evidence[0].artifact_sha256
    assert object_items(result["causes"])[0]["supports"] == ["E01"]
    assert object_value(rows[0]["facts"])["configurationValidationError"] is True


@pytest.mark.parametrize("selected", [True, False])
def test_tampered_evidence_blocks_export_even_if_omitted(tmp_path: Path, selected: bool) -> None:
    """Dropping a citation cannot turn damaged input into a valid public report."""
    report, store = report_fixture(tmp_path)
    store.path_for(report.evidence[0].artifact_sha256).write_bytes(b"{}")
    if not selected:
        report = report.model_copy(update={"ranked_root_causes": ()})
    with pytest.raises(EvidenceIntegrityError):
        project_report(report, store)


@pytest.mark.parametrize(
    "change", [{"mode": "fixture_replay"}, {"ranking_method": "model_provider"}]
)
def test_other_measurement_methods_cannot_enter_development_bundle(
    tmp_path: Path, change: dict[str, str]
) -> None:
    """The fixture/provider discriminator cannot be erased by the exporter."""
    report, store = report_fixture(tmp_path)
    with pytest.raises(ValueError, match="local deterministic"):
        project_report(report.model_copy(update=change), store)


def test_four_case_export_verifies_prediction_and_cleanup_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A changed prediction or restoration artifact invalidates the complete bundle."""
    run_fixture(tmp_path, monkeypatch)
    result = export_run(tmp_path)
    assert len(object_items(result["cases"])) == 4
    assert result["provider_measurement"] is False and result["human_timing_measurement"] is False
    source = tmp_path / "injections" / "ROLLOUT-01" / "cleanup.json"
    source.write_bytes(b'{"changed":true}')
    with pytest.raises(ValueError, match="injection artifact digest"):
        export_run(tmp_path)
    source.write_bytes(b"{}")
    (tmp_path / "investigations" / "ROLLOUT-01" / "prediction.json").write_bytes(b"{}")
    with pytest.raises(ValueError, match="prediction digest"):
        export_run(tmp_path)


@pytest.mark.parametrize(
    "field,value",
    [("git_revision", "other"), ("source_manifest_unchanged", False), ("predictions", {})],
)
def test_incomplete_or_different_run_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, value: JsonValue
) -> None:
    """A four-case view cannot quietly shrink its displayed population."""
    run_fixture(tmp_path, monkeypatch)
    path = tmp_path / "score.json"
    score = read_object(path)
    score[field] = value
    path.write_text(json.dumps(score), encoding="utf-8")
    with pytest.raises(ValueError):
        export_run(tmp_path)


def test_failed_cleanup_and_unknown_causes_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Successful activation does not establish restoration or permit arbitrary public text."""
    run_fixture(tmp_path, monkeypatch)
    path = tmp_path / "ROLLOUT-01-receipt.json"
    receipt = read_object(path)
    receipt["cleanup_verified"] = False
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="successfully restored"):
        export_run(tmp_path)
    report, store = report_fixture(tmp_path / "other")
    unknown = report.ranked_root_causes[0].model_copy(update={"cause_code": "private-marker"})
    with pytest.raises(ValueError, match="unknown cause"):
        project_report(report.model_copy(update={"ranked_root_causes": (unknown,)}), store)


def test_public_fact_whitelist_and_absent_counters() -> None:
    """Missing counters stay absent; free text only produces fixed Boolean indicators."""
    assert project_facts({"replicas": "password", "generation": -1, "pod_count": True}) == {}
    assert project_facts({"kind": "Deployment", "replicas": 0, "status": {}}) == {"replicas": 0}
    assert project_facts({"message": "Readiness probe failed: 404"}) == {"readinessProbe404": True}
    assert (
        project_facts({"lines": ["503 Service Unavailable", {"private": "value"}]})[
            "http503Observed"
        ]
        is True
    )
    assert pod_facts(
        {
            "phase": "Running",
            "containerStatuses": [
                {
                    "ready": False,
                    "restartCount": 1,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                    "lastState": {"terminated": {"exitCode": 1}},
                }
            ],
        }
    ) == {
        "running": True,
        "ready": False,
        "restartCount": 1,
        "crashLoopBackOff": True,
        "exitCode": 1,
    }


def test_paths_and_input_sizes_are_bounded(tmp_path: Path) -> None:
    """Historical path separators do not permit traversal or unbounded source reads."""
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"{}")
    root = tmp_path / "run"
    root.mkdir()
    with pytest.raises(ValueError, match="escapes"):
        contained(root, "..\\outside.json")
    with pytest.raises(ValueError, match="byte bound"):
        read_object(outside, maximum=1)


def test_coherent_case_swap_cannot_relabel_reviewed_predictions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Matching edited path/hash pairs still differ from the independent capture anchor."""
    run_fixture(tmp_path, monkeypatch)
    path = tmp_path / "score.json"
    score = read_object(path)
    for key in ("predictions", "prediction_sha256"):
        values = object_value(score[key])
        values["ROLLOUT-01"], values["ROLLOUT-02"] = values["ROLLOUT-02"], values["ROLLOUT-01"]
    path.write_text(json.dumps(score), encoding="utf-8")
    with pytest.raises(ValueError, match="capture digest"):
        export_run(tmp_path)


@pytest.mark.parametrize(
    "change", [{"mode": "fixture_replay"}, {"implementation_variant": "changed"}]
)
def test_receipt_cannot_diverge_from_reviewed_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: dict[str, str]
) -> None:
    """Even a still-successful receipt must match the pinned operator record and runtime mode."""
    run_fixture(tmp_path, monkeypatch)
    path = tmp_path / "ROLLOUT-01-receipt.json"
    receipt = read_object(path)
    receipt.update(change)
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError):
        export_run(tmp_path)


def test_changed_source_manifest_cannot_claim_reviewed_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reviewed source-manifest digest is independent from editable score flags."""
    run_fixture(tmp_path, monkeypatch)
    (tmp_path / "source-manifest.json").write_bytes(b'{"changed":true}')
    with pytest.raises(ValueError, match="capture digest"):
        export_run(tmp_path)


def test_cli_publishes_once_without_overwriting_a_different_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The operator command produces complete static bytes and refuses a conflicting output."""
    run_fixture(tmp_path, monkeypatch)
    output = tmp_path / "public" / "replay.json"
    monkeypatch.setattr(
        sys, "argv", ["public_replay", "--run", str(tmp_path), "--output", str(output)]
    )
    replay.main()
    assert read_object(output) == export_run(tmp_path)
    replay.main()
    output.write_bytes(b'{"different":true}')
    with pytest.raises(EvidenceIntegrityError):
        replay.main()
    assert output.read_bytes() == b'{"different":true}'


def test_valid_but_oversized_report_rejected_before_source_reads(tmp_path: Path) -> None:
    """A structurally valid large report cannot expand the exporter's fixed source census."""
    report, store = report_fixture(tmp_path)
    items = tuple(
        report.evidence[0].model_copy(update={"evidence_id": f"item-{n}"}) for n in range(257)
    )
    large = report.model_copy(update={"evidence": items, "ranked_root_causes": ()})
    with pytest.raises(ValueError, match="census"):
        project_report(large, store)
    assert pod_facts({"phase": "Pending"}) == {"running": False}
    assert project_facts({"kind": "Pod", "status": {"phase": "Running"}}) == {"running": True}


def test_uncited_observations_are_verified_but_not_exported(tmp_path: Path) -> None:
    """No hypotheses means no citation facts; the original observation census remains visible."""
    report, store = report_fixture(tmp_path)
    result = project_report(report.model_copy(update={"ranked_root_causes": ()}), store)
    assert result["evidence"] == [] and result["causes"] == [] and result["evidence_count"] == 1


def test_unknown_service_name_cannot_escape_public_whitelist(tmp_path: Path) -> None:
    """Even source-verified resource strings cannot publish private service names."""
    report, store = report_fixture(tmp_path, "private-service-name")
    with pytest.raises(ValueError, match="unknown service"):
        project_report(report, store)
