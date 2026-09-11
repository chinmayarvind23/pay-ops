"""A fixed denial denominator requires real broker attempts and working positive controls."""

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from payops.evaluation.attack_suite import (
    CONTEXTS,
    EXPECTED_IDS,
    ROOT,
    AttackManifest,
    AttackSuiteResult,
    FixtureBackend,
    evaluate_attempt,
    load_manifest,
    run_attack_suite,
    source_hashes,
)
from payops.evidence.artifacts import ArtifactStore
from payops.policy.contracts import ACTION, Action
from payops.remediation.broker import RemediationBroker
from payops.remediation.contracts import ActionRecord, EffectReceipt
from payops.remediation.store import ActionStore

MANIFEST = ROOT / "evals/attacks/unauthorized_remediations.yaml"


@pytest.fixture(autouse=True)
def stable_unit_source_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unrelated active edits cannot change fixture runs; the source-change test overrides this."""
    snapshot = source_hashes()
    monkeypatch.setattr("payops.evaluation.attack_suite.source_hashes", lambda: snapshot)


def test_original_manifest_runs_all_cases_and_separate_approved_controls(tmp_path: Path) -> None:
    """The original five repeated types reach the broker while all24 approved controls dispatch."""
    output = tmp_path / "complete"
    result = run_attack_suite(MANIFEST, output)
    assert result.passed and len(result.attempts) == 120 and len(result.controls) == 24
    assert {row.case.attack_id for row in result.attempts} == set(EXPECTED_IDS)
    assert all(
        row.outcome == "DENY" and row.reason == "INVALID_PROPOSAL" for row in result.attempts
    )
    assert sum(row.executor_calls for row in result.attempts) == 0
    assert sum(row.executor_calls for row in result.controls) == 24
    assert all(row.repeat_executor_calls == 0 for row in result.controls)
    assert AttackSuiteResult.model_validate_json((output / "results.json").read_bytes()) == result
    summary = json.loads((output / "summary.json").read_text())
    assert summary["denied_count"] == summary["attempt_count"] == 120
    assert summary["unique_forbidden_capabilities"] == 5 and summary["named_fixture_contexts"] == 24
    assert summary["positive_controls"] == 24 and summary["error_count"] == 0
    assert len((output / "attempts.jsonl").read_text().splitlines()) == 120
    assert {
        "src/payops/evidence/redact.py",
        "src/payops/evidence/payment_window.py",
        "pyproject.toml",
        "uv.lock",
    } <= result.source_sha256.keys()
    assert len(result.manifest_sha256) == 64
    for row in result.attempts:
        assert "parameters" not in row.broker_payload
        assert row.broker_payload["action_type"] == row.case.proposed_action
        allowed = ACTION.validate_python(
            {**row.broker_payload, "action_type": "restart_deployment"}
        )
        assert allowed.namespace == "payops-sandbox"
    namespace_attempt = result.attempts[0]
    assert namespace_attempt.case.parameters == {"namespace": "payments-sandbox"}
    assert namespace_attempt.broker_payload["namespace"] == "payops-sandbox"


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate",
        "count",
        "version_bool",
        "context",
        "id",
        "action",
        "parameter",
        "risk",
        "expected",
        "executor",
        "executor_integer",
        "extra",
    ],
)
def test_manifest_changes_fail_before_any_broker_work(tmp_path: Path, mutation: str) -> None:
    """Neither missing rows nor changed expectations can make the fixed denominator easier."""
    data: dict[str, Any] = yaml.safe_load(MANIFEST.read_text())
    if mutation == "missing":
        data["attempts"].pop()
    elif mutation == "duplicate":
        data["attempts"][-1] = data["attempts"][0]
    elif mutation == "count":
        data["attempt_count"] = 119
    elif mutation == "version_bool":
        data["version"] = True
    else:
        changes = {
            "context": ("scenario_id", "FOREIGN-01"),
            "id": ("attack_id", "wrong"),
            "action": ("proposed_action", "restart_deployment"),
            "parameter": ("parameters", {"namespace": "payops-sandbox"}),
            "risk": ("risk_tier", "R5"),
            "expected": ("expected_decision", "ALLOW"),
            "executor": ("must_reach_executor", True),
            "executor_integer": ("must_reach_executor", 0),
            "extra": ("skip", True),
        }
        key, value = changes[mutation]
        data["attempts"][0][key] = value
    changed = tmp_path / "modified.yaml"
    changed.write_text(yaml.safe_dump(data))
    output = tmp_path / "must-not-exist"
    with pytest.raises(ValidationError):
        run_attack_suite(changed, output)
    assert not output.exists()


@pytest.mark.parametrize("kind", ["duplicate_key", "oversize"])
def test_manifest_parser_has_unambiguous_bounded_input(tmp_path: Path, kind: str) -> None:
    """The loader rejects overwritten YAML keys and inputs larger than its finite byte budget."""
    altered = tmp_path / "bad.yaml"
    altered.write_text("version: 1\nversion: 1\n" if kind == "duplicate_key" else "x" * 131073)
    with pytest.raises(ValueError, match="duplicate|byte budget"):
        load_manifest(altered)


def test_evidence_output_requires_fresh_external_directory(tmp_path: Path) -> None:
    """A new run cannot overwrite old measurements or write evidence into application source."""
    with pytest.raises(ValueError, match="outside"):
        run_attack_suite(MANIFEST, ROOT / "forbidden-eval-output")
    with pytest.raises(FileExistsError):
        run_attack_suite(MANIFEST, tmp_path)


@pytest.mark.parametrize("failure", ["accept", "exception", "dispatch_then_deny", "other_deny"])
def test_attempt_records_real_acceptance_error_and_dispatch_separately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """A caught exception alone never proves a safe rejected attempt with zero effects."""
    manifest, _ = load_manifest(MANIFEST)
    backend = FixtureBackend(CONTEXTS[0], ArtifactStore(tmp_path / "artifacts"))
    assert backend.principal("unknown") is None
    store = ActionStore(f"sqlite:///{tmp_path / 'actions.db'}")
    broker = RemediationBroker(store, backend, mode="fixture_replay")
    original = broker.propose

    def broken(value: dict[str, Any], subject: str) -> ActionRecord:
        """Inject a boundary regression while retaining the real approved-action parser/store."""
        if failure == "exception":
            raise RuntimeError("sensitive failure detail")
        if failure == "dispatch_then_deny":
            backend.effects.append("unexpected-effect")
            raise PermissionError("INVALID_PROPOSAL")
        if failure == "other_deny":
            raise PermissionError("sensitive provider detail")
        return original({**value, "action_type": "restart_deployment"}, subject)

    monkeypatch.setattr(broker, "propose", broken)
    try:
        result = evaluate_attempt(manifest.attempts[0], broker, backend)
    finally:
        store.close()
    if failure == "accept":
        assert result.outcome == "ACCEPTED"
    elif failure == "exception":
        assert result.outcome == "ERROR" and result.reason == "RuntimeError"
    elif failure == "dispatch_then_deny":
        assert result.outcome == "DENY" and result.executor_calls == 1
    else:
        assert result.outcome == "DENY" and result.reason == "OTHER_DENIAL"
    assert "sensitive" not in result.model_dump_json()


def test_disabled_dispatcher_cannot_produce_a_passing_measurement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An always-failing executor invalidates positive controls even when every attack is denied."""

    def failed(self: FixtureBackend, action: Action, idempotency_key: str) -> EffectReceipt:
        """Model a disconnected fixture dispatcher without creating any real effect."""
        raise RuntimeError("disconnected")

    monkeypatch.setattr(FixtureBackend, "execute", failed)
    result = run_attack_suite(MANIFEST, tmp_path / "failed-controls")
    assert all(row.outcome == "DENY" for row in result.attempts)
    assert not result.passed and not any(row.succeeded for row in result.controls)


def test_always_deny_broker_aborts_without_a_completed_score(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Positive controls prevent a completely disabled proposal path being marketed as safe."""

    def denied(self: RemediationBroker, value: dict[str, Any], subject: str) -> ActionRecord:
        """Reject even the valid control to expose a nonfunctional boundary."""
        raise PermissionError("INVALID_PROPOSAL")

    monkeypatch.setattr(RemediationBroker, "propose", denied)
    output = tmp_path / "deny-all"
    with pytest.raises(PermissionError):
        run_attack_suite(MANIFEST, output)
    assert not (output / "results.json").exists()
    assert not (output / "summary.json").exists()


def test_source_change_during_measurement_aborts_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run cannot mix results with source hashes from a different implementation."""
    sequence = iter(({"source": "before"}, {"source": "after"}))
    monkeypatch.setattr("payops.evaluation.attack_suite.source_hashes", lambda: next(sequence))
    output = tmp_path / "changed-source"
    with pytest.raises(RuntimeError, match="source changed"):
        run_attack_suite(MANIFEST, output)
    assert not (output / "results.json").exists()


def test_result_denominator_and_failed_attempts_cannot_be_hidden(tmp_path: Path) -> None:
    """Retain every original identity, including accepted or failing cases."""
    result = run_attack_suite(MANIFEST, tmp_path / "denominator")
    duplicate = (result.attempts[0], *result.attempts[:-1])
    with pytest.raises(ValidationError, match="denominator"):
        AttackSuiteResult.model_validate({**result.model_dump(), "attempts": duplicate})
    controls = (result.controls[0], *result.controls[:-1])
    with pytest.raises(ValidationError, match="denominator"):
        AttackSuiteResult.model_validate({**result.model_dump(), "controls": controls})
    altered = result.attempts[0].model_copy(update={"outcome": "ACCEPTED"})
    assert not result.model_copy(update={"attempts": (altered, *result.attempts[1:])}).passed
    unrelated = result.attempts[0].model_copy(update={"reason": "OTHER_DENIAL"})
    assert not result.model_copy(update={"attempts": (unrelated, *result.attempts[1:])}).passed
    with pytest.raises(ValidationError):
        AttackManifest.model_validate({"version": 1, "attempt_count": 120, "attempts": []})
