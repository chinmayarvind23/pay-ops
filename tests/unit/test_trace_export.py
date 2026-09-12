"""Trace export must retain measurements without leaking operational receipt content."""

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from langsmith import Client
from pydantic import ValidationError

from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.orchestrator.loop_records import ModelReceipt, retain
from payops.orchestrator.model_runtime import ModelObservation
from payops.orchestrator.reasoning import ProviderUsage
from payops.telemetry import export
from payops.telemetry.export import ExportWindow, TraceSummary, export_langsmith, summarize


def receipt(store: ArtifactStore, *, usage: bool = True) -> str:
    """Sensitive identifiers are deliberate canaries, separate from metric values."""
    return retain(
        store,
        ModelReceipt(
            run_id="private-incident",
            operation_id="private-operation",
            prompt_sha256="a" * 64,
            observation=ModelObservation(
                status="INVALID_OUTPUT",
                provider_seconds=1.25,
                usage=ProviderUsage(
                    input_tokens=10, output_tokens=5, total_tokens=15, cached_input_tokens=2
                )
                if usage
                else None,
            ),
        ),
    )


def test_summary_has_no_receipt_identifiers(tmp_path: Path) -> None:
    """The exported schema preserves measured counts and excludes all raw input fields."""
    store = ArtifactStore(tmp_path)
    summary = summarize(store, receipt(store))
    raw = summary.model_dump_json()
    assert "private" not in raw and "prompt" not in raw and "operation" not in raw
    assert (summary.input_tokens, summary.output_tokens, summary.cached_input_tokens) == (10, 5, 2)
    assert summary.provider_seconds == 1.25


def test_unknown_usage_is_not_zero(tmp_path: Path) -> None:
    """Transport failures cannot look like a measured free invocation."""
    store = ArtifactStore(tmp_path)
    summary = summarize(store, receipt(store, usage=False))
    assert summary.input_tokens is None and summary.output_tokens is None


def test_tampered_receipt_is_not_exported(tmp_path: Path) -> None:
    """Changing a retained measurement invalidates the digest before projection."""
    store = ArtifactStore(tmp_path)
    digest = receipt(store)
    store.path_for(digest).write_text("{}", encoding="utf-8")
    with pytest.raises(EvidenceIntegrityError):
        summarize(store, digest)


def test_sdk_export_is_explicit_and_stable(tmp_path: Path) -> None:
    """Repeated exports identify the same receipt and expose only the reviewed scalar schema."""
    store = ArtifactStore(tmp_path)
    summary = summarize(store, receipt(store))
    client = create_autospec(Client, instance=True)
    now = datetime.now(UTC)
    window = ExportWindow(started_at=now, ended_at=now + timedelta(seconds=2))
    first = export_langsmith(client, summary, window, "payops-test")
    assert export_langsmith(client, summary, window, "payops-test") == first
    assert client.create_run.call_count == 2
    args, kwargs = client.create_run.call_args
    assert args == ("payops.model.receipt", {}, "llm")
    assert kwargs["outputs"] == {} and kwargs["start_time"] == now
    assert kwargs["extra"] == {"metadata": summary.model_dump(mode="json")}
    assert "private" not in json.dumps(kwargs, default=str)


@pytest.mark.parametrize("project", ["", "x" * 101])
def test_invalid_project_never_dispatches(tmp_path: Path, project: str) -> None:
    """Invalid host configuration fails before any SDK call."""
    store = ArtifactStore(tmp_path)
    client = create_autospec(Client, instance=True)
    now = datetime.now(UTC)
    with pytest.raises(ValueError):
        export_langsmith(
            client,
            summarize(store, receipt(store)),
            ExportWindow(started_at=now, ended_at=now),
            project,
        )
    client.create_run.assert_not_called()


def test_window_requires_real_ordered_aware_times() -> None:
    """Naive and inverted bounds are rejected, preserving the meaning of trace duration."""
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        ExportWindow(started_at=now, ended_at=now - timedelta(seconds=1))
    with pytest.raises(ValidationError):
        ExportWindow(started_at=now.replace(tzinfo=None), ended_at=now)


def test_summary_forbids_extra_evidence_fields() -> None:
    """No caller can extend the hosted metadata payload with prompt or log content."""
    with pytest.raises(ValidationError):
        TraceSummary.model_validate(
            {
                "receipt_sha256": "a" * 64,
                "run_sha256": "b" * 64,
                "status": "ERROR",
                "prompt": "private",
            }
        )


@pytest.mark.parametrize("hosted", [False, True])
def test_cli_default_is_local_and_hosted_requires_explicit_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, hosted: bool
) -> None:
    """Exercise the entry point and ensure local review never constructs a hosted client."""
    store = ArtifactStore(tmp_path / "artifacts")
    digest = receipt(store)
    output = tmp_path / "summary.json"
    factory = create_autospec(Client)
    monkeypatch.setattr(export, "Client", factory)
    argv = ["export", "--artifacts", str(store.root), "--receipt", digest, "--output", str(output)]
    if hosted:
        key = tmp_path / "key"
        key.write_text("test-only-key", encoding="utf-8")
        argv += [
            "--langsmith-key-file",
            str(key),
            "--started-at",
            "2026-01-01T00:00:00Z",
            "--ended-at",
            "2026-01-01T00:00:02Z",
        ]
    monkeypatch.setattr(sys, "argv", argv)
    export.main()
    assert json.loads(output.read_text(encoding="utf-8"))["input_tokens"] == 10
    assert factory.call_count == int(hosted)
    if hosted:
        assert factory.call_args.kwargs["hide_inputs"] is True
        assert factory.call_args.kwargs["omit_traced_runtime_info"] is True
        factory.return_value.create_run.assert_called_once()
        factory.return_value.close.assert_called_once()
    with pytest.raises(FileExistsError):
        export.main()


def test_empty_key_never_constructs_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty explicit file must not fall back to ambient LangSmith credentials."""
    store = ArtifactStore(tmp_path / "artifacts")
    key = tmp_path / "key"
    key.write_text(" ", encoding="utf-8")
    factory = create_autospec(Client)
    monkeypatch.setattr(export, "Client", factory)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export",
            "--artifacts",
            str(store.root),
            "--receipt",
            receipt(store),
            "--output",
            str(tmp_path / "summary.json"),
            "--langsmith-key-file",
            str(key),
            "--started-at",
            "2026-01-01T00:00:00Z",
            "--ended-at",
            "2026-01-01T00:00:02Z",
        ],
    )
    with pytest.raises(ValueError, match="credential"):
        export.main()
    factory.assert_not_called()
