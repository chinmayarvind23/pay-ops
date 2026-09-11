"""Evidence must survive source tampering, wrong windows and hostile context."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest

from payops.contracts import utc_now
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize, select_context
from payops.evidence.redact import redact, redact_text


def observation() -> Observation:
    """Keep fixtures label-free so observation handling cannot learn a gold cause."""
    return Observation(
        source="LOG",
        resource="payments-api",
        observed_at=utc_now(),
        query="logs.recent",
        summary="Processor returned unavailable",
        payload={"status": 503, "token": "sensitive-test-value"},
    )


def test_roundtrip_redacts_and_verifies(tmp_path: Path) -> None:
    """Both stored payload and context are sanitized before becoming evidence."""
    store = ArtifactStore(tmp_path)
    now = utc_now()
    item = normalize(
        observation(), "incident-1", now - timedelta(minutes=1), now + timedelta(minutes=1), store
    )
    payload = store.verify(item)
    assert payload["payload"] == {"status": 503, "token": "[REDACTED]"}
    assert item.untrusted_text is True
    assert "sensitive-test-value" not in store.path_for(item.artifact_sha256).read_text()


def test_modified_artifact_fails(tmp_path: Path) -> None:
    """A resolving evidence ID is insufficient if underlying bytes changed."""
    store = ArtifactStore(tmp_path)
    now = utc_now()
    item = normalize(
        observation(), "incident-1", now - timedelta(minutes=1), now + timedelta(minutes=1), store
    )
    store.path_for(item.artifact_sha256).write_text('{"changed":true}')
    with pytest.raises(EvidenceIntegrityError):
        store.verify(item)


def test_source_scope_and_metadata_checked(tmp_path: Path) -> None:
    """A valid digest cannot authorize changing normalized source metadata."""
    store = ArtifactStore(tmp_path)
    now = utc_now()
    item = normalize(
        observation(), "incident-1", now - timedelta(minutes=1), now + timedelta(minutes=1), store
    )
    for changes in (
        {"incident_id": "incident-2"},
        {"summary": "different"},
        {"source": "KUBERNETES"},
        {"artifact_uri": "https://attacker.test/x"},
    ):
        with pytest.raises(EvidenceIntegrityError):
            store.verify(item.model_copy(update=changes))


def test_window_and_context_bounds(tmp_path: Path) -> None:
    """Stale telemetry stays outside the current incident's reasoning context."""
    store = ArtifactStore(tmp_path)
    now = utc_now()
    with pytest.raises(ValueError, match="window"):
        normalize(
            observation(), "incident-1", now - timedelta(hours=2), now - timedelta(hours=1), store
        )
    item = normalize(
        observation(), "incident-1", now - timedelta(minutes=1), now + timedelta(minutes=1), store
    )
    assert select_context((item,), store, max_characters=1) == ()
    assert select_context((item,), store) == (item,)


def test_artifact_path_cannot_escape_root(tmp_path: Path) -> None:
    """Only digest-derived paths are accepted; model-supplied URLs are never fetched."""
    with pytest.raises(EvidenceIntegrityError):
        ArtifactStore(tmp_path).path_for("../../outside")


def test_artifact_reuse_corruption_missing_and_size(tmp_path: Path) -> None:
    """Deduplication cannot mask corrupt existing bytes or missing retained evidence."""
    store = ArtifactStore(tmp_path)
    uri, digest = store.write({"same": True})
    assert store.write({"same": True}) == (uri, digest)
    store.path_for(digest).write_text("corrupt")
    with pytest.raises(EvidenceIntegrityError, match="corrupt"):
        store.write({"same": True})
    with pytest.raises(EvidenceIntegrityError, match="budget"):
        store.write({"large": "x" * 1_048_576})
    now = utc_now()
    item = normalize(
        observation(), "incident-1", now - timedelta(minutes=1), now + timedelta(minutes=1), store
    )
    store.path_for(item.artifact_sha256).unlink()
    with pytest.raises(EvidenceIntegrityError, match="unavailable"):
        store.verify(item)


def test_context_rejects_invalid_inputs(tmp_path: Path) -> None:
    """Budgeting must not hide duplicate IDs, mixed scopes or corrupt dropped evidence."""
    store = ArtifactStore(tmp_path)
    now = utc_now()
    item = normalize(
        observation(), "incident-1", now - timedelta(minutes=1), now + timedelta(minutes=1), store
    )
    with pytest.raises(ValueError, match="budget"):
        select_context((item,), store, max_items=65)
    with pytest.raises(EvidenceIntegrityError, match="duplicate"):
        select_context((item, item), store)
    with pytest.raises(EvidenceIntegrityError, match="mixed"):
        select_context((item, item.model_copy(update={"incident_id": "other"})), store)
    store.path_for(item.artifact_sha256).write_text("broken")
    with pytest.raises(EvidenceIntegrityError):
        select_context((item,), store, max_items=0)


def test_recursive_redaction_and_depth_limit() -> None:
    """Sensitive keys and common inline credentials are sanitized at every nesting level."""
    assert redact({"nested": [{"password": "value"}, "token=abc"]}) == {
        "nested": [{"password": "[REDACTED]"}, "token=[REDACTED]"]
    }
    assert redact_text("Authorization: Bearer abc") == "Authorization: Bearer [REDACTED]"
    with pytest.raises(ValueError, match="nesting"):
        redact({}, depth=17)


def test_kubernetes_env_and_quoted_credentials_are_redacted() -> None:
    """Kubernetes encodes secret meaning in the name sibling, not the value key."""
    assert redact({"env": [{"name": "PROCESSOR_API_KEY", "value": "test-secret-marker"}]}) == {
        "env": [{"name": "PROCESSOR_API_KEY", "value": "[REDACTED]"}]
    }
    for value in ('password="two word-secret-marker"', "password='two word-secret-marker'"):
        assert redact_text(value) == "password=[REDACTED]"
    assert redact({"name": "API_KEY", "value": "first", "password": "second"}) == {
        "name": "API_KEY",
        "value": "[REDACTED]",
        "password": "[REDACTED]",
    }


def test_concurrent_artifact_publication(tmp_path: Path) -> None:
    """Racing identical collectors must see complete bytes at the digest path."""
    store = ArtifactStore(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(store.write, [{"same": "x" * 50000}] * 16))
    assert len(set(results)) == 1
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert not list(tmp_path.glob("*.pending"))
