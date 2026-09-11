"""Outer content hashes cannot substitute for retained and reverified source provenance."""

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import JSON_OBJECT, ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize
from payops.memory.data_clients import (
    CacheEntry,
    EvidenceScope,
    RedisDerivedCache,
    RetrievalLineage,
    verify_retrieval_evidence,
)


def original(store: ArtifactStore) -> EvidenceItem:
    """An old runbook retains its real observation date when retrieved for a new incident."""
    observed = utc_now() - timedelta(days=30)
    return normalize(
        Observation(
            source="RUNBOOK",
            resource="payments",
            observed_at=observed,
            query="operator-runbook-v1",
            summary="Check upstream availability",
            payload={"namespace": "payops-sandbox", "service": "payments"},
        ),
        "source-incident",
        observed,
        utc_now(),
        store,
    )


def retrieved(store: ArtifactStore, source: EvidenceItem) -> EvidenceItem:
    """Construct a correctly hashed wrapper without invoking the verifier under test."""
    now = utc_now()
    lineage = RetrievalLineage(
        scope=EvidenceScope(incident_id="current", namespace="payops-sandbox", service="payments"),
        index="payops-runbooks-v1",
        source=source,
        retrieved_at=now,
    )
    return normalize(
        Observation(
            source="RUNBOOK",
            resource="payments",
            observed_at=source.observed_at,
            query="elasticsearch://payops-runbooks-v1/fixed-scope-match",
            summary=source.summary,
            payload=JSON_OBJECT.validate_python(lineage.model_dump(mode="json")),
        ),
        "current",
        source.observed_at,
        now,
        store,
    )


def republish(store: ArtifactStore, envelope: dict[str, Any]) -> EvidenceItem:
    """Rehash modified bytes so only semantic lineage validation can catch the forgery."""
    provisional = EvidenceItem.model_validate(
        {**envelope["evidence"], "artifact_uri": "pending://artifact", "artifact_sha256": "0" * 64}
    )
    envelope["evidence"] = provisional.model_dump(
        mode="json", exclude={"artifact_uri", "artifact_sha256"}
    )
    uri, digest = store.write(JSON_OBJECT.validate_python(envelope))
    return EvidenceItem.model_validate(
        {**envelope["evidence"], "artifact_uri": uri, "artifact_sha256": digest}
    )


def test_complete_source_and_times_survive_serialization(tmp_path: Path) -> None:
    """A later process can reverify the original source without relying on an in-memory lookup."""
    store = ArtifactStore(tmp_path)
    source = original(store)
    item = EvidenceItem.model_validate_json(retrieved(store, source).model_dump_json())
    reopened = ArtifactStore(tmp_path)
    lineage = verify_retrieval_evidence(item, reopened)
    assert lineage.source == source
    assert item.observed_at == source.observed_at
    assert lineage.source.collected_at < item.collected_at
    assert (utc_now() - item.observed_at).days == 30
    assert lineage.retrieval_only is True


@pytest.mark.parametrize(
    "change", ["index", "scope", "summary", "source_time", "retrieved_time", "outer_time", "kind"]
)
def test_rehashed_wrapper_cannot_relabel_source_truth(tmp_path: Path, change: str) -> None:
    """A valid outer digest cannot manufacture scope, timestamps, kind or summary lineage."""
    store = ArtifactStore(tmp_path)
    item = retrieved(store, original(store))
    data: dict[str, Any] = dict(store.verify(item))
    payload, metadata = data["payload"], data["evidence"]
    if change == "index":
        payload["index"] = "payops-memory-v1"
    elif change == "scope":
        payload["scope"]["namespace"] = "foreign"
    elif change == "summary":
        metadata["summary"] = "invented conclusion"
    elif change == "source_time":
        payload["source"]["observed_at"] = utc_now().isoformat()
    elif change == "retrieved_time":
        payload["retrieved_at"] = (utc_now() - timedelta(days=60)).isoformat()
    elif change == "outer_time":
        metadata["observed_at"] = utc_now().isoformat()
    else:
        metadata["source"] = "LOG"
    forged = republish(store, data)
    store.verify(forged)
    with pytest.raises(EvidenceIntegrityError):
        verify_retrieval_evidence(forged, store)


def test_source_tamper_after_retrieval_invalidates_cache_hit(tmp_path: Path) -> None:
    """A cached wrapper must traverse its nested source again on every retrieval."""
    store = ArtifactStore(tmp_path)
    source = original(store)
    item = retrieved(store, source)
    binding = EvidenceScope(incident_id="current", namespace="payops-sandbox", service="payments")

    class Wire:
        """Preserve the exact wrapper while a separate source artifact is corrupted."""

        data = b""

        def put(self, key: str, value: bytes, ttl: int) -> None:
            """Store the correctly validated envelope."""
            self.data = value

        def read(self, key: str) -> bytes:
            """Return unchanged outer bytes to isolate nested provenance verification."""
            return self.data

    cache = RedisDerivedCache(Wire(), store)
    cache.put(binding, (item,))
    assert cache.get(binding) == (item,)
    store.path_for(source.artifact_sha256).write_text("tampered after cache write")
    store.verify(item)
    with pytest.raises(EvidenceIntegrityError, match="digest mismatch"):
        cache.get(binding)


def test_recursive_source_lineage_is_verified_and_depth_bounded(tmp_path: Path) -> None:
    """Re-indexed retrieval remains one original source and cannot create unbounded traversal."""
    store = ArtifactStore(tmp_path)
    source = original(store)
    current = source
    for _ in range(8):
        current = retrieved(store, current)
        verify_retrieval_evidence(current, store)
    excessive = retrieved(store, current)
    with pytest.raises(EvidenceIntegrityError, match="depth"):
        verify_retrieval_evidence(excessive, store)
    store.path_for(source.artifact_sha256).write_text("corrupt original")
    with pytest.raises(EvidenceIntegrityError, match="digest mismatch"):
        verify_retrieval_evidence(current, store)


def test_source_payload_must_supply_verified_scope(tmp_path: Path) -> None:
    """A malformed source cannot borrow namespace/service declarations from the index hit."""
    store = ArtifactStore(tmp_path)
    source = original(store)
    raw: dict[str, Any] = dict(store.verify(source))
    raw["payload"] = []
    forged = republish(store, raw)
    item = retrieved(store, forged)
    with pytest.raises(EvidenceIntegrityError, match="source payload"):
        verify_retrieval_evidence(item, store)


def test_valid_foreign_retrieval_cannot_enter_another_namespace_cache(tmp_path: Path) -> None:
    """Matching incident/service IDs cannot override a verified source's different namespace."""
    store = ArtifactStore(tmp_path)
    raw: dict[str, Any] = dict(store.verify(original(store)))
    raw["payload"]["namespace"] = "foreign"
    source = republish(store, raw)
    outer: dict[str, Any] = dict(store.verify(retrieved(store, source)))
    outer["payload"]["scope"]["namespace"] = "foreign"
    valid_foreign = republish(store, outer)
    assert verify_retrieval_evidence(valid_foreign, store).scope.namespace == "foreign"
    binding = EvidenceScope(incident_id="current", namespace="payops-sandbox", service="payments")
    now = utc_now()

    class Wire:
        """Return a valid foreign citation inside a cache envelope that claims the local scope."""

        writes = 0

        def put(self, key: str, value: bytes, ttl: int) -> None:
            """Record any forbidden publication rather than silently accepting it."""
            self.writes += 1

        def read(self, key: str) -> bytes:
            """A poisoned envelope alone cannot establish namespace authority."""
            return (
                CacheEntry(
                    scope=binding,
                    evidence=(valid_foreign,),
                    created_at=now,
                    expires_at=now + timedelta(seconds=60),
                )
                .model_dump_json()
                .encode()
            )

    wire = Wire()
    cache = RedisDerivedCache(wire, store)
    with pytest.raises(EvidenceIntegrityError, match="namespace scope"):
        cache.put(binding, (valid_foreign,))
    assert wire.writes == 0
    with pytest.raises(EvidenceIntegrityError, match="namespace scope"):
        cache.get(binding)
