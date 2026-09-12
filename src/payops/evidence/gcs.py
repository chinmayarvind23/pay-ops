"""Opt-in GCS archival keeps local evidence verification and content addressing intact."""

import re
from hashlib import sha256
from typing import Protocol, cast

from google.api_core.exceptions import PreconditionFailed
from google.cloud.storage import Bucket  # pyright: ignore[reportMissingTypeStubs]

from payops.contracts import EvidenceItem
from payops.evidence.artifacts import (
    JSON_OBJECT,
    MAX_ARTIFACT_BYTES,
    ArtifactStore,
    EvidenceIntegrityError,
    publish_once,
)


class ArchiveBlob(Protocol):
    """The bounded subset of Google's partially annotated Blob API used here."""

    name: str

    def download_as_bytes(
        self,
        *,
        start: int,
        end: int,
        raw_download: bool,
        timeout: int,
        retry: None,
        checksum: None,
    ) -> bytes:
        """Read a bounded range without automatic retries or corruption deletion."""
        ...

    def upload_from_string(
        self,
        data: bytes,
        *,
        content_type: str,
        if_generation_match: int,
        timeout: int,
        retry: None,
        checksum: None,
    ) -> None:
        """Conditionally create one object using the verified local bytes."""
        ...


class ArchiveBucket(Protocol):
    """Keep the SDK typing boundary narrow and exclude listing or destructive methods."""

    name: str

    def blob(self, name: str) -> ArchiveBlob:
        """Bind a known content-addressed object without fetching remote metadata."""
        ...


def validate_content(content: bytes, evidence: EvidenceItem) -> None:
    """Remote bytes must support the same identity and metadata as local citations."""
    if len(content) > MAX_ARTIFACT_BYTES:
        raise EvidenceIntegrityError("artifact exceeds byte budget")
    if sha256(content).hexdigest() != evidence.artifact_sha256:
        raise EvidenceIntegrityError("artifact digest mismatch")
    payload = JSON_OBJECT.validate_json(content)
    expected = evidence.model_dump(mode="json", exclude={"artifact_uri", "artifact_sha256"})
    if payload.get("evidence") != expected:
        raise EvidenceIntegrityError("artifact metadata mismatch")


class GcsArtifactArchive:
    """A configured private bucket archives evidence; it never becomes an agent tool."""

    def __init__(self, bucket: Bucket, prefix: str = "payops/evidence") -> None:
        """Only the operator chooses the namespace; observations cannot supply URLs."""
        if re.fullmatch(r"[a-zA-Z0-9_-]+(?:/[a-zA-Z0-9_-]+)*", prefix) is None:
            raise ValueError("invalid archive prefix")
        if len(prefix) > 200:
            raise ValueError("archive prefix exceeds budget")
        self.bucket = cast(ArchiveBucket, bucket)
        self.prefix = prefix

    def _blob(self, evidence: EvidenceItem) -> ArchiveBlob:
        """Reject invalid identities before constructing any remote object request."""
        digest = evidence.artifact_sha256
        if re.fullmatch(r"[a-f0-9]{64}", digest) is None:
            raise EvidenceIntegrityError("invalid artifact digest")
        if evidence.artifact_uri != f"sha256://{digest}":
            raise EvidenceIntegrityError("artifact URI and digest disagree")
        return self.bucket.blob(f"{self.prefix}/{digest}.json")

    def _read(self, blob: ArchiveBlob, evidence: EvidenceItem) -> bytes:
        """Request at most limit+1 bytes, then verify SHA-256 without SDK deletion behavior."""
        content = blob.download_as_bytes(
            start=0,
            end=MAX_ARTIFACT_BYTES,
            raw_download=True,
            timeout=5,
            retry=None,
            checksum=None,
        )
        validate_content(content, evidence)
        return content

    def upload(self, store: ArtifactStore, evidence: EvidenceItem) -> str:
        """Create once; existing or uncertain objects require verified readback, never overwrite."""
        blob = self._blob(evidence)
        store.verify(evidence)
        with store.path_for(evidence.artifact_sha256).open("rb") as stream:
            content = stream.read(MAX_ARTIFACT_BYTES + 1)
        validate_content(content, evidence)
        try:
            blob.upload_from_string(
                content,
                content_type="application/json",
                if_generation_match=0,
                timeout=5,
                retry=None,
                checksum=None,
            )
        except PreconditionFailed:
            pass
        self._read(blob, evidence)
        return f"gs://{self.bucket.name}/{blob.name}"

    def restore(self, store: ArtifactStore, evidence: EvidenceItem) -> None:
        """Publish verified complete bytes atomically; corrupt local evidence is not replaced."""
        content = self._read(self._blob(evidence), evidence)
        publish_once(store.path_for(evidence.artifact_sha256), content)
        store.verify(evidence)
