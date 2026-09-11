"""Content-addressed local evidence; cloud object retention is a separate adapter."""

import json
import os
import re
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile

from pydantic import JsonValue, TypeAdapter

from payops.contracts import EvidenceItem

JSON_OBJECT = TypeAdapter(dict[str, JsonValue])
MAX_ARTIFACT_BYTES = 1_048_576


def publish_once(path: Path, content: bytes) -> None:
    """Publish only fsynced complete bytes; a hard link prevents an overwrite race."""
    with NamedTemporaryFile(dir=path.parent, suffix=".pending", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise EvidenceIntegrityError("existing artifact is corrupt") from None
    finally:
        temporary.unlink(missing_ok=True)


class EvidenceIntegrityError(ValueError):
    """Broken provenance invalidates an observation instead of silently degrading it."""


class ArtifactStore:
    """Digests detect tampering; they do not assert that a telemetry source is truthful."""

    def __init__(self, root: Path) -> None:
        """Artifacts live in an explicitly supplied directory outside application source."""
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, digest: str) -> Path:
        """Never interpret evidence URLs as file paths or outbound requests."""
        if re.fullmatch(r"[a-f0-9]{64}", digest) is None:
            raise EvidenceIntegrityError("invalid artifact digest")
        path = self.root / f"{digest}.json"
        if path.resolve().parent != self.root:
            raise EvidenceIntegrityError("artifact path escapes store")
        return path

    def write(self, payload: dict[str, JsonValue]) -> tuple[str, str]:
        """Exclusive creation avoids overwriting evidence already named by its digest."""
        content = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        if len(content) > MAX_ARTIFACT_BYTES:
            raise EvidenceIntegrityError("artifact exceeds byte budget")
        digest = sha256(content).hexdigest()
        path = self.path_for(digest)
        publish_once(path, content)
        return f"sha256://{digest}", digest

    def verify(self, evidence: EvidenceItem) -> dict[str, JsonValue]:
        """Bind digest, source metadata and incident ownership before using a citation."""
        digest = evidence.artifact_sha256
        if evidence.artifact_uri != f"sha256://{digest}":
            raise EvidenceIntegrityError("artifact URI and digest disagree")
        try:
            with self.path_for(digest).open("rb") as stream:
                content = stream.read(MAX_ARTIFACT_BYTES + 1)
        except OSError as error:
            raise EvidenceIntegrityError("artifact unavailable") from error
        if len(content) > MAX_ARTIFACT_BYTES or sha256(content).hexdigest() != digest:
            raise EvidenceIntegrityError("artifact digest mismatch")
        payload = JSON_OBJECT.validate_json(content)
        expected = evidence.model_dump(mode="json", exclude={"artifact_uri", "artifact_sha256"})
        if payload.get("evidence") != expected:
            raise EvidenceIntegrityError("artifact metadata mismatch")
        return payload
