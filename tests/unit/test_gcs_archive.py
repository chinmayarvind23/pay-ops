"""Archive contracts preserve verified local evidence across explicit SDK transfers."""

from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.cloud.storage import Bucket, Client  # pyright: ignore[reportMissingTypeStubs]
from urllib3.response import HTTPResponse

from payops.contracts import EvidenceItem, utc_now
from payops.evidence.artifacts import MAX_ARTIFACT_BYTES, ArtifactStore, EvidenceIntegrityError
from payops.evidence.gcs import GcsArtifactArchive, validate_content
from payops.evidence.normalize import Observation, normalize


def fixture(tmp_path: Path) -> tuple[ArtifactStore, EvidenceItem, bytes, MagicMock]:
    """Normalize a real observation so the archive cannot bypass metadata verification."""
    store = ArtifactStore(tmp_path / "source")
    now = utc_now()
    item = normalize(
        Observation(
            source="LOG",
            resource="payments-api",
            observed_at=now,
            query="logs.recent",
            summary="Unavailable",
            payload={"status": 503},
        ),
        "incident-1",
        now - timedelta(minutes=1),
        now + timedelta(minutes=1),
        store,
    )
    content = store.path_for(item.artifact_sha256).read_bytes()
    bucket = MagicMock(spec=Bucket)
    bucket.name = "private-evidence"
    bucket.blob.return_value.name = f"payops/evidence/{item.artifact_sha256}.json"
    bucket.blob.return_value.download_as_bytes.return_value = content
    return store, item, content, bucket


@pytest.mark.parametrize("existing", [False, True])
def test_archive_roundtrip_and_duplicate(tmp_path: Path, existing: bool) -> None:
    """New and preexisting objects are read back before a receipt is returned."""
    store, item, content, bucket = fixture(tmp_path)
    if existing:
        bucket.blob.return_value.upload_from_string.side_effect = PreconditionFailed("exists")
    archive = GcsArtifactArchive(bucket)
    assert archive.upload(store, item).startswith("gs://private-evidence/payops/evidence/")
    blob = bucket.blob.return_value
    blob.upload_from_string.assert_called_once_with(
        content,
        content_type="application/json",
        if_generation_match=0,
        timeout=5,
        retry=None,
        checksum=None,
    )
    blob.download_as_bytes.assert_called_once_with(
        start=0,
        end=MAX_ARTIFACT_BYTES,
        raw_download=True,
        timeout=5,
        retry=None,
        checksum=None,
    )
    restored = ArtifactStore(tmp_path / "restored")
    archive.restore(restored, item)
    assert restored.verify(item) == store.verify(item)


@pytest.mark.parametrize("prefix", ["../escape", "/absolute", "a//b", "", "a" * 201])
def test_invalid_prefix(tmp_path: Path, prefix: str) -> None:
    """Reject ambiguous operator namespaces before creating remote object handles."""
    _, _, _, bucket = fixture(tmp_path)
    with pytest.raises(ValueError):
        GcsArtifactArchive(bucket, prefix)
    bucket.blob.assert_not_called()


@pytest.mark.parametrize(
    "field,value", [("artifact_sha256", "../bad"), ("artifact_uri", "https://untrusted.invalid")]
)
def test_invalid_identity(tmp_path: Path, field: str, value: str) -> None:
    """Even bypassed schema validation cannot turn a citation into a remote path."""
    store, item, _, bucket = fixture(tmp_path)
    with pytest.raises(EvidenceIntegrityError):
        GcsArtifactArchive(bucket).restore(store, item.model_copy(update={field: value}))
    bucket.blob.assert_not_called()


@pytest.mark.parametrize(
    "content", [b"corrupt", b"x" * (MAX_ARTIFACT_BYTES + 1)], ids=["corrupt", "oversize"]
)
def test_invalid_remote_bytes_do_not_publish(tmp_path: Path, content: bytes) -> None:
    """Transport success does not establish digest integrity or enforce the byte budget."""
    _, item, _, bucket = fixture(tmp_path)
    bucket.blob.return_value.download_as_bytes.return_value = content
    destination = ArtifactStore(tmp_path / "destination")
    with pytest.raises(EvidenceIntegrityError):
        GcsArtifactArchive(bucket).restore(destination, item)
    assert not destination.path_for(item.artifact_sha256).exists()


def test_matching_digest_wrong_metadata(tmp_path: Path) -> None:
    """A valid object digest cannot authorize a different incident's evidence."""
    _, item, _, _ = fixture(tmp_path)
    content = b'{"evidence": {}}'
    altered = item.model_copy(update={"artifact_sha256": sha256(content).hexdigest()})
    with pytest.raises(EvidenceIntegrityError, match="metadata"):
        validate_content(content, altered)


def test_uncertain_upload_does_not_retry_or_claim_success(tmp_path: Path) -> None:
    """An operator can later retry create-once, but an uncertain request is not replayed here."""
    store, item, _, bucket = fixture(tmp_path)
    bucket.blob.return_value.upload_from_string.side_effect = ServiceUnavailable("uncertain")
    with pytest.raises(ServiceUnavailable):
        GcsArtifactArchive(bucket).upload(store, item)
    bucket.blob.return_value.upload_from_string.assert_called_once()
    bucket.blob.return_value.download_as_bytes.assert_not_called()


def test_corrupt_local_destination_is_preserved(tmp_path: Path) -> None:
    """Restore reports local tampering instead of quietly replacing it."""
    _, item, _, bucket = fixture(tmp_path)
    destination = ArtifactStore(tmp_path / "destination")
    path = destination.path_for(item.artifact_sha256)
    path.write_bytes(b"corrupt")
    with pytest.raises(EvidenceIntegrityError):
        GcsArtifactArchive(bucket).restore(destination, item)
    assert path.read_bytes() == b"corrupt"


def test_real_sdk_request_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise actual SDK serialization over an isolated HTTP session without cloud access."""
    store, item, content, _ = fixture(tmp_path)
    monkeypatch.setenv("DISABLE_GCS_PYTHON_CLIENT_OTEL_BUCKET_METADATA", "true")
    session = MagicMock(spec=requests.Session)
    session.is_mtls = False
    uploaded = requests.Response()
    uploaded.status_code = 200
    uploaded._content = b'{"generation":"7"}'  # pyright: ignore[reportPrivateUsage]
    downloaded = requests.Response()
    downloaded.status_code = 206
    downloaded.raw = HTTPResponse(body=BytesIO(content), preload_content=False)
    downloaded._content = content  # pyright: ignore[reportPrivateUsage]
    downloaded.headers.update(
        {
            "Content-Length": str(len(content)),
            "Content-Range": f"bytes 0-{len(content) - 1}/{len(content)}",
        }
    )
    session.request.side_effect = [uploaded, downloaded]
    client = Client(project="fixture-project", credentials=AnonymousCredentials(), _http=session)
    bucket = Bucket(client, "private-evidence")
    assert GcsArtifactArchive(bucket).upload(store, item).startswith("gs://private-evidence/")
    assert session.request.call_count == 2
    upload, download = session.request.call_args_list
    assert upload.args[0] == "POST"
    assert "ifGenerationMatch=0" in upload.args[1]
    assert content in upload.kwargs["data"]
    assert download.args[0] == "GET"
    assert "generation=7" in download.args[1]
    assert download.kwargs["headers"]["range"] == f"bytes=0-{MAX_ARTIFACT_BYTES}"
