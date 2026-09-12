# Optional GCS evidence archive

For an existing private bucket, grant the operator identity object create and get
permissions on the archive prefix. This adapter does not list, replace or delete
objects. Configure Application Default Credentials outside the repository and
disable the SDK's optional tracing metadata fetch before creating its client:

```powershell
$env:DISABLE_GCS_PYTHON_CLIENT_OTEL_BUCKET_METADATA = "true"
```

Use a trusted, retained `EvidenceItem` from an incident report:

```python
from pathlib import Path
from google.cloud.storage import Client
from payops.contracts import EvidenceItem
from payops.evidence.artifacts import ArtifactStore
from payops.evidence.gcs import GcsArtifactArchive

item = EvidenceItem.model_validate_json(Path("evidence-item.json").read_text())
with Client(project="your-project-id") as client:
    archive = GcsArtifactArchive(client.bucket("your-private-evidence-bucket"))
    location = archive.upload(ArtifactStore(Path("artifacts")), item)
    archive.restore(ArtifactStore(Path("restored-artifacts")), item)
```

An object key contains a fixed prefix and the artifact's SHA-256 digest. Uploads
use `if_generation_match=0`, so an existing object cannot be overwritten. A 412
response triggers a verified read of the existing object. Other upload errors
propagate without automatic retries or a success receipt; a later explicit retry
can safely resolve an upload whose acknowledgement was lost.

Readback checks the byte limit, SHA-256 and exact evidence metadata, including the
incident identity. Downloads request at most 1 MiB plus one byte, without content
decompression. The SDK timeout is five seconds per request, not an end-to-end
deadline. SHA-256 verification replaces the SDK checksum recovery path so the
adapter never asks the SDK to delete an object after a checksum failure. Restore
publishes only complete verified bytes and refuses to replace corrupt local files.
Bucket retention and IAM remain operator-managed; content addressing is not a
claim that administrators cannot modify a bucket.

The request options follow Google's [Blob API documentation](https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob).
