# Optional Cloud Logging and Monitoring reads

`payops.tools.cloud_observability` supplies read-only Google SDK adapters for GKE
container logs and native Kubernetes restart counters. They return the same
`Observation` contract used by local collection. Normalize those observations
through the existing artifact store before including them in an investigation.
The local operator's default readers remain Kubernetes and Prometheus; cloud reads
require explicit host configuration and current authorization at dispatch.

```python
from datetime import timedelta
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.monitoring_v3 import MetricServiceClient
from payops.contracts import utc_now
from payops.evidence.normalize import normalize
from payops.tools.cloud_observability import CloudScope, read_logs, read_restarts

scope = CloudScope(project="your-project", cluster="your-cluster", location="us-central1")
end = utc_now()
start = end - timedelta(minutes=5)
with LoggingServiceV2Client() as logs, MetricServiceClient() as metrics:
    observations = read_logs(logs, scope, "payments-api", start, end)
    observations += read_restarts(metrics, scope, "payments-api", start, end)
# store and incident are the existing authorized investigation's artifact store and incident.
evidence = tuple(normalize(row, incident.incident_id, start, end, store) for row in observations)
```

Use a configured Google identity with read-only Logging and Monitoring permissions.
The cluster must already export container logs and the native
`kubernetes.io/container/restart_count` metric. Container names must match PayOps's
service allowlist. No arbitrary filter, endpoint or namespace comes from an alert
or model. The fixed namespace is `payops-sandbox`.

Each operation requests only its first page, with a five-second RPC timeout and no
automatic retry. Windows must be aware, ordered and at most fifteen minutes long.
Responses are limited to 128 KiB of serialized JSON, twenty entries/series and
sixty-four points per series. Returned project, cluster, location, namespace,
container and timestamps are checked again. A next-page token marks observations
as partial; empty results do not establish health or completeness. The common
normalizer applies redaction and binds incident metadata to content hashes.

The SDK protobuf contracts and evidence normalization are tested locally. No GKE
cloud deployment or hosted observability read was performed. These adapters add no
new diagnosis benchmark result.

References: Google's [Logging client contracts](https://docs.cloud.google.com/python/docs/reference/logging/latest/upgrading)
and [Monitoring time-series API](https://docs.cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/list).
