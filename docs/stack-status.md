# Stack coverage

| Requested technology | Implemented use | Verification scope |
| --- | --- | --- |
| Python | Typed API, investigation, evidence, policy and scenario code | Strict type checks and automated tests |
| LangChain / LangGraph | Real message types, bounded reasoning, durable investigation state | Provider adapter smoke and graph/recovery integration tests |
| GKE MCP | Source-pinned read adapter, schema verification, bounded stdio transport | Contract/transport tests; no deployed GKE benchmark |
| Kubernetes | Five-service kind environment, 24 fault scenarios, conditional remediation | Actual local processes, faults and restoration |
| Prometheus | Service, dependency and payment-slice observations | Retained local snapshots and controlled experiments |
| Elasticsearch | Scoped evidence retrieval with source verification | Local TLS/scoped-identity integration |
| Redis | Derived caching and real dependency-failure scenarios | Local TLS and outage/recovery controls |
| LangSmith / OTel | Explicit receipt export; model and service tracing | LangSmith SDK contract tests; real local OTel captures; no hosted LangSmith ingestion |
| GCP | Optional GKE, VPC, bucket and Pub/Sub Terraform foundation | Provider-schema validation and mock-provider plans; not provisioned |
| AWS | Optional Lightsail replay setup instructions | Documentation only, as requested; not provisioned |
| Hugging Face | Public sanitized Static Space | Hosted release files verified |

Cloud SQL, Memorystore, a live Pub/Sub worker and GCS artifact transport remain
architecture extensions. Listing an installed SDK does not establish an integration.
The zero-spend release runs locally and publishes credential-free replay assets.

See [GCP setup](../infra/terraform/gcp/README.md), [AWS setup](../infra/terraform/aws-lightsail/README.md),
[telemetry](telemetry.md) and [results](results.md).
