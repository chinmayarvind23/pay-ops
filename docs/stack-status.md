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

Cloud SQL and Memorystore remain
architecture extensions. Listing an installed SDK does not establish an integration.
The zero-spend release runs locally and publishes credential-free replay assets.

See [GCP setup](../infra/terraform/gcp/README.md), [AWS setup](../infra/terraform/aws-lightsail/README.md),
[telemetry](telemetry.md) and [results](results.md).


## Source map for the full resource-plan stack

The technology list in `resources/pay_ops/PROJECT_PLAN.md` is broader than the resume heading. This map separates executable local integrations, tested optional adapters, and unimplemented cloud extensions. The free release does not require paid cloud infrastructure.

| Technology | Correct responsibility and current implementation |
| --- | --- |
| Python / FastAPI | [Authenticated API](../src/payops/protected_api.py), [action routes](../src/payops/remediation/api.py), typed investigation and policy modules |
| LangChain | Real message types and bounded model decisions in [operator host](../src/payops/operator_host.py) and [reasoning loop](../src/payops/orchestrator/loop.py) |
| LangGraph | Durable orchestration and checkpoint recovery in [graph](../src/payops/orchestrator/graph.py); policy and approvals remain outside model decisions |
| Kubernetes / Docker | Actual synthetic services and scenario runtimes; [isolated eviction gateway](../src/payops/scenarios/eviction_gateway.py), conditional remediation and containerized replay |
| GKE / GKE MCP | [Read-only MCP adapter](../src/payops/tools/gke_mcp.py) and optional [GKE Terraform](../infra/terraform/gcp/main.tf); transport contracts tested, no hosted cluster run |
| Prometheus | Bounded metric acquisition in [Prometheus reader](../src/payops/tools/prometheus.py); payment-slice arithmetic and scenario controls consume observations |
| Elasticsearch | Fixed-scope runbook/incident retrieval in [data clients](../src/payops/memory/data_clients.py); returned evidence retains source lineage |
| PostgreSQL / Cloud SQL | Local PostgreSQL transport and SQL incident/action stores; Cloud SQL is the proposed managed equivalent, not a provisioned integration |
| Redis / Memorystore | Verified TLS derived-cache transport in [data clients](../src/payops/memory/data_clients.py), plus real Redis outage scenarios; Memorystore remains the managed deployment option |
| OpenTelemetry | Cross-service tracing in [sandbox telemetry](../src/payops/sandbox/telemetry.py) and retained trace evidence |
| LangSmith | Explicit sanitized model-receipt export in [telemetry export](../src/payops/telemetry/export.py); SDK contract tested, hosted ingestion not verified |
| TypeScript / Bun | Built and tested static replay in [apps/web](../apps/web/package.json); no privileged browser access |
| Google Identity Platform | [Firebase Admin token adapter](../src/payops/auth/firebase.py) and backend grant checks; provider responses tested, enterprise SAML tenant configuration not deployed |
| GCP / Terraform | Disabled-by-default GKE/VPC/bucket/Pub/Sub [foundation](../infra/terraform/gcp/main.tf), schema validation and mock-provider plans |
| Cloud Logging / Cloud Monitoring | [Fixed-scope SDK reads](cloud-observability.md) ingest container logs and native Kubernetes restart counters into the standard evidence contract; bounded pages, returned scope and timestamps tested locally, no hosted reads |
| Pub/Sub / GCS | Optional Terraform resources exist; [GCS archive transport](gcs-archive.md) verifies create-once uploads and local restore through the real SDK. The [Pub/Sub worker](pubsub-worker.md) dispatches stored incident references through durable investigation and commits reports before acknowledgement; hosted consumption is not claimed |
| AWS Lightsail | [Self-service replay deployment instructions](../infra/terraform/aws-lightsail/README.md), as requested; no AWS resources created |
| Hugging Face Spaces | Public free Static Space serving the sanitized replay and compiled frontend |
| Vercel | Excluded by user direction; Hugging Face is the selected host |
| Supabase | Excluded by user direction; current demo uses frozen static JSON |
| GraphQL | [Authenticated nested incident/report/evidence reads](../src/payops/graphql_api.py), bounded AST and pagination; API integration tested |
| Slack | [Opt-in incident-reference notifications](../src/payops/slack_notifications.py), scoped responder authorization, fixed webhook, no automatic retry; fixture delivery tested, no live workspace message sent |

See [GraphQL and Slack setup](graphql-and-slack.md). Managed cloud extensions remain explicitly scoped above; current resume bullets describe the measured local system and tested adapters.
