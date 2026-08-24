# Deployment

Local:

```text
kind + Docker Compose
Postgres
Redis
Elasticsearch
Prometheus/Grafana
synthetic payment stack
```

GCP benchmark environment:

```text
GKE
Identity Platform
Pub/Sub
Cloud SQL
Memorystore
GCS
Cloud Monitoring / Managed Prometheus
Cloud Logging
Terraform
```

AWS Lightsail hosts `processor-sim` outside GCP.

Hugging Face Docker Space + Supabase provides public replay. Vercel hosts the operator UI.

Use separate GKE identities for evidence reader, remediation executor, scenario injector, and payment services.

Deployment order:

```text
local loop
-> local scenarios/evals
-> GKE read-only
-> policy/approval
-> bounded remediation
-> Lightsail dependency cases
-> full benchmark
-> sanitized public export
-> HF demo
```
