# Data and Storage

The implemented local system uses SQL stores for incidents, checkpoints, budgets and action state, immutable local evidence files with SHA-256 verification, Redis for derived caching, and Elasticsearch for scoped runbook/incident retrieval. PostgreSQL, Redis and Elasticsearch transports have local TLS integration evidence. SQLite supports local workflow and budget execution.

Cloud SQL and Memorystore are proposed managed counterparts. Optional GCP Terraform defines a bucket and Pub/Sub, but a GCS artifact transport and live queue worker are not implemented. Supabase is a planned sanitized public metadata/feedback store; the deployed demo reads frozen static JSON and does not use Supabase.

Public sanitization produces a separate manifest and hashes. Operational evidence and credentials never enter the public replay. See [source-by-source stack coverage](stack-status.md).
