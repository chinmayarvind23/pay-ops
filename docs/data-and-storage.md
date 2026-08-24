# Data and Storage

Cloud SQL PostgreSQL stores authoritative incidents, transitions, evidence metadata, hypotheses, policy decisions, approvals, action executions, postchecks, checkpoints, and benchmark/eval metadata.

GCS stores large immutable evidence exports and benchmark artifacts.

Redis/Memorystore stores ephemeral rate limits, locks, progress, and safe read cache.

Elasticsearch stores rebuildable search indexes.

Supabase stores only sanitized public incident replay data, benchmark summaries, annotations, and feedback with RLS.

Public sanitization produces its own manifest/hash.
