# Architecture Alternatives

## Raw shell agent

Rejected. A generic shell makes deterministic safety and reproducible evaluation much harder.

## Raw GKE MCP agent

Rejected. PayOps uses the integration for approved reads but does not register mutation tools with the reasoning model.

## Deterministic runbooks only

Kept as baseline. Strong for known cases, weaker for noisy multi-signal hypothesis ranking.

## Agent swarm

Not selected initially. One explicit state machine is easier to audit, evaluate, and budget. Add a specialist only if an eval shows measurable value.

## Full control-plane microservices

Not selected initially. Use a modular backend plus async workers. The payment sandbox itself is multi-service because distributed failure correlation is the product domain.

## Redis as authoritative state

Rejected. Cache loss must not destroy incident/audit truth.

## Elasticsearch as telemetry truth

Rejected. It is a derived retrieval layer. Current metrics/logs/traces remain authoritative in their source systems.

## Supabase as operational DB

Rejected. It has a separate sanitized public-demo role.

## Lightsail as main backend

Rejected. It is valuable specifically as an external processor failure domain.

## Hugging Face with live cluster tools

Rejected. The public demo is replay-only.

## Kafka and Spark

Not selected. Managed Pub/Sub handles incident work delivery; the benchmark corpus does not justify distributed data processing.
