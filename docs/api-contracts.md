# API Contracts

The executable API contract is defined by the [protected FastAPI factory](../src/payops/protected_api.py) and [remediation router](../src/payops/remediation/api.py). Incident creation, scoped reads and investigation are implemented. Action proposal, audit, approval and execution use authenticated routes and the deterministic broker. The default development API uses fixture investigation data.

GraphQL exploration and opt-in Slack reference notifications are implemented; see [contracts, setup and limits](graphql-and-slack.md). A benchmark HTTP endpoint and cancellation routes in the original architecture are not implemented. Use the existing CLI evaluation runners for benchmarks. Interactive OpenAPI documentation reflects the configured FastAPI application rather than the original route sketches.

Backend actor scope and current action/resource checks remain authoritative; neither a browser payload nor a retrieved document grants approval. See [stack coverage](stack-status.md) and [security](security.md).
