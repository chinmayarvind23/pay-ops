# API contracts

The [protected FastAPI factory](../src/payops/protected_api.py) defines authenticated incident creation, scoped reads and investigation. The [remediation router](../src/payops/remediation/api.py) exposes proposal, audit, approval and execution operations through the deterministic broker.

GraphQL shares the REST application's authentication and incident scope. Its read schema exposes incidents, reports and paginated evidence. Slack reference notifications are enabled through explicit host configuration. See [GraphQL and Slack setup](graphql-and-slack.md).

Run the configured application and open `/docs` for its OpenAPI contract. The default development server uses fixture investigations. Backend identity and current resource checks apply to operational requests; client payloads cannot grant approval.
