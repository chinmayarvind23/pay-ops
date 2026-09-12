# PayOps product requirements

## User and problem

PayOps serves responders and platform engineers investigating Kubernetes payment-service failures. A request failure often spans several services, while its logs, resource state and deployment history live in different systems.

## Investigation workflow

An incident identifies the affected service and namespace. A current responder identity authorizes the investigation. Registered readers collect observations, the evidence store binds them to their sources, and the reasoning loop ranks possible causes with citations. The report preserves uncertainty when the available observations do not support a diagnosis.

## Operational controls

The backend checks identity and incident scope before work begins and again before publication. Work budgets and checkpoints survive process restarts. Completed observations are reused from verified records.

Remediation runs through a separate broker. A proposal identifies an allowed action, its evidence and the expected resource state. Approval, current authorization and resource preconditions are checked before execution. Follow-up health checks establish the action's observed outcome.

## Product boundary

The local payment services use synthetic requests. The system does not move funds, edit real account balances, read Kubernetes secrets, execute arbitrary shell commands or apply arbitrary manifests. An alert or retrieved document cannot grant permissions.

See [architecture](docs/architecture.md), [API contracts](docs/api-contracts.md) and [operator setup](docs/commands.md).
