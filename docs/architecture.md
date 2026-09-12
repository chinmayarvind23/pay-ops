# Architecture

PayOps separates evidence gathering, investigation and remediation. A model can propose a diagnosis or request a registered read. The backend decides whether that operation is authorized and whether its output can enter the incident record.

## Incident and evidence

The SQL incident store owns incident records and published reports. Each evidence item records its incident, source, resource, observation time and artifact digest. The artifact store verifies content and metadata before a citation is accepted. Elasticsearch retrieves scoped runbook and incident context; Redis stores derived cache entries.

## Investigation

The operational host binds the current operator identity, Kubernetes context, data readers and model adapter. LangGraph stores investigation checkpoints. The reasoning loop reserves work before dispatch, validates structured decisions and retains completed operation receipts.

The local host coordinates work through SQLite and filesystem locks. Run a single operational host against that checkpoint directory. Pub/Sub deliveries use the same durable path and publish a report before acknowledging the message.

## Remediation

The remediation broker accepts a closed action schema. It checks current identity, incident scope and evidence, binds approval to the proposed action and revalidates the current resource. A SQL claim precedes the external effect. Kubernetes actions include UID, resource-version and specification preconditions, followed by health checks.

## Interfaces and adapters

REST and GraphQL share backend authorization. Slack sends incident references through an operator-configured webhook. Google Identity Platform tokens map to current application grants. GKE MCP provides constrained reads; GCS archives verified evidence. Cloud Logging and Monitoring readers produce the same observation contract as local readers.

OpenTelemetry traces service and model operations. LangSmith export includes selected receipt metadata. Credentials belong to the configured operator environment and stay outside the source repository.

## Source entry points

- [Operational host](../src/payops/operator_host.py)
- [Investigation graph](../src/payops/orchestrator/graph.py)
- [Protected API](../src/payops/protected_api.py)
- [Evidence store](../src/payops/evidence/artifacts.py)
- [Remediation broker](../src/payops/remediation/broker.py)
- [Pub/Sub worker](../src/payops/pubsub_worker.py)
