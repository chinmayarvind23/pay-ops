# Tool Contracts

Core tools are narrow:

```text
get_k8s_resource
list_k8s_events
get_container_logs
query_prometheus
query_cloud_logs
lookup_trace
get_deployment_history
get_payment_slice
search_runbooks
load_prior_incidents
propose_remediation
request_approval
execute_approved_action
run_postcheck
```

Every tool declares input/output schema, capability, environment, timeout, retry policy, idempotency behavior, failure codes, and telemetry.

Capabilities include:

```text
ops.read.kubernetes
ops.read.metrics
ops.read.logs
ops.read.traces
ops.read.deployments
ops.read.payments
ops.read.runbooks
ops.propose.remediation
ops.execute.restart
ops.execute.rollback
ops.execute.scale
```

The model cannot grant itself a capability.

Structured errors contain an error code and `retryable` flag.
