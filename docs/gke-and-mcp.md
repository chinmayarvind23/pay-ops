# GKE and MCP

PayOps uses Kubernetes objects/events, kube-state metrics, kubelet/cAdvisor metrics where needed, API-server metrics, scheduler metrics, workload metrics, and Cloud Logging.

Scheduler scenarios can use signals such as:

```text
scheduler_pending_pods
scheduler_schedule_attempts_total
scheduler_pod_scheduling_sli_duration_seconds
kube_pod_resource_request
kube_pod_resource_limit
```

Exact metric availability is recorded with the GKE/Kubernetes version.

GKE MCP is wrapped behind an approved read adapter. Remediation follows a different path:

```text
model proposal
-> action schema
-> policy
-> approval when required
-> PayOps executor
-> Kubernetes API
-> postcheck
```

Local `kind` comes first; GKE is added for real control-plane telemetry, Workload Identity, cloud scheduling/node behavior, and GKE MCP integration.
