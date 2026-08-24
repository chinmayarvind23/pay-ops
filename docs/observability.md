# Observability

Shared IDs:

```text
incident_id
scenario_id
trace_id
evidence_id
hypothesis_id
action_id
deployment_sha
```

OTel trace:

```text
incident
  +-- triage
  +-- evidence
      +-- kubernetes
      +-- metrics
      +-- logs
      +-- traces
      +-- deployment
      +-- payment
  +-- retrieval
  +-- root-cause model
  +-- policy
  +-- approval
  +-- action
  +-- postcheck
```

LangSmith records semantic/model traces, tool choices, model/prompt version, structured validation, tokens/cost, eval case, and feedback.

Prometheus/Cloud Monitoring expose incident duration, agent-step latency, tool errors, policy decisions, unauthorized attempts, cost, and evidence counts.

Avoid request/evidence IDs as Prometheus labels. High-cardinality detail belongs in traces/logs.
