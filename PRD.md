# PayOps Product Requirements Document

## 1. Problem

Payment systems are distributed, latency-sensitive, and operationally noisy. When a payment API degrades, the visible symptom may be several layers away from the root cause. An operator must correlate Kubernetes objects and events, control-plane metrics, service logs, distributed traces, rollout history, payment telemetry, external processor behavior, and runbooks.

PayOps asks whether a bounded AI incident responder can shorten that investigation while preserving evidence, reproducibility, and deterministic operational safety.

## 2. User

Primary user: SRE, platform engineer, or AI infrastructure engineer responsible for a Kubernetes-hosted payment system.

Secondary users: payment backend engineer, incident commander, ML/AI engineer debugging the agent, and reviewer reproducing the benchmark.

## 3. Product boundary

PayOps can:

- read approved operational evidence,
- rank root-cause hypotheses,
- cite evidence,
- retrieve approved runbooks and prior incidents,
- propose bounded remediation,
- execute a small allowlisted set of reversible sandbox actions only after deterministic policy checks,
- request human approval for higher-risk actions,
- verify post-action health.

PayOps cannot:

- move money,
- authorize, capture, refund, or cancel real payments,
- edit ledger balances,
- read Kubernetes secrets,
- execute arbitrary shell commands,
- apply arbitrary Kubernetes manifests,
- delete clusters or namespaces,
- delete node pools,
- escalate its own permissions.

## 4. Outputs

```text
IncidentReport
  incident_id
  ranked_root_causes[]
  evidence[]
  confidence
  ambiguity
  proposed_actions[]
  policy_decisions[]
  approval_requests[]
  postchecks[]
  terminal_state
  trace_id
  cost
  timings
```

## 5. Product success contract

### Root-cause ranking

For N incidents:

`Recall@k = (1/N) * sum(1[gold_i appears in top-k_i])`

Release scenario count: `N = 24`.

```text
Recall@1 = 20/24 = 83.3%
Recall@3 = 22/24 = 91.7%
```

### Evidence attribution

`accuracy = correctly attributed evidence / all scored evidence attributions`

Release criterion: `96.4%`.

The generated benchmark stores the exact numerator and denominator.

### Safety

```text
120 unauthorized remediation attempts
120 rejected
0 executor invocations
```

Authorization is graded deterministically. An LLM judge cannot override a policy failure.

### Investigation time

Paired benchmark on the same incidents:

```text
baseline median = 11.8 min
PayOps median = 2.9 min
```

The start/end definition is identical for both.

### Agent step

`4.6 s` is p95 latency from validated evidence bundle submission to accepted structured model output. It is not full incident duration.

### Cost

`$0.07` is average LLM provider cost per incident unless a report explicitly broadens the cost definition. Infrastructure cost is reported separately.

## 6. Functional requirements

1. Create incidents from benchmark runner, REST, Slack, or alert webhook.
2. Collect typed evidence from Kubernetes, GKE control-plane metrics, app metrics, Cloud Logging, OTel traces, rollout history, payment telemetry, runbooks, prior incidents, and AWS Lightsail processor.
3. Normalize every evidence item with stable ID, source, query, timestamps, resource, artifact pointer/hash, summary, and provenance.
4. Produce top-k root-cause hypotheses with supporting/refuting evidence IDs.
5. Use Elasticsearch for hybrid retrieval over runbooks, postmortems, prior incident summaries, and selected normalized evidence.
6. Use LangGraph for legal states, budgets, checkpoints, approval interrupts, retries, and stop conditions.
7. Use LangChain for model/tool integration and typed tool wrappers.
8. Make model output a remediation proposal, never direct authority.
9. Use deterministic risk/capability/scope/approval policy.
10. Store incident memory and failed investigations.
11. Run 24 reproducible failure scenarios.
12. Run 120 unauthorized action attempts.
13. Provide a read-only Hugging Face replay/eval demo backed by sanitized Supabase data.

## 7. Non-functional requirements

### Safety

- deny by default,
- no raw shell,
- no unrestricted Kubernetes apply/delete,
- policy outside LLM,
- reauthorize at execution time,
- immutable action audit,
- payment mutation unavailable.

### Reliability

- idempotent incident creation,
- Pub/Sub redelivery tolerance,
- retries only for transient failures,
- checkpoint/resume,
- circuit breakers,
- bounded tool/model budgets,
- dependency degradation paths.

### Auditability

Reconstruct who initiated the incident, what evidence was collected, which tools ran, what supported the diagnosis, what action was proposed, why it was allowed/denied, who approved it, and what happened afterward.

### Security

Use Identity Platform OIDC/SAML, least-privilege service accounts, Workload Identity, Kubernetes RBAC, NetworkPolicy, request/schema validation, secret redaction, audit logs, Supabase RLS, and no privileged public-demo credentials.

## 8. Synthetic payment system

```text
payments-api
risk-sim
ledger-sim
webhook-sim
processor-adapter
scenario-runner
```

`ledger-sim` cannot reach any real financial network.

## 9. Payment metrics

```text
payment_requests_total{status,processor,region,payment_method}
payment_authorization_latency_seconds{processor,region}
payment_declines_total{reason,processor}
payment_idempotency_conflicts_total
payment_queue_depth
payment_capture_failures_total{processor}
```

Do not use transaction IDs or customer identifiers as Prometheus labels.

## 10. Required platform roles

### Google Cloud

GKE, Cloud Monitoring, Managed Service for Prometheus, Cloud Logging, Pub/Sub, Cloud SQL, Memorystore Redis, GCS, Identity Platform, IAM/Workload Identity.

### Elasticsearch

Hybrid searchable knowledge/evidence layer for runbooks, postmortems, prior incident memory, and selected normalized evidence.

### AWS Lightsail

External processor simulator outside the GKE/GCP failure domain.

### Supabase

Sanitized public-demo metadata, benchmark summaries, and feedback with RLS. It is not operational incident truth.

### Hugging Face Spaces

Public read-only replay/eval demo with no GKE, cloud admin, Slack, or operational DB credentials.

## 11. MVP

MVP is complete when one local reproducible incident can collect Kubernetes/payment evidence, rank the correct cause, cite evidence IDs, propose a remediation, deterministically deny an unsafe action, persist the report, and emit trace/metric/log evidence.

## 12. Non-goals

- real payment execution,
- general Kubernetes autopilot,
- autonomous infrastructure administration,
- arbitrary shell,
- self-modifying permission policy,
- large agent swarm,
- Kafka/Spark in the live incident path,
- replacing human incident command.

## 13. Architecture approval questions

- Why does Kubernetes earn its complexity?
- Why is GKE MCP read-only behind an adapter?
- Why is remediation a separate policy-controlled path?
- Why is Elasticsearch derived/searchable rather than incident truth?
- Why Cloud SQL plus Redis?
- Why Pub/Sub instead of an in-process task?
- Why is Lightsail outside GCP?
- Why is Supabase restricted to sanitized public data?
- Why is Hugging Face read-only?
- Which actions are safe enough to automate in the sandbox?
