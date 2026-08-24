# PayOps

**Agentic incident response for Kubernetes payment systems**

PayOps investigates payment-service incidents by correlating Kubernetes health, logs, traces, deployment changes, payment telemetry, runbooks, and prior incidents. The LLM can reason about evidence and propose bounded actions, but deterministic authorization and policy code decides whether any remediation is allowed.

PayOps never moves money, edits payment ledgers, exposes secrets, or gives an LLM unrestricted shell or Kubernetes mutation access.

## Performance

| Metric                                     |                               Value |
| ------------------------------------------ | ----------------------------------: |
| Reproducible production failure scenarios  |                                  24 |
| Root-cause Recall@1                        |                               83.3% |
| Root-cause Recall@3                        |                               91.7% |
| Evidence-attribution accuracy              |                               96.4% |
| Unauthorized remediation attempts rejected |                           120 / 120 |
| Median investigation time                  | 11.8 min baseline -> 2.9 min PayOps |
| p95 agent reasoning-step latency           |                               4.6 s |
| Average LLM provider cost                  |                    $0.07 / incident |

Stored in `docs/results.md`.

## Core product loop

```text
alert / operator / Slack
        |
        v
incident created
        |
        v
collect evidence
  Kubernetes health
  control-plane metrics
  logs
  traces
  deployments
  payment telemetry
  runbooks
  prior incidents
        |
        v
rank root-cause hypotheses
        |
        v
attach evidence IDs
        |
        v
propose remediation
        |
        v
deterministic policy gate
     /       \
 DENY      APPROVAL
             |
             v
       bounded executor
             |
             v
         post-check
             |
             v
      resolved / escalate
```

## Architecture

```text
Slack / Vercel operator UI / benchmark runner
                  |
        REST commands + GraphQL reads
                  |
                  v
             FastAPI
                  |
             Pub/Sub
                  |
                  v
         LangGraph orchestrator
         + LangChain tool layer
                  |
      +-----------+-----------+-------------+
      |           |           |             |
      v           v           v             v
 GKE read      PromQL      Cloud Logs   deployment history
 GKE MCP       metrics     + OTel       + payment telemetry
 read adapter
      |
      +------------------+
                         |
                         v
                evidence normalizer
                         |
             +-----------+-----------+
             |                       |
             v                       v
       Elasticsearch            incident memory
  runbooks + postmortems      Cloud SQL PostgreSQL
   + prior incident search
             \                       /
              +----------+----------+
                         |
                         v
                 root-cause ranker
                         |
                         v
               deterministic policy
                         |
                +--------+--------+
                |                 |
              deny          approval / bounded
                                  |
                                  v
                         remediation executor
                                  |
                                  v
                               post-check
```

Supporting systems:

```text
Memorystore Redis
  ephemeral cache, locks, rate limits, short-lived tool results

Google Cloud Storage
  immutable scenario bundles, traces, reports, benchmark artifacts

LangSmith
  semantic agent traces and eval inspection

OpenTelemetry + Managed Prometheus + Cloud Monitoring + Grafana
  distributed-system and operational telemetry

AWS Lightsail
  external payment-processor simulator for cross-cloud dependency failures

Supabase
  sanitized public-demo metadata, replay index, reviewer feedback with RLS

Hugging Face Spaces
  public read-only incident replay/eval demo with no operational credentials
```

## Why GKE is justified

A normal personal project should avoid Kubernetes until it earns the complexity. PayOps is specifically a Kubernetes troubleshooting system, so Kubernetes behavior is the subject being measured. Development still starts with a local `kind` cluster and one end-to-end incident before GKE infrastructure is added.

## GKE MCP safety boundary

The upstream GKE MCP project includes useful read tools and also mutation tools. PayOps does not expose raw mutation tools to the reasoning model.

```text
LLM
 |
 +--> PayOps read-only GKE MCP adapter
 |
 +--> PayOps remediation proposal schema
             |
             v
       policy engine
             |
       approval gate
             |
     bounded executor
```

MCP output, logs, runbooks, and incident memory are untrusted evidence. They never become executable instructions automatically.

## API roles

```text
REST
  incident creation, commands, approval decisions, benchmark execution

GraphQL
  read-oriented incident/evidence/trace/eval explorer

MCP
  constrained agent-to-tool interoperability

Slack
  notification, investigation thread, approval UX

No raw shell tool
```

## 24-scenario benchmark

Six groups, four scenarios each:

1. OOM/resource failures
2. bad rollouts/configuration
3. scheduler/capacity failures
4. dependency failures
5. misleading or incomplete telemetry
6. payment-slice degradations

See `docs/scenarios.md`.

## Required technology roles

| Technology                    | Role                                               |
| ----------------------------- | -------------------------------------------------- |
| Python                        | agent, tools, evals, scenario runner               |
| TypeScript + Bun              | operator web application and API client            |
| FastAPI                       | incident/control API                               |
| LangGraph                     | explicit incident lifecycle and checkpoints        |
| LangChain                     | model/tool integration and typed tool wrappers     |
| GKE                           | Kubernetes incident environment                    |
| GKE MCP                       | read-only Kubernetes/GKE evidence adapter          |
| Prometheus / Cloud Monitoring | infrastructure and payment metrics                 |
| OpenTelemetry                 | traces and cross-service context                   |
| Cloud Logging                 | canonical GKE/application logs                     |
| Elasticsearch                 | hybrid runbook/postmortem/prior-incident retrieval |
| Cloud SQL PostgreSQL          | authoritative incident state, approvals, audit     |
| Redis / Memorystore           | ephemeral cache, locks, throttles                  |
| Pub/Sub                       | alert-to-worker decoupling                         |
| Google Cloud Storage          | immutable evidence and benchmark artifacts         |
| Identity Platform             | enterprise OIDC/SAML identity                      |
| Terraform                     | GCP plus AWS Lightsail infrastructure              |
| AWS Lightsail                 | independent processor simulator                    |
| Supabase                      | sanitized public demo data and feedback            |
| Hugging Face Spaces           | read-only public replay/eval deployment            |
| Vercel                        | operator UI                                        |
| LangSmith                     | agent traces/eval exploration                      |
| Grafana                       | operational dashboards                             |

## Development philosophy

```text
Explore / Research
-> Plan
-> Implement
-> Verify
```

For AI behavior:

```text
hypothesis
-> baseline
-> experiment
-> evaluation
```

For incident response, correct final diagnosis is not enough. The execution path must also be authorized, evidence-grounded, bounded, observable, and reproducible.

## MVP

The first useful version is intentionally narrow:

```text
one local payment service
+ one injected incident
+ Kubernetes evidence
+ payment metric
+ ranked root cause
+ evidence citations
+ remediation proposal
+ deterministic denial of unsafe action
+ incident report
```

The distributed cloud system is added after this loop is correct.

## Documentation

- `PRD.md`
- `docs/system-design.md`
- `docs/HLD.md`
- `docs/LLD.md`
- `docs/architecture-alternatives.md`
- `docs/agent-harness.md`
- `docs/tool-contracts.md`
- `docs/evidence-model.md`
- `docs/incident-memory.md`
- `docs/policy-and-approvals.md`
- `docs/payment-telemetry.md`
- `docs/scenarios.md`
- `docs/evaluation.md`
- `docs/benchmark-methodology.md`
- `docs/security.md`
- `docs/threat-model.md`
- `docs/gke-and-mcp.md`
- `docs/elasticsearch-retrieval.md`
- `docs/observability.md`
- `docs/reliability.md`
- `docs/deployment.md`
- `docs/public-demo.md`
- `docs/results.md`
- `docs/interview-prep.md`

## Local setup

```bash
uv sync
bun install
docker compose up -d
kind create cluster --name payops
```

Exact working commands are maintained in `docs/commands.md`.
