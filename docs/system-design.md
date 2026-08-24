# System Design

## Design question

Design a bounded incident-response system that can diagnose failures in a Kubernetes payment environment by correlating heterogeneous telemetry, safely propose remediation, preserve evidence, and demonstrate measurable investigation improvement.

## Constraints driving architecture

```text
high root-cause recall
high evidence correctness
zero unauthorized action execution
auditable decisions
bounded agent cost
bounded model latency
reproducible failure injection
cross-signal correlation
checkpoint/recovery
```

## Capacity intuition

Let incident arrival rate be `lambda`, average investigation duration be `T`, and concurrent workers be `C`.

`rho ~= lambda * T / C`

As `rho` approaches 1, queueing becomes sensitive to bursts. Pub/Sub therefore decouples incident ingestion from idempotent workers instead of hiding long investigations inside HTTP requests.

## Alternatives

### One-shot LLM with shell

Rejected because arbitrary command text collapses validation, authorization, and execution.

### Deterministic runbook engine

Kept as a safety/performance baseline. It is predictable but weak when telemetry conflicts or symptoms cross Kubernetes and payment boundaries.

### Generic agent with raw GKE MCP

Rejected as the direct model surface because read and mutation authority should not be equivalent.

## Selected architecture

```text
Slack / operator UI / benchmark
              |
         FastAPI REST
              |
           Pub/Sub
              |
       LangGraph worker
       + LangChain tools
              |
  +-----------+------------+-----------+
  |           |            |           |
GKE read   PromQL       logs/traces  deploy/payment
MCP        metrics      OTel/Cloud   telemetry
  |           |            |           |
  +-----------+-----+------+-----------+
                    |
             EvidenceNormalizer
                    |
        +-----------+-----------+
        |                       |
 Elasticsearch             Cloud SQL
 runbooks/memory       incidents/checkpoints
        |                       |
        +-----------+-----------+
                    |
              ContextAssembler
                    |
             RootCauseRanker
                    |
          RemediationProposal
                    |
          DeterministicPolicy
          /       |        \
       DENY    APPROVAL    AUTO
                  |          |
                  +----+-----+
                       |
                 BoundedExecutor
                       |
                    PostCheck
```

## Evidence-first reasoning

The model does not receive giant raw telemetry dumps.

```text
bounded query
-> raw artifact
-> normalized evidence
-> evidence ID + provenance
-> compact context
```

Logs, runbooks, MCP output, and prior incident text are untrusted data.

## Risk tiers

```text
R0 read approved evidence
R1 bounded diagnostics
R2 reversible namespace-scoped sandbox action
R3 rollback/scale/config change requiring human approval
R4 destructive/shared-infrastructure action denied
R5 payment movement, ledger mutation, secret access unavailable
```

## Storage

Cloud SQL is authoritative for incidents, transitions, approvals, action audit, and checkpoint metadata.

GCS stores large immutable evidence and benchmark artifacts.

Redis/Memorystore holds ephemeral cache, rate limits, progress, and locks.

Elasticsearch is a rebuildable search layer for runbooks, postmortems, prior incidents, and selected normalized evidence.

Supabase contains only sanitized public-demo data.

## Multi-cloud failure domain

AWS Lightsail hosts an external synthetic processor so dependency failures can be independent of GKE/GCP.

## Fail closed

```text
policy uncertain -> no mutation
auth unavailable -> no mutation
Elasticsearch unavailable -> continue without memory retrieval
Redis unavailable -> bypass cache
LLM unavailable -> deterministic baseline + escalation
Cloud SQL unavailable -> do not acknowledge undurable state-changing progress
postcheck inconclusive -> escalate
```
