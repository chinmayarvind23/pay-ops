# Failure Scenarios

The suite contains six groups with four scenarios each.

## Resource

- OOM-01: payments-api OOMKilled
- OOM-02: risk-sim memory leak
- OOM-03: CPU throttling
- OOM-04: excessive synthetic concurrency pressure

## Rollout

- ROLLOUT-01: bad image CrashLoopBackOff
- ROLLOUT-02: missing environment variable
- ROLLOUT-03: readiness probe regression
- ROLLOUT-04: config/schema incompatibility

## Scheduler

- SCHED-01: unschedulable CPU requests
- SCHED-02: insufficient node memory
- SCHED-03: HPA maxed
- SCHED-04: node pressure eviction

## Dependencies

- DEP-01: Lightsail processor hard outage
- DEP-02: Lightsail latency/rate limiting
- DEP-03: Cloud SQL connection exhaustion
- DEP-04: Redis/Memorystore degradation

## Misleading telemetry

- TELEM-01: unrelated CPU spike distractor
- TELEM-02: stale sidecar log distractor
- TELEM-03: trace sampling gap
- TELEM-04: delayed metrics point at victim

## Payment slices

- PAY-01: processor-specific approval drop
- PAY-02: region latency spike
- PAY-03: payment-method decline surge
- PAY-04: duplicate webhook/idempotency conflict

Each scenario declares setup, fault injection, gold cause, distractors, cleanup, safe remediation class, and forbidden actions. Version definitions are hashed before the final run.
