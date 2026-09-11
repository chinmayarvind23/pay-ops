# Failure Scenarios

The planned suite contains six groups with four scenarios each. Four local variants are
implemented and have live activation and restoration evidence: startup failure
(ROLLOUT-01), invalid sandbox configuration (ROLLOUT-02), readiness path regression
(ROLLOUT-03), and a local processor outage (DEP-01). The configuration variant does
not claim a missing production environment variable; the processor variant does
not claim an AWS outage. The remaining cases below are planned.

The operator harness only targets the dedicated `kind-payops-dev` cluster. It saves
the original deployment, records activation, restores the exact specification and
requires a successful synthetic payment before releasing its persistent run latch.
Unverified cleanup blocks later runs. This harness is not a model-facing tool.

From the repository root, after the local sandbox is running:

```powershell
docker build -f src/payops/scenarios/Dockerfile.startup_failure -t payops-sandbox:startup-failure .
../resources/pay_ops/tools/kind-v0.33.0.exe load docker-image payops-sandbox:startup-failure --name payops-dev
uv run python -m payops.scenarios.run --scenario ROLLOUT-01 --kubeconfig ../resources/pay_ops/runtime/kubeconfig --output ../resources/pay_ops/evidence/scenarios
```

Receipts distinguish injection/cleanup time from investigation time, retain failures,
and hash every captured artifact. See `src/payops/scenarios/` for the closed recipes.

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
