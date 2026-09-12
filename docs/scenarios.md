# Failure Scenarios

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
- OOM-04: concurrent request allocations cause an owned OOM at fixed 256Mi

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

## Versioned risk request

`ProtocolHarness` now implements four stages: original v1, risk v2 with a v1 caller,
matching v2 caller, and restored v1. It journals both complete Deployment specs before
the first write, uses UID/version/spec compare-and-swap, and attempts each restoration
even when the other write or evidence persistence fails. Unknown replacement specs
are not overwritten; unverified cleanup retains the shared scenario latch.

Each stage sends one fresh A/us/credit payment. Positive controls require its complete
nine-span path across five owned processes. The mismatch requires the actual payments
502, the risk 422 access record within the request window, and two error spans without
a successful downstream path. The risk access record is temporal corroboration; it
does not contain a request ID. Trace sources are bounded and verified against current
runtime identities. The generic scenario runner rejects this case. Lifecycle, semantic
source and streamed HTTP tests pass. The retained live experiment reproduced all
four stages and verified exact restoration; model diagnosis remains unevaluated.

## CPU workload prerequisite

The sandbox's deployment configuration now accepts `cpu_rounds` (default 0, maximum
200,000). A fresh non-declined request performs that many fixed SHA-256 operations
over a bounded buffer before its normal dependency path. Completed idempotent replay
skips the work. HTTP input cannot set this value.

One worker per service keeps the ASGI event loop available and rejects excess work
instead of queueing it. Cancellation does not release admission until the worker
exits. A cooperative five-second wall deadline, checked every 128 rounds, bounds
the work; it does not preempt native execution or eliminate OS scheduling delays.
Spans record configured rounds and observed thread CPU time, which may round to
zero for short work on coarse clocks. The default profile creates no worker pool.

Calibration at `d262df5` used three sequential 50,000-round tasks per isolated
container. Mean workload time was 0.159 seconds at one CPU, 0.255 at the sandbox's
normal half-CPU limit and 1.668 at one-tenth CPU. Mean thread CPU time stayed between
0.151 and 0.170 seconds. Mean kernel throttled-time deltas were 0.000040, 0.102 and
1.502 seconds respectively. Even the normal half-CPU control throttled, so a positive
counter alone cannot establish the incident. The later HTTP scenario must compare
matched workload and quota controls, not require an unrealistically zero baseline.
These nine tasks are calibration, not a qualified Kubernetes scenario or agent
latency measurement. Source, image, raw counters and successful container exits are
retained outside the repository under `audit/evidence/cpu-calibration-*`.
