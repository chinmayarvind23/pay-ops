# Failure Scenarios

The suite contains six groups with four scenarios each. Seventeen local variants now
have activation and restoration evidence; see [results](results.md) for scope and
provenance. The remaining seven are SCHED-03/04, DEP-03/04 and
TELEM-01/02/04. Local processor variants do not establish an AWS outage, and local
dependency behavior does not establish a deployed cloud integration.

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
- OOM-02: risk-sim memory leak (bounded retention with two OOM lifetimes qualified at `7d3e204`)
- OOM-03: CPU throttling
- OOM-04: concurrent request allocations cause an owned OOM at fixed 256Mi; serial controls and recovery qualified at `926bbd5`

## Rollout

- ROLLOUT-01: bad image CrashLoopBackOff
- ROLLOUT-02: missing environment variable
- ROLLOUT-03: readiness probe regression
- ROLLOUT-04: config/schema incompatibility

## Scheduler

- SCHED-01: unschedulable CPU requests
- SCHED-02: insufficient node memory (local 16Gi request qualified at `e69b814`)
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

The synthetic sandbox supports two deployment-selected risk request forms. The default
v1 is the flat Sample body. V2 is a strict `payops-risk-v2` envelope containing that
same Sample. Payments serializes its risk call using its configured `risk_protocol`;
risk accepts only its deployed protocol. The other three peers retain v1 bodies.
Mismatches produce an actual risk422 and propagated payments502 while liveness stays
healthy. Matching versions complete the full synthetic path. ROLLOUT-04 was qualified
locally at `5978c4f` with exact cleanup evidence; see [Results](results.md).

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

Other fault harnesses reject an active CPU workload as a healthy baseline. This
control enables the planned OOM-03 quota experiment, but does not itself qualify
CPU throttling. Qualification still requires a frozen workload, actual cgroup
throttled-period/time deltas, matched controls and exact cleanup.

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

The specialized `CpuHarness` now implements the five-stage experiment with a
persisted original/control/restricted journal and exact conditional restoration.
Each stage collects three fresh accepted payments with complete nine-span paths;
work stages also retain raw kernel sources from the verified payments container.
Both normal-quota controls must pass the frozen work-duration and throttled-time
contrasts. The generic runner rejects OOM-03. Lifecycle and acquisition fixtures
are tested. The committed-image Kubernetes run at `56e6337` qualified all five
stages and exact cleanup. Mean work duration was 0.290 seconds at 500m, 1.504 seconds
at 100m and 0.275 seconds after recovery. See [results](results.md) for evidence and
the distinction between workload measurements and agent performance.
