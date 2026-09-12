# Low-Level Design

## Executable implementation reference

The sections below this reference preserve the original design sketches. The source
contracts and transitions in this section describe the implemented local path.

| Responsibility | Source | Runtime contract |
| --- | --- | --- |
| Incident, evidence and report schemas | `src/payops/contracts/__init__.py` | Frozen objects, closed source/mode vocabularies, scoped citations |
| Immutable evidence | `evidence/artifacts.py`, `evidence/verification.py` | Bounded content, SHA-256 and nested source verification |
| Graph ownership and checkpoints | `orchestrator/graph.py`, `state.py`, `nodes.py` | Local file lock, synchronous SQLite checkpoints, immutable method/profile |
| Context and model decisions | `evidence/context.py`, `orchestrator/reasoning.py` | Verified facts, closed read/finish/refusal schema, exact citation IDs |
| Budget authority | `orchestrator/budget.py` | SQL compare-and-set, reservation before dispatch, no refunds for uncertainty |
| Durable model/read execution | `orchestrator/loop.py`, `loop_records.py` | Fsynced receipts anchored in SQL; no repeat dispatch after completed replay |
| Model transport | `orchestrator/model_runtime.py`, `openai_adapter.py`, `openai_wire.py` | One active model slot, stage authorization, bounded raw response parsing |
| Read dispatch | `tools/registry.py`, `operational_reads.py` | Fixed tool names and scope, whole-batch reservation, two worker slots |
| Approval and execution | `policy/engine.py`, `remediation/broker.py`, `store.py` | Deterministic policy, distinct approver, current identity/resource checks, SQL claim |

Paths in the table are relative to `src/payops` unless explicitly prefixed. Detailed
reasoning limits and provider assumptions are maintained in [reasoning.md](reasoning.md).

### Actual schema differences from the original sketches

An `EvidenceItem` requires a resource, artifact URI and 64-character SHA-256 digest.
Its source is a closed enum and `untrusted_text` can only be true. Observation and
collection timestamps must be timezone-aware. A hypothesis has no stored numeric
rank or hypothesis ID: its position in `ranked_root_causes` determines rank.
Confidence remains an uncalibrated score. Citation lists are immutable tuples.

`IncidentReport` validates unique evidence IDs, same-incident ownership, unique causes,
existing citations and disjoint supporting/refuting citations. It records runtime
mode separately from ranking method (`deterministic`, `model_fixture`, `model_provider`),
the reasoning stop reason and at most twenty receipt digests. A supported diagnosis
ends as `ESCALATED`; it does not imply an action was executed or an incident resolved.

### Actual graph and replay behavior

The current nodes are `triage -> reserve -> collect -> rank -> finish`. Conditional
edges can route a stopped investigation directly to `finish`. Remediation policy and
the action broker are separate components; the longer action lifecycle sketch below
is not the current graph topology.

The worker pins runtime mode, collection profile, ranking profile and ranking method
before collection. A resumed worker must match them. `pause_before_ranking` permits
inspection after collection. Completed graph checkpoints return saved state; a host
publishing that state must still check current authority and verify retained artifacts.
The current provider host integration is implementing that publication boundary.

Within ranking, the reasoning loop saves an immutable run binding containing incident,
initial evidence, subject, cause vocabulary, model settings and limits. It saves each
prepared prompt before SQL reserves the operation. Only a newly committed reservation
authorizes dispatch. The result is fsynced before SQL anchors its digest. A charge
without a completion digest stops as `UNKNOWN_COMPLETION`, even when a crash might
have happened before the remote request. This avoids guessing whether retry is safe.

Replay validates prompt/context identity, request order, incident/service scope and
nested evidence lineage. A crash after loop completion but before graph checkpointing
replays the loop receipts and performs zero additional model or read calls. Integrity
failures become `SECURITY_BLOCK`; model failures retain their selected model method
and do not silently receive deterministic baseline answers.

### Time, resource and cost boundaries

Wall time locates evidence. Monotonic clocks enforce acceptance deadlines and measure
elapsed work. A timed-out model or read operation keeps its worker slot until underlying
I/O finishes; the caller discards late output. Concrete transports also have deadlines.
This does not establish cancellation at a remote provider.

Context input is bounded to 256 artifacts, with at most 64 selected and 24,000 serialized
characters. All input sources are verified, including those excluded from the prompt.
Model JSON is capped at 16 KiB. Provider input counting receives the exact request
shape only after SQL reserves its input ceiling, output limit and two provider calls.
The runtime checks the server count and current authorization before generation.
Pinned prices calculate generation-token allowance; count-endpoint fees and real
provider billing require separate reconciliation.

### Actions and evidence authority

The model read loop has no action or approval fields. A separate proposal enters the
deterministic policy and approval broker. The broker stores the immutable proposal,
requires an authorized distinct approver, then refreshes identities, evidence age and
resource identity/version before claiming execution. Unknown executor completion is
not retried automatically. The current executor is instrumented test code; no model
has been given a Kubernetes mutation tool.

Runbooks, memory and diagnostic traces can guide a diagnosis without granting action
authority. Operational payment evidence requires complete verified source windows.
The attribution metric uses the same nested source verifier so a valid outer artifact
cannot receive credit after one of its original sources is corrupted.

## Original design sketches

## Package map

```text
src/payops/
  contracts/
  orchestrator/
  tools/
  evidence/
  retrieval/
  policy/
  remediation/
  memory/
  evaluation/
  telemetry/
  reliability/
```

## Core schemas

```python
class EvidenceItem(BaseModel):
    evidence_id: str
    incident_id: str
    source: str
    observed_at: datetime
    collected_at: datetime
    query: str
    resource: str | None
    artifact_uri: str | None
    artifact_sha256: str | None
    summary: str
    untrusted_text: bool = True
```

```python
class RootCauseHypothesis(BaseModel):
    hypothesis_id: str
    cause_code: str
    rank: int
    confidence: float
    supporting_evidence_ids: list[str]
    refuting_evidence_ids: list[str]
    missing_evidence: list[str]
```

```python
class RemediationProposal(BaseModel):
    action_id: str
    incident_id: str
    action_type: Literal[
        "restart_deployment",
        "rollback_deployment",
        "scale_deployment",
        "pause_synthetic_traffic",
    ]
    namespace: str
    resource: str
    parameters: dict[str, int | str | bool]
    reason_evidence_ids: list[str]
```

No generic `command: str` action exists.

## State machine

```text
INCIDENT_RECEIVED
-> TRIAGED
-> EVIDENCE_COLLECTING
-> HYPOTHESES_READY
-> ROOT_CAUSE_RANKED
-> REMEDIATION_PROPOSED
-> POLICY_CHECKED
   -> APPROVAL_REQUIRED
   -> ACTION_EXECUTING
   -> RESOLVED_WITHOUT_ACTION
   -> SECURITY_BLOCK
-> POSTCHECK
-> RESOLVED / ESCALATED
```

Other terminal states:

```text
EVIDENCE_INSUFFICIENT
BUDGET_EXHAUSTED
DEPENDENCY_UNAVAILABLE
ACTION_FAILED
UNRECOVERABLE
```

## Checkpoints

Persist after triage, evidence collection, root-cause ranking, policy decision, action execution, postcheck, and terminal report.

## Idempotency

Action key includes incident, action type, resource, and expected resource revision. Duplicate Pub/Sub delivery must not duplicate remediation.

## Implemented bounded synthetic CPU execution

`sandbox.cpu.CpuWork` adds an optional deployment-owned workload to the existing
request path. A single ThreadPoolExecutor and lock protect admission; no second
request is queued. The underlying future is shielded from caller cancellation so
the worker retains its slot even if cancellation arrives before execution begins.
Only the worker's finally block releases a successfully submitted slot. Shutdown
closes admission without cancelling admitted work; the cooperative deadline bounds
normal completion but cannot preempt a native hash or a suspended process.

The workload uses a fixed hash count and bounded buffer rather than sleep or an
invented utilization metric. Configuration defaults to zero; requests cannot alter
it. Work occurs after local idempotency reservation and before peer calls. Completed
replay skips execution, and failure abandons the reservation. Runtime quota and
cgroup measurements belong to the separate scenario harness; this component alone
does not establish a measured CPU-throttling incident.

`scenarios.cpu_counters` validates bounded raw cgroup-v2 records before computing
integer microsecond deltas. It requires all six bandwidth/accounting fields,
retains additional kernel fields, and rejects duplicates, resets, field-set drift,
quota changes, overlapping observations and changed Pod/container identities.
Each snapshot must be acquired within two seconds; a pair spans at most thirty.
Missing counters are errors rather than zero utilization. The module revalidates
immutable model copies at the arithmetic boundary.

These checks validate supplied records; they do not acquire or authenticate them.
The remaining CPU scenario collector must establish ownership around acquisition
and retain the original sources. Qualification must compare restricted execution
with the same workload at the normal quota, since calibration found throttling at
both quotas. Neither this validator nor the calibration qualifies OOM-03.

`sandbox.cpu_observation` now acquires the two fixed cgroup files inside the
admitted worker when deployment configuration sets `cpu_capture=true`. Capture
requires nonzero CPU rounds. Reads are capped at 4096 bytes and must finish within
two seconds. One JSON stdout record contains the sample ID, raw before/after files,
UTC acquisition timestamps, work wall time and worker thread CPU time. Failed work
emits no completion record; replay performs neither work nor capture. The HTTP
schema cannot enable capture or choose paths. Missing cgroup-v2 files fail capture.

The record deliberately contains no self-reported Pod identity. The scenario
collector must acquire it through bounded logs from a verified current container
and check ownership and restart state around collection. Counter deltas cover the
whole container, including background activity; thread CPU time covers the hash
worker. The two are retained separately. A local three-request container check
verified real file acquisition at 500m; Kubernetes qualification remains pending.

`scenarios.cpu_sources.CpuGateway` reads only the current payments container log,
with a 128KiB/2000-line ceiling. It reuses the five-service owner, image and exact
spec checks before and after acquisition and rejects any changed identity. Its
inherited mutation interface is limited to the payments Deployment. The source
selector requires one completion for the fresh sample within the operator's HTTP
window plus the existing one-second cross-host trace clock allowance. It rejects
duplicates and inconsistent durations, and then binds both kernel
snapshots to the collector's incident and process identity for delta validation.
The final harness must retain the returned raw bytes and runtime snapshots before
qualification; this adapter does not itself publish a scenario result.

`scenarios.cpu_contract.PLAN` freezes the five-stage order and three 50,000-round
requests per stage. Work is disabled in original/final stages. The two work specs
enable capture and use Recreate; only the CPU limit differs between 500m control
and 100m restriction. Memory, resource requests and peer settings are preserved.
Stage aggregation requires distinct sequential samples, matching actual kernel
quotas and positive CPU consumption. Both control and recovered mean work times
must be at most one second. Restricted mean time must be 1â€“4.5 seconds and at least
twice each control; mean throttled time must exceed 0.5 seconds and three times
each control. These thresholds come from the retained prequalification calibration
and must not be tuned against a qualification run. They measure work duration,
not end-to-end investigation latency.

`CpuHarness` now executes that lifecycle using the shared cross-process latch.
Before mutation it saves the five-service original runtime and both complete work
specs. Each transition saves the current expected deployment and next spec before
CAS; readiness checks pin unaffected peers and require a fresh payments process.
`RuntimeCpuObserver` retains three independently traced accepted payment paths per
stage and the matching raw CPU logs for work stages. Sample and trace IDs cannot
repeat, and request windows must be sequential. Both comparisons must pass; final
cleanup restores the original disabled configuration and verifies three new paths.

Cleanup recognizes only the original/control/restricted specs on the original
Deployment UID. A foreign identity or spec is never overwritten. Restoration runs
before final evidence persistence; a failed write, failed final observation or
failed receipt/latch operation leaves the experiment blocked for inspection.
Fixture tests exercise normal execution, rejected and ambiguous writes, cancelled
observations, foreign states and failed restoration. Plan v3 subsequently passed
live qualification at `56e6337`; see the measured result below.

Trace acquisition rechecks actual wall time after waiting for the twelve-second
export offset. The first live CPU attempt found two intervals slightly short of
that boundary after a single requested sleep. The observer now performs bounded
additional waits and rejects stalled or reversed clocks. Source acceptance still
requires the full offset; the failed attempt and its verified cleanup are retained.

CPU acquisition plan v2 also binds each kernel interval to its matching payments
SERVER span and requires completion before the first CLIENT dependency span.
Those timestamps share the container clock, so this check needs no cross-host
tolerance. The second live attempt exposed a 3.7ms container/host offset; it remains
unqualified with verified cleanup. Diagnostic replay verifies its three control
records under the revised clock rules, but does not retroactively qualify that
attempt. Work counts, quota treatments and performance thresholds are unchanged.

Plan v3 retains monotonic nanosecond timestamps at the start and end of each
kernel-file acquisition. Work duration is compared with that monotonic interval,
not the difference between UTC timestamps. The third live attempt observed a
77.5ms disagreement between those clocks during one restricted request. Comparing
them as interchangeable durations was invalid. UTC still locates the request and
its container-local spans; monotonic time validates elapsed work and acquisition
bounds. Negative or stale monotonic intervals fail validation. Three real container
captures verify the new fields; the third Kubernetes attempt remains unqualified
with exact restoration. A fresh v3 run at `56e6337` then passed all five stages:
335 retained files, 15 successful nine-span payment paths and exact original-spec
restoration were reverified. Mean control/restricted/recovered work durations were
0.289804/1.503687/0.274641 seconds. These qualify local CPU throttling, not model
performance or investigation latency.

### Insufficient-memory scheduler contract (SCHED-02, locally qualified)

The closed memory recipe requests and limits payments at 16Gi. Its node guard
requires both reviewed healthy node identities and positive allocatable memory
strictly below 16Gi; the journal identity includes capacity in bytes so a changed
placement envelope invalidates later evidence. The current local nodes report
16124080Ki each. No memory workload is needed: an admitted request larger than
node capacity should remain unscheduled.

Admission expansion changes only memory: ResourceQuota requests become 18Gi,
limits become 20Gi, and the container LimitRange maximum becomes 16Gi. This leaves
room for the pending pod, four peers and one replacement during restoration of
the original RollingUpdate strategy. CPU allowances, container configuration and
all unrelated fields remain unchanged. Injection uses Recreate. This follows
[Kubernetes request-based placement](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
and [quota admission accounting](https://kubernetes.io/docs/concepts/policy/resource-quotas/).

The shared three-object journal now selects this recipe for SCHED-02. Node
identity and capacity are checked before admission expansion, again immediately
before workload injection, and during activation. Qualification requires a fresh
`Insufficient memory` event naming the current owned Pending pod; CPU-only and
admission-error events reject. Lost responses at every write boundary exercise
independent restoration. Live activation and cleanup were verified at `e69b814` (run
`05baf15280ff466395e32655c94a54e9`). The actual event reported insufficient memory;
all three resource specs and identities were restored and all five services healthy.

### Retained-allocation worker (OOM-02, locally qualified)

The startup-only risk worker has two closed modes: retained-v1 keeps each touched
8Mi allocation; released-v1 drops each allocation immediately. Both attempt at
most40 chunks, wait0.5seconds between chunks and use the same256Mi cgroup limit.
There is a10second startup grace and35second monotonic lifetime limit. Kernel
memory.max is checked before allocation and on every step. Host execution, other
roles, unspecified modes and other limits reject. No HTTP request can enable it.

Each record includes actual memory.current/memory.max, retained byte count, step,
PID, UTC timestamp and monotonic time. These logs establish allocation progression,
not OOM termination. Qualification still requires actual Kubernetes OOMKilled
termination and restart identities, repeated progression under an unchanged limit,
a successful released control, bounded recovery and healthy payment traffic.
The kernel's [cgroup memory controller](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
provides the memory limit and OOM accounting; an application exception is insufficient.
The worker has unit coverage but is not yet wired to a container entrypoint or harness.

OOM-02 now has a separate container entrypoint and Dockerfile. It validates the
startup guard before constructing the normal risk application, starts one worker
inside the original application lifespan, and signals/joins it before dependency
teardown. Worker exceptions are surfaced during shutdown; neither an exception nor
a completed thread qualifies an OOM. The Dockerfile layers only these modules onto
the normal sandbox image. Frozen image provenance remains required before live use.

At frozen source9d4efd5, isolated network-disabled containers with identical256Mi
memory/swap bounds exercised the real risk application lifespan. Release mode
completed40 allocations, reported zero retained bytes and exited0 without OOM.
Retained mode recorded27 successive8Mi allocations, reached268214272bytes of
kernel-accounted memory and exited137 with Docker OOMKilled=true. Three installed
scenario modules matched frozen source hashes. Raw stdout/stderr, container/image
inspection and11 artifact hashes are retained under evidence/leak-container-9d4efd5.
This is a container contrast, not repeated Kubernetes OOM/restart qualification.

OOM-02 allocation evidence uses a strict immutable record schema and bounded raw
kubectl-log parser (256KiB/2000lines, at most42workload records). Runtime timestamps
must agree with the application's UTC within1second. Container start/end times
bound the records; monotonic timestamps enforce order and the35second duration
ceiling. A release control requires all40steps plus terminal release. Retention
requires5–39successive steps, matching8Mi increments and at least64Mi of observed
kernel memory growth. Declared retained bytes alone cannot pass. Mixed modes/PIDs,
duplicate starts, stale records and premature release reject.

These checks validate one process's allocation evidence only. A caller must still
bind raw current/previous logs to owned container IDs and actual OOMKilled
termination records, capture two distinct OOM lifetimes and verify restoration.
No repeated-OOM or Kubernetes qualification is claimed by the record parser.

OOM-02 previous-log validation now brackets the read with two owned risk rollout
snapshots. The exact Deployment UID/name/generation/template, ReplicaSet ownership
and fresh pod creation are checked using the shared provenance logic with an
explicit closed risk target. Both snapshots must identify the same previous
containerd container, image, restart count and start/finish times. Exit137 requires
OOMKilled; invalid or future timestamps reject. Allocation progression must fit
that lifetime before its raw log digest is retained.

Repeated-OOM acceptance requires two distinct container IDs with adjacent restart
counts, nonoverlapping lifetimes and different log digests from the same pod/image.
Duplicate polling cannot increase the count. Collector integration must preserve
both snapshots and raw logs, including rejected reads; this component does not by
itself qualify a Kubernetes case. Existing payments provenance defaults remain.

OOM-02 now has a risk-only gateway with bounded JSON responses and current/previous
container log reads. It retains raw text for storage before semantic validation;
reaching a byte cap rejects rather than treating truncation as a complete capture.
The API cap is256KiB, log cap256KiB/2000lines, and log window3minutes with12second
read deadlines. Pod names/namespace/UID are checked before requesting logs; the
lifecycle caller remains responsible for before/after ownership validation.

The two generated risk specs use the verified leak-9d4efd5 image and Recreate while
preserving baseline resources and configuration. Both keep50m/96Mi requests and
500m/256Mi limits. Release control and retained fault differ only in the explicit
PAYOPS_SYNTHETIC_LEAK mode. Changed entrypoints, roles, limits or duplicated env
reject. Exact original restoration and image-runtime checks belong to the upcoming
journaled harness; these adapters alone do not increase qualified scenario count.

OOM-02 lifecycle integration now journals original state and both complete variants
before CAS. A complete current-container release log, zero restarts, stable process
identity and healthy payment are required before the retained transition. Fault
capture preserves before/raw/after artifacts, then requires two distinct adjacent
OOM lifetimes whose runtime image matches the control. Duplicate polls remain
artifacts but do not count as new failures. Each stage has a bounded wait.

Recovery recognizes only original/control/retained specs under the original risk
UID. It restores before audit writes, checks all five exact specs and unchanged
non-risk process identities, and requires accepted payment traffic. Unknown specs,
failed restore or final receipt failure retain the shared latch. Interruptions still
run recovery. This integration has fixture recovery and real-validator aggregation
tests; final control-capture/persistence failure checks and live qualification remain.

The OOM-02 lifecycle validation now includes actual raw control collection,
container-change/restart/incomplete-log negatives, pre-journal write prevention,
final audit failure and final receipt failure. The focused suite passes84tests;
harness statement/branch coverage is88%. A test-only60ms positive-control deadline
was too short under coverage instrumentation and was raised to2seconds. Live
stage bounds remain unchanged. These checks precede the first Kubernetes attempt.


The first frozen OOM-02 attempt at bb4f85c stopped in scope preflight: the new
512KiB API cap exceeded the shared subprocess reader's256KiB maximum. No workload
mutation or latch occurred; final runtime/source verification passed. The adapter
now uses256KiB and a real inert subprocess test verifies that interface contract.
The failed attempt remains under chunk-11-leak/dfa8e9abd17b4a0f8f03e5a08cc49215.

The second OOM-02 attempt252d2c2 completed the release allocation sequence but
failed the five-service settle check. The reused sampling validator intentionally
requires unchanged original images; the retention experiment changes risk's image.
Attempt30f426c939064fd09a19d1da4b050c4d/run79fc69dffa80489583cd88a72842423d remains
unqualified. Original risk restoration, accepted payment, final source/runtime and
absence of the latch were verified.

The runtime validator now accepts an explicit risk-only image identity while
retaining original image checks for every other service. OOM-02 accepts that override
only after matching the pinned kind-import digest
sha256:a86b7fed7a764c470d118c6e1e610f8fd5b697686c5334e78b95a2bb800a7106.
The imported config86c1984cd9bdd5b79362eafe4d4263ad398cc78213b4b93d22e2747d88f7cd30
matches the independently tested9d4efd5 Docker image config and installed-source
proof. Control and retained processes must still use the same runtime image.
Restoration uses the original image rule. A new source revision/run is required;
the previous attempt is not retroactively qualified.


Frozen7d3e204 subsequently passed OOM-02. Run d2a14f09ce8349be8fd2c86fab89f44a
completed the40-step release control, captured two distinct27-second OOM container
lifetimes with converged restart counts1/2, then restored the original risk spec
and image and verified all five services plus payment health. Independent artifact
review reopened121hashes and111source/dependency hashes. Earlier failed attempts
remain retained and unqualified.

The kubelet briefly exposed the second termination while restartCount still read1;
later snapshots converged to2. The runner waited for adjacent counts before
activation. The independent verifier initially kept only the first snapshot of
each container ID; it now retains the latest count for that same immutable
termination, verifies unchanged times/pod/image, and still counts exactly two
container IDs. No capture or acceptance threshold was changed.

### Request-concurrency memory experiment (OOM-04, not yet qualified)

The optional deployment-only concurrency_memory profile holds32Mi of touched
resident memory for1second per accepted non-replay payment request. Up to8requests
can be admitted on the owning ASGI loop; excess work rejects503 without queuing.
It requires the synthetic payments role in a Linux256Mi cgroup. Closed or disabled
profiles cannot allocate; ordinary disabled startup does not read cgroup files.
Each request releases its allocation and admission slot in finally, including
cancellation or capture failure. No background allocator is created.

Logs link each sample to admitted/allocated/released phases, actual active count,
fixed workload size, kernel current/maximum memory, UTC and monotonic time. The
profile runs after idempotency reservation and before peer calls; successful
replays skip work and failures abandon only the local reservation. It cannot be
combined with CPU work or enabled through an HTTP sample. Other scenario baseline
checks reject an already-active memory profile.

The planned contrast keeps the same image, resource limits and per-request work,
changing only traffic concurrency. Acceptance still needs complete low-concurrency
controls, measured overlap/kernel growth under high concurrency, actual pressure
or OOM evidence, payment effects and verified restoration. This worker/integration
is not live qualification. Twelve worker tests reached100%statement/branch coverage;
37focused worker/service/CPU regression tests passed with strict targeted typing.

OOM-04's frozen ab9a869 image passed an isolated worker contrast at identical256Mi
memory/swap bounds and0.5CPU. Eight sequential calls completed before and after the
parallel treatment. The parallel run recorded seven active admissions before a
confirmed OOMKilled/137 exit; no request completed. Raw event reconstruction
verified each active-count transition and the full1second holds in both controls.
Peak recorded kernel memory was95830016/238202880/69885952bytes for
control/parallel/recovered. All110installed Python source files matched the frozen
checkout. Evidence: concurrency-container-ab9a869. This calibrates the worker;
HTTP effects and Kubernetes activation/recovery are still unqualified.

OOM-04 now has strict timestamped memory-event parsing and deterministic occupancy
reconstruction. Records are limited to24events from eight planned unique samples
within a45second window. Admission, allocation and release must occur in order;
reported active counts must match derived occupancy. Completed allocations must
have a full1second hold. Serial controls require all eight completed lifecycles
with peak-active1. The parallel proof requires at least six simultaneously
allocated blocks and128Mi of actual kernel memory growth; admission alone cannot
prove that the allocation happened. HTTP outcomes and owned OOM termination still
require separate validation.

Eighteen adversarial tests achieved100%statement/branch coverage. Replaying actual
Docker-timestamped calibration logs passed unchanged: controls8completed each;
parallel7admitted/6allocated concurrently before OOM. This validates the parser
against retained sources and does not create Kubernetes qualification.

OOM-04 reuses TrafficDriver for eight payments with fixed slice A/us/credit,
5-second request timeouts and a 45-second batch deadline. Serial controls use
concurrency 1; treatment uses 8. Persisted plans must exactly match receipt
identities and settings. Validation rejects incomplete or unstarted requests,
invalid timing, repeated traces, mismatched responses and idempotency conflicts.
Controls require eight accepted responses; treatment requires an availability
failure. Transport failures cannot claim an HTTP response. Independent memory
records and owned Kubernetes OOM termination are still required for qualification.
The driver drains all tasks before closing its client and port-forward. Seventeen
fixture-transport tests passed with 100% statement/branch validator coverage and
strict typing. They verify transport orchestration, not live Kubernetes effects.

OOM-04 deployment derivation now requires the normal payments entrypoint, exact
role/config environment, v1 risk protocol, 2-second peer timeout, no existing CPU
or memory workload, and normal 50m/96Mi requests with 500m/256Mi limits. It changes
only the image to concurrency-ab9a869, strategy to Recreate and the deployment-only
concurrency_memory flag. Serial, parallel and recovery traffic use this same spec;
unrelated fields, including ephemeral-storage bounds, are preserved. The caller
must still verify the imported runtime image digest before live evidence is valid.
OOM-04 is registered for specialized receipts and explicitly rejected by both the
generic runner and generic recipe. Twelve focused tests cover unchanged originals,
invalid baselines and rejection before writes; 54 tests including generic scenario
regressions pass. Strict typing and lint pass; spec coverage is 100% statement and
branch. Kubernetes lifecycle and live qualification remain pending (16/24).

OOM-04 image import is verified on both kind nodes: runtime manifest
sha256:5cc92f97bc22d16d26ce5882569aa92f421e30eb6237d5d602669ed2146734c4
resolves to calibrated config
sha256:b9a090e9060793f1a85ed7371aab0141577df837bced9b5c11ffd13bf5d67ad4.
Raw CRI records and hashes are in audit/evidence/concurrency-kind-import.
Importing the image did not change a running Deployment.

The concurrency lifetime validator brackets previous-container logs with matching
owned Pod/ReplicaSet/Deployment observations. The terminated container must equal
the process identity captured for traffic, use the pinned image, and advance its
restart count exactly once. Its OOMKilled/137 timestamp must overlap the failed
traffic batch, allowing one second for Kubernetes timestamp precision. Memory
records must fit both that process lifetime and the traffic window; existing
validators require six allocated blocks and 128Mi kernel growth. The result keeps
the raw-log SHA-256 and derived occupancy. Callers must supply previously validated
traffic and runtime identity. Nine join tests reached 100% statement/branch
coverage; 67 related regressions, strict typing and lint passed. The full lifecycle
collector and live recovery experiment are still pending; qualification is 16/24.

OOM-04 acquisition reuses LeakGateway's bounded Kubernetes transport through a
payments-only ConcurrencyGateway. The target selects both the Deployment/ReplicaSet
snapshot and current/previous pod logs; write validation independently rejects all
other resources. The existing risk harness keeps its original target. Both paths
retain 256KiB/12-second read bounds and exact namespace/container restrictions.
Runtime identity verification now accepts an explicit payments image expectation,
forwarded through protocol_identities. All other image and ownership checks remain
in force; an omitted override still requires the original image. The lifecycle
caller must verify the worker against the pinned import manifest before supplying
this expectation. Four new transport/isolation tests plus existing leak, sampling,
protocol and CPU observer regressions passed (57 tests); strict typing and lint
passed. This implements acquisition dependencies, not live OOM-04 qualification.

OOM-04 now has ConcurrencyHarness: capture the five-service baseline, journal the
original and enabled specs, then run control/parallel/recovered stages. Each stage
starts from the exact original, applies the same enabled spec, proves a newly owned
process, and sends eight fresh payments. Full restoration and an accepted payment
separate stages. This deliberate reset keeps current/previous memory logs free of
earlier batches while holding the enabled image and quotas identical. Cross-stage
sample IDs and process IDs must be distinct. All unchanged peer identities are
checked against the initial baseline.

Serial stages require complete memory lifecycles and unchanged healthy processes
before and after log capture. Parallel traffic executes once; only kubelet evidence
is polled afterward. The investigation callback follows verified OOM and precedes
restoration. A failed recovery stage leaves a failure receipt even if the earlier
OOM activated. Original restoration runs in finally, before audit writes, and an
unknown UID/spec blocks overwrite. Cleanup or receipt persistence failures retain
the shared cluster latch. Driver plans and receipts are copied into hashed stage
artifacts alongside raw Kubernetes snapshots and logs. No model-facing mutation
capability is added.

Fifteen harness tests include actual owned-state, traffic and memory/OOM validators,
plus substituted transport and stage failures. Harness statement/branch coverage
is 87%; 75 related tests, strict typing and lint passed. These are offline tests;
the frozen live run and independent artifact review remain pending (16/24).

OOM-04 qualified at `926bbd5` using three fresh payments processes with identical
worker image and 256Mi memory limits. Eight serial requests completed before and
after the parallel treatment. The parallel batch produced eight request failures,
seven recorded admissions, six overlapping allocated blocks, and an owned
OOMKilled/137 termination. Peak recorded kernel memory was 122859520 bytes in the
first control, 244191232 in treatment and 88715264 in recovery. Original deployment
state was restored between stages and at exit; unchanged peers retained their
identities and final synthetic payment health passed.

Separate post-run review checked 74 artifact hashes and 118 source/dependency
hashes, rederived the deployment spec, validated raw plans/receipts and memory
records, joined the OOM to the tested process, and verified image provenance and
exact restoration. Evidence: `chunk-12-concurrency/953e7735aa9546b9b586371586a83536`,
run `79a03f72e74943bb95504b7e6e09ddca`. The operator experiment ran from
23:28:09 to 23:29:55 UTC on September 11, 2026. This duration includes rollouts and
controls; it is not agent latency or a diagnosis measurement. The qualified count
is now 17/24; model quality, timing and cost targets remain unmeasured.

SCHED-03 requires an actual autoscaler backed by observed resource metrics. The
local cluster initially had no metrics.k8s.io API registration. Metrics Server
v0.9.0 is vendored separately from payment manifests, with its upstream image
pinned to the inspected OCI digest. Server-side dry run accepted all nine upstream
objects on Kubernetes v1.35.8. Kubelet certificate verification is initially kept
enabled; actual scrape/API readiness must be established before HPA experiments.
This prerequisite does not add scenario qualification or model metrics.

Metrics Server readiness was verified after applying the kind overlay: the
aggregated API is Available and returns CPU/memory samples less than 60 seconds
old for all five payment services. The deployment rollout completed successfully.
Raw API, deployment and PodMetrics records plus hashes are retained under
resources/pay_ops/audit/evidence/metrics-server. This is an autoscaling prerequisite;
SCHED-03 still needs its capped-HPA load experiment and recovery checks.

SCHED-03 now has a closed autoscaling/v2 HPA contract for payments-api in
payops-sandbox. The fixed CPU target is 50% of requests, minimum one replica;
only maxReplicas changes from one to two. Explicit scaling policies permit one
pod per 15 seconds with zero stabilization in this bounded local experiment.
Status acceptance requires exact UID/name/namespace/spec, AbleToScale and
ScalingActive true, current/desired replicas equal to the cap, and CPU utilization
at least 100% with ScalingLimited=True/TooManyReplicas for saturation. Low-demand
control requires utilization at most 50% and ScalingLimited=False. Invalid metric
sources, ambiguous conditions and boolean-as-integer fields reject.

Kubernetes v1.35.8 controller setStatus does not populate the optional
observedGeneration field. The validator rejects a conflicting value if present,
but does not invent freshness when absent. The lifecycle must independently bind
raw Metrics API sample timestamps/windows and owned pods to the active load, then
show actual scale-out with maxReplicas=2 and exact restoration. Fifteen contract
tests pass with 100% statement/branch coverage, strict typing and lint. No HPA was
created in this increment and qualification remains 17/24.

References: https://kubernetes.io/docs/reference/kubernetes-api/autoscaling/horizontal-pod-autoscaler-v2/
and https://github.com/kubernetes/kubernetes/blob/v1.35.8/pkg/controller/podautoscaler/horizontal.go

SCHED-03 raw CPU validation now parses bounded Metrics API nanocore, microcore,
millicore and decimal values with Decimal arithmetic. It requires one or two
unchanged, never-restarted payments identities bracketing acquisition. The expected
pod set must match exactly, each pod has one sandbox container, and the full
1-to-60-second averaging window must lie after the declared load start. Samples
older than45seconds, duplicate/foreign pods, invalid quantities and capped byte
responses reject. Utilization uses the frozen50m CPU request. Callers still own
controller-chain/resource validation and must prove the load actually ran.

Twenty tests passed with95%statement/branch coverage; all35HPA contract/metric
tests pass, strict typing and lint pass. A real idle capture validated the unchanged
payments process at0.003618311cores (7.236622%of50m), with a complete window from
23:41:21.674 to23:41:33UTC and capture23:41:45UTC. Raw before/metrics/after sources
and SHA-256 records are in audit/evidence/hpa-idle-metrics. This verifies parsing
and acquisition against real resource metrics; it is not an HPA load result.
Qualification remains17/24.

SCHED-03 uses an in-cluster HpaLoadDriver because kubectl port-forward to a Service
selects one pod; it does not exercise Service balancing across replicas. The worker
reuses TrafficDriver planning, attempts, deadlines and receipts, overriding only
client setup. Its destination is fixed to the payments-api cluster-local Service
on8080. It requires Linux and explicit PAYOPS_HPA_LOAD=kind-v1, disables environment
proxies/redirects and connection reuse, and caps HTTP connections at4. Its fixed
batch is256A/us/credit payments, concurrency4, request timeout5seconds and overall
deadline180seconds. Job-level deadline/resource limits and retained raw output are
still required before live use. The worker performs no Kubernetes API operations.

The process emits the original plan and full receipt after a completed driver run;
incomplete batch status produces a failed exit. Five tests passed with91%statement/
branch coverage, strict typing and lint. The worker has not yet been built into a
Job image or used for HPA qualification. Actual distribution across both scaled
pods must be checked from their traffic/CPU sources, not assumed from client
connection settings. Source: https://kubernetes.io/docs/reference/kubectl/generated/kubectl_port-forward/

SCHED-03 load Job configuration is now fixed: one completion, one pod, no retries,
Never restart policy, 210-second active deadline and5-second termination grace.
The pod uses the existing unprivileged service account with token automount disabled,
nonroot UID/GID1000, RuntimeDefault seccomp, no added capabilities or privilege
escalation, read-only root filesystem and a16Mi temporary volume. Requests are
100mCPU/96Mi memory; limits500m/256Mi. Only a validated32hex run identity varies;
image, command and destination remain fixed. Six tests, strict typing and lint
passed; Kubernetes server-side dry run accepted the complete Job.

Built the worker from frozen c0b1792 with locked dependencies. The Docker index is
a0fa016b73f294534a9aea91b3ce3a74d079f4b0292e4319c8e9f58df549b2e7;
config24ed386d554d1f756832a5b2610c30872c0446ca823784d33ad21ec8a2041f49.
Both kind nodes imported manifest8addf3e215722e4eb8606bcc8a30d97faa5a36ba4b1f219020e890f09f1882d6
with that same config. An isolated network-none inspection matched installed worker
and TrafficDriver hashes to frozen source and confirmed256requests/4concurrency.
Raw image inspections are in audit/evidence/hpa-load-image. No Job or HPA has run
in the cluster yet; qualification remains17/24.

SCHED-03 HpaGateway now provides create-only HPA/Job operations, bounded resource
reads, cap replacement using the captured resourceVersion, and raw DELETE with
UID/resourceVersion preconditions. Names, namespace, API versions and run labels
are closed and validated. JSON writes use stdin rather than shell-interpreted
arguments. Each write revalidates local cluster scope; unrecognized HPA specs and
foreign ownership reject before mutation. Cleanup still requires lifecycle-level
journaling, ambiguity recovery and terminal-state checks before releasing the latch.

Nine gateway tests passed with97%statement/branch coverage; strict typing/lint pass.
A full256-attempt HpaLoadDriver test also verified complete plan/receipt output fits
the existing256KiB log bound and closes its client. These are intercepted-transport
checks, not a live autoscaling result. No HPA or Job was created in this increment;
qualification remains17/24.

The HPA gateway passed a real idle-cluster smoke test from frozen ea5c06f.
It created the one-replica cap, replaced it with the exact two-replica cap, and
attempted deletion with the earlier resourceVersion. Kubernetes rejected that
request with Conflict: expected52684 versus current52687. Deletion with current
UID/version preconditions then succeeded. The HPA was confirmed absent, all five
original service identities/specs remained unchanged, and the canonical latch was
released. No load Job ran and no autoscaling fault is qualified by this check.

Separate evidence review checked ten hashes, matching UID across cap changes and
exact specs for both caps. Evidence:
audit/evidence/hpa-gateway-smoke/b1c82bed78fc45a8a9d868f3bfef9326.
This verifies actual kubectl stdin/raw-DELETE behavior in addition to intercepted
unit tests. The next lifecycle work must support two owned payments pods while
preserving peer identities and separately accounting for the bounded load Job.
Qualification remains17/24.

SCHED-03 runtime validation now handles one or two real payments replicas. Shared
deployment/pod identity checks were exposed for reuse; existing single-replica
callers retain their defaults. Deployment convergence requires exact integer
replica counts, the captured UID, expected full spec and observed generation.
Every payments pod must have matching ReplicaSet/Deployment ownership, exact
container template, captured image, no restart, and distinct pod/container IDs.
All four peer service identities must remain unchanged.

Nine scale-out tests and44related sampling/protocol/concurrency regressions passed
(53total); new runtime coverage88%, strict typing and lint passed. The runtime
validator intentionally rejects unaccounted pods. The lifecycle must separately
verify the load Job's ownership before passing the service-only pod set; it cannot
silently drop extra workload pods. This implements scale-out evidence checking,
not live HPA qualification. Count remains17/24.


SCHED-03 load-pod accounting now requires the fixed Job spec, exact batch/v1
controller name and UID, namespace, reviewed pod template, pinned image and one
unrestarted process. It excludes exactly that pod from service replica validation;
extra pods remain visible. Invalid identifier types and ambiguous or waiting
container states reject. A terminated process can be attributed without claiming
that the Job succeeded. Completion, traffic receipts and live scale-out still
require separate lifecycle evidence. Qualification remains 17/24.

Validation: 32 identity/runtime/gateway tests passed; load-pod module statement
coverage 100%; Ruff (including C901) and strict Pyright passed.


## Free public deployment and revised AWS scope

The owner requested free Hugging Face hosting and optional AWS instructions only.
The four-case replay now runs as SDK `static` with no paid compute or database.
Space: https://huggingface.co/spaces/chinmayarvind/payops-incident-replay
HF revision: ccdbfdbc1e3473cbbeb1ab2acafecfc1a0b2c23d.
The static host is returned by the API and ends in `.static.hf.space`; the Docker
hostname is not valid for this SDK. Served JS/CSS match release bytes exactly.
Served HTML matches after removing HF's metadata script and normalizing line ends.
All four repository files match the release. Application CSP resides in HTML;
Nginx-specific headers and health routes are not claimed on Static hosting.
Evidence: audit/evidence/hf-static-deployment-v2/verification.json.
Browser inventory is empty, so layout/click verification and recording remain open.
AWS was not provisioned; infra/terraform/aws-lightsail/README.md provides optional
setup and explicitly identifies the unfinished cloud processor adapter.


The HPA scenario is now live-qualified. A fixed 0.5-second launch interval prevents
rapid rejected requests from exhausting its 256-sample batch before controller
sampling. Pacing is persisted with the original plan. Runtime validation accounts
for an owned load Job separately from one/two actual payments pods; independent
resource-metric windows must follow each stage's observed start. The cap changes
through UID/version CAS and all resources are removed before original-spec recovery.

Live corrections: HPA REST metadata can omit generation; Deployment generation
requirements remain unchanged. CRI can fragment one long log record, so the Job
reader retains up to 2000 fragments within the original256KiB cap. Whole-second
kubelet termination times provide an exclusive next-second upper bound for receipt
completion. Neither correction changes the frozen CPU demand thresholds.

Source c59892c passed the full live experiment and post-run evidence review.
See docs/results.md and external audit/evidence/hpa-live-verification.json.


The dependency scenario group uses the existing TLS-enabled local PostgreSQL and
Redis services. The synthetic payment path now supports a deployment-only dependency
profile: one constant SELECT 1 through a dedicated read-only PostgreSQL identity,
or PING through Redis's existing probe-only identity. Destinations, commands, TLS
verification and timeout bounds are fixed. HTTP samples cannot configure clients.
A failure prevents synthetic payment completion and releases its idempotency claim;
accepted replay still avoids duplicate work. Default service behavior has no new
network dependency or extra dependency span.

The same image contains two observation conditions. The cache scenario can emit an
explicitly archived synthetic database error dated one day earlier. The database
scenario can serve a real previous metrics snapshot for60seconds, retaining its
original timestamp in both a gauge and response header. These conditions are kept
separate from primary-cause labels. Live qualification and cleanup remain required.


### Dependency group harness

`dependency_specs.py` preserves startup and resource checks, pins the imported worker image and projects one fixed read-only credential directory. `dependency_gateway.py` confines data writes to Redis replicas 0/1 under UID/version/full-spec tests. `dependency_pressure.py` owns its loopback forward and four SELECT-1 sessions, closes them on every exit and relies on a 45-second server idle timeout as a second bound. No credentials enter evidence. `dependency_evidence.py` requires exact three-request plans, current HTTP 503 plus cause/sample/time/trace matches, and original timestamps for archived or delayed observations. `DependencyHarness` journals both namespaces and attempts data and payments restoration independently; incomplete recovery retains the cross-harness latch. Free HF hosting remains a credential-free replay; AWS setup is optional documentation only.


### CPU noise lifecycle

`NoiseHarness` uses the shared cluster latch, immutable receipt writer and finally-recovery path. `noise_contract.py` freezes a 120-second hashing script inside a hardened one-shot Job with a 150-second deadline, 500m/256Mi limits and no service-account token. `NoiseGateway` validates the Job template, pod owner, actual imported image and zero restarts before reading per-second CPU counters. Every traffic window must be bracketed by advancing CPU/time samples. Four sandbox peer processes remain unchanged. Processor restoration and Job deletion are attempted independently; cleanup failure retains the latch. There are no model-selected scripts or command parameters.


### Eviction implementation

`EvictionGateway` pins kind-payops-eviction, its sole Docker node and payops-eviction namespace. It reads only the fixed kubelet config path; no certificate contents are read. `EvictionHarness` journals original config/container identity before creating a token-free webhook Pod. Treatment changes evictionHard.memory.available, uses a one-second pressure transition period and enforceNodeAllocatable=[none]. Acceptance joins the owned Pod Failed/Evicted memory status, its Evicted event, node MemoryPressure=True, effective configz threshold and availableBytes below that threshold. OOMKilled, missing events and foreign Pod UIDs are rejected. Configuration and namespace cleanup are independently attempted under the shared latch. Raw failed acquisition observations survive kubelet restarts.
