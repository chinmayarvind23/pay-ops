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
