# Low-Level Design

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
