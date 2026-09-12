# Remediation approval and execution

The model can propose four closed actions: restart a synthetic Deployment, scale it to
one through three replicas, roll back to an operator-approved image digest, or pause an
identified synthetic traffic run. Every permitted proposal requires human approval.
Ledger changes, secret reads, arbitrary manifests and shell commands are not expressible.

`payops.policy` checks backend identity, namespace and service scope, incident ownership,
resource UID/version, current artifact bytes and evidence age. It binds the operation
mode into the action digest. Fixture evidence cannot authorize a live target. These
checks establish approval eligibility; artifact integrity alone does not prove that
an action will address the incident's cause.

Action evidence IDs must refer to current operational observations. RUNBOOK,
MEMORY and TRACE items can guide an investigation but cannot justify an effect. A PAYMENT
item must also pass nested source verification and arithmetic recomputation, and
its window must be complete. A valid outer artifact hash is insufficient.

`payops.remediation` persists proposals, approvals and execution claims in SQL. The
approver must be a different current identity from the proposer. Approval covers the
exact digest and expires after five minutes. Before execution the broker refreshes the
proposer, approver and executor identities, re-evaluates policy, and checks the current
resource precondition. A conditional SQL update admits one execution claimant. Its
audit event commits in the same transaction, before the backend can cause an effect.

After that claim, identity refreshes run again and all identities must remain current.
The approval deadline and the earliest resource/evidence freshness deadline are checked
immediately before dispatch. A database or identity-provider delay does not extend those
deadlines. The operational executor enforces UID/version preconditions at the
resource API; identity-provider revocation and a remote resource write cannot be one
distributed atomic transaction.

An ordinary transport exception after dispatch produces `UNKNOWN`. Process interruption
leaves `EXECUTING`. Neither state permits another dispatch. Terminal results also remain
idempotent. A known failure of the final authority check produces `FAILED` without a
backend call. These states stop automatic dispatch and require operator review.

The store validates decoded records against database keys and validates approval and
result invariants. Its audit API only inserts and reads events. Database administration
can still alter rows; this is not cryptographic audit immutability or a claim about
production database grants.

## Current integration boundary

Only authenticated backend code may call the broker with a subject. A caller-supplied
subject string is not authentication. Operator configuration supplies the trusted
identity/resource/executor implementation and its fixed execution mode. Configure verified identity, restricted database roles, bounded remote calls, exact resource preconditions and independent health checks for the operational binding.
