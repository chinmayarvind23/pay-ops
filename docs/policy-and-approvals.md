# Policy and Approvals

The model recommends. Deterministic code authorizes.

```text
proposal
-> identity/role
-> environment/scope
-> action/risk
-> parameter bounds
-> incident linkage
-> approval state
-> ALLOW_AUTOMATIC / REQUIRE_APPROVAL / DENY
```

Risk tiers:

- R0: read approved evidence
- R1: bounded diagnostics
- R2: reversible sandbox namespace operation
- R3: rollback/scale/config change requiring approval
- R4: destructive/shared infrastructure operation denied
- R5: financial-state mutation or secret access unavailable

Approval is rechecked immediately before execution because resource state and user authority may have changed.

The 120-case attack suite must stop every forbidden action before the executor.

## Authenticated action API

An operational host explicitly supplies a `RemediationBroker` to `create_protected_app`.
All routes require bearer authentication and bind the URL incident to the stored proposal:

| Method | Path suffix under `/api/incidents/{incident_id}/actions` | Purpose |
| --- | --- | --- |
| POST | empty | Propose as a current responder |
| GET | `/{action_id}` | Read the scoped immutable action |
| GET | `/{action_id}/audit` | Read its ordered SQL transition history |
| POST | `/{action_id}/approve` | Approve as a distinct current approver |
| POST | `/{action_id}/execute` | Claim and execute as a current executor |

The request never supplies the authenticated subject. Missing/out-of-scope actions return
404; authorization denial returns 403; concurrent transition conflicts return 409.
Execution revalidates all three identities, approval expiry, evidence bytes and freshness,
resource identity and version. Repeated execution returns the persisted result.

## Local effect boundary

`OperationalBackend` uses the identity service's current principal resolver and the same
incident/artifact stores as investigation. `LocalDeploymentExecutor` takes explicit kubectl
and kubeconfig paths, the sandbox namespace UID, service-to-Deployment UID inventory and
per-service approved digest-to-immutable-image mappings. It fixes context `kind-payops-dev`
and namespace `payops-sandbox`; ledger is excluded.

Restart adds a canonical action-digest annotation. Scale accepts 1–3 replicas. Image rollback
changes only the single `sandbox` container image to the operator-approved `@sha256` reference.
It does not restore prior environment variables or an arbitrary prior manifest. Traffic pause
requires a separate admission-control backend and is rejected by this executor.

One JSON Patch tests UID, resourceVersion and the entire prior spec before changing it.
The executor never refreshes a stale approved precondition or retries a write. A bounded
postcheck requires the exact desired spec and current-generation replica readiness.
This measures Kubernetes rollout health, not recovered payment business outcomes.

An applied patch whose rollout misses the deadline returns `FAILED` with a resulting version.
Ambiguous transport or unexpected state retains `UNKNOWN` in the broker. Both are terminal
and require operator investigation; neither automatically rolls back or dispatches again.
The public replay remains disconnected from this operational host.
