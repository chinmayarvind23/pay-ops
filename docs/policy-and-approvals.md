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
