# src/payops/remediation

The SQL broker owns two-person approval, fresh identity/evidence checks and one-time dispatch.
`OperationalBackend` composes the incident/artifact stores, an authoritative principal resolver
and `LocalDeploymentExecutor`. The executor supports restart, scale (1–3 replicas), and rollback
to an operator-inventoried immutable image in the fixed `kind-payops-dev` sandbox. Namespace
and Deployment UIDs bind the operator inventory. Every write tests UID, resourceVersion and
the entire original spec atomically, then checks the exact desired rollout.

Pass the broker explicitly as `remediation=` to `create_protected_app` to expose incident-scoped
proposal, read, audit, approval and execution routes. The broker and app must have the same mode.
The default development API and public static replay have no operational broker.

`FAILED` with a resulting version can mean the patch was applied but readiness timed out.
`UNKNOWN` means a transport/postcheck outcome was ambiguous. Neither state permits automatic
redispatch or rollback. Controller readiness does not establish payment-level recovery.
Synthetic traffic pause is schema-defined but not enabled by this Deployment executor.
