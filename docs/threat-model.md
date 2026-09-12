# Threat model

Protected assets include Kubernetes authority, payment-service availability, operational credentials, incident evidence and action approvals.

The main threats are prompt injection in logs and runbooks, malicious tool output, a confused deputy acting outside incident scope, stale approvals, resource replacement and repeated effects after an uncertain response.

Evidence stays untrusted when it enters model context. Fixed tool schemas restrict reads, backend grants establish authority, and the remediation broker rechecks the current identity and resource before execution. SQL claims and conditional resource updates prevent a repeated request from silently becoming another action.

The synthetic environment has no real financial connectivity. Separate service, evidence-reader, scenario-controller and executor identities limit the authority available to each process.
