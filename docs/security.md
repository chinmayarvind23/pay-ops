# Security

Security lives outside model reasoning.

The Firebase Admin adapter verifies Google Identity Platform tokens and backend grants.

Kubernetes uses separate evidence-reader and remediation-executor identities with Workload Identity, RBAC, NetworkPolicy, and resource bounds.

The reasoning model never gets secret-reading capability or raw GKE MCP mutation tools.

Logs/runbooks/MCP output may contain attacker text. They are labeled untrusted evidence and cannot grant capabilities.

Backend policy and user authorization are revalidated at execution. The Slack adapter sends incident references only; a scoped responder and explicit host configuration are required. No inbound command can approve or execute an action.

Bound incidents, tool calls, log bytes, trace count, model tokens, provider dollars, wall time, and remediation attempts.
