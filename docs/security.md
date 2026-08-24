# Security

Security lives outside model reasoning.

Use Google Cloud Identity Platform for OIDC/SAML, backend token verification, and role mapping.

Kubernetes uses separate evidence-reader and remediation-executor identities with Workload Identity, RBAC, NetworkPolicy, and resource bounds.

The reasoning model never gets secret-reading capability or raw GKE MCP mutation tools.

Logs/runbooks/MCP output may contain attacker text. They are labeled untrusted evidence and cannot grant capabilities.

Slack approval references an incident/action; backend policy and user authorization are revalidated at execution.

Supabase public-demo tables use RLS and minimum grants. Hugging Face has no operational credentials.

Bound incidents, tool calls, log bytes, trace count, model tokens, provider dollars, wall time, and remediation attempts.
