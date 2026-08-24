# Threat Model

Assets include Kubernetes authority, payment availability, operational telemetry, cloud credentials, deployment authority, incident evidence, Slack approvals, and benchmark integrity.

Threats:

- confused deputy,
- prompt injection inside logs/runbooks,
- malicious MCP/tool output,
- unauthorized remediation,
- stale approval,
- benchmark manipulation,
- public-demo pivot into operational systems.

Controls:

```text
identity
capability
typed schema
policy
approval
executor validation
audit
```

Fail closed on auth/policy uncertainty, changed resource revision, invalid action schema, or action outside the sandbox.
