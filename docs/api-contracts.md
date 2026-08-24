# API Contracts

REST commands:

```text
POST /api/incidents
POST /api/incidents/{id}/investigate
POST /api/incidents/{id}/approvals/{approval_id}
POST /api/incidents/{id}/cancel
POST /api/benchmarks
GET  /api/health
```

GraphQL supports nested read-oriented incident/evidence/eval exploration. It is protected by auth, resolver authorization, pagination, and query depth/complexity limits.

Slack sends incident/approval references, not infrastructure command text.

Structured errors include code, message, retryable flag, and incident ID.
