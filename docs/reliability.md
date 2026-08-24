# Reliability

Pub/Sub workers tolerate redelivery:

```text
receive
-> idempotency key
-> load durable state
-> execute next legal transition
-> persist
-> acknowledge
```

Retry transient rate limits, 502/503, network timeouts, and temporary telemetry failures with exponential backoff and jitter.

Do not retry policy denial, auth failure, invalid action, or payment mutation.

Circuit breakers may protect model provider, Elasticsearch, and external processor calls.

Dependency degradation:

- Elasticsearch down: direct telemetry only
- Redis down: bypass cache
- LangSmith down: continue with OTel/structured logs
- model down: deterministic baseline and escalate
- Cloud SQL down: no undurable state-changing progress acknowledged
- GKE MCP down: use alternative approved collectors or mark missing evidence

Per-incident budgets cap tools, model calls, wall time, provider dollars, and remediation attempts.
