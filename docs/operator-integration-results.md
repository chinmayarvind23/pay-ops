# Local operator integration

The September 12 UTC healthy-control run used the real `kind-payops-dev` cluster,
loopback Prometheus, an expiring OS-account grant, SQLite persistence and the pinned
local Qwen2.5 3B adapter. No fault was injected and no paid provider was called.

- Initial collection retained 25 verified observations with zero collection failures.
- The full prompt contained 10,935 tokens against the configured 4,096-token ceiling.
  The adapter rejected it before generation. The graph recorded `ERROR` and ended
  with `DEPENDENCY_UNAVAILABLE`; this is not a successful diagnosis.
- A fresh host resumed the completed incident. Its durable budget ledger was equal
  before and after, and all hashed runtime files were unchanged. Completed publication
  independently reconstructs receipts using dispatch guards.
- The reservation retained one model operation, two potential provider requests and
  zero provider cost. Reservations are not actual request or token usage measurements.

Evidence is retained outside the repository in
`resources/pay_ops/evidence/operator-live-release-v2`: start/resume state, budget,
verification and a separate tokenizer diagnostic. The preceding v1 setup failed
because its empty knowledge bundle named a nonexistent directory; it is retained.

This validates collection, fail-closed token enforcement and completed recovery.
It does not validate model-selected reads, Elasticsearch retrieval, a successful full
model investigation or full-agent latency. Those components have separate tests;
the [24-case compact replay](model-replay-results.md) measures a smaller diagnosis path.
The next engineering change is a model-aware context budget with explicit omissions,
followed by a fresh live run. Increasing the claimed context limit without qualifying
the runtime and latency would not resolve the integration requirement.
