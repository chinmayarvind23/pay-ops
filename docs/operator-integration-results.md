# Local operator integration

## Context-budget correction

The local v2 reasoning profile bounds context before prompting and omits duplicate
summaries/storage metadata. A subsequent Qwen3 run (`operator-live-release-v5`)
measured 1,381 and 1,391 input tokens, with 20.422 and 20.000 seconds of generation
time. One response was invalid; the second passed validation and requested two
retrieval tools. This proves generation can now fit the configured bounds, not
successful diagnosis. The two-turn run exhausted its allowance. Its retrieval reads
failed because the verification script supplied the wrong password for the fixed
reader account; a later script uses the existing scoped reader credential.

Four-turn controls v6 and v7 both stopped after two invalid model responses. V7 used
the corrected reader credential but never reached a model-selected read. Every
completed resume retained an equal SQL budget ledger and unchanged hashed runtime
files. Failed runs remain part of the evidence, including intermediate timeouts.

The change passes 48 focused loop tests with 100% statement coverage, Ruff and
strict Pyright. Before the final prompt projection, 123 combined loop/operator/
adapter tests passed. Omitted sources remain verified, omitted IDs cannot be cited,
and the complete included context remains stored. The local binding version changes;
old journals need their original code/configuration. See [reasoning](reasoning.md).

The remaining integration limitation is model decision validity and diagnostic
quality. No successful full-agent diagnosis or new Recall score is claimed.

## Original oversized-prompt control

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
This failure motivated the context-budget correction above. It was preserved rather
than reclassified as a successful investigation.
