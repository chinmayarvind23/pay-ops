# Local operator integration

## Completed bounded control

`operator-live-release-v11` completed the real local workflow with
`reasoning_stop=FINISHED`, terminal `EVIDENCE_INSUFFICIENT`, and zero root-cause
hypotheses. Its four original model responses passed canonical validation. It
collected 25 initial observations and two additional payment snapshots; two pod-event
reads returned empty, valid results. A further requested read batch was denied before
dispatch by the original budget. The remaining model turn finished from existing
evidence. A fresh host resumed with equal SQL ledger contents and no changed hashed
runtime files.

The four model calls used 1,434, 1,512, 1,586 and 1,605 input tokens; output counts
were 114, 121, 133 and 60. Generation times were 18.157, 20.469, 24.047 and 18.093
seconds. These are individual CPU-local healthy-control observations, not a p95
latency benchmark or incident-time improvement. Provider charges were zero.

Original responses, model/read receipts, receipt hashes, source snapshots and
restart verification are retained outside the repository. The model repeated reads
and made an unsupported readiness assertion in an intermediate summary; no diagnosis
was published. This validates bounded live execution and safe insufficient-evidence
completion, not successful root-cause diagnosis. The compact 24-case diagnosis
benchmark remains unchanged.

Separate scoped Elasticsearch controls in v8 returned valid empty results for both
runbook and incident search in 2.140 and 0.016 seconds. The preceding deadline failure
is retained. This checks live authenticated search, not successful retrieval of a
nonempty source document. See the [local decision contract](local-model-contract.md).

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

The bounded control above supersedes the decision-validity failure for that run.
Diagnostic quality and repeated-read efficiency remain limitations. No successful
root-cause diagnosis or new Recall score is claimed.

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
