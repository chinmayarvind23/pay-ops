# Replay attribution review

The published compact replay outputs have a source-bound support-link review:

| Treatment | Reference matches | Unresolved links | Cases with citations |
| --- | --- | --- | --- |
| Qwen3 1.7B compact | 15/72 (20.8%) | 0/72 | 24/24 |
| Qwen2.5 3B definitions | 9/55 (16.4%) | 0/55 | 23/24 |

This is a **post-hoc Codex development review**, performed after predictions were
visible. It is not independent human adjudication, a blinded evaluation or evidence
of 96.4% attribution accuracy. The two treatments also changed prompt/cache settings.
These values are retained for diagnosis and are omitted from résumé achievements.

The reviewer annotated all 28 exact public source excerpts with supported cause
codes and a rationale. Credit requires direct, current evidence for the named
mechanism or payment-slice contrast. Mere compatibility, stale distractors and
missing localization receive no credit. A link can support a cause without proving
it uniquely. Cases with no sufficiently supported cause retain an empty annotation;
their predictions remain in the denominator.

Examples explain why citation-ID validity differs from this reference score:

- A zero-replica processor Deployment supports processor unavailability, not scheduler
  memory saturation merely because a memory request is also present.
- A current Redis failure does not make an explicitly archived PostgreSQL error
  evidence for the current incident.
- A generic idempotency counter does not localize a conflict to webhook processing.
- An HTTP 422 translated to a gateway 502 lacks the schema/version controls needed
  to distinguish protocol incompatibility from another validation error.
- The configuration-failure excerpt supports startup failure but does not contain
  a configuration validation error. Its attribution reference differs from its
  scenario's primary cause label for that reason.

The source corpus and original predictions are unchanged. Review labels are in
[`attribution-review-v1.json`](../evals/replay-v1/attribution-review-v1.json), separately
from model inputs and primary Recall labels. Every source hash is reverified before
scoring, including uncited sources. Case IDs scope repeated evidence IDs such as
`e1`; cross-case substitution cannot earn credit. Repeated identical links are
deduplicated, invalid links stay in the denominator, and zero citations yield
undefined accuracy. All 24 explicit outcome rows are required, including failures.

The reference covers supporting links. Neither evaluated compact treatment emits
refuting links; this review does not establish refutation accuracy. Unlisted relations
receive no credit. Output coverage is reported separately so abstention cannot
silently improve apparent performance.

Reproduce without inference or credentials:

```bash
uv run python scripts/score_replay_attribution.py \
  --results evals/results/qwen3-1.7b-compact/results.json \
  --output attribution-report.json
```

The output path must be new. The report binds corpus, review, result and scorer
hashes. Both treatment reports are committed beside their original results.

The next evaluation improvement is independent review of these annotations and
a fresh, held-out corpus with complete discriminating evidence. Repeatedly editing
these excerpts or labels to improve the existing score would not establish generalization.
