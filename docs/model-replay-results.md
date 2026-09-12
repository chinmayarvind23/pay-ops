# Local model replay results

The first complete model study used 24 curated packets from the qualified local
Kubernetes experiments. Every packet was frozen before prediction; all cases stayed
in the denominator. This measures diagnosis from selected recorded observations,
not a live autonomous investigation or production generalization.

| Treatment | Recall@1 | Recall@3 | Median replay call | p95 replay call |
| --- | ---: | ---: | ---: | ---: |
| Qwen3 1.7B Q4_K_M, compact output | 9/24 (37.5%) | 16/24 (66.7%) | 13.016 s | 24.344 s |
| Qwen2.5 3B Q4_K_M, explicit category definitions and prefix caching | 5/24 (20.8%) | 9/24 (37.5%) | 14.351 s | 26.453 s |

The treatments differ in model, template and prompt, so the table does not isolate
model size as the cause of the difference. Both used the same selected observations.
Several earlier diagnostic runs stopped after invalid output or repeated timeouts;
their outcomes are retained, and no full-run score is claimed for them.

The Qwen3 run measured 26,048 input and 1,867 output tokens across 24 valid responses.
The Qwen2.5 runtime retained usage for 23 cases: 26,395 input and 850 output tokens.
One response repeated the same cause three times and failed validation; that case
remains a miss and is excluded from that runtime usage subtotal. Both runs incurred
zero provider charges. Electricity and existing hardware costs are unmeasured.

The Qwen3 run has 72/72 resolvable citation references. That checks citation identity
and retained source integrity, not whether each citation supports the proposed cause.
The model often proposed causes unsupported by the observations. No 96.4% semantic
attribution result or human investigation speedup is claimed.

These accuracy and latency values are below the original targets and are omitted
from the résumé. The stronger verified results remain 24 local fault reproductions,
120/120 forbidden fixture capabilities denied, durable replay and real conditional
remediation controls. The study demonstrates why schema validity alone cannot
justify an operational diagnosis.

## Recompute and inspect

The [public replay corpus and command](../evals/replay-v1/README.md) preserve the
selected facts and source hashes. Frozen outputs are in `evals/results/`.
Complete operator artifacts live outside the repository:

- `resources/pay_ops/evidence/model-benchmark-v4`: Qwen3 compact baseline.
- `resources/pay_ops/evidence/model-benchmark-v6`: Qwen2.5 definition/caching treatment.
- `resources/pay_ops/evidence/model-replay-v2`: selected observations and source lineage.
- `resources/pay_ops/runtime/verify_model_replay.py`: independent recomputation.

Verification reopens source bytes, repeats mechanical projection, checks prompt and
receipt hashes, joins SQL reservations/completions, and recomputes rankings and token
totals. Future improvements need a separately versioned run and held-out cases;
changing these results or presenting development tuning as a blind evaluation would
invalidate the comparison.
