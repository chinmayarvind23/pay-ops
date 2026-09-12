# Local model replay results

The first complete model study used 24 curated packets from the qualified local
Kubernetes experiments. Every packet was frozen before prediction; all cases stayed
in the denominator. This measures diagnosis from selected recorded observations,
not a live autonomous investigation or production generalization.

| Treatment | Recall@1 | Recall@3 | Median replay call | p95 replay call |
| --- | ---: | ---: | ---: | ---: |
| Qwen3 1.7B Q4_K_M, compact output | 9/24 (37.5%) | 16/24 (66.7%) | 13.016 s | 24.344 s |
| Qwen2.5 3B Q4_K_M, explicit category definitions and prefix caching | 5/24 (20.8%) | 9/24 (37.5%) | 14.351 s | 26.453 s |
| Qwen3 1.7B Q4_K_M, predicate-assisted single diagnosis | 20/24 (83.3%) | 20/24 (83.3%) | 8.547 s | 17.390 s |
| Qwen3, predicate-assisted with expanded HTTP evidence | 22/24 (91.7%) | 22/24 (91.7%) | 8.414 s | 15.578 s |

The latest [expanded corpus](../evals/replay-v2/README.md) adds two previously omitted
HTTP observations from retained, checksum-verified executed traffic receipts. It preserves
all original facts and labels. Processor-specific HTTP 429 controls and an explicit
webhook HTTP 409 conflict resolve two earlier abstentions. This treatment returned 23
diagnoses and one refusal; 22 matched the frozen labels, and 23/23 support links matched
its development reference. The reference informed predicate design and is not independent
semantic adjudication. Usage was 20,697 input and 779 output tokens across 24 cases,
with zero provider charges. Results are in `evals/results/qwen3-http-supported` and raw
evidence in `resources/pay_ops/evidence/supported-model-v2`.

The predicate-assisted treatment uses [source-specific diagnostic checks](diagnostic-support.md)
to restrict model output to a supported candidate and its evidence ID. It generated 21
diagnoses and three refusals; 20 diagnoses matched the frozen cause labels. All 21
published links matched the existing development attribution reference. That reference
informed predicate development, so this is not an independent or held-out semantic
accuracy result. The predicates generally leave one candidate: Python performs most
of the classification in this treatment, and the model selects from its checked output.

The new run measured 20,263 input and 736 output tokens across all 24 cases, with zero
provider charges. Its 8.547-second median and 17.390-second p95 include local tokenization
and model calls, excluding live collection and human investigation. The comparison
used the same frozen corpus on a shared workstation, without repeated trials or
confidence intervals. It meets the original Recall@1 value in development replay;
Recall@3 and p95 remain below target. Results and source bindings are in
`evals/results/qwen3-predicate-assisted`; raw receipts are in
`resources/pay_ops/evidence/supported-model-v1`.

The treatments differ in model, template, prompt and diagnostic predicates, so the table
does not isolate model size as the cause of the difference. The first three use the same
observations; the fourth adds two source-bound HTTP packets.
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

The original model-only accuracy and latency values remain below target. The résumé
now uses the expanded predicate-assisted 91.7% Recall with its development-replay scope,
alongside 24 fault reproductions and 120/120 forbidden fixture capabilities denied.
The study demonstrates why schema validity alone cannot justify a diagnosis.

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
