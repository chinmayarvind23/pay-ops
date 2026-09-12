# Recorded-observation development replay

This dataset contains mechanically selected observations from **24 real local
Kubernetes fault reproductions**. It is a curated development set, not production
incident data or a held-out benchmark. Case IDs and source paths are scorer metadata;
the model receives only the complete cause vocabulary and opaque cited observations.

`corpus.json` binds 28 projected fact files by SHA-256. `source-lineage.json` records
the original private artifact paths and hashes for operator audit. The public facts
exclude environment configuration, experiment plans and credentials. They include
synthetic resource names, timestamps and diagnostic observations. Selection remains
curator-dependent; not every packet contains enough evidence to distinguish all causes.
`export-verification.json` confirms identical model inputs before and after export.

## Run for zero provider charges

Start the pinned Qwen3 1.7B server from [free inference setup](../../docs/free-inference.md),
then run from the repository root:

```bash
uv run python scripts/run_free_replay.py --output /absolute/new/benchmark-directory
```

Use `--prepare-only` with a separate new directory to inspect all 24 prompts without
model requests. Existing output directories are rejected. The script reserves one
count/generation pair per case in SQL, retains raw local responses and validated
receipts, freezes all predictions, then computes Recall@1/3 over all 24 cases.
Missing or invalid answers remain misses. It has no paid fallback.

The diagnosis-only adapter asks for compact cause/citation pairs and translates
them into the shared decision contract. Confidence is explicitly unestimated and
stored as zero; it is not a model probability. This replay does not exercise live
tool selection, initial collection, human approvals or full-agent timing.

The measured Qwen3 baseline produced **9/24 Recall@1 and 16/24 Recall@3**, with
13.016-second median and 24.344-second p95 replay-call time on a shared CPU host.
All 24 calls retained usage: 26,048 input and 1,867 output tokens, with zero provider
charges. All 72 citation references resolve; semantic attribution accuracy is not
established. Raw observations, prompts and model versions matter more than reproducing
the same wall-clock latency on a different machine.

See [full results and limitations](../../docs/model-replay-results.md).
