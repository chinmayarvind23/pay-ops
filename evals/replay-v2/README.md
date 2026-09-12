# Expanded HTTP evidence

This corpus retains all 24 cases, frozen cause labels and the 28 original observation files byte-for-byte. It adds two observed HTTP packets: processor-specific 429 responses with matched successful controls, and a webhook 409 idempotency conflict with successful controls.

Each addition is projected from an executed traffic receipt whose SHA-256 matches its qualified activation journal. The source lineage records exact selected attempt indices and projection fields. No injector configuration, case identifier, expected answer, request ID or source path is supplied to the model.

The projection retains the first two attempts per `(http_status, processor)` group and selects processor, region, payment method, HTTP status, observed latency and conflict status. Endpoint role comes from the executed receipt. The corpus is curated development data, not a held-out production dataset. Its semantic review informed predicate development.

```bash
uv run python scripts/run_free_replay.py --support-gated --corpus evals/replay-v2 --output /new/run
uv run python scripts/score_replay_attribution.py --corpus evals/replay-v2/corpus.json --results evals/results/qwen3-http-supported/results.json --output /new/attribution.json
```

The recorded run has 22/24 root-cause hits, 23 diagnoses, one refusal, and 23/23 support links matching the development reference. The broader startup diagnosis still misses the configuration-specific label; the protocol-mismatch case still lacks sufficient diagnostic context.
