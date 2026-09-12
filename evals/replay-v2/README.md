# HTTP observations

This input corpus extends the recorded observations with executed HTTP traffic. It preserves frozen source facts and adds processor-specific rate-limit responses and webhook conflict responses with corresponding successful controls.

Source lineage binds each projected packet to its original receipt. The model receives observed endpoint role, response status and payment-slice fields. Scenario identifiers, expected answers, injector configuration and local source paths are excluded.

From the repository root:

```bash
uv run python scripts/run_free_replay.py --support-gated --corpus evals/replay-v2 --output /absolute/new/output-directory
```

Keep generated output outside the source repository.
