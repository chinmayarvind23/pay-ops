# Recorded observations

This directory contains frozen synthetic incident inputs. The corpus binds projected facts to source digests; lineage metadata identifies the retained original artifacts. Model input excludes scenario identifiers, expected answers, source paths and injector configuration.

Use the local inference configuration in [operator setup](../../docs/free-inference.md). From the repository root:

```bash
uv run python scripts/run_free_replay.py --output /absolute/new/output-directory
```

Use `--prepare-only` to inspect prepared input without model requests. Keep generated output outside the source repository. Existing output directories are rejected.
