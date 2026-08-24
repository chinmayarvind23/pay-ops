# Quality Gates

Python: Ruff, Pyright strict, pytest, coverage, Hypothesis, mutation testing on critical deterministic logic.

TypeScript: strict TypeScript, Biome, unit tests, Playwright, screenshot tests.

Defaults:

```text
cyclomatic complexity <= 10
functions generally <= 50 logical lines
>= 85% deterministic-core coverage
>= 95% policy/auth/action/eval-metric coverage
```

Mutation-test policy, authorization, idempotency, Recall@k, attribution math, and attack-runner logic.

AI gates cover root-cause ranking, evidence attribution, path correctness, prompt injection, misleading telemetry, and missing evidence.

Security gates include secret/dependency/container/IaC scans plus the 120-case unauthorized-action suite.
