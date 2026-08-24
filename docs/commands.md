# Commands

```bash
uv sync
uv run ruff check .
uv run pyright
uv run pytest

bun install
bun test
bun run build

docker compose up -d
kind create cluster --name payops

uv run python -m payops.scenarios.run --scenario ROLLOUT-01
uv run python -m payops.evaluation.run --suite release
uv run python -m payops.evaluation.unauthorized --config evals/attacks/unauthorized_remediations.yaml
```

Terraform commands are documented once the modules are runnable.
