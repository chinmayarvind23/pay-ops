# Commands

## Verified local walking skeleton

```bash
uv sync --frozen
uv run payops serve
curl http://127.0.0.1:8000/api/health
uv run pytest
uv run ruff check .
uv run pyright
```

Create an incident with `POST /api/incidents`, JSON `{"title":"Payment failures"}`,
then `POST /api/incidents/{incident_id}/investigate`. The current result is explicitly mock.

## Planned commands (not implemented yet)

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
