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

## Operational deterministic investigation

The local cluster and Prometheus must already be running. Use an external directory
for checkpoints and evidence; the CLI does not start infrastructure.

```bash
uv run python -m payops.orchestrator.run --runtime ../resources/pay_ops/runtime/investigation --kubeconfig ../resources/pay_ops/runtime/kubeconfig
```

Add `--payment-windows` for paired payment snapshots or `--pause-before-ranking`
to persist collection before ranking. Resume with the same runtime, collection profile
and `--resume INCIDENT_ID`. These commands use deterministic ranking.

## Local fault and development evaluation

These operator commands inject faults into the synthetic cluster and verify cleanup.
Keep the cluster free of other traffic or fault runs during measurement. Each output
directory must be new and outside the code repository.

```bash
uv run python -m payops.scenarios.run --scenario ROLLOUT-01 --kubeconfig ../resources/pay_ops/runtime/kubeconfig --output ../resources/pay_ops/evidence/manual-rollout
uv run python -m payops.evaluation.run --kubeconfig ../resources/pay_ops/runtime/kubeconfig --output ../resources/pay_ops/evidence/manual-initial-suite
```

The evaluation entry point runs the initial four-case deterministic suite. SCHED-01
and TELEM-03 require their specialized lifecycle classes and reject the generic fault
runner. The full 24-case provider benchmark and provider operator command remain open.

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

```

Terraform commands are documented once the modules are runnable.
