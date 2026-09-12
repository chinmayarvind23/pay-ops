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

## Trusted local operator investigation

The operator host connects the native investigation graph, durable reasoning loop,
local Qwen or OpenAI adapter and all six read tools. It uses local SQLite and the current OS account
plus an expiring local grant file. This is a local operator command, not public API
authentication. The cluster, Prometheus and Elasticsearch must already be available;
the host does not start infrastructure.

Read the native account and prepare an operator-controlled config outside the repository:

```bash
uv run python -m payops.operator_host identity
```

All paths must be absolute and resolve locally. This example names credentials explicitly;
replace the paths and credential reference names with your own. No default environment
variable, dotenv file or keychain is searched.

```json
{
  "runtime": "C:/payops-operator/runtime",
  "kubeconfig": "C:/payops-operator/kubeconfig",
  "grant_file": "C:/payops-operator/grant.json",
  "release_labels": "C:/path/to/pay_ops/evals/golden/release-v2.json",
  "knowledge_bundle": "C:/payops-operator/knowledge.json",
  "elastic_ca": "C:/payops-operator/ca.crt",
  "provider_key": {"environment": "EXPLICIT_PROVIDER_KEY"},
  "elastic_key": {"file": "C:/payops-operator/elastic-password"},
  "prometheus_port": 19090,
  "elastic_port": 29200
}
```

The grant JSON requires `account` equal to the identity command output, boolean `enabled`,
and timezone-aware `issued_at`/`expires_at` timestamps no more than eight hours apart.
Its namespace and role are fixed to `payops-sandbox` and `responder`. The knowledge bundle
contains `artifacts_root` and an `items` array of original RUNBOOK/MEMORY EvidenceItems.
It may be empty; each Elasticsearch hit still needs its original artifact for verification.
The host accepts at most 16 originals and 1 MiB combined source bytes.

```bash
uv run python -m payops.operator_host plan --config C:/payops-operator/config.json
uv run python -m payops.operator_host start --config C:/payops-operator/config.json --incident C:/payops-operator/incident.json
uv run python -m payops.operator_host resume --config C:/payops-operator/config.json --incident-id operator-demo
```

An incident file can contain
`{"incident_id":"operator-demo","request":{"title":"Investigate payment failures"}}`.
`plan` validates configuration and prints allowances without loading credentials or sending
requests. `start` and an unfinished `resume` can issue paid provider requests and operational
reads. Defaults reserve four model turns, eight count/generation requests and 26/39 total
logical/backend reads. The generation-token reservation is $0.084864; count-endpoint fees
remain unestablished, so it is not an all-fees spend cap.

Completed resume rechecks current authority, source artifacts and the complete SQL journal
without new provider/backend requests. The CLI prints a verified summary; full reports remain
in native graph checkpoints. Runnable integration is fixture-tested; live provider diagnosis,
latency and billing remain unmeasured.

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
runner. The full 24-case provider benchmark remains open; the operator command above is
implemented and validated with synthetic transport fixtures.

## Other setup paths

Use [free local inference](free-inference.md) for the zero-provider-charge operator
profile. The OpenAI configuration above is optional and may incur charges.
Use the exact [local Kubernetes setup](../infra/kubernetes/local/README.md) and
[web app commands](../apps/web/README.md), rather than creating an unconfigured cluster.
[GCP Terraform](../infra/terraform/gcp/README.md) supports local validation and mock
plans; [AWS instructions](../infra/terraform/aws-lightsail/README.md) are optional.
