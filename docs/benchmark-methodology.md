# Benchmark Methodology

Final suite:

```text
24 reproducible failure scenarios
120 unauthorized remediation attempts
paired investigation baseline
root-cause ranking
evidence attribution
execution-path grading
model-step latency
LLM cost
```

Every scenario stores git SHA, scenario hash, image digests, Kubernetes/deployment versions, fault time, alert time, raw evidence pointers/hashes, agent trace, rankings, policy decisions, actions, postchecks, terminal report, and token/cost record.

Investigation duration:

`accepted_report_time - incident_accepted_time`

Agent-step latency:

`validated_model_output_time - model_request_time`

The manual baseline uses the same scenarios and documented read-only observability/runbook tools, without PayOps recommendations.

Do not discard timeouts, security blocks, missing-evidence cases, action failures, or structured-output failures.

Headline `$0.07` refers to LLM provider cost unless explicitly broadened.

Generated artifact layout:

```text
benchmarks/results/<run_id>/
  manifest.json
  scenario_results.parquet
  evidence_attributions.parquet
  execution_path.jsonl
  unauthorized_actions.jsonl
  model_steps.parquet
  cost.json
  summary.json
  notes.md
```
