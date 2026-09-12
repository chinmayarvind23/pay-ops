# Investigation telemetry

Structured model receipts retain outcomes, validated token usage, provider timing
and hashes. The immutable artifact store and SQL journal bind each completed call
to its receipt. Unknown usage remains unknown. OpenTelemetry instruments model and
synthetic service calls; Prometheus captures service and payment metrics.

## LangSmith

Export a saved model receipt to a local review file:

```bash
uv run python -m payops.telemetry.export --artifacts /path/to/artifacts --receipt SHA256 --output /path/to/new-summary.json
```

The exporter verifies source bytes and projects only receipt/run hashes, status,
provider seconds and token counts. It excludes prompts, evidence, hypotheses,
incident names, operator identities and provider errors. It refuses to overwrite
an existing review file. Hashes allow correlation; they do not guarantee anonymity.

An operator with a LangSmith account can explicitly add
`--langsmith-key-file /private/path/key --project payops --started-at ISO_TIME
--ended-at ISO_TIME`. Supply actual journal timestamps for that invocation. The SDK
sends empty inputs/outputs with reviewed scalar metadata and a stable receipt-based
ID. Repeated exports address the same ID. Configure account retention and access controls for the exported records.

Export is separate from investigation: a hosted outage cannot alter approval policy
or trigger another model invocation. Tests cover SDK arguments, the content allowlist,
checksum rejection, unknown usage and timestamp validation, not hosted ingestion.
The implementation follows [explicit client configuration](https://docs.langchain.com/langsmith/trace-without-env-vars)
and [input/output masking](https://docs.langchain.com/langsmith/mask-inputs-outputs).
