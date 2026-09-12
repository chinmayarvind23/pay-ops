# Telemetry

`export.py` verifies model receipts and projects scalar telemetry for local review
or explicit LangSmith SDK export. It excludes raw evidence and prompts. OTel spans
live in the model runtime and sandbox tracing modules; Prometheus instrumentation
lives in sandbox telemetry. See [configuration](../../../docs/telemetry.md).
