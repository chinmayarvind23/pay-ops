# Payment Telemetry

Synthetic flow:

```text
payments-api
  +-> risk-sim
  +-> ledger-sim
  +-> processor-adapter -> AWS Lightsail processor-sim
  +-> webhook-sim
```

Metrics:

```text
payment_requests_total{status,processor,region,payment_method}
payment_authorization_latency_seconds{processor,region}
payment_declines_total{reason,processor}
payment_idempotency_conflicts_total
payment_queue_depth
payment_capture_failures_total{processor}
processor_requests_total{status}
processor_latency_seconds
```

Transaction/customer/request IDs must not become Prometheus labels.

Payment-slice scenarios test whether the agent notices a processor-, region-, or payment-method-specific degradation hidden by healthy global averages.
