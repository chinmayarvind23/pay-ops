# Optional Pub/Sub incident worker

`PubSubIncidentWorker` consumes a fixed subscription and starts or resumes incidents
already present in the authenticated API's SQL store. The message is only:

```json
{"incident_id":"existing-incident-id"}
```

Unknown fields, action requests and envelopes larger than 1 KiB are rejected.
The worker gets current responder authority from trusted host configuration before
investigating and again before committing the report. A message cannot supply an
identity, approval, model configuration, namespace, tool or remediation instruction.

Wire it to the same `IncidentStore` as the protected API and an existing configured
`OperatorHost`. That host retains the read registry, model budgets and checkpoint
authority described in [operator setup](operator-integration-results.md). For example:

```python
from google.cloud.pubsub_v1 import SubscriberClient
from payops.pubsub_worker import PubSubIncidentWorker

# host and store are the existing trusted operational host and SQL incident store.
worker = PubSubIncidentWorker(store, host, host.authority.principal, mode="local_kind")
client = SubscriberClient()
future = worker.subscribe(client, "projects/your-project/subscriptions/payops-incidents")
```

The operator owns the returned streaming handle and client. On shutdown, cancel the
future and wait for its callbacks to drain before closing the client, host or SQL
store. Grant the subscriber identity access only to this subscription and restrict
topic publishing to trusted alert ingress. Configure a dead-letter topic and retry
backoff: invalid messages and revoked authority receive a negative acknowledgement,
so they must not loop indefinitely without operator review.

Flow control permits one outstanding message and 64 KiB in flight, with a ten-minute
lease-management limit. These are SDK delivery limits, not a guarantee that Python
work is cancelled at the lease deadline. The host's existing budgets still bound
investigation work. Deploy one worker against the local checkpoint store; distributed
multi-host execution requires a different coordination backend.

A report is committed to SQL before acknowledgement. Lost acknowledgements can
cause redelivery; a stored report is reused after fresh authorization. A crash
before SQL publication can resume the durable graph without repeating completed
observations. There is no queue-based action execution and no exactly-once delivery
claim. Conditional remediation continues through the separate authenticated broker.

Verification uses actual SQL persistence and LangGraph checkpoints, including a
paused investigation, redelivery, revocation, bad envelopes and report-mode mismatch.
The subscription binding is SDK-contract tested. No hosted subscription was created
or consumed, and no paid resources were provisioned.

Google documents the [subscriber lifecycle and callback acknowledgement contract](https://docs.cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.client.Client).
