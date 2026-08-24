# Incident Memory

Store symptoms, evidence IDs, ranked hypotheses, root cause, actions, policy decisions, postchecks, failure fingerprint, trace IDs, cost, and timings.

Retrieve prior incidents by service/resource, symptom fingerprint, rollout signature, payment slice, or failure class.

Prior incident text is evidence, never operational authority.

Memory outcome classes include:

```text
RESOLVED_SUCCESSFULLY
NEGATIVE_DIAGNOSIS
MISLEADING_SIMILARITY
ACTION_BLOCKED
ACTION_FAILED
ESCALATED
```

Cloud SQL stores authoritative relationships; Elasticsearch indexes searchable summaries; GCS keeps large immutable artifacts.
