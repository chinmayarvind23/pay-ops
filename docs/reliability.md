# Reliability

The investigation records work reservations and completed receipts before advancing its durable state. A repeated delivery loads those records and reuses completed observations. A reservation without a trustworthy completion receipt stops automatic redispatch.

The Pub/Sub worker commits a scoped incident report before acknowledgement. Failed processing receives a negative acknowledgement; configure subscription backoff and dead-letter handling. The local checkpoint directory belongs to a single operational host.

A timeout limits acceptance of a result. It does not establish that the underlying transport stopped. Active work retains its concurrency slot until the operation returns, and late output is excluded from the incident response.

Provider and retrieval failures remain explicit observations or terminal states. Model requests do not receive automatic retries or an implicit provider substitution. Current identity and artifact checks still apply when resuming completed work.

The action broker claims execution before effects. Uncertain execution stops another dispatch. Resource identity and version checks prevent an old approval from applying to replaced state.
