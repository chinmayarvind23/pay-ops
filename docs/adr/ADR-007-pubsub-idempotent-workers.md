# ADR-007: Pub/Sub plus idempotent workers

Incident triggers are asynchronous and workers tolerate redelivery. State-changing work is persisted before acknowledgment.
