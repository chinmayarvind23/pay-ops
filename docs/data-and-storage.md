# Data and storage

SQL owns incident records, reports, work reservations and action claims. The local host uses SQLite for checkpoints. PostgreSQL supports the application data path through SQLAlchemy and scoped connections.

Redis holds derived cache entries. Elasticsearch retrieves allowed runbooks and prior incident evidence. Their contents are checked against the original stored artifacts before use in an investigation.

The local evidence store publishes content-addressed artifacts and verifies their metadata and hashes. The [GCS archive](gcs-archive.md) supports create-once remote storage and verified local restoration. The [Pub/Sub worker](pubsub-worker.md) delivers references to incidents already present in SQL.

Credential paths, TLS certificates and application identities are configured by the operator. See [data service setup](../infra/kubernetes/data/README.md).
