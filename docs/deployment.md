# Infrastructure setup

Start the synthetic payment environment using the [local Kubernetes instructions](../infra/kubernetes/local/README.md), then configure the [operational host](commands.md). The operator supplies the kubeconfig, runtime directory, model profile and scoped data credentials.

The [GCP Terraform configuration](../infra/terraform/gcp/README.md) defines a cluster, networking, private object storage and messaging resources. Infrastructure provisioning is an explicit operator action. Keep evidence readers, remediation executors, scenario controllers and service identities separate.

Integration setup is documented for [GCS](gcs-archive.md), [Pub/Sub](pubsub-worker.md), [Google observability](cloud-observability.md) and [GraphQL and Slack](graphql-and-slack.md). Configure only the integrations your deployment needs.
