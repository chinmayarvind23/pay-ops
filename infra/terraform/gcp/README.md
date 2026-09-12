# Optional GCP infrastructure

This configuration defines a GKE Autopilot cluster, dedicated network, restricted API access, node identity, private versioned object storage and a Pub/Sub subscription. Provisioning is controlled by the operator configuration.

## Validate without deploying

```bash
terraform init -backend=false
terraform fmt -check
terraform validate
```

These validate configuration/provider schemas, not cloud behavior or permissions.
The example keeps `enabled = false` and uses documentation placeholders.

## Optional operator deployment

1. Review current prices; use your own existing billed project and Application
   Default Credentials. Keep credentials and Terraform state outside Git.
2. Copy the example outside the repository, supply your project, region and actual
   public IPv4 `/32`, and review VPC ranges. Enabling creates billable resources.
3. Run `terraform plan -var-file=/path/to/operator.tfvars -out=/path/to/reviewed.plan`,
   review every resource and IAM binding, then apply that exact plan yourself.
4. Obtain credentials with `gcloud container clusters get-credentials payops
   --region YOUR_REGION --project YOUR_PROJECT`. Review namespace-specific RBAC in
   `infra/kubernetes`; do not grant the investigation reader cluster-admin.
5. Push immutable images to your registry. Configure scoped Workload Identities and
   TLS data endpoints separately, then configure the application adapters using the operator documentation.

Deletion protection and nonempty-bucket protection prevent accidental teardown. For
intentional cleanup, preserve evidence, review and apply a change disabling cluster
deletion protection, then review destruction. Handle bucket objects explicitly.

## GKE MCP

The read adapter pins upstream source `a97e99d852c70cf9eb5b85ddf6504a8f0ee8d9d4`
and validates exact resource/event/log tool schemas. Its bounded stdio transport is
operator-configured. Upstream mutation tools never become model capabilities.
Validate GKE authorization, networking and inventory bindings during deployment.

References: [GKE Terraform resource](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_cluster),
[Workload Identity](https://cloud.google.com/kubernetes-engine/docs/concepts/workload-identity).
