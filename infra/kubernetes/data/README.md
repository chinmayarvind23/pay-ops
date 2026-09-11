# Local data services

This independent kustomization deploys one PostgreSQL, Redis and Elasticsearch instance in `payops-data`. It requires three separately provisioned Secrets: `payops-postgres-credentials`, `payops-redis-credentials` and `payops-elasticsearch-credentials`. Secret values and certificates are intentionally absent from the repository.

Use an explicit kubeconfig and context for every command:

```powershell
kubectl --kubeconfig <project-kubeconfig> --context kind-payops-dev apply -f infra/kubernetes/data/namespace.yaml
# Provision service-specific TLS/password Secrets using trusted local setup.
kubectl --kubeconfig <project-kubeconfig> --context kind-payops-dev apply -k infra/kubernetes/data
```

Provision Elasticsearch's restricted probe user before expecting its readiness probe to pass. Applications need separate scoped identities; the PostgreSQL and Elasticsearch bootstrap superusers are not application credentials. All client connections require TLS with CA verification.

The services expose ClusterIP ports 5432, 6379 and 9200 only. Combined memory limits are 2.25 GiB; requested PVC storage totals 5 GiB. Local-path PVC capacity is not a filesystem quota. Small synthetic datasets, client query limits and explicit retention remain necessary. The included NetworkPolicy does not enforce isolation under default kind networking.

This is a local integration environment with single replicas. It provides no high availability, production sizing, cloud deployment or benchmark claims. Configuration changes require an explicit coordinated rollout because ConfigMap names remain stable.
