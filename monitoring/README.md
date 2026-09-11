# Local payment metrics

`infra/kubernetes/observability` deploys Prometheus 3.14.0 in the dedicated `payops-observability` namespace. It scrapes `/metrics` from the five synthetic services every five seconds, with two-second scrape timeouts. Static service DNS targets require no Kubernetes API token or discovery permissions.

From the repo root, after the local sandbox is running:

```powershell
$payopsKubeconfig = '../resources/pay_ops/runtime/kubeconfig'
kubectl --kubeconfig $payopsKubeconfig --context kind-payops-dev apply -k infra/kubernetes/observability
kubectl --kubeconfig $payopsKubeconfig --context kind-payops-dev -n payops-observability rollout status deployment/prometheus --timeout=180s
kubectl --kubeconfig $payopsKubeconfig --context kind-payops-dev -n payops-observability port-forward service/prometheus 19090:9090 --address 127.0.0.1
```

The local query endpoint is `http://127.0.0.1:19090`. The in-cluster endpoint is `http://prometheus.payops-observability.svc.cluster.local:9090`. Readiness is `/-/ready`, and queries use `/api/v1/query` or `/api/v1/query_range`.

Useful queries include `up{job="payops-sandbox"}`, `payment_requests_total{service="payments-api"}` and `payment_authorization_latency_seconds_count{service="payments-api"}`. Each series carries a static `service` label; counts from the five roles must not be summed as though each role were a separate user payment. Request and trace IDs remain in logs, not metric labels.

The collector runs without root, Linux capabilities, Kubernetes tokens or public service ports. A 1 GiB PVC stores local samples; retention is bounded to 24 hours and 256 MB, with 512 MiB container memory. These are local experiment defaults. The default kind storage is tied to its node and does not establish durable cloud storage or high availability.

Only payment-service metrics are scraped in this configuration. Kubelet/cAdvisor and scheduler metrics require separate authenticated collection; no cluster-wide discovery or admin grant is hidden here. Grafana, Managed Prometheus and Cloud Monitoring remain separate integration work.

Each service currently has one replica. Before evaluating HPA or multi-replica capacity,
replace these ClusterIP targets with per-pod targets: otherwise a scrape can switch
between backends and invalidate counter deltas.
