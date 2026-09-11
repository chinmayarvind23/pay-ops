# Local autoscaling metrics

Vendored from the Apache-2.0 licensed [Metrics Server v0.9.0 release](https://github.com/kubernetes-sigs/metrics-server/releases/tag/v0.9.0).
The upstream components.yaml SHA-256 is `1cec29a5267809306a2c6ec74a3e449abbb705b4a8beed0c8a1963910f72c79b`.
The only initial manifest change pins the upstream image to digest
`sha256:d9862115e7c7881280d3d75ca26bda8ffc0fc213315979575bf23ce9826205c0`.

This separate local prerequisite supplies resource metrics for SCHED-03. It does
not create an HPA or qualify a scenario. It installs the upstream service account,
read permissions, delegated authentication bindings, service, deployment and
Metrics API registration in the dedicated kind cluster. It retains upstream's
aggregated-API TLS setting; kubelet certificate verification remains enabled.
This is not the cloud deployment configuration.

From the repository root:

```powershell
kubectl --kubeconfig ../resources/pay_ops/runtime/kubeconfig --context kind-payops-dev apply -f infra/kubernetes/metrics-server/components.yaml
kubectl --kubeconfig ../resources/pay_ops/runtime/kubeconfig --context kind-payops-dev get apiservice v1beta1.metrics.k8s.io
kubectl --kubeconfig ../resources/pay_ops/runtime/kubeconfig --context kind-payops-dev top pods -n payops-sandbox
```

Do not claim metrics availability until the API is Available and returns fresh
measurements for all five sandbox services. See the [upstream requirements](https://kubernetes-sigs.github.io/metrics-server/) for network and certificate prerequisites.
