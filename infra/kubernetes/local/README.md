# Local synthetic sandbox

Run `scripts/local_cluster.ps1` from PowerShell with `-Action Create`, `Build`, `Deploy`, `Status`, or `Forward`. Docker Desktop must use Linux containers. The script only addresses cluster `payops-dev`, namespace `payops-sandbox`, and the sibling `resources/pay_ops/runtime/kubeconfig`; it never selects a global Kubernetes context. Forward binds `127.0.0.1:18080`, configurable with `-LocalPort`.

Install the pinned kind binary once from the repository root:

```powershell
$toolsDirectory = Join-Path (Split-Path -Parent (Get-Location).Path) 'resources/pay_ops/tools'
New-Item -ItemType Directory -Path $toolsDirectory -Force | Out-Null
$binaryPath = Join-Path $toolsDirectory 'kind-v0.33.0.exe'
$checksumPath = Join-Path $toolsDirectory 'kind-v0.33.0.sha256sum'
Invoke-WebRequest -Uri 'https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-windows-amd64' -OutFile $binaryPath
Invoke-WebRequest -Uri 'https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-windows-amd64.sha256sum' -OutFile $checksumPath
$expectedChecksum = ((Get-Content -Raw -LiteralPath $checksumPath).Trim() -split '\s+')[0]
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $binaryPath).Hash -ne $expectedChecksum) {
    throw 'Checksum mismatch; do not execute the download'
}
& $binaryPath version
```

The application image is built with the committed Python dependency lock. Five internal services run without Kubernetes tokens or privileges, with resource limits and bounded health probes. This local kind network does not enforce NetworkPolicy; there is no claim of pod egress isolation. No service is published through NodePort, LoadBalancer or Ingress. Public and cloud deployments require separate configuration.

The Kubernetes API and payment port-forward are loopback-only. This sandbox never moves money; `/simulate` accepts only synthetic requests, `/health` reports availability, and `/metrics` exposes bounded-label Prometheus telemetry.
