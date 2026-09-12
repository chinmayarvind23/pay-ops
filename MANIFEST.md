# Repository guide

| Path | Responsibility |
| --- | --- |
| `src/payops/contracts` | Validated incident, evidence and report schemas |
| `src/payops/orchestrator` | Investigation state, checkpoints and model decisions |
| `src/payops/tools` | Scoped operational readers |
| `src/payops/evidence` | Normalization, source verification and archival |
| `src/payops/memory` | Incident persistence, caching and retrieval |
| `src/payops/auth` | Backend identity verification |
| `src/payops/policy` | Deterministic permission checks |
| `src/payops/remediation` | Proposals, approvals and conditional execution |
| `src/payops/sandbox` | Synthetic payment services |
| `src/payops/scenarios` | Controlled faults and restoration |
| `apps/web` | Read-only incident interface |
| `infra` | Container, Kubernetes and infrastructure configuration |
| `tests` | Contract, unit and integration checks |
| `docs` | Product and operator documentation |
