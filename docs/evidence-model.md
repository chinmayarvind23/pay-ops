# Evidence Model

A root-cause statement is auditable only if it links to the exact evidence that supported it.

Evidence sources:

```text
KUBERNETES
PROMETHEUS
LOG
TRACE
DEPLOYMENT
PAYMENT
RUNBOOK
MEMORY
```

For incident start `t0`, evidence queries use explicit time windows such as `[t0 - pre, t0 + post]`.

Preserve raw artifact pointers/hashes and a normalized summary. The summary is model context, not a replacement for raw evidence.

## Implemented local artifact layer

`payops.evidence` validates source observations, retains sanitized JSON under content hashes,
and verifies the bytes, incident ownership and normalized metadata before context selection.
Local `sha256://` references resolve only inside a caller-configured artifact directory.
The store never fetches an arbitrary URI. Atomic publication prevents concurrent readers
from observing a partially written final artifact.

Retained artifacts are sanitized derivatives with a recorded transformation version.
Their hashes do not claim to identify the original unredacted source. Common-secret
redaction covers structured credential keys, Kubernetes environment entries and common
inline credentials; it is not a universal personal-data detector. Operational collectors
must restrict returned fields and apply source authorization separately.

Context selection checks integrity before enforcing its 64-item/24,000-character default
budget. The character bound is not an exact provider-token count. Live collection and
source authenticity remain separate acceptance gates.

Each hypothesis records supporting evidence, refuting evidence, and missing evidence. Material conflicts can produce abstention/escalation.
