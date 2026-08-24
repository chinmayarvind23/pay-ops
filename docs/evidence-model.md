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

Each hypothesis records supporting evidence, refuting evidence, and missing evidence. Material conflicts can produce abstention/escalation.
