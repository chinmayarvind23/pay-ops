# ADR-002: LangGraph owns the incident lifecycle

Use explicit states/checkpoints instead of an open-ended loop. Incidents need legal transitions, budgets, approval interrupts, restartability, and deterministic terminal states.

The implemented local graph is `triage -> reserve -> collect -> rank -> finish`,
with early terminal branches for invalid scope, budget exhaustion and unavailable or
invalid evidence. It uses native LangGraph checkpoints with synchronous SQLite writes.
Each checkpoint stores the full validated state as JSON; artifact bytes remain in the
content-addressed evidence store. The ranker is currently the deterministic baseline.

A native file lock serializes workers for an incident on one host. Durable node-attempt
records consume budget before work, including attempts interrupted before a checkpoint.
The collection batch reserves 20 logical wrapper calls (some wrappers issue more than
one Kubernetes request). An exclusive dispatch marker prevents an ambiguous interrupted
batch from being repeated; retained complete output can be reused. This trades missing
evidence for bounded execution. It establishes process-restart behavior, not an atomic
transaction across SQLite and files under power loss.

`node_start_deadline` stops new nodes after the cutoff. Existing reader timeouts bound
in-flight I/O; this field is not a hard end-to-end cancellation deadline. The report's
duration sums completed node work and excludes paused time, crashes and checkpoint
overhead. Step timestamps and attempt files retain the broader audit timeline.

Start is idempotent and rejects a reused thread with changed incident input. Resume takes
only the incident ID and rejects a changed execution mode. Approval/action nodes,
multi-host PostgreSQL coordination and provider reasoning remain subsequent work.

```powershell
uv run python -m payops.orchestrator.run --runtime ../resources/pay_ops/runtime/investigations --kubeconfig ../resources/pay_ops/runtime/kubeconfig --pause-before-ranking
uv run python -m payops.orchestrator.run --runtime ../resources/pay_ops/runtime/investigations --kubeconfig ../resources/pay_ops/runtime/kubeconfig --resume INCIDENT_ID
```

References: [LangGraph checkpoints](https://docs.langchain.com/oss/python/langgraph/checkpointers)
and [native file locking](https://py-filelock.readthedocs.io/en/latest/).
