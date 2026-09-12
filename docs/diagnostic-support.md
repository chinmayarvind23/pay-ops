# Predicate-assisted diagnosis

The local reasoning loop now combines model ranking with explicit diagnostic predicates. Python checks mechanism fields before a cause can appear in the final report. It does not load case IDs, golden answers or attribution-review annotations.

The predicates cover processor availability, database connection exhaustion, Redis outages, kernel CPU throttling, memory growth, concurrency pressure, OOM termination, startup exits, observed readiness failures, scheduler rejection, HPA limits, node-pressure eviction, matched payment slices and caller-observed processor latency. They inspect source fields; a probe's configured failure threshold is not a failed probe. Archived records cannot establish a current dependency failure.

These are development heuristics, not proof of unique causality. OOM termination supports a memory-limit/working-set conflict but does not alone identify why memory grew. When allocation evidence identifies a leak or concurrency pressure, that mechanism suppresses the generic OOM candidate. Region latency requires at least five samples per compared region and a twofold mean difference at the same processor. Caller processor spans require two samples exceeding every observed peer duration by threefold. These thresholds need validation on held-out incidents.

Only included, verified source facts supply candidates. Retrieved guidance, omitted payloads and prose summaries cannot establish support. Each local model claim must cite IDs checked for that particular mechanism; unsupported claims and unverified refutation links are omitted from the final report. The original model receipt remains intact for audit. The remote-model path retains its existing behavior.

## Context and repeated reads

Local context selection now orders fresh observations across source/service groups, with recently requested evidence first. Repeated snapshots from one group cannot occupy every available slot. All artifacts are verified even when their contents do not fit.

Operational JSON log lines are exposed as structured records, preserving outer timestamps and all unparsed text. Duplicate-key, malformed and nonfinite JSON remains raw evidence and cannot supply predicate fields. This lets database and allocation records reach the same checks used on recorded evidence.

Within one local investigation, an exact tool/service/query request is issued once. Reentry reconstructs completed requests from verified SQL-anchored receipts. Mixed batches dispatch only unseen requests. A wholly repeated batch moves the next model turn to finish/refuse. Failed completed reads are not silently retried. Refreshing a changing system requires a new investigation; this is snapshot reuse, not a cache with an implied freshness guarantee.

The local run binding is versioned `reasoning-loop-local-contract-v4`. Existing runs from an older contract cannot silently resume under changed prompt or support semantics.

## Evaluation

```bash
uv run python scripts/run_free_replay.py --support-gated --output /new/evidence/directory
```

This treatment adds checked candidate mechanisms to the source observations, permits at most one diagnosis and validates its evidence link. The constrained model selects from predicate-supported candidates. Report its results as **predicate-assisted development replay**, separate from the original unrestricted compact model. In this corpus the predicates generally leave one candidate, so much of the classification work belongs to Python, not the language model.

The corpus and frozen labels remain unchanged. Missing diagnostic details remain missing: generic processor errors do not prove rate limiting, a generic idempotency counter does not localize a webhook defect, and HTTP 422 alone does not establish a protocol mismatch. The configuration-failure recording lacks a configuration error log and supports only the broader startup-failure candidate.

The existing attribution reference was written after earlier predictions were visible. Matching it measures agreement with a development review, not independent semantic accuracy. Always report citation coverage and all 24 case outcomes alongside that agreement.
