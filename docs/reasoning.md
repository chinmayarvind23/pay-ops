# Reasoning contract

The first model boundary accepts a closed read request, a supported ranked finish, an evidence-insufficient finish, or a refusal. It has no action or approval fields. JSON is bounded to 16 KiB and rejects duplicate keys, nonfinite values, unknown tools and scope overrides. The host supplies the cause vocabulary and exact evidence IDs; unresolved, duplicated or conflicting citations fail validation.

The usage contract accounts for ordinary text and cached input with integer nanodollar rates supplied by trusted configuration. Missing usage remains unknown. Unknown top-level usage fields and nonzero unsupported token categories fail validation. These are fixture-tested arithmetic contracts, not measured provider costs.

LangChain may coerce provider metadata before constructing an AIMessage. The OpenAI adapter validates original usage before constructing its message, binds it to the pinned model and price profile, and preserves unknown completion after a crash. The runtime invokes real LangChain message interfaces and is connected to the read dispatcher and native investigation graph. Tests use scripted LangChain responses and synthetic HTTP transport. Live model quality, cost and latency remain unmeasured.

The read dispatcher exposes six fixed tools with a shared argument schema. It revalidates complete batches before a trusted callback reserves their command/query cost. Two worker slots bound concurrency and queue growth. Timed-out operations retain their slots until their underlying I/O finishes; no late result can enter the returned batch. Result deadlines are separate from transport cancellation. A final identity check covers time spent waiting for sibling reads, with an additional eight-second maximum wait and the same slot bound. Identity lookups are excluded from the command/query census. [Python documents that running futures cannot be cancelled](https://docs.python.org/3.12/library/concurrent.futures.html).

The local operator host binds current identity, durable reservations, scoped readers and source verification. Returned evidence must belong to the requested service and incident. Exact pod names remain in source payloads when the adapter normalizes their service scope. Errors contain typed statuses without raw provider exception text. Tool spans carry the investigation's trace context, bounded tool/service names, query digests and evidence IDs.

Model context includes verified payload facts and marks omitted payloads explicitly. It admits at most 256 input artifacts, selects at most 64 and limits the serialized bundle to 24,000 characters. All input artifacts, including dropped entries, undergo source verification. Payment windows include actual arithmetic and their input IDs; trace facts include duration and original span time; retrieval retains original summary and time as guidance. These character limits do not replace tokenizing the full provider prompt. No model-level prompt-injection resistance has been measured yet.

The investigation graph rechecks nested trace and retrieval source artifacts when collecting or resuming, in addition to payment lineage. The deterministic investigation command retains deterministic ranking. The separate operator host configures a reasoner factory with an explicit model method and stable ranking profile. Both are recorded before collection and checked on restart, so failed model attempts remain identifiable in evaluation denominators.


## Durable execution

The reasoning loop prepares a fixed system message and one JSON data message. Fixture preparation supplies a declared exact count. Provider preparation supplies the explicit configured input ceiling; it performs no network work. SQL reserves that input allowance, capped output tokens, conservative generation-token cost and two provider requests before remote counting or generation. A model operation runs only after a new reservation commits. Its typed result is fsynced into content-addressed storage before the SQL ledger records its digest. Read batches use the same reservation-before-dispatch and result-before-completion order. Reservations never refund uncertain work.

A restart walks the original turns and loads verified receipts. It checks prompt/context identity, exact read requests, incident/service scope and nested source artifacts. A reservation without a completion receipt stops with UNKNOWN_COMPLETION, including a crash before the actual call. This conservative choice avoids a possible duplicate dispatch. A crash after loop completion but before the LangGraph rank checkpoint replays results without another model or read call. SQL compare-and-set protects reservations; local file locks exclude overlapping runs on one host.

Finish and refusal are explicit stops. One invalid response gets schema feedback; a second invalid response stops. Model or read timeout, busy admission, revoked authority and exhausted budget also stop. Read errors become typed feedback. Model authorization and final answer publication share the persistent one-slot model worker, with an acceptance deadline of at most eight seconds. Timed-out workers retain capacity until transport finishes. Each concrete transport still needs its own timeout; a returned timeout does not claim remote cancellation.

The graph validates the reasoner's artifact root and remaining logical/backend read allowances after initial collection. Reports retain deterministic/model_fixture/model_provider method, stop reason and receipt digests. Failed model work does not silently become a baseline answer. Fixture costs and timing never qualify as live-provider cost or latency. Provider usage, configured prices and measured invocation time remain in individual receipts for evaluation; absent usage is unknown.

Operational bindings cover fixed Kubernetes reads, payment snapshots and scoped runbook/incident searches. Host code supplies endpoints, credentials and the bound responder identity. The model cannot select namespaces, URLs, SQL, Elasticsearch DSL, actions or approvals. Traces collected separately remain available through verified context; there is no hidden trace-read command in the six-tool catalog.

## Trusted local operator host

`payops.operator_host` connects the concrete provider and read adapters to ModelRuntime,
ReasoningLoop and the native InvestigationWorker. Its authority is the initiating OS account
and an explicit local grant with fixed responder role, sandbox namespace and expiry. Each
check rereads the bounded grant; account changes, revocation, expiry and slow or reversed-clock
verification fail closed. This is local OS/file authority, not Firebase or public HTTP
authentication. Configuration names credentials explicitly, and plan mode does not load them.

The host uses local SQLite for the reasoning journal and native graph checkpoints. Its profile
binds the account, configuration, budgets, knowledge bundle and whole release cause vocabulary.
No selected case's gold assignment enters the model prompt. Original runbook/memory artifacts
are verified and copied into the incident store before retrieval can use their direct lineage.
Default allowances include the initial 20/30 logical/backend reads plus six/nine selected reads.

Finished publication verifies matching report/checkpoint fields, nested sources and a complete
SQL receipt census. It reconstructs the actual loop with a read-only ledger that refuses new
reservations, an inert provider and inert read handlers. Reconstructed evidence, hypotheses,
stop condition and receipts must match the report. A safely empty unreasoned stop requires no
hidden reasoning journal. An incomplete journal fails publication rather than authorizing a
retry. Current authority is required again before the verified summary is returned.

Host shutdown stops admission immediately and defers owned client/engine cleanup until active
provider or retrieval methods return. A result timeout does not cancel remote work, and a Python
worker may keep the process alive until its concrete transport finishes. Synthetic end-to-end
tests exercise all six tools, durable reservations, native completed restart with zero repeated
requests, staged revocation and held-provider cleanup. They do not establish live provider
quality, latency or billing. Setup and invocation are in [Commands](commands.md#trusted-local-operator-investigation).


## Provider ceiling and stage accounting

A live adapter first sends a bounded token-count request for the exact generation request shape. Its count enters a runtime-owned hook before generation: strict integer, nonnegative, at most the reserved ceiling. The hook refreshes authority inside the already-owned model worker and checks the original deadline both before and after that refresh. An oversized count, late authorization or revoked identity cannot start generation. Calling the public authorization method recursively from that worker would deadlock admission, so the stage hook performs no new submission.

Fixture charges retain the fixture_exact discriminator and reserve zero actual provider requests. Provider charges use provider_ceiling and reserve both count and generation requests even if work stops after counting. The separate provider-request allowance defaults to20 and cannot exceed20. These requests are not Kubernetes, telemetry or search reads. Actual provider usage must match the server count and fit the input/output reservation. Receipts retain count time and generation time separately; provider_seconds refers to generation only. Failed unobserved stages retain unknown timing/usage.

Configured text prices bound generation-token reservation only. A fee for the count endpoint has not been established, so these reservations do not claim an all-fees provider-spend cap. A paid evaluation must reconcile actual billing before publishing total cost. No paid requests have been made in this increment.

Adding the accounting discriminator changes the run binding. Existing ledger records default to their historical fixture semantics, but a changed host model/prompt binding cannot resume under a different interpretation. Already completed graph reports remain readable; an unfinished run requiring old configuration must retain that configuration/source or start a separate incident.

## OpenAI Responses adapter

The adapter pins `gpt-5.4-mini-2026-03-17` and the standard text price profile: $0.75 uncached input, $0.075 cached input and $4.50 output per million tokens. It requires the actual response service tier to be `default`. Configuration cannot silently substitute a model, tier or lower price. The host explicitly supplies the API key; constructing the adapter neither discovers credentials nor sends requests.

Two fixed HTTPS endpoints perform input counting and generation. They receive the same immutable message/schema shape, with no tools or conversation history. Generation disables streaming, background execution, storage and truncation. A one-slot transport admits no queue or retries. Byte, nesting, token and deadline checks reject malformed or late responses. Missing usage and uncertain remote completion remain unknown. `store=false` does not establish zero provider retention.

Raw provider refusals become a fixed host refusal with an explicit normalization marker. Receipts retain the request-shape digest and separate count/generation durations. They contain no raw rejected output. HTTP transport logging preflight rejects known configurations that could log server-controlled headers; hosts must keep logging configuration stable.

The adapter and wire parser passed 107 synthetic tests with 100% branch coverage; twelve targeted mutations were caught by assertions. These controls establish local behavior, not provider availability or diagnosis quality. A 16,000-input/2,048-output reservation is $0.021216 in generation tokens at the pinned prices; it is a calculated allowance, not measured billing.
