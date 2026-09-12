# Local model decisions

The `reasoning-loop-local-contract-v3` profile uses disjoint typed generation shapes
for `read`, `finish` and `refuse`. Fixed operational tools require a null query;
only runbook and incident search accept bounded search text. All objects reject
extra properties. The generated JSON Schema is derived from Pydantic models in
`payops.orchestrator.local_schema`, with local references inlined.

This fixes a measured failure: the small model repeatedly supplied a search query
to `workload_status`, which the original Python validator correctly rejected but
the original generation schema allowed. Generation now constrains that combination.
Python still checks the full canonical decision, uniqueness, confidence bounds,
cause vocabulary and citation membership before accepting output.

The pinned [llama.cpp grammar guide](https://github.com/ggml-org/llama.cpp/blob/b10809/grammars/README.md)
documents incomplete JSON Schema support, including numeric bounds, nested references
and uniqueness. A valid generated JSON shape does not establish correct diagnosis.
The model receives explicit instructions because the grammar itself is not prompt text.

On the final allowed model turn, the host advertises only finish/refuse and the
adapter uses the matching terminal schema. If a read reservation is denied while
model allowance remains, the next turn is terminal. No read budget is replenished,
no denied operation is sent, and no model allowance is added. Refusal stays terminal;
one invalid response may receive the existing bounded repair turn. There is no
automatic retry of ambiguous effects, stronger-model fallback or paid inference.

Local read feedback includes service, query and returned evidence IDs. The small
model can still repeat reads; deterministic budgets bound that behavior. Journal
reconstruction follows the same transitions without dispatching completed work.
Earlier local profiles require their original source/configuration; start a new
incident to use this generation contract.

Contract tests compare generated JSON Schema validation, typed local parsing and
canonical decision parsing. Vectors cover all six legal tools, finish, refusal,
wrong query types, empty search text, empty read decisions, reads attached to finish
or refusal, unknown tools and injected approval fields. Separate tests cover the
terminal schema, nested untrusted markers, budget denial and completed replay.

The primary target is the existing free local llama.cpp/Qwen runtime. This change
does not change the OpenAI adapter or claim compatibility with OpenAI strict mode.
