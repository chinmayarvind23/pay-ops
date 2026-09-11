# Reasoning contract

The first model boundary accepts a closed read request, a supported ranked finish, an evidence-insufficient finish, or a refusal. It has no action or approval fields. JSON is bounded to 16 KiB and rejects duplicate keys, nonfinite values, unknown tools and scope overrides. The host supplies the cause vocabulary and exact evidence IDs; unresolved, duplicated or conflicting citations fail validation.

The usage contract accounts for ordinary text and cached input with integer nanodollar rates supplied by trusted configuration. Missing usage remains unknown. Unknown top-level usage fields and nonzero unsupported token categories fail validation. These are fixture-tested arithmetic contracts, not measured provider costs.

LangChain may coerce provider metadata before constructing an AIMessage. A provider adapter must retain and validate original usage before that coercion, bind it to the model and current price provenance, and preserve unknown completion after a crash. This increment is not yet connected to a model, tool dispatcher or investigation graph. Live model quality, cost and latency remain unmeasured.
