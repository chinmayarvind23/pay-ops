# Performance and Cost

Full incident duration decomposes into queue, evidence, retrieval, model, policy, approval, action, and postcheck time.

Do not optimize model latency if evidence collection dominates.

The model-step p95 target is `4.6 s`. No provider-backed model timing has been measured yet.
When available, compute it from raw timings between evidence submission and accepted output.

The investigation-time target is `11.8 min -> 2.9 min`. Measuring it requires paired
human investigations and one completion rule; that study has not run.

LLM provider cost:

`cost = input_tokens * input_rate + output_tokens * output_rate`

Report mean, median, p95, and total.

Cost/latency levers include bounded log windows, deterministic pre-aggregation, compact evidence context, selective incident-memory retrieval, and model choice verified through evals. Required evidence is never removed merely to reduce cost.
