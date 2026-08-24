# Performance and Cost

Full incident duration decomposes into queue, evidence, retrieval, model, policy, approval, action, and postcheck time.

Do not optimize model latency if evidence collection dominates.

Model-step p95 is `4.6 s`, computed from raw model-step timings.

The investigation-time comparison `11.8 min -> 2.9 min` uses paired scenarios and one completion rule.

LLM provider cost:

`cost = input_tokens * input_rate + output_tokens * output_rate`

Report mean, median, p95, and total.

Cost/latency levers include bounded log windows, deterministic pre-aggregation, compact evidence context, selective incident-memory retrieval, and model choice verified through evals. Required evidence is never removed merely to reduce cost.
