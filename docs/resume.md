# PayOps

- Built an agentic incident-response system with LangChain/LangGraph that correlated Kubernetes health, logs, traces, deployment changes, and payment telemetry across **24 reproducible Kubernetes failure scenarios**, achieving **91.7% root-cause Recall@3 and Recall@1** in predicate-assisted development replay.
- Developed a failure-oriented evaluation harness covering OOMKills, bad rollouts, scheduler saturation, dependency outages, misleading telemetry, and payment-slice degradations, rejecting **100% of 120 unauthorized capability attempts** with **zero executor dispatches** in component evaluations.
- Added structured investigation traces, incident memory, deterministic approval gates, authenticated GraphQL exploration, and Slack incident notifications, measuring **8.41-second median diagnostic replay latency** with **$0 provider charges** through local inference; deployed an interactive incident replay on **free Hugging Face hosting**.

Implemented local stack: Python, LangChain/LangGraph, Kubernetes, Prometheus,
Elasticsearch, PostgreSQL, Redis, OpenTelemetry, FastAPI and local Qwen inference.
Additional delivered scope includes a constrained GKE MCP adapter, tested LangSmith
receipt export and optional GCP Terraform. AWS deployment has self-service instructions.
See [stack coverage](stack-status.md) and [measured results](results.md) for verification scope.
