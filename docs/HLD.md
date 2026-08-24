# High-Level Design

```mermaid
flowchart LR
    U[Slack / UI / Benchmark] --> A[FastAPI]
    A --> Q[Pub/Sub]
    Q --> G[LangGraph worker]
    G --> K[GKE MCP read adapter]
    G --> M[Prometheus / Cloud Monitoring]
    G --> L[Cloud Logging / OTel]
    G --> D[Deploy + payment telemetry]
    G --> E[Elasticsearch]
    G --> DB[Cloud SQL]
    DB --> R[Redis]
    G --> P[Policy engine]
    P --> H[Human approval]
    P --> X[Bounded executor]
    X --> GKE[GKE payment sandbox]
    GKE --> EXT[AWS Lightsail processor]
    G --> LS[LangSmith]
    G --> OT[OpenTelemetry]
    OT --> OBS[Grafana / Cloud Monitoring]
    G --> GCS[GCS]
    S[Supabase sanitized data] --> HF[Hugging Face replay demo]
```

## Planes

**Payment data plane:** synthetic payment services and external processor simulator.

**Incident control plane:** evidence collection, orchestration, diagnosis, policy, action, and postcheck.

**Safety plane:** capabilities, deterministic policy, approval, and executor identity.

**Public presentation plane:** Vercel and Hugging Face surfaces with sanitized or authorized views.
