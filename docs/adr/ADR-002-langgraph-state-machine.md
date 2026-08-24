# ADR-002: LangGraph owns the incident lifecycle

Use explicit states/checkpoints instead of an open-ended loop. Incidents need legal transitions, budgets, approval interrupts, restartability, and deterministic terminal states.
