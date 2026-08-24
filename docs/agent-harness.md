# Agent Harness

## Layers

```text
model
tool contracts
context assembly
LangGraph orchestration
checkpointed state
incident memory
policy + budgets
observability
deterministic verification
```

LangGraph owns state transitions, retries, checkpoints, budgets, approval interrupts, stop conditions, and terminal states.

The model owns bounded judgment: which evidence gap to investigate, how to rank supported hypotheses, whether evidence conflicts, and which allowlisted remediation to propose.

LangChain supplies model/provider integrations and typed tool wrappers. Tools execute typed operations.

## Context

Each model call receives a compact bundle:

```text
incident objective
state summary
selected evidence
selected runbook/prior incident snippets
remaining budgets
allowed proposal vocabulary
validation feedback
```

Do not blindly append the full transcript.

## Stop conditions

Stop/escalate on completion, insufficient evidence, tool/model/cost budget, iteration cap, security block, dependency unavailability, or inconclusive postcheck.
