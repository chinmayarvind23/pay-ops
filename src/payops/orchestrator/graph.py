"""Native LangGraph lifecycle with synchronous SQLite durability and local worker exclusion."""

from pathlib import Path
from typing import Literal, cast

from filelock import FileLock
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver

# The pinned graph wheel omits its typing marker; its runtime annotations remain available.
from langgraph.graph import END, START, StateGraph  # pyright: ignore[reportMissingTypeStubs]
from langgraph.graph.state import CompiledStateGraph  # pyright: ignore[reportMissingTypeStubs]

from payops.contracts import Incident
from payops.orchestrator.nodes import Collector, InvestigationNodes, incident_directory, route
from payops.orchestrator.state import (
    CollectionProfile,
    Envelope,
    InvestigationBudget,
    InvestigationState,
    pack,
    unpack,
)


def compile_graph(
    nodes: InvestigationNodes, saver: SqliteSaver, pause: bool
) -> CompiledStateGraph[Envelope, None, Envelope, Envelope]:
    """Linear lifecycle stages branch only on explicit terminal states, never free-form text."""
    builder = StateGraph(Envelope)
    # Upstream overloads contain unbound cache-policy generics; our node signatures are typed.
    for name, node in (
        ("triage", nodes.triage),
        ("reserve", nodes.reserve),
        ("collect", nodes.collect),
        ("rank", nodes.rank),
        ("finish", nodes.finish),
    ):
        builder.add_node(name, node)  # pyright: ignore[reportUnknownMemberType]
    builder.add_edge(START, "triage")
    for node in ("triage", "reserve", "collect"):
        builder.add_conditional_edges(node, route, ["reserve", "collect", "rank", "finish"])
    builder.add_edge("rank", "finish")
    builder.add_edge("finish", END)
    return builder.compile(  # pyright: ignore[reportUnknownMemberType]
        checkpointer=saver, interrupt_before=["rank"] if pause else []
    )


class InvestigationWorker:
    """SQLite and native file locks support one host; multi-host workers require PostgreSQL."""

    def __init__(
        self,
        root: Path,
        collect: Collector,
        *,
        pause_before_ranking: bool = False,
        mode: Literal["local_kind", "fixture_replay"] = "local_kind",
        collection_profile: CollectionProfile = "instant_v1",
    ) -> None:
        """Only trusted code binds the database, readers and optional inspection breakpoint."""
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.database = self.root / "checkpoints.sqlite"
        self.nodes = InvestigationNodes(self.root / "incidents", collect)
        self.pause = pause_before_ranking
        self.mode: Literal["local_kind", "fixture_replay"] = mode
        self.collection_profile: CollectionProfile = collection_profile

    def start(
        self, incident: Incident, budget: InvestigationBudget | None = None
    ) -> InvestigationState:
        """Repeated deliveries return saved state without replacing inputs or refilling budgets."""
        initial = InvestigationState(
            incident=incident,
            budget=budget or InvestigationBudget(),
            mode=self.mode,
            collection_profile=self.collection_profile,
        )
        return self._invoke(incident.incident_id, initial)

    def resume(self, incident_id: str) -> InvestigationState:
        """Resume only the recorded next node; no user-controlled state override is accepted."""
        return self._invoke(incident_id, None)

    def _invoke(self, incident_id: str, initial: InvestigationState | None) -> InvestigationState:
        """A kernel-backed lock prevents overlapping graph execution for the same incident."""
        lock_path = incident_directory(self.root, incident_id).with_suffix(".lock")
        config: RunnableConfig = {"configurable": {"thread_id": incident_id}, "recursion_limit": 16}
        with (
            FileLock(lock_path, timeout=0),
            SqliteSaver.from_conn_string(str(self.database)) as saver,
        ):
            graph = compile_graph(self.nodes, saver, self.pause)
            snapshot = graph.get_state(config)
            if snapshot.values:
                existing = unpack(cast(Envelope, snapshot.values))
                if existing.mode != self.mode:
                    raise ValueError("worker mode differs from saved investigation")
                if existing.collection_profile != self.collection_profile:
                    raise ValueError("collection profile differs from saved investigation")
                if initial is not None:
                    if existing.incident != initial.incident:
                        raise ValueError("thread already belongs to a different incident")
                    return existing
                if existing.phase == "FINISHED":
                    return existing
            elif initial is None:
                raise KeyError(incident_id)
            # The upstream overload includes generic Command; this boundary accepts only JSON.
            result = graph.invoke(  # pyright: ignore[reportUnknownMemberType]
                pack(initial) if initial else None, config, durability="sync"
            )
            return unpack(cast(Envelope, result))

    def history(self, incident_id: str) -> tuple[InvestigationState, ...]:
        """Expose full validated checkpoints for auditing without changing any graph state."""
        config: RunnableConfig = {"configurable": {"thread_id": incident_id}}
        with SqliteSaver.from_conn_string(str(self.database)) as saver:
            graph = compile_graph(self.nodes, saver, self.pause)
            return tuple(
                unpack(cast(Envelope, snapshot.values))
                for snapshot in graph.get_state_history(config)
                if snapshot.values
            )
