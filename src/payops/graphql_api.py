"""Bounded read-only GraphQL reuses the REST incident authorization boundary."""

from typing import cast

from fastapi import APIRouter, HTTPException
from graphql import (
    GraphQLError,
    GraphQLResolveInfo,
    build_schema,
    graphql_sync,  # pyright: ignore[reportUnknownVariableType]
    parse,
)
from graphql.language import FieldNode, OperationDefinitionNode, OperationType, SelectionSetNode
from pydantic import Field, JsonValue

from payops.contracts import Contract
from payops.memory.store import IncidentStore
from payops.protected_api import Authenticator, Bearer, actor, scoped_incident

SCHEMA = build_schema("""
type Evidence { id: ID!, source: String!, summary: String! }
type Cause { code: String!, confidence: Float!, supportingEvidenceIds: [ID!]! }
type Report { terminalState: String!, mode: String!, durationSeconds: Float!, causes: [Cause!]! }
type Incident { id: ID!, title: String!, service: String!, namespace: String!,
  report: Report, evidence(offset: Int = 0, limit: Int = 20): [Evidence!]! }
type Query { incident(id: ID!): Incident }
""")


class QueryRequest(Contract):
    """Reject batching and bound parser input before examining the query document."""

    query: str = Field(min_length=1, max_length=8192)
    variables: dict[str, JsonValue] = Field(default_factory=dict, max_length=20)


def selections(node: SelectionSetNode, depth: int = 1) -> int:
    """Bound alias fanout and depth; fragments and introspection are outside this small API."""
    if depth > 5:
        raise ValueError("query depth exceeded")
    count = 0
    for item in node.selections:
        if not isinstance(item, FieldNode) or item.name.value.startswith("__"):
            raise ValueError("unsupported selection")
        count += 1 + (selections(item.selection_set, depth + 1) if item.selection_set else 0)
        if count > 50:
            raise ValueError("query complexity exceeded")
    return count


def check_query(query: str) -> None:
    """One query operation is allowed; mutations cannot reach resolvers even by alias."""
    document = parse(query, max_tokens=500)
    if len(document.definitions) != 1:
        raise ValueError("one query required")
    operation = document.definitions[0]
    if (
        not isinstance(operation, OperationDefinitionNode)
        or operation.operation != OperationType.QUERY
    ):
        raise ValueError("read-only query required")
    selections(operation.selection_set)


def graphql_router(store: IncidentStore, identity: Authenticator) -> APIRouter:
    """Each root incident read refreshes authorization and scopes evidence through its parent."""
    router = APIRouter(tags=["graphql"])

    @router.post("/api/graphql")
    def query(request: QueryRequest, bearer: Bearer) -> dict[str, JsonValue]:
        """Resolve bounded projections only; failures never expose backend exception details."""
        actor(identity, bearer)
        try:
            check_query(request.query)
        except (ValueError, GraphQLError, RecursionError):
            raise HTTPException(400, detail="GRAPHQL_QUERY_REJECTED") from None

        def incident(info: GraphQLResolveInfo, id: str) -> dict[str, object]:
            """Reuse current REST scope checks instead of trusting a GraphQL-supplied actor."""
            item = scoped_incident(store, id, actor(identity, bearer))
            report = item.report

            def evidence(
                info: GraphQLResolveInfo, offset: int = 0, limit: int = 20
            ) -> list[dict[str, str]]:
                """Bound evidence pages and omit artifact filesystem paths."""
                if not 0 <= offset <= 10000 or not 1 <= limit <= 50:
                    raise ValueError("invalid pagination")
                return [
                    {"id": row.evidence_id, "source": row.source, "summary": row.summary}
                    for row in (report.evidence if report else ())[offset : offset + limit]
                ]

            return {
                "id": item.incident_id,
                "title": item.request.title,
                "service": item.request.service,
                "namespace": item.request.namespace,
                "evidence": evidence,
                "report": None
                if report is None
                else {
                    "terminalState": report.terminal_state,
                    "mode": report.mode,
                    "durationSeconds": report.duration_seconds,
                    "causes": [
                        {
                            "code": row.cause_code,
                            "confidence": row.confidence,
                            "supportingEvidenceIds": list(row.supporting_evidence_ids),
                        }
                        for row in report.ranked_root_causes[:3]
                    ],
                },
            }

        result = graphql_sync(
            SCHEMA,
            request.query,
            root_value={"incident": incident},
            variable_values=request.variables,
        )
        if result.errors:
            return {"data": None, "errors": [{"message": "QUERY_REJECTED"}]}
        actor(identity, bearer)
        return {"data": cast(JsonValue, result.data)}

    return router
