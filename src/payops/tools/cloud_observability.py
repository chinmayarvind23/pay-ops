"""Optional fixed-scope Google observability reads produce ordinary verifiable observations."""

from datetime import datetime
from typing import Literal

from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.types import ListLogEntriesResponse
from google.cloud.monitoring_v3 import MetricServiceClient
from google.cloud.monitoring_v3.types import ListTimeSeriesResponse
from pydantic import AwareDatetime, Field, JsonValue, TypeAdapter

from payops.contracts import Contract
from payops.evidence.artifacts import JSON_OBJECT
from payops.evidence.normalize import Observation
from payops.tools.kubernetes import SERVICES, items, object_value

STAMP: TypeAdapter[datetime] = TypeAdapter(AwareDatetime)
RESTART_METRIC = "kubernetes.io/container/restart_count"


class CloudScope(Contract):
    """Operator configuration fixes the project, cluster and namespace before any read."""

    project: str = Field(pattern=r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")
    cluster: str = Field(pattern=r"^[a-z][a-z0-9-]{0,62}$")
    location: str = Field(pattern=r"^[a-z][a-z0-9-]{0,62}$")
    namespace: Literal["payops-sandbox"] = "payops-sandbox"

    def labels(self, service: str) -> dict[str, str]:
        """Only known sandbox containers can become a filter value."""
        if service not in SERVICES:
            raise ValueError("service outside cloud scope")
        return {
            "project_id": self.project,
            "cluster_name": self.cluster,
            "location": self.location,
            "namespace_name": self.namespace,
            "container_name": service,
        }

    def filter(self, service: str) -> str:
        """Validated identifiers cannot add clauses to the provider query language."""
        clauses = ['resource.type="k8s_container"']
        clauses += [
            f'resource.labels.{key}="{value}"' for key, value in self.labels(service).items()
        ]
        return " AND ".join(clauses)


def window(start: datetime, end: datetime) -> None:
    """Reject naive, reversed or excessive observation windows before contacting a provider."""
    if start.tzinfo is None or end.tzinfo is None or not 0 < (end - start).total_seconds() <= 900:
        raise ValueError("cloud window must be aware and at most fifteen minutes")


def scoped_resource(value: JsonValue, expected: dict[str, str]) -> None:
    """Provider filters are not a substitute for checking returned resource ownership."""
    resource = object_value(value)
    labels = object_value(resource.get("labels"))
    if resource.get("type") != "k8s_container" or any(
        labels.get(k) != v for k, v in expected.items()
    ):
        raise ValueError("cloud response resource scope mismatch")


def document(raw: str) -> dict[str, JsonValue]:
    """One bounded page is explicit partial evidence, never an implicit complete inventory."""
    if len(raw.encode()) > 131072:
        raise ValueError("cloud response exceeds byte budget")
    return JSON_OBJECT.validate_json(raw)


def read_logs(
    client: LoggingServiceV2Client, scope: CloudScope, service: str, start: datetime, end: datetime
) -> tuple[Observation, ...]:
    """Fetch only the first scoped page; normalize returned timestamps and retain partiality."""
    window(start, end)
    query = (
        scope.filter(service)
        + f' AND timestamp>="{start.isoformat()}" AND timestamp<="{end.isoformat()}"'
    )
    pager = client.list_log_entries(  # pyright: ignore[reportUnknownMemberType]
        request={
            "resource_names": [f"projects/{scope.project}"],
            "filter": query,
            "order_by": "timestamp desc",
            "page_size": 20,
        },
        timeout=5,
        retry=None,
    )
    page = next(iter(pager.pages))
    data = document(ListLogEntriesResponse.to_json(page, preserving_proto_field_name=True))  # pyright: ignore[reportUnknownMemberType]
    observations: list[Observation] = []
    for entry in items(data.get("entries", []), 20):
        scoped_resource(entry.get("resource"), scope.labels(service))
        stamp = STAMP.validate_python(entry.get("timestamp"))
        if not start <= stamp <= end:
            raise ValueError("cloud log outside requested window")
        observations.append(
            Observation(
                source="LOG",
                resource=service,
                observed_at=stamp,
                query=f"cloud-logging://{scope.project}/{scope.cluster}",
                summary="Scoped Cloud Logging container entry",
                payload={
                    "text": entry.get("text_payload"),
                    "structured": entry.get("json_payload"),
                    "resource": entry.get("resource"),
                    "partial": bool(data.get("next_page_token")),
                },
            )
        )
    return tuple(observations)


def read_restarts(
    client: MetricServiceClient, scope: CloudScope, service: str, start: datetime, end: datetime
) -> tuple[Observation, ...]:
    """Read native Kubernetes restart counters, retaining series and interval semantics."""
    window(start, end)
    query = scope.filter(service) + f' AND metric.type="{RESTART_METRIC}"'
    pager = client.list_time_series(  # pyright: ignore[reportUnknownMemberType]
        request={
            "name": f"projects/{scope.project}",
            "filter": query,
            "interval": {"start_time": start, "end_time": end},
            "view": "FULL",
            "page_size": 20,
        },
        timeout=5,
        retry=None,
    )
    page = next(iter(pager.pages))
    data = document(ListTimeSeriesResponse.to_json(page, preserving_proto_field_name=True))  # pyright: ignore[reportUnknownMemberType]
    observations: list[Observation] = []
    for series in items(data.get("time_series", []), 20):
        scoped_resource(series.get("resource"), scope.labels(service))
        if object_value(series.get("metric")).get("type") != RESTART_METRIC:
            raise ValueError("unexpected cloud metric")
        points = items(series.get("points", []), 64)
        if not points:
            raise ValueError("cloud metric has no observed points")
        latest = start
        for point in points:
            stamp = STAMP.validate_python(object_value(point.get("interval")).get("end_time"))
            if not start <= stamp <= end:
                raise ValueError("cloud metric outside requested window")
            latest = max(latest, stamp)
        observations.append(
            Observation(
                source="KUBERNETES",
                resource=service,
                observed_at=latest,
                query=f"cloud-monitoring://{scope.project}/{scope.cluster}/restart-count",
                summary="Scoped Cloud Monitoring restart counter series",
                payload={"series": series, "partial": bool(data.get("next_page_token"))},
            )
        )
    return tuple(observations)
