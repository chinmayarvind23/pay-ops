"""Trusted host bindings connect the closed registry to existing bounded read clients."""

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Literal

from pydantic import TypeAdapter

from payops.contracts import Contract, EvidenceItem, Identifier, utc_now
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError
from payops.evidence.normalize import Observation, normalize
from payops.evidence.payment_window import Service, snapshot_observation, snapshot_queries
from payops.evidence.verification import verify_evidence
from payops.memory.data_clients import (
    ElasticsearchRetrieval,
    EvidenceScope,
    SearchRequest,
    verify_retrieval_evidence,
)
from payops.orchestrator.reasoning import ReadRequest
from payops.policy.contracts import Principal
from payops.policy.engine import identity_valid
from payops.tools.kubernetes import KubernetesRead
from payops.tools.payment import PaymentRead
from payops.tools.registry import CATALOG, ReadRegistry, Reserve


class ReadBinding(Contract):
    """An authenticated host selects incident and actor; model arguments contain neither."""

    incident_id: Identifier
    subject: Identifier
    namespace: Literal["payops-sandbox"] = "payops-sandbox"


class OperationalReads:
    """Borrow clients and artifacts; their lifetime and all credentials remain host-owned."""

    def __init__(
        self,
        binding: ReadBinding,
        artifacts: ArtifactStore,
        kubernetes: KubernetesRead,
        payment: PaymentRead,
        retrieval: ElasticsearchRetrieval,
        principal: Callable[[], Principal | None],
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """No default endpoints, environment discovery, mutation clients or credential loading."""
        self._binding = ReadBinding.model_validate(binding.model_dump())
        self._artifacts, self._kubernetes = artifacts, kubernetes
        self._payment, self._retrieval = payment, retrieval
        self._principal, self._clock = principal, clock

    def registry(self, reserve: Reserve) -> ReadRegistry:
        """The caller supplies durable charging; identity refresh is outside backend read counts."""
        return ReadRegistry(
            {name: self._read for name in CATALOG},
            self._authorize,
            reserve,
            self._verify,
            self._binding.incident_id,
        )

    def _authorize(self) -> bool:
        """A fresh grant must still belong to the authenticated initiating responder."""
        principal = self._principal()
        return (
            principal is not None
            and principal.subject == self._binding.subject
            and identity_valid(principal, "responder", self._binding.namespace, self._clock())
        )

    def _verify(self, item: EvidenceItem) -> None:
        """Reverify the complete lineage and scope, including completion-relative freshness."""
        verify_evidence(item, self._artifacts)
        now = self._clock()
        if item.incident_id != self._binding.incident_id:
            raise EvidenceIntegrityError("foreign read incident")
        if item.source in {"RUNBOOK", "MEMORY"}:
            lineage = verify_retrieval_evidence(item, self._artifacts)
            if lineage.scope != EvidenceScope(
                incident_id=self._binding.incident_id,
                namespace=self._binding.namespace,
                service=item.resource,
            ):
                raise EvidenceIntegrityError("foreign retrieval scope")
            observed = lineage.retrieved_at
        elif item.source == "PROMETHEUS":
            payload = self._artifacts.verify(item).get("payload")
            if not isinstance(payload, dict):
                raise EvidenceIntegrityError("invalid payment snapshot payload")
            parsed = snapshot_observation(
                payload, TypeAdapter[Service](Service).validate_python(item.resource)
            )
            if (item.query, item.observed_at) != (parsed.query, parsed.observed_at):
                raise EvidenceIntegrityError("payment snapshot metadata mismatch")
            observed = item.observed_at
        else:
            payload = self._artifacts.verify(item).get("payload")
            if not isinstance(payload, dict) or (
                payload.get("namespace"),
                payload.get("service"),
            ) != (self._binding.namespace, item.resource):
                raise EvidenceIntegrityError("foreign raw scope")
            observed = item.observed_at
        if not now - timedelta(minutes=5) <= observed <= now:
            raise EvidenceIntegrityError("read evidence outside freshness window")

    def _read(self, request: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Revalidate before I/O even when a trusted caller bypasses registry admission."""
        request = ReadRequest.model_validate(request.model_dump())
        if request.tool in {"runbook_search", "incident_search"}:
            return self._search(request)
        if request.tool == "workload_status":
            observations = self._kubernetes.collect(request.service)
        elif request.tool == "pod_events":
            observations = self._kubernetes.events(request.service)
        elif request.tool == "recent_logs":
            observations = (self._kubernetes.logs(request.service),)
        else:
            observations = (self._payment.snapshot(request.service),)
        now = self._clock()
        if len(observations) > 64:
            raise ValueError("read evidence count exceeded")
        # Validate all observations before writing any normalized artifact.
        projected = tuple(self._project(request, item, now) for item in observations)
        evidence = tuple(
            normalize(
                item, self._binding.incident_id, now - timedelta(minutes=5), now, self._artifacts
            )
            for item in projected
        )
        for item in evidence:
            self._verify(item)
        return evidence

    def _project(self, request: ReadRequest, item: Observation, now: datetime) -> Observation:
        """Only current pod status may carry a pod resource name; all output binds the service."""
        item = Observation.model_validate(item.model_dump())
        expected = {
            "pod_events": {("KUBERNETES", "events.current-pod-uid")},
            "recent_logs": {("LOG", "logs.5m.100")},
            "payment_snapshot": {("PROMETHEUS", snapshot_queries(request.service)[0])},
            "workload_status": {
                ("DEPLOYMENT", "kubernetes.status-snapshot"),
                ("KUBERNETES", "kubernetes.status-snapshot"),
                ("KUBERNETES", "pods.count"),
            },
        }
        if (item.source, item.query) not in expected[request.tool]:
            raise ValueError("read source or query mismatch")
        pod = (
            request.tool == "workload_status"
            and item.source == "KUBERNETES"
            and item.query == "kubernetes.status-snapshot"
            and item.payload.get("kind") == "Pod"
            and item.resource.startswith(request.service + "-")
        )
        if item.resource != request.service and not pod:
            raise ValueError("read resource mismatch")
        if not now - timedelta(minutes=5) <= item.observed_at <= now:
            raise ValueError("raw observation outside freshness window")
        if item.source == "PROMETHEUS":
            parsed = snapshot_observation(item.payload, request.service)
            if item.observed_at != parsed.observed_at:
                raise ValueError("payment snapshot observation time mismatch")
            return item
        for key, value in (("namespace", self._binding.namespace), ("service", request.service)):
            if key in item.payload and item.payload[key] != value:
                raise ValueError("raw payload scope mismatch")
        # Host assignments deliberately override any untrusted original_resource collision.
        return Observation.model_validate(
            {
                **item.model_dump(),
                "resource": request.service,
                "payload": {
                    **item.payload,
                    "original_resource": item.resource,
                    "namespace": self._binding.namespace,
                    "service": request.service,
                },
            }
        )

    def _search(self, request: ReadRequest) -> tuple[EvidenceItem, ...]:
        """Plain search text selects no URL, index, namespace, DSL or result count."""
        search = SearchRequest(
            scope=EvidenceScope(
                incident_id=self._binding.incident_id,
                namespace=self._binding.namespace,
                service=request.service,
            ),
            kind="RUNBOOK" if request.tool == "runbook_search" else "MEMORY",
            text=request.query or "",
            size=3,
        )
        evidence = self._retrieval.search(search)
        if len(evidence) > 3:
            raise ValueError("retrieval count exceeded")
        for item in evidence:
            if item.source != search.kind or item.resource != request.service:
                raise ValueError("retrieval source or service mismatch")
            self._verify(item)
        return evidence
