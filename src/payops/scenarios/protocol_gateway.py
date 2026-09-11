"""Restrict wire-rollout writes to two Deployments and retain owned runtime/log evidence."""

from copy import deepcopy
from datetime import UTC, datetime

from payops.evidence.trace_span import PodIdentity
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.memory_provenance import timestamp
from payops.scenarios.protocol_contract import PLAN
from payops.scenarios.sampling_gateway import (
    SERVICES,
    SamplingGateway,
    deployment_map,
    runtime_identities,
)
from payops.tools.traces import bounded_read


class ProtocolGateway(SamplingGateway):
    """The inherited exact CAS cannot be used on a processor, arbitrary resource or namespace."""

    @staticmethod
    def validate_target(name: str) -> None:
        """Runtime validation remains narrower than the shared static target vocabulary."""
        if name not in {"payments-api", "risk-sim"}:
            raise ValueError("protocol Deployment outside two-resource allowlist")

    def risk_log(self, identity: PodIdentity, start: datetime) -> JsonObject:
        """Capture access records from the already verified owned risk pod only."""
        if not identity.pod_name.startswith("risk-sim-") or start.tzinfo is None:
            raise ValueError("invalid risk access-log scope")
        raw = bounded_read(
            (
                *self._prefix,
                "logs",
                identity.pod_name,
                "--container=sandbox",
                "--timestamps=true",
                "--tail=200",
                f"--since-time={start.isoformat()}",
                f"--limit-bytes={PLAN.risk_log_bytes}",
            ),
            PLAN.risk_log_bytes,
            12,
        )
        if len(raw) >= PLAN.risk_log_bytes:
            raise ValueError("risk access log is capped")
        return {
            "identity": identity.model_dump(mode="json"),
            "since": start.isoformat(),
            "captured_at": datetime.now(UTC).isoformat(),
            "text": raw.decode("utf-8"),
        }


def protocol_identities(
    state: JsonObject,
    original: JsonObject,
    payments: JsonObject,
    risk: JsonObject,
    *,
    risk_image_id: str | None = None,
) -> dict[str, PodIdentity]:
    """Reuse all five owner/template/image checks before projecting the complete identity map."""
    reference = deepcopy(original)
    expected = deployment_map(reference)
    expected["payments-api"]["spec"], expected["risk-sim"]["spec"] = payments, risk
    runtime_identities(
        state,
        reference,
        object_value(expected["processor-adapter"]["spec"]),
        risk_image_id=risk_image_id,
    )
    documents = deployment_map(state)
    identities: dict[str, PodIdentity] = {}
    for service in SERVICES:
        pod = next(
            item
            for item in object_items(state["pods"])
            if object_value(object_value(item["metadata"])["labels"])["app.kubernetes.io/name"]
            == service
        )
        metadata = object_value(pod["metadata"])
        status = object_items(object_value(pod["status"])["containerStatuses"])[0]
        owner = next(
            row
            for row in object_items(metadata["ownerReferences"])
            if row.get("controller") is True
        )
        identities[service] = PodIdentity.model_validate(
            {
                "pod_name": metadata["name"],
                "pod_uid": metadata["uid"],
                "deployment_uid": object_value(documents[service]["metadata"])["uid"],
                "replica_set_uid": owner["uid"],
                "container_id": status["containerID"],
                "restart_count": status["restartCount"],
            }
        )
    return identities


def fresh_protocol_process(
    state: JsonObject, service: str, current: PodIdentity, previous: PodIdentity, requested_at: str
) -> None:
    """A rollout requires a new process created after its own recorded mutation request."""
    pod = next(
        item
        for item in object_items(state["pods"])
        if object_value(item["metadata"])["uid"] == current.pod_uid
    )
    created = timestamp(object_value(pod["metadata"]).get("creationTimestamp"))
    requested = timestamp(requested_at)
    if (
        service not in {"payments-api", "risk-sim"}
        or requested is None
        or created is None
        or not (requested.replace(microsecond=0) <= created <= datetime.now(UTC))
        or current.pod_uid == previous.pod_uid
        or current.container_id == previous.container_id
    ):
        raise ValueError("protocol rollout lacks a fresh owned process")
