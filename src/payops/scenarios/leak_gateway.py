"""Fixed risk-only mutation target and bounded raw current/previous container log reads."""

import re
from datetime import UTC, datetime

from payops.evidence.artifacts import JSON_OBJECT
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.sampling_gateway import SamplingGateway
from payops.tools.traces import bounded_read

LOG_BYTES = 262144


class LeakGateway(SamplingGateway):
    """The trusted lifecycle runner can alter only risk-sim in the dedicated local namespace."""

    @staticmethod
    def validate_target(name: str) -> None:
        """Reject runtime targets outside the single reviewed fault surface."""
        if name != "risk-sim":
            raise ValueError("leak scenario only permits risk-sim")

    def _json(self, args: tuple[str, ...]) -> JsonObject:
        """Cap API bytes as well as parsed object counts, including inherited scope reads."""
        raw = bounded_read((*self._prefix, *args, "-o", "json"), 524288, 12)
        if len(raw) >= 524288:
            raise ValueError("leak Kubernetes response is capped")
        return JSON_OBJECT.validate_json(raw)

    def snapshot(self) -> JsonObject:
        """Retain the risk controller chain without silently filtering multiple target pods."""
        result = self.observe("risk-sim")
        replicas = object_items(
            self._json(("get", "replicasets", "-l", "app.kubernetes.io/name=risk-sim"))["items"]
        )
        if len(object_items(result["pods"])) > 2 or len(replicas) > 32:
            raise ValueError("risk rollout exceeds reviewed object counts")
        result["replica_sets"] = list(replicas)
        return result

    def read_log(self, pod: JsonObject, previous: bool) -> JsonObject:
        """Return raw logs for persistence before the caller validates their lifetime."""
        metadata = object_value(pod["metadata"])
        name = str(metadata.get("name", ""))
        if (
            re.fullmatch(r"risk-sim-[a-z0-9]+-[a-z0-9]+", name) is None
            or metadata.get("namespace") != "payops-sandbox"
            or not metadata.get("uid")
            or type(previous) is not bool
        ):
            raise ValueError("invalid risk log identity")
        args = (
            *self._prefix,
            "logs",
            "pod/" + name,
            "--container=sandbox",
            "--timestamps=true",
            "--tail=2000",
            "--since=3m",
            f"--limit-bytes={LOG_BYTES}",
        )
        raw = bounded_read((*args, "--previous=true") if previous else args, LOG_BYTES, 12)
        if len(raw) >= LOG_BYTES:
            raise ValueError("risk log capture is capped")
        return {
            "pod_uid": metadata["uid"],
            "pod_name": name,
            "previous": previous,
            "captured_at": datetime.now(UTC).isoformat(),
            "text": raw.decode("utf-8"),
        }
