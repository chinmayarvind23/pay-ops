"""Correlate retained kernel records with one verified payments process and request."""

from datetime import datetime, timedelta
from typing import Literal

from pydantic import Field

from payops.evidence.trace_span import Immutable, PodIdentity
from payops.sandbox.cpu_observation import KernelSnapshot
from payops.sandbox.models import Sample
from payops.scenarios.contracts import JsonObject
from payops.scenarios.cpu_counters import CpuDelta, CpuSnapshot, delta
from payops.scenarios.protocol_contract import PLAN as TRACE_PLAN
from payops.scenarios.protocol_gateway import protocol_identities
from payops.scenarios.sampling_gateway import SamplingGateway
from payops.tools.traces import bounded_read

MAX_BYTES = 131072


class CpuRecord(Immutable):
    """Only the fixed work record schema can become quota evidence."""

    event: Literal["synthetic.cpu"]
    sample_id: str
    rounds: Literal[50000]
    before: KernelSnapshot
    after: KernelSnapshot
    wall_seconds: float = Field(gt=0, le=5)
    thread_cpu_seconds: float = Field(ge=0, le=5)


def select_record(raw: bytes, sample: Sample, start: datetime, end: datetime) -> CpuRecord:
    """Require exactly one matching completion entirely inside the HTTP request window."""
    if len(raw) >= MAX_BYTES or len(raw.splitlines()) >= 2000:
        raise ValueError("CPU log source is capped")
    if start.tzinfo is None or end.tzinfo is None or not 0 < (end - start).total_seconds() <= 10:
        raise ValueError("CPU HTTP window is invalid")
    matches: list[CpuRecord] = []
    for line in raw.decode("utf-8").splitlines():
        stamp, _, payload = line.partition(" ")
        if '"synthetic.cpu"' not in payload:
            continue
        record = CpuRecord.model_validate_json(payload)
        if record.sample_id != sample.sample_id:
            continue
        occurred = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        tolerance = timedelta(seconds=TRACE_PLAN.clock_tolerance_seconds)
        if (
            not start - tolerance
            <= record.before.started_at
            <= record.after.completed_at
            <= occurred
            <= end + tolerance
        ):
            raise ValueError("CPU completion is outside its request window")
        gap = (record.after.monotonic_started_ns - record.before.monotonic_completed_ns) / 1e9
        if (
            not 0 <= gap <= 30
            or not 0 <= record.wall_seconds <= gap + 0.001
            or record.thread_cpu_seconds > record.wall_seconds + 0.001
        ):
            raise ValueError("CPU work duration exceeds acquisition interval")
        matches.append(record)
    if len(matches) != 1:
        raise ValueError("CPU request lacks exactly one kernel completion")
    return matches[0]


def record_delta(record: CpuRecord, incident: str, identity: PodIdentity) -> CpuDelta:
    """Attach collector-established ownership to both raw snapshots before arithmetic."""
    record = CpuRecord.model_validate_json(record.model_dump_json())
    snapshots = [
        CpuSnapshot.model_validate(
            {
                **item.model_dump(exclude={"monotonic_started_ns", "monotonic_completed_ns"}),
                "incident_id": incident,
                "identity": identity,
            }
        )
        for item in (record.before, record.after)
    ]
    return delta(*snapshots)


class CpuGateway(SamplingGateway):
    """Only the payments Deployment can change in the CPU quota experiment."""

    @staticmethod
    def validate_target(name: str) -> None:
        """Keep inherited compare-and-swap writes within the single reviewed target."""
        if name != "payments-api":
            raise ValueError("CPU scenario target must be payments-api")

    def cpu_log(
        self,
        identity: PodIdentity,
        original: JsonObject,
        payments: JsonObject,
        risk: JsonObject,
        start: datetime,
    ) -> bytes:
        """Verify all owned runtime specs before and after a bounded current-container read."""
        if start.tzinfo is None:
            raise ValueError("CPU log start must be aware")
        before = protocol_identities(self.state(), original, payments, risk)
        if before["payments-api"] != identity:
            raise ValueError("CPU source process differs from probe")
        raw = bounded_read(
            (
                *self._prefix,
                "logs",
                identity.pod_name,
                "--container=sandbox",
                "--timestamps=true",
                "--tail=2000",
                f"--since-time={start.isoformat()}",
                f"--limit-bytes={MAX_BYTES}",
            ),
            MAX_BYTES,
            12,
        )
        after = protocol_identities(self.state(), original, payments, risk)
        if before != after:
            raise ValueError("CPU source runtime changed during log acquisition")
        if len(raw) >= MAX_BYTES or len(raw.splitlines()) >= 2000:
            raise ValueError("CPU log source is capped")
        return raw
