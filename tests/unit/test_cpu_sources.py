"""CPU sources must match the request, bounded acquisition and unchanged owned process."""

from datetime import timedelta
from typing import Any

import pytest
from test_cpu_counters import pair

from payops.evidence.trace_span import PodIdentity
from payops.sandbox.cpu_observation import KernelSnapshot
from payops.sandbox.models import Sample
from payops.scenarios.cpu_sources import (
    MAX_BYTES,
    CpuGateway,
    CpuRecord,
    record_delta,
    select_record,
)


def source() -> tuple[CpuRecord, bytes]:
    """Use independently known counter differences with timestamped Kubernetes log framing."""
    before, after = pair()
    record = CpuRecord(
        event="synthetic.cpu",
        sample_id="synthetic-source",
        rounds=50000,
        before=KernelSnapshot.model_validate(
            before.model_dump(exclude={"incident_id", "identity"})
        ),
        after=KernelSnapshot.model_validate(after.model_dump(exclude={"incident_id", "identity"})),
        wall_seconds=1.9,
        thread_cpu_seconds=0.2,
    )
    raw = f"{after.completed_at.isoformat()} {record.model_dump_json()}\n".encode()
    return record, raw


def test_known_request_projects_actual_counter_difference() -> None:
    """Unrelated ordinary console lines do not become CPU measurements."""
    record, raw = source()
    actual = select_record(
        b"ordinary log\n" + raw,
        Sample(sample_id=record.sample_id),
        record.before.started_at,
        record.after.completed_at,
    )
    assert actual == record
    assert record_delta(actual, "incident", pair()[0].identity).usage_usec == 200000


@pytest.mark.parametrize(
    "fault",
    ["missing", "duplicate", "bytes", "lines", "stale", "duration", "cpu", "window", "naive"],
)
def test_unusable_source_cannot_qualify(fault: str) -> None:
    """Well-shaped records still fail when their source census or time scope changes."""
    record, raw = source()
    start, end = record.before.started_at, record.after.completed_at
    if fault == "missing":
        raw = raw.replace(b"synthetic-source", b"synthetic-other")
    elif fault == "duplicate":
        raw *= 2
    elif fault == "bytes":
        raw = b"x" * MAX_BYTES
    elif fault == "lines":
        raw = b"\n" * 2000
    elif fault == "stale":
        start += timedelta(milliseconds=1)
    elif fault == "duration":
        raw = raw.replace(b'"wall_seconds":1.9', b'"wall_seconds":4.9')
    elif fault == "cpu":
        raw = raw.replace(b'"thread_cpu_seconds":0.2', b'"thread_cpu_seconds":4.9')
    elif fault == "window":
        end = start + timedelta(seconds=11)
    else:
        start = start.replace(tzinfo=None)
    with pytest.raises(ValueError):
        select_record(raw, Sample(sample_id=record.sample_id), start, end)


@pytest.mark.parametrize("fault", ["none", "before", "after", "cap", "naive"])
def test_gateway_checks_runtime_around_fixed_log_command(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Both runtime snapshots are checked and only the exact owned container is read."""
    identity = pair()[0].identity
    maps = [{"payments-api": identity}, {"payments-api": identity}]
    if fault in {"before", "after"}:
        maps[0 if fault == "before" else 1] = {
            "payments-api": identity.model_copy(update={"container_id": "changed"})
        }
    states = iter(maps)
    gateway = object.__new__(CpuGateway)
    monkeypatch.setattr(
        gateway, "_prefix", ("kubectl", "--namespace", "payops-sandbox"), raising=False
    )
    monkeypatch.setattr(gateway, "state", lambda: next(states))

    def identities(state: dict[str, PodIdentity], *args: Any) -> dict[str, PodIdentity]:
        """Use explicit identity transitions; shared runtime verification has separate tests."""
        return state

    monkeypatch.setattr("payops.scenarios.cpu_sources.protocol_identities", identities)
    calls: list[tuple[str, ...]] = []

    def read(args: tuple[str, ...], maximum: int, timeout: float) -> bytes:
        """Capture argv and bounds without launching kubectl in the fixture test."""
        calls.append(args)
        assert maximum == MAX_BYTES and timeout == 12
        assert args[3:6] == ("logs", identity.pod_name, "--container=sandbox")
        return b"x" * MAX_BYTES if fault == "cap" else b"retained"

    monkeypatch.setattr("payops.scenarios.cpu_sources.bounded_read", read)
    start = pair()[0].started_at
    if fault == "naive":
        start = start.replace(tzinfo=None)
    if fault == "none":
        assert gateway.cpu_log(identity, {}, {}, {}, start) == b"retained"
    else:
        with pytest.raises(ValueError):
            gateway.cpu_log(identity, {}, {}, {}, start)
    assert len(calls) == (0 if fault in {"before", "naive"} else 1)


@pytest.mark.parametrize("name", ["payments-api", "risk-sim", "processor-adapter", "other"])
def test_cpu_write_scope_is_one_deployment(name: Any) -> None:
    """The inherited mutation capability remains narrower than generic scenario targets."""
    if name == "payments-api":
        CpuGateway.validate_target(name)
    else:
        with pytest.raises(ValueError):
            CpuGateway.validate_target(name)
