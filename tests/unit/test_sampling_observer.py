"""Exercise actual observer arithmetic and capture ordering with virtual bounded readers."""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import test_sampling_observation as fixtures
from test_payment_window import raw_snapshot, rows, set_value
from test_sampling_harness import Clock

from payops.evidence.artifacts import ArtifactStore
from payops.evidence.normalize import Observation
from payops.evidence.payment_window import (
    COUNT,
    REQUESTS,
    SUM,
    MeasurementInterval,
    Service,
    snapshot_observation,
)
from payops.evidence.trace_span import TraceScope
from payops.scenarios.sampling_contract import sampling_workload
from payops.scenarios.sampling_observation import verify_sampling_stage
from payops.scenarios.sampling_observer import RuntimeSamplingObserver
from payops.scenarios.traffic import TrafficReceipt, Workload
from payops.tools.traces import TraceCollection


class Sources:
    """A fixed virtual batch feeds real raw snapshots and published trace artifacts."""

    def __init__(self, clock: Clock) -> None:
        """The first fresh scrape occurs six seconds after the readiness boundary."""
        self.clock, self.ready_at = clock, clock.now()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(fixtures, "utc_now", lambda: clock.now() + timedelta(seconds=106))
            self.traffic = fixtures.traffic()
        self.period = MeasurementInterval(
            incident_id="sampling", start=self.traffic.started_at, end=self.traffic.completed_at
        )
        self.snapshots: list[Service] = []
        self.captures: list[datetime] = []
        self.workloads: list[Workload] = []
        self.cleaned = False
        self.stale = False
        self.cancel = False
        self.capture_delay = 1

    def sleep(self, seconds: float) -> None:
        """Advance time without a real sleep or process."""
        assert 0 < seconds <= 17
        self.clock.value += timedelta(seconds=seconds)

    def snapshot(self, service: Service) -> Observation:
        """Return full initialized metric vectors through the real snapshot validator."""
        after = len(self.snapshots) >= 2
        self.snapshots.append(service)
        raw = raw_snapshot(self.period, after, service)
        if after:
            for processor in ("A", "B"):
                set_value(
                    raw,
                    REQUESTS,
                    "4",
                    processor=processor,
                    region="us",
                    payment_method="credit",
                    status="accepted",
                )
                set_value(raw, COUNT, "4", processor=processor, region="us")
                set_value(raw, SUM, "0.04", processor=processor, region="us")
        elif self.stale:
            rows(raw, "watermark")[0]["value"][1] = str(self.ready_at.timestamp() - 1)
        return snapshot_observation(raw, service)

    async def run(self, workload: Workload) -> TrafficReceipt:
        """Record exactly one bounded batch; cancellation still closes its transport scope."""
        self.workloads.append(workload)
        try:
            if self.cancel:
                raise asyncio.CancelledError("fixture cancellation")
            assert self.clock.now() <= self.traffic.started_at
            self.clock.value = self.traffic.completed_at
            return self.traffic
        finally:
            self.cleaned = True

    def collect(self, scopes: tuple[TraceScope, ...], store: ArtifactStore) -> TraceCollection:
        """Record actual capture instants and publish independently verified source logs."""
        self.captures.append(self.clock.now())
        assert tuple(scope.service for scope in scopes) == ("payments-api", "processor-adapter")
        offset = int((self.clock.now() - self.traffic.completed_at).total_seconds())
        logs = tuple(fixtures.log(self.traffic, scope.service, False, offset) for scope in scopes)
        result = fixtures.collection(store, (logs[0], logs[1]))
        self.clock.value += timedelta(seconds=self.capture_delay)
        return result


def setup(tmp_path: Path) -> tuple[RuntimeSamplingObserver, Sources, ArtifactStore]:
    """No default runtime reader or process is instantiated by this fixture."""
    clock = Clock()
    sources = Sources(clock)

    def driver(_: Path) -> Sources:
        """The typed transport substitution remains operator-only."""
        return sources

    observer = RuntimeSamplingObserver(
        tmp_path / "kubeconfig", sources, sources, driver, clock.now, sources.sleep
    )
    return observer, sources, ArtifactStore(tmp_path / "artifacts")


def collect(
    observer: RuntimeSamplingObserver, sources: Sources, store: ArtifactStore, tmp_path: Path
) -> None:
    """Run and independently verify all arithmetic, source identity and request parents."""
    result = observer.collect(
        "original",
        "sampling",
        (
            fixtures.identity("payments-api"),
            fixtures.identity("processor-adapter", sources.traffic),
        ),
        sources.ready_at,
        tmp_path,
        store,
    )
    verify_sampling_stage("original", result, store)


def test_actual_adapter_has_four_snapshots_one_batch_and_two_captures(tmp_path: Path) -> None:
    """No readiness request contaminates the eight-request census between metric snapshots."""
    observer, sources, store = setup(tmp_path)
    collect(observer, sources, store, tmp_path)
    assert sources.snapshots == ["payments-api", "processor-adapter"] * 2
    assert sources.workloads == [sampling_workload()] and sources.cleaned
    assert sources.captures == [
        sources.traffic.completed_at + timedelta(seconds=n) for n in (12, 17)
    ]
    assert len(tuple((tmp_path / "original").glob("*.json"))) == 5


def test_stale_before_scrape_prevents_traffic(tmp_path: Path) -> None:
    """A recent-looking evaluation time cannot hide a scrape preceding process readiness."""
    observer, sources, store = setup(tmp_path)
    sources.stale = True
    with pytest.raises(ValueError, match="predates"):
        collect(observer, sources, store, tmp_path)
    assert not sources.workloads and not sources.captures
    assert (tmp_path / "original" / "before.json").is_file()


def test_cancelled_driver_closes_before_propagating(tmp_path: Path) -> None:
    """Interrupted traffic cannot proceed into false post-batch metric or trace observations."""
    observer, sources, store = setup(tmp_path)
    sources.cancel = True
    with pytest.raises(asyncio.CancelledError):
        collect(observer, sources, store, tmp_path)
    assert sources.cleaned and len(sources.snapshots) == 2 and not sources.captures


def test_slow_first_capture_cannot_extend_second_window_past_bound(tmp_path: Path) -> None:
    """Actual elapsed time, rather than the requested 17-second schedule, gates the window."""
    observer, sources, store = setup(tmp_path)
    sources.capture_delay = 121
    with pytest.raises(ValueError, match="interval"):
        collect(observer, sources, store, tmp_path)
    assert len(sources.captures) == 1


@pytest.mark.parametrize("naive", [True, False])
def test_invalid_readiness_time_never_touches_sources(tmp_path: Path, naive: bool) -> None:
    """Future or timezone-free readiness boundaries cannot start a census."""
    observer, sources, store = setup(tmp_path)
    sources.ready_at = (
        sources.ready_at.replace(tzinfo=None) if naive else sources.ready_at + timedelta(seconds=1)
    )
    with pytest.raises(ValueError, match="readiness"):
        collect(observer, sources, store, tmp_path)
    assert not sources.snapshots and not sources.workloads
