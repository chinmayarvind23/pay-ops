"""CPU throttling evidence cannot be fabricated through resets, quota drift or stale identities."""

from datetime import timedelta
from typing import Any

import pytest

from payops.contracts import utc_now
from payops.evidence.trace_span import PodIdentity
from payops.scenarios.cpu_counters import CpuSnapshot, counters, delta, quota

RAW = (
    "usage_usec 1000\nuser_usec 900\nsystem_usec 100\nnr_periods 10\n"
    "nr_throttled 2\nthrottled_usec 500\nnr_bursts 0\n"
)


def pair() -> tuple[CpuSnapshot, CpuSnapshot]:
    """Use independent known counter increases with a stable owned-process identity."""
    now = utc_now()
    before = CpuSnapshot(
        incident_id="cpu-test",
        identity=PodIdentity(
            pod_name="payments-api-test",
            pod_uid="pod",
            deployment_uid="deployment",
            replica_set_uid="rs",
            container_id="containerd://abc",
            restart_count=0,
        ),
        started_at=now,
        completed_at=now + timedelta(milliseconds=1),
        cpu_stat=RAW,
        cpu_max="10000 100000\n",
    )
    after = before.model_copy(
        update={
            "started_at": now + timedelta(seconds=2),
            "completed_at": now + timedelta(seconds=2.001),
            "cpu_stat": (
                "usage_usec 201000\nuser_usec 190900\nsystem_usec 10100\nnr_periods 30\n"
                "nr_throttled 20\nthrottled_usec 1500500\nnr_bursts 0\n"
            ),
        }
    )
    return before, after


def test_integer_counter_deltas_keep_kernel_units() -> None:
    """Known arithmetic distinguishes consumed CPU from time lost to bandwidth throttling."""
    before, after = pair()
    assert delta(before, after).model_dump() == {
        "usage_usec": 200000,
        "nr_periods": 20,
        "nr_throttled": 18,
        "throttled_usec": 1500000,
    }
    assert quota(before.cpu_max) == (10000, 100000)


@pytest.mark.parametrize(
    "raw",
    [
        "",
        RAW + "usage_usec 1\n",
        RAW.replace("1000", "-1"),
        RAW.replace("1000", "1.0"),
        RAW.replace("1000", "9223372036854775808"),
        RAW.replace("nr_periods 10\n", ""),
        "x" * 4097,
        RAW + "extra 0\n" * 33,
    ],
)
def test_malformed_or_missing_kernel_counters_fail(raw: str) -> None:
    """Counter absence never becomes zero; malformed and duplicate fields cannot be averaged."""
    with pytest.raises(ValueError):
        counters(raw)


@pytest.mark.parametrize(
    "raw",
    [
        "max 100000\n",
        "0 100000",
        "999 100000",
        "10000 0",
        "10000 1000001",
        "10000 100000 extra",
        "10000\t100000",
    ],
)
def test_unbounded_or_malformed_quota_fails(raw: str) -> None:
    """A configured deployment quantity cannot replace the actual finite cpu.max record."""
    with pytest.raises(ValueError):
        quota(raw)


@pytest.mark.parametrize(
    "fault",
    [
        "incident",
        "pod",
        "restart",
        "container",
        "quota",
        "overlap",
        "stale",
        "slow",
        "reset",
        "extra_reset",
        "fields",
        "impossible",
        "malformed",
    ],
)
def test_valid_shaped_pair_changes_are_rejected(fault: str) -> None:
    """Unchecked immutable copies still undergo full validation at the arithmetic boundary."""
    before, after = pair()
    changes: dict[str, Any] = {
        "incident": {"incident_id": "other"},
        "quota": {"cpu_max": "50000 100000"},
        "overlap": {"started_at": before.started_at},
        "stale": {
            "started_at": before.started_at + timedelta(seconds=31),
            "completed_at": before.started_at + timedelta(seconds=31.1),
        },
        "slow": {"completed_at": after.started_at + timedelta(seconds=3)},
        "reset": {"cpu_stat": after.cpu_stat.replace("usage_usec 201000", "usage_usec 999")},
        "fields": {"cpu_stat": after.cpu_stat + "extra 0\n"},
        "impossible": {"cpu_stat": after.cpu_stat.replace("nr_throttled 20", "nr_throttled 99")},
        "malformed": {"cpu_stat": "unavailable"},
    }.get(fault, {})
    if fault in {"pod", "restart", "container"}:
        key, value = {
            "pod": ("pod_uid", "other"),
            "restart": ("restart_count", 1),
            "container": ("container_id", "other"),
        }[fault]
        changes = {"identity": after.identity.model_copy(update={key: value})}
    if fault == "extra_reset":
        before = before.model_copy(
            update={"cpu_stat": before.cpu_stat.replace("nr_bursts 0", "nr_bursts 1")}
        )
    with pytest.raises(ValueError):
        delta(before, after.model_copy(update=changes))
