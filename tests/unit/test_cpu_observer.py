"""Concrete stage collection retains each bounded path and CPU source without overwriting."""

from pathlib import Path
from typing import Any, cast

import pytest
from test_cpu_harness import Observer
from test_sampling_harness import Clock, SamplingCluster

from payops.evidence.artifacts import ArtifactStore
from payops.scenarios.contracts import object_value
from payops.scenarios.cpu_contract import CpuStage
from payops.scenarios.cpu_observer import RuntimeCpuObserver, verify_observation
from payops.scenarios.cpu_sources import CpuGateway
from payops.scenarios.protocol_gateway import protocol_identities
from payops.scenarios.protocol_observation import ProtocolObservation
from payops.scenarios.sampling_gateway import deployment_map


@pytest.mark.parametrize("stage", ["original", "control"])
def test_runtime_adapter_retains_three_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: CpuStage
) -> None:
    """Only subprocess transport is replaced; real collection layout and verification run."""
    clock = Clock()
    cluster = SamplingCluster(clock)
    state = cluster.state()
    documents = deployment_map(state)
    payments, risk = (
        object_value(documents["payments-api"]["spec"]),
        object_value(documents["risk-sim"]["spec"]),
    )
    identities = protocol_identities(state, state, payments, risk)
    store = ArtifactStore(tmp_path / "artifacts")
    expected = Observer(clock).collect(
        stage, "incident", identities, tmp_path, store, state, payments, risk
    )
    paths = iter(expected.paths)
    logs = iter(expected.logs)

    class Paths:
        """Return independently sourced fixture traces through the concrete adapter contract."""

        def collect(self, *args: Any) -> ProtocolObservation:
            """All requests use successful original protocol semantics."""
            assert args[0] == "original"
            return next(paths)

    class Gateway:
        """Match each fixed log request to its already verified payments identity."""

        def cpu_log(self, *args: Any) -> bytes:
            """Retain exact source bytes and require the same current payments process."""
            assert args[0] == identities["payments-api"]
            return next(logs).encode()

    def factory(kubeconfig: Path) -> Paths:
        """Avoid constructing network adapters while retaining the real outer observer."""
        return Paths()

    monkeypatch.setattr("payops.scenarios.cpu_observer.RuntimeProtocolObserver", factory)
    observer = RuntimeCpuObserver(tmp_path / "kubeconfig", cast(CpuGateway, Gateway()))
    actual = observer.collect(stage, "incident", identities, tmp_path, store, state, payments, risk)
    assert actual == expected
    records = verify_observation(stage, actual, store)
    assert len(records) == (3 if stage == "control" else 0)
    for index, raw in enumerate(expected.logs):
        assert (tmp_path / stage / str(index) / "cpu.log").read_bytes() == raw.encode()
    with pytest.raises(ValueError):
        verify_observation(stage, actual.model_copy(update={"paths": actual.paths[:2]}), store)
    if stage == "original":
        with pytest.raises(ValueError):
            verify_observation(
                stage, actual.model_copy(update={"logs": ("unexpected", "", "")}), store
            )
