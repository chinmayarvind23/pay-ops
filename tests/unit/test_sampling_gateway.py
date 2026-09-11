"""Reject ownership, template, image and freshness substitutions at the runtime boundary."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.sampling_gateway import (
    SamplingGateway,
    runtime_identities,
    validate_runtime_baseline,
)


def mutate(state: JsonObject, field: str) -> None:
    """Change one independently meaningful runtime boundary."""
    pod = object_items(state["pods"])[1]
    metadata = object_value(pod["metadata"])
    status = object_items(object_value(pod["status"])["containerStatuses"])[0]
    deployment = object_items(state["deployments"])[1]
    replica = object_items(state["replicas"])[1]
    changes = {
        "image": lambda: status.update(imageID="sha256:substituted"),
        "restart": lambda: status.update(restartCount=1),
        "unready": lambda: status.update(ready=False),
        "namespace": lambda: metadata.update(namespace="foreign"),
        "owner": lambda: object_items(metadata["ownerReferences"])[0].update(uid="foreign"),
        "deployment": lambda: object_value(deployment["metadata"]).update(uid="foreign"),
        "generation": lambda: object_value(deployment["status"]).update(observedGeneration=0),
        "replica": lambda: object_items(object_value(replica["metadata"])["ownerReferences"])[
            0
        ].update(uid="foreign"),
        "command": lambda: object_items(object_value(pod["spec"])["containers"])[0].update(
            command=["unreviewed"]
        ),
        "init": lambda: object_value(pod["spec"]).update(initContainers=[{"name": "extra"}]),
        "extra": lambda: object_items(state["pods"]).append(deepcopy(pod)),
    }
    if field == "extra":
        state["pods"] = [*object_items(state["pods"]), deepcopy(pod)]
    else:
        changes[field]()


@pytest.mark.parametrize(
    "field",
    [
        "image",
        "restart",
        "unready",
        "namespace",
        "owner",
        "deployment",
        "generation",
        "replica",
        "command",
        "init",
        "extra",
    ],
)
def test_runtime_rejects_independent_substitutions(field: str) -> None:
    """An otherwise healthy matching-label process cannot replace the reviewed one."""
    cluster = SamplingCluster(Clock())
    original, changed = cluster.state(), cluster.state()
    validate_runtime_baseline(original)
    mutate(changed, field)
    with pytest.raises(ValueError):
        runtime_identities(
            changed, original, object_value(cluster.documents["processor-adapter"]["spec"])
        )


@pytest.mark.parametrize("created", ["2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z", "bad"])
def test_new_process_must_have_valid_post_mutation_time(created: str) -> None:
    """New UID alone cannot qualify a startup sampler change."""
    cluster = SamplingCluster(Clock())
    original = cluster.state()
    spec = object_value(cluster.documents["processor-adapter"]["spec"])
    previous = runtime_identities(original, original, spec)[1]
    changed = deepcopy(original)
    pod = object_items(changed["pods"])[1]
    object_value(pod["metadata"]).update(uid="new-uid", creationTimestamp=created)
    object_items(object_value(pod["status"])["containerStatuses"])[0]["containerID"] = (
        "new-container"
    )
    with pytest.raises(ValueError, match="fresh"):
        runtime_identities(changed, original, spec, cluster.clock.now().isoformat(), previous)


def test_sampler_change_rejects_reused_process() -> None:
    """The current process cannot be offered as its own post-mutation replacement."""
    cluster = SamplingCluster(Clock())
    state = cluster.state()
    spec = object_value(cluster.documents["processor-adapter"]["spec"])
    previous = runtime_identities(state, state, spec)[1]
    with pytest.raises(ValueError, match="fresh"):
        runtime_identities(state, state, spec, cluster.clock.now().isoformat(), previous)


def gateway(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SamplingGateway:
    """Fake only executable discovery; no process or real kubeconfig is used."""
    config = tmp_path / "kubeconfig"
    config.write_text("fixture", encoding="utf-8")

    def executable(_: str) -> str:
        """Return a nonexecutable marker; the process boundary remains mocked."""
        return "fixture-kubectl"

    monkeypatch.setattr("payops.scenarios.kubectl.shutil.which", executable)
    return SamplingGateway(config)


def test_snapshot_fixed_argv_and_budgets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exactly three closed reads share the deadline and byte limit."""
    access = gateway(tmp_path, monkeypatch)
    calls: list[tuple[str, ...]] = []

    def read(argv: tuple[str, ...], limit: int, timeout: float) -> str:
        """Record fixed scope and reject an unbounded subprocess contract."""
        calls.append(argv)
        assert limit == 262144 and 0 < timeout <= 12
        assert "kind-payops-dev" in argv and "payops-sandbox" in argv
        return json.dumps({"items": []})

    monkeypatch.setattr("payops.scenarios.sampling_gateway.bounded_read", read)
    assert access.state() == {"deployments": [], "pods": [], "replicas": []}
    assert [call[-3] for call in calls] == ["deployments", "pods", "replicasets"]
    for timeout in (0, 31):
        with pytest.raises(ValueError):
            access.state(timeout)


def test_snapshot_rejects_unexpected_namespace_users(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Extra deployments cannot disappear behind a favorable label selector."""
    access = gateway(tmp_path, monkeypatch)

    def oversized(*_: object) -> str:
        """Return one more Deployment than the frozen namespace limit."""
        return json.dumps({"items": [{}] * 6})

    monkeypatch.setattr("payops.scenarios.sampling_gateway.bounded_read", oversized)
    with pytest.raises(ValueError, match="bound"):
        access.state()


@pytest.mark.parametrize("times", [(0, 31), (0, 1, 2, 3, 31)])
def test_snapshot_enforces_shared_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, times: tuple[int, ...]
) -> None:
    """The deadline covers both the first invocation and completion of all three reads."""
    access = gateway(tmp_path, monkeypatch)
    clock = iter(times)
    monkeypatch.setattr("payops.scenarios.sampling_gateway.monotonic", lambda: next(clock))

    def read(*_: object) -> str:
        """Read latency is represented solely by the injected monotonic clock."""
        return '{"items": []}'

    monkeypatch.setattr("payops.scenarios.sampling_gateway.bounded_read", read)
    with pytest.raises(TimeoutError):
        access.state()


@pytest.mark.parametrize("field", ["deployments", "peer_image", "pod_label"])
def test_preflight_rejects_changed_baseline(field: str) -> None:
    """Scope and normal-image requirements also apply before the first healthy control."""
    from payops.scenarios.recipes import container

    state = SamplingCluster(Clock()).state()
    if field == "deployments":
        state["deployments"] = []
    elif field == "peer_image":
        container(object_value(object_items(state["deployments"])[2]["spec"]))["image"] = "other"
    else:
        metadata = object_value(object_items(state["pods"])[1]["metadata"])
        metadata["labels"] = {"app.kubernetes.io/name": "payments-api"}
    with pytest.raises(ValueError):
        validate_runtime_baseline(state)
