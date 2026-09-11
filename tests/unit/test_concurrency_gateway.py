"""The reused transport keeps payments experiment writes and reads within their fixed target."""

from pathlib import Path
from unittest.mock import patch

import pytest
from test_sampling_harness import Clock, SamplingCluster

from payops.scenarios.concurrency_gateway import ConcurrencyGateway
from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.protocol_gateway import protocol_identities
from payops.scenarios.sampling_gateway import deployment_map


def gateway(tmp_path: Path) -> ConcurrencyGateway:
    """Intercept executable discovery while leaving all scope checks enabled."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        return ConcurrencyGateway(config)


@pytest.mark.parametrize("previous", [False, True])
def test_fixed_payments_log_and_snapshot(tmp_path: Path, previous: bool) -> None:
    """Current and previous reads share the inherited byte limits and exact payments selector."""
    adapter = gateway(tmp_path)
    pod: JsonObject = {
        "metadata": {
            "name": "payments-api-abc-xyz",
            "namespace": "payops-sandbox",
            "uid": "owned",
        }
    }
    with patch("payops.scenarios.leak_gateway.bounded_read", return_value=b"raw\n") as read:
        assert adapter.read_log(pod, previous)["text"] == "raw\n"
    args = read.call_args.args[0]
    assert "pod/payments-api-abc-xyz" in args
    assert ("--previous=true" in args) is previous
    assert read.call_args.args[1:] == (262144, 12)
    with (
        patch.object(adapter, "observe", return_value={"pods": []}) as observe,
        patch.object(adapter, "_json", return_value={"items": []}) as query,
    ):
        assert adapter.snapshot()["replica_sets"] == []
    observe.assert_called_once_with("payments-api")
    query.assert_called_once_with(
        ("get", "replicasets", "-l", "app.kubernetes.io/name=payments-api")
    )


def test_foreign_targets_reject_before_transport(tmp_path: Path) -> None:
    """The subclass cannot acquire risk logs or use inherited CAS for other services."""
    adapter = gateway(tmp_path)
    adapter.validate_target("payments-api")
    for name in ("risk-sim", "processor-adapter", "nodes"):
        with pytest.raises(ValueError):
            adapter.validate_target(name)
    pod: JsonObject = {
        "metadata": {
            "name": "risk-sim-abc-xyz",
            "namespace": "payops-sandbox",
            "uid": "foreign",
        }
    }
    with patch("payops.scenarios.leak_gateway.bounded_read") as read:
        with pytest.raises(ValueError):
            adapter.read_log(pod, True)
    read.assert_not_called()


def test_payments_override_preserves_every_peer_image_check() -> None:
    """A reviewed payments image cannot authorize a wrong digest or changed peer image."""
    cluster = SamplingCluster(Clock())
    original, changed = cluster.state(), cluster.state()
    documents = deployment_map(original)
    payments = object_value(documents["payments-api"]["spec"])
    risk = object_value(documents["risk-sim"]["spec"])
    pods = {
        str(object_value(p["metadata"])["name"]).split("-pod")[0]: p
        for p in object_items(changed["pods"])
    }
    payment_pod = next(
        p
        for p in pods.values()
        if str(object_value(p["metadata"])["name"]).startswith("payments-api-")
    )
    object_items(object_value(payment_pod["status"])["containerStatuses"])[0]["imageID"] = (
        "reviewed"
    )
    with pytest.raises(ValueError, match="image identity"):
        protocol_identities(changed, original, payments, risk)
    assert "payments-api" in protocol_identities(
        changed, original, payments, risk, payments_image_id="reviewed"
    )
    with pytest.raises(ValueError, match="image identity"):
        protocol_identities(changed, original, payments, risk, payments_image_id="wrong")
    peer = next(
        p for p in pods.values() if str(object_value(p["metadata"])["name"]).startswith("risk-sim-")
    )
    object_items(object_value(peer["status"])["containerStatuses"])[0]["imageID"] = "foreign"
    with pytest.raises(ValueError, match="image identity"):
        protocol_identities(changed, original, payments, risk, payments_image_id="reviewed")
