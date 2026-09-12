"""CPU-noise transport must preserve ownership, bounds and atomic cleanup preconditions."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from test_noise_scenarios import RUN, fixture

from payops.scenarios.contracts import object_value
from payops.scenarios.noise_gateway import NoiseGateway


def gateway(tmp_path: Path) -> NoiseGateway:
    """Use explicit fixture configuration so no global kubeconfig can authorize a test."""
    config = tmp_path / "config"
    config.write_text("fixture")
    with patch("payops.scenarios.kubectl.shutil.which", return_value="kubectl"):
        return NoiseGateway(config)


def test_create_and_delete_use_reviewed_job_and_atomic_identity(tmp_path: Path) -> None:
    """The actual serialized delete must require the current Job UID and resource version."""
    adapter = gateway(tmp_path)
    job, _ = fixture()
    with (
        patch.object(adapter, "verify_scope"),
        patch(
            "payops.scenarios.hpa_gateway.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, "{}", ""),
        ) as write,
    ):
        adapter.create_noise(RUN)
        assert json.loads(write.call_args.kwargs["input"])["kind"] == "Job"
        adapter.remove_noise(job, RUN)
        body = json.loads(write.call_args.kwargs["input"])
        assert body["preconditions"] == {"uid": "owned", "resourceVersion": "1"}
        assert body["propagationPolicy"] == "Foreground"
        assert write.call_args.args[0][-5:] == (
            "delete",
            "--raw",
            "/apis/batch/v1/namespaces/payops-sandbox/jobs/cpu-noise-" + RUN,
            "-f",
            "-",
        )
        object_value(job["metadata"])["uid"] = ""
        with pytest.raises(ValueError, match="identity or template"):
            adapter.remove_noise(job, RUN)
        assert write.call_count == 2
    adapter.validate_target("processor-adapter")
    with pytest.raises(ValueError):
        adapter.validate_target("payments-api")


def test_inventory_and_logs_retain_identity_and_reject_wrong_or_capped_source(
    tmp_path: Path,
) -> None:
    """Complete inventories and bounded logs prevent hidden controllers or truncated CPU proof."""
    adapter = gateway(tmp_path)
    job, pod = fixture()
    with patch.object(adapter, "_json", side_effect=[{"items": [job]}, {"items": [pod]}]):
        assert adapter.noise_state(RUN) == {"jobs": [job], "pods": [pod]}
    with patch("payops.scenarios.noise_gateway.bounded_read", return_value=b"raw") as read:
        assert adapter.noise_log(pod, RUN) == {
            "text": "raw",
            "pod_uid": "pod",
            "pod_name": "cpu-noise-" + RUN + "-abcde",
        }
        assert "--limit-bytes=32768" in read.call_args.args[0]
        with pytest.raises(ValueError, match="invalid noise log source"):
            adapter.noise_log(pod, "b" * 32)
        assert read.call_count == 1
    with patch("payops.scenarios.noise_gateway.bounded_read", return_value=b"x" * 32768):
        with pytest.raises(ValueError, match="log capped"):
            adapter.noise_log(pod, RUN)
