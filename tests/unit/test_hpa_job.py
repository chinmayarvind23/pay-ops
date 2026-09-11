"""A job identity cannot broaden its image, namespace or retry budget."""

import pytest

from payops.scenarios.contracts import object_items, object_value
from payops.scenarios.hpa_job import load_job


@pytest.mark.parametrize("identity", ["../other", "A" * 32, "a" * 31, "a" * 33, ""])
def test_invalid_run_identity_rejects(identity: str) -> None:
    """Unsafe or ambiguous identities must fail before any Kubernetes resource is built."""
    with pytest.raises(ValueError):
        load_job(identity)


def test_job_has_no_retry_or_api_token_and_does_not_share_mutable_state() -> None:
    """A failed batch must retain its original outcome and cannot restart as a new success."""
    job = load_job("a" * 32)
    spec = object_value(job["spec"])
    pod = object_value(object_value(spec["template"])["spec"])
    assert spec["backoffLimit"] == 0 and spec["activeDeadlineSeconds"] == 210
    assert pod["restartPolicy"] == "Never" and pod["automountServiceAccountToken"] is False
    item = object_items(pod["containers"])[0]
    assert item["command"] == ["python", "-m", "payops.scenarios.hpa_load"]
    item["image"] = "foreign"
    assert load_job("a" * 32) != job
