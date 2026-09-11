"""Counterfactuals keep the fixed sampling experiment and real SDK behavior reviewable."""

import json
import os
import subprocess
import sys
from copy import deepcopy

import pytest
from pydantic import ValidationError

from payops.scenarios.contracts import JsonObject, object_items, object_value
from payops.scenarios.recipes import container
from payops.scenarios.sampling_contract import (
    PLAN,
    SamplingPlan,
    sampling_payment_baseline,
    sampling_specs,
    sampling_workload,
)


def document() -> JsonObject:
    """Use an independent normal processor document with captured CAS and resource fields."""
    return {
        "metadata": {
            "name": "processor-adapter",
            "namespace": "payops-sandbox",
            "uid": "processor-uid",
            "resourceVersion": "1",
            "generation": 1,
            "labels": {"app.kubernetes.io/part-of": "payops"},
        },
        "spec": {
            "replicas": 1,
            "strategy": {"type": "RollingUpdate"},
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "sandbox",
                            "image": "payops-sandbox:local",
                            "resources": {"limits": {"cpu": "500m", "memory": "256Mi"}},
                            "env": [
                                {"name": "PAYOPS_SANDBOX_ROLE", "value": "processor"},
                                {"name": "PAYOPS_SANDBOX_CONFIG", "value": "{}"},
                            ],
                        }
                    ]
                }
            },
        },
    }


def test_sampling_only_difference_and_original_unchanged() -> None:
    """The delayed counterfactual must isolate sampling while preserving the captured baseline."""
    original = document()
    before = deepcopy(original)
    suppressed, restored = sampling_specs(original)
    assert original == before
    assert suppressed is not restored
    env = object_items(container(suppressed)["env"])
    assert env.pop() == {"name": "OTEL_TRACES_SAMPLER", "value": "always_off"}
    container(suppressed)["env"] = list(env)
    assert suppressed == restored
    fault = json.loads(str(env[-1]["value"]))
    assert fault["delay_ms"] == 600
    assert all(fault[key] is None for key in ("processor", "region", "payment_method"))
    assert not any(fault[key] for key in ("unavailable", "rate_limit_every", "decline_every"))
    assert container(restored)["resources"] == container(object_value(before["spec"]))["resources"]


def test_workload_and_capture_contract_are_fixed() -> None:
    """Eight exact accepted attempts fit the bounded reader without hidden control traffic."""
    workload = sampling_workload()
    assert workload.role == "payments" and workload.concurrency == 2
    assert workload.request_timeout_seconds == 5 and workload.deadline_seconds == 30
    assert [
        (row.processor, row.region, row.payment_method, row.count) for row in workload.distribution
    ] == [("A", "us", "credit", 4), ("B", "us", "credit", 4)]
    assert PLAN.capture_offsets_seconds == (12, 17)
    assert PLAN.evidence_scope == "bounded_sample"
    with pytest.raises(ValidationError):
        SamplingPlan(delay_ms=1000)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        SamplingPlan(delayed_mean_min_seconds=0)
    with pytest.raises(ValidationError):
        PLAN.delay_ms = 600


@pytest.mark.parametrize("field", ["command", "args", "lifecycle", "envFrom"])
def test_added_execution_authority_rejected(field: str) -> None:
    """Even empty authority fields differ from the reviewed fixed image startup contract."""
    original = document()
    container(object_value(original["spec"]))[field] = []
    with pytest.raises(ValueError):
        sampling_specs(original)


@pytest.mark.parametrize("field", ["initContainers", "ephemeralContainers"])
def test_added_containers_rejected(field: str) -> None:
    """The capture and mutation scope contains exactly one synthetic application container."""
    original = document()
    template = object_value(object_value(original["spec"])["template"])
    object_value(template["spec"])[field] = [{"name": "other"}]
    with pytest.raises(ValueError):
        sampling_specs(original)


@pytest.mark.parametrize("field", ["uid", "resourceVersion", "generation"])
def test_missing_identity_rejected(field: str) -> None:
    """A future journal cannot authorize CAS without complete original resource identity."""
    original = document()
    object_value(original["metadata"]).pop(field)
    with pytest.raises(ValueError):
        sampling_specs(original)


@pytest.mark.parametrize("variant", ["duplicate", "wrong-role", "sampler", "timeout"])
def test_nonbaseline_environment_rejected(variant: str) -> None:
    """Existing faults, duplicate keys and incompatible timing cannot become controls."""
    original = document()
    item = container(object_value(original["spec"]))
    env = object_items(item["env"])
    if variant == "duplicate":
        env.append(deepcopy(env[0]))
    elif variant == "wrong-role":
        env[0]["value"] = "payments"
    elif variant == "sampler":
        env.append({"name": "OTEL_TRACES_SAMPLER", "value": "always_off"})
    else:
        env[1]["value"] = '{"timeout_seconds": 5}'
    item["env"] = list(env)
    with pytest.raises(ValueError):
        sampling_specs(original)


SDK_PROBE = """
import json
from opentelemetry import propagate, trace
from payops.sandbox.tracing import configure_tracing
configure_tracing("processor")
parent = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"
with trace.get_tracer("test").start_as_current_span(
    "sampling.test", context=propagate.extract({"traceparent": parent})
) as span:
    recording = span.is_recording()
    outgoing = {}
    propagate.inject(outgoing)
provider = trace.get_tracer_provider()
flushed = provider.force_flush(timeout_millis=5000)
provider.shutdown()
print(json.dumps({"recording": recording, "flushed": flushed, "outgoing": outgoing["traceparent"]}))
"""


def test_actual_payments_timeout_is_a_separate_preflight() -> None:
    """A normal processor config cannot certify the timeout used by its payments caller."""
    payment = document()
    object_value(payment["metadata"])["name"] = "payments-api"
    item = container(object_value(payment["spec"]))
    env = object_items(item["env"])
    env[0]["value"] = "payments"
    item["env"] = list(env)
    assert sampling_payment_baseline(payment) == payment["spec"]
    env[1]["value"] = '{"timeout_seconds": 5}'
    with pytest.raises(ValueError, match="peer timeout"):
        sampling_payment_baseline(payment)


@pytest.mark.parametrize(
    "sampler,expected",
    [(None, True), ("always_off", False), ("parentbased_always_off", True), (None, True)],
)
def test_actual_sdk_sampling_in_fresh_process(sampler: str | None, expected: bool) -> None:
    """Process isolation proves environment startup semantics without replacing the provider."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("OTEL_")}
    if sampler is not None:
        env["OTEL_TRACES_SAMPLER"] = sampler
    run = subprocess.run(
        [sys.executable, "-c", SDK_PROBE],
        env=env,
        capture_output=True,
        timeout=15,
        shell=False,
        check=True,
    )
    records: list[JsonObject] = []
    remaining = run.stdout.decode().strip()
    while remaining:
        raw, end = json.JSONDecoder().raw_decode(remaining)
        records.append(raw)
        remaining = remaining[end:].strip()
    result = records.pop()
    assert result["recording"] is expected and result["flushed"] is True
    assert str(result["outgoing"]).split("-")[1] == "1234567890abcdef1234567890abcdef"
    assert str(result["outgoing"]).endswith("-01" if expected else "-00")
    assert len(records) == int(expected)
    if expected:
        assert records[0]["parent_id"] == "0x1234567890abcdef"
