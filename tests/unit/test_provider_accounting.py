"""Staged transport fixtures prove conservative reservations before either provider request."""

import json
from collections.abc import Callable, Iterator
from threading import Event

import pytest
from langchain_core.messages import AIMessage, BaseMessage
from test_model_runtime import Adapter, reply
from test_reasoning_loop import Harness
from test_reasoning_loop import harness as harness

from payops.orchestrator.budget import ModelCharge
from payops.orchestrator.model_runtime import ModelRuntime, ModelSettings, ProviderDetails


class StagedAdapter(Adapter):
    """No live provider is contacted; stage counters and timings are explicitly synthetic."""

    def __init__(self) -> None:
        """Reserve150 input tokens while the fixture count and usage both report100."""
        super().__init__()
        self.settings = ModelSettings.model_validate(
            {
                **self.settings.model_dump(),
                "mode": "provider",
                "token_accounting": "provider_ceiling",
                "input_token_limit": 150,
            }
        )
        self.stages: list[str] = []
        self.count = 100
        self.response = reply()

    def count_tokens(self, messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Preparation returns an explicitly labeled ceiling without executing a network stage."""
        return self.settings.input_token_limit

    def invoke_staged(
        self,
        messages: tuple[BaseMessage, BaseMessage],
        output_limit: int,
        before_generation: Callable[[int], bool],
    ) -> AIMessage:
        """Fixture stage ordering mirrors count, refreshed grant, then one generation request."""
        self.stages.append("count")
        if not before_generation(self.count):
            raise PermissionError("stage not authorized")
        self.stages.append("generate")
        return self.response

    def details(self, message: AIMessage) -> ProviderDetails:
        """Fixed fixture durations verify separation, not any provider performance target."""
        return ProviderDetails(
            counted_input_tokens=self.count,
            provider_requests=2,
            count_seconds=0.4,
            generation_seconds=0.2,
            request_shape_sha256="a" * 64,
            normalized_refusal=False,
        )


@pytest.fixture
def staged() -> Iterator[tuple[StagedAdapter, ModelRuntime]]:
    """Drain bounded fixture operations before releasing their test resources."""
    adapter = StagedAdapter()
    runtime = ModelRuntime(adapter, lambda: adapter.allow)
    try:
        yield adapter, runtime
    finally:
        runtime.close()
        runtime._pool.shutdown(wait=True)  # pyright: ignore[reportPrivateUsage]


def test_ceiling_is_not_claimed_as_actual_input_or_generation_time(
    staged: tuple[StagedAdapter, ModelRuntime],
) -> None:
    """Count/create details remain separate from conservative token and network reservations."""
    adapter, runtime = staged
    prompt = runtime.prepare("Host", "Data")
    assert prompt.input_tokens == 150 and prompt.token_accounting == "provider_ceiling"
    assert not adapter.stages
    result = runtime.observe(prompt, frozenset({"e1"}), frozenset({"dependency_unavailable"}))
    assert result.status == "OK" and result.usage is not None
    assert result.usage.input_tokens == 100
    assert result.provider_details is not None and result.provider_details.count_seconds == 0.4
    assert result.provider_seconds == 0.2 and result.provider_details.provider_requests == 2
    assert adapter.stages == ["count", "generate"]


@pytest.mark.parametrize("count", [99, 151])
def test_usage_or_count_cannot_escape_reserved_contract(
    staged: tuple[StagedAdapter, ModelRuntime],
    count: int,
) -> None:
    """An inconsistent provider response never enlarges its reservation after dispatch."""
    adapter, runtime = staged
    adapter.count = count
    result = runtime.observe(runtime.prepare("Host", "Data"), frozenset(), frozenset())
    assert result.status == "ERROR" and result.decision is None
    assert adapter.stages == (["count"] if count > 150 else ["count", "generate"])


@pytest.mark.parametrize("blocked_call", [1, 2])
def test_late_authority_cannot_start_next_network_stage(
    staged: tuple[StagedAdapter, ModelRuntime],
    blocked_call: int,
) -> None:
    """The same original deadline covers initial and count-to-generation authority refresh."""
    adapter, runtime = staged
    release = Event()
    calls: list[int] = []
    runtime.settings = adapter.settings.model_copy(update={"timeout_seconds": 0.03})
    adapter.settings = runtime.settings

    def authority() -> bool:
        """Hold one real worker until result acceptance has timed out."""
        calls.append(1)
        if len(calls) == blocked_call:
            assert release.wait(3)
        return True

    runtime.authorize = authority
    try:
        result = runtime.observe(runtime.prepare("Host", "Data"), frozenset(), frozenset())
        assert result.status == "TIMEOUT"
    finally:
        release.set()
    runtime._pool.shutdown(wait=True)  # pyright: ignore[reportPrivateUsage]
    assert adapter.stages == ([] if blocked_call == 1 else ["count"])


@pytest.mark.parametrize("provider_requests,expected", [(1, "BUDGET_EXHAUSTED"), (2, "FINISHED")])
def test_sql_reserves_both_requests_before_provider_preflight(
    harness: Harness,
    provider_requests: int,
    expected: str,
) -> None:
    """A count endpoint is charged even though it is not a Kubernetes or telemetry read."""
    h = harness
    h.runtime.close()
    adapter = StagedAdapter()
    adapter.response = reply(json.dumps(h.finish()))
    h.adapter = adapter
    h.runtime = ModelRuntime(adapter, lambda: True)
    h.loop.runtime = h.runtime
    h.loop.limits = h.limits.model_copy(update={"provider_requests": provider_requests})
    result = h.run()
    assert result.stop_reason == expected
    if provider_requests == 1:
        assert not adapter.stages and not h.ledger.get("incident").charges
    else:
        charge = h.ledger.get("incident").charges[0]
        assert isinstance(charge, ModelCharge)
        assert charge.input_tokens == 150 and charge.provider_requests == 2
        assert charge.token_accounting == "provider_ceiling"
        assert h.run() == result and adapter.stages == ["count", "generate"]


def test_live_mode_requires_explicit_ceiling_contract() -> None:
    """Legacy fixture semantics cannot silently claim locally exact provider framing."""
    config = Adapter().settings.model_dump()
    with pytest.raises(ValueError):
        ModelSettings.model_validate({**config, "mode": "provider"})


@pytest.mark.parametrize("accounting,requests", [("provider_ceiling", 1), ("fixture_exact", 2)])
def test_provider_request_census_cannot_undercharge_or_mislabel(
    accounting: str,
    requests: int,
) -> None:
    """The validated charge binds both network stages independently of caller-supplied counts."""
    with pytest.raises(ValueError, match="reservation differs"):
        ModelCharge.model_validate(
            {
                "operation_id": "model-1",
                "prompt_sha256": "a" * 64,
                "input_tokens": 150,
                "output_token_limit": 20,
                "price": Adapter().settings.price.model_dump(),
                "token_accounting": accounting,
                "provider_requests": requests,
            }
        )


def test_provider_mode_cannot_bypass_stage_hook_with_plain_invoke() -> None:
    """An adapter lacking staged dispatch never executes its old one-call method in live mode."""
    adapter = Adapter()
    adapter.settings = ModelSettings.model_validate(
        {
            **adapter.settings.model_dump(),
            "mode": "provider",
            "token_accounting": "provider_ceiling",
        }
    )
    adapter.tokens = adapter.settings.input_token_limit
    runtime = ModelRuntime(adapter, lambda: True)
    try:
        result = runtime.observe(runtime.prepare("Host", "Data"), frozenset(), frozenset())
        assert result.status == "ERROR" and not adapter.calls
    finally:
        runtime.close()
        runtime._pool.shutdown(wait=True)  # pyright: ignore[reportPrivateUsage]


def test_provider_preparation_cannot_lower_configured_ceiling(
    staged: tuple[StagedAdapter, ModelRuntime], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preparation is a configured reservation, not an unverified lower local token estimate."""
    adapter, runtime = staged
    def lower_count(messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Supply a lower unverified local estimate to the provider reservation boundary."""
        return 100

    monkeypatch.setattr(adapter, "count_tokens", lower_count)
    with pytest.raises(ValueError):
        runtime.prepare("Host", "Data")
    assert not adapter.stages
