"""Actual LangChain message fixtures exercise provider execution without paid model calls."""

import json
from collections.abc import Iterator
from threading import Event
from time import monotonic
from typing import Any

import pytest
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from test_reasoning import finished

from payops.orchestrator.model_runtime import (
    ModelObservation,
    ModelPrompt,
    ModelRuntime,
    ModelSettings,
    PromptMessage,
)
from payops.orchestrator.reasoning import ProviderUsage, TextPrice, provider_usage


def reply(content: str | list[Any] | None = None) -> AIMessage:
    """Synthetic usage numbers validate plumbing; they are not a provider token measurement."""
    return AIMessage(
        content=json.dumps(finished()) if content is None else content,
        usage_metadata={"input_tokens": 100, "output_tokens": 20, "total_tokens": 120},
    )


class Adapter:
    """The fake LangChain model stands in for a future separately verified concrete provider."""

    def __init__(self) -> None:
        """Keep bounded fixture model settings and observable invocation arguments."""
        self.settings = ModelSettings(
            provider="fixture",
            model="scripted",
            mode="fixture",
            input_token_limit=200,
            output_token_limit=20,
            timeout_seconds=2,
            price=TextPrice(input_nano_usd=1, cached_input_nano_usd=1, output_nano_usd=2),
        )
        self.chat = FakeMessagesListChatModel(responses=[reply()])
        self.calls: list[tuple[tuple[BaseMessage, BaseMessage], int]] = []
        self.tokens = 100
        self.allow = True

    def count_tokens(self, messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Return a declared fixture count after inspecting real LangChain message roles."""
        assert isinstance(messages[0], SystemMessage) and isinstance(messages[1], HumanMessage)
        return self.tokens

    def invoke(self, messages: tuple[BaseMessage, BaseMessage], output_limit: int) -> AIMessage:
        """Execute the installed LangChain fake model with typed messages and record the cap."""
        self.calls.append((messages, output_limit))
        return AIMessage.model_validate(self.chat.invoke(messages).model_dump())

    def usage(self, message: AIMessage) -> ProviderUsage | None:
        """Only fixture data is projected here; real adapters must validate original raw usage."""
        return provider_usage(message)


@pytest.fixture
def runtime_pair() -> Iterator[tuple[ModelRuntime, Adapter]]:
    """Close and drain bounded fixture work before test cleanup."""
    adapter = Adapter()
    runtime = ModelRuntime(adapter, lambda: adapter.allow)
    yield runtime, adapter
    runtime.close()
    runtime._pool.shutdown(wait=True)  # pyright: ignore[reportPrivateUsage]


def observe(runtime: ModelRuntime) -> ModelObservation:
    """Use the exact submitted evidence and public cause vocabulary from the fixture prompt."""
    return runtime.observe(
        runtime.prepare("Host instruction", "Untrusted evidence data"),
        frozenset({"e1"}),
        frozenset({"dependency_unavailable"}),
    )


def test_actual_langchain_output_is_validated_with_separate_provider_timing(
    runtime_pair: tuple[ModelRuntime, Adapter],
) -> None:
    """Fixture provider calls preserve structured output, usage and exact output cap forwarding."""
    runtime, adapter = runtime_pair
    result = observe(runtime)
    assert result.status == "OK" and result.decision is not None
    assert result.decision.hypotheses[0].supporting_evidence_ids == ("e1",)
    assert result.usage is not None and result.usage.total_tokens == 120
    assert result.provider_seconds is not None and result.provider_seconds >= 0
    assert len(adapter.calls) == 1 and adapter.calls[0][1] == 20


@pytest.mark.parametrize(
    "content",
    [
        "private reasoning that must not be retained",
        "x" * 16385,
        "é" * 10000,
        [{"type": "text", "text": "malformed"}],
    ],
    ids=["nonjson", "oversized", "multibyte", "blocks"],
)
def test_invalid_output_retains_no_freeform_reasoning(
    runtime_pair: tuple[ModelRuntime, Adapter], content: Any
) -> None:
    """Only a digest may survive unsupported provider output; no raw content becomes trace data."""
    runtime, adapter = runtime_pair
    adapter.chat = FakeMessagesListChatModel(responses=[reply(content)])
    result = observe(runtime)
    assert result.status == "INVALID_OUTPUT" and result.decision is None
    assert "private reasoning" not in result.model_dump_json()


def test_explicit_refusal_and_missing_usage_stay_distinct(
    runtime_pair: tuple[ModelRuntime, Adapter],
) -> None:
    """A provider may refuse without usage; absence never becomes zero-cost successful diagnosis."""
    runtime, adapter = runtime_pair
    adapter.chat = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content=json.dumps(
                    {
                        "decision": "refuse",
                        "summary": "Cannot continue",
                        "reads": [],
                        "hypotheses": [],
                    }
                )
            )
        ]
    )
    result = observe(runtime)
    assert result.status == "REFUSED" and result.usage is None


@pytest.mark.parametrize("phase", ["before", "after"])
def test_current_authority_required_around_provider(
    runtime_pair: tuple[ModelRuntime, Adapter], phase: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Revocation prevents invocation or discards the completed decision at the next boundary."""
    runtime, adapter = runtime_pair
    if phase == "before":
        adapter.allow = False
    else:
        original = adapter.usage

        def usage(message: AIMessage) -> ProviderUsage | None:
            """Revoke the grant after provider I/O, before decision publication."""
            adapter.allow = False
            return original(message)

        monkeypatch.setattr(adapter, "usage", usage)
    result = observe(runtime)
    assert result.status == "DENIED" and result.decision is None
    assert len(adapter.calls) == (0 if phase == "before" else 1)


@pytest.mark.parametrize("tokens", [-1, True, 201])
def test_prepared_input_must_fit_exact_token_contract(
    runtime_pair: tuple[ModelRuntime, Adapter], tokens: int
) -> None:
    """Invalid tokenizer output fails before reservation or provider invocation."""
    runtime, adapter = runtime_pair
    adapter.tokens = tokens
    with pytest.raises(ValueError):
        runtime.prepare("system", "data")
    assert not adapter.calls


@pytest.mark.parametrize(
    "changes",
    [{"input_tokens": 99, "total_tokens": 119}, {"output_tokens": 21, "total_tokens": 121}],
)
def test_provider_census_violation_discards_decision(
    runtime_pair: tuple[ModelRuntime, Adapter], changes: dict[str, int]
) -> None:
    """A provider that exceeds prepared input/output bounds cannot continue the loop."""
    runtime, adapter = runtime_pair
    message = AIMessage.model_construct(
        content=json.dumps(finished()),
        usage_metadata={"input_tokens": 100, "output_tokens": 20, "total_tokens": 120, **changes},
    )
    adapter.chat = FakeMessagesListChatModel(responses=[message])
    result = observe(runtime)
    assert result.status == "ERROR" and result.decision is None and result.usage is not None


def test_provider_error_has_no_raw_exception_text(
    runtime_pair: tuple[ModelRuntime, Adapter], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Provider errors preserve unknown usage and cannot export credentials in error strings."""
    runtime, adapter = runtime_pair

    def invoke(messages: tuple[BaseMessage, BaseMessage], limit: int) -> AIMessage:
        """Simulate an SDK failure containing text that is not safe for model feedback."""
        raise RuntimeError("provider-secret")

    monkeypatch.setattr(adapter, "invoke", invoke)
    result = observe(runtime)
    assert result.status == "ERROR" and result.usage is None and result.provider_seconds is None
    assert "provider-secret" not in result.model_dump_json()


def test_timeout_keeps_single_slot_and_no_late_decision(
    runtime_pair: tuple[ModelRuntime, Adapter], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A timed-out remote call remains unknown and prevents a second queued provider invocation."""
    runtime, adapter = runtime_pair
    release = Event()
    adapter.settings = adapter.settings.model_copy(update={"timeout_seconds": 0.1})
    runtime.settings = adapter.settings

    def invoke(messages: tuple[BaseMessage, BaseMessage], limit: int) -> AIMessage:
        """Use bounded fixture blocking to model a still-running provider transport."""
        adapter.calls.append((messages, limit))
        assert release.wait(3)
        return reply()

    monkeypatch.setattr(adapter, "invoke", invoke)
    try:
        result = observe(runtime)
        assert result.status == "TIMEOUT" and result.decision is None and result.usage is None
        assert observe(runtime).status == "BUSY"
        assert len(adapter.calls) == 1
    finally:
        release.set()


def test_prompt_roles_settings_changes_and_closed_admission(
    runtime_pair: tuple[ModelRuntime, Adapter],
) -> None:
    """Constructed roles and changing provider configuration cannot escape the host binding."""
    runtime, adapter = runtime_pair
    with pytest.raises(ValueError):
        ModelPrompt(
            messages=(
                PromptMessage(role="user", content="x"),
                PromptMessage(role="user", content="x"),
            ),
            input_tokens=1,
        )
    adapter.settings = adapter.settings.model_copy(update={"model": "changed"})
    assert observe(runtime).status == "DENIED" and not adapter.calls
    runtime.close()
    with pytest.raises(ValueError):
        observe(runtime)


@pytest.mark.parametrize(
    "value",
    [
        {"status": "OK"},
        {"status": "REFUSED", "decision": finished()},
        {"status": "ERROR", "decision": finished()},
    ],
)
def test_outcome_cannot_publish_incoherent_decision(value: dict[str, Any]) -> None:
    """Stored outcome envelopes are validated again before replay or report construction."""
    with pytest.raises(ValueError):
        ModelObservation.model_validate(value)


def test_interrupted_first_clock_does_not_leak_model_slot(
    runtime_pair: tuple[ModelRuntime, Adapter], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every operation after slot acquisition belongs inside the cleanup exception boundary."""
    runtime, adapter = runtime_pair
    first = True

    def clock() -> float:
        """Interrupt before submission once, then permit the next real provider call."""
        nonlocal first
        if first:
            first = False
            raise KeyboardInterrupt
        return monotonic()

    monkeypatch.setattr("payops.orchestrator.model_runtime.monotonic", clock)
    with pytest.raises(KeyboardInterrupt):
        observe(runtime)
    assert not adapter.calls
    assert observe(runtime).status == "OK" and len(adapter.calls) == 1
