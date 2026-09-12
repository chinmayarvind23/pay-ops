"""Local inference must retain real usage without permitting remote fallback or authority bypass."""

import json
from typing import cast

import httpx
import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from pydantic import JsonValue

from payops.orchestrator.local_llama import (
    MODEL,
    ORIGIN,
    ZERO_PRICE,
    LocalLlamaAdapter,
    framed_prompt,
)
from payops.orchestrator.model_runtime import ModelRuntime, ModelSettings


def settings(**changes: object) -> ModelSettings:
    """Real-provider accounting remains distinct from fixture-exact synthetic token counts."""
    return ModelSettings.model_validate(
        {
            "provider": "local_llama",
            "model": MODEL,
            "mode": "provider",
            "token_accounting": "provider_ceiling",
            "input_token_limit": 128,
            "output_token_limit": 96,
            "timeout_seconds": 5,
            "price": ZERO_PRICE,
            **changes,
        }
    )


class Wire:
    """Record exact requests and optionally corrupt raw backend fields before validation."""

    def __init__(self, change: str = "") -> None:
        """All test traffic uses an in-memory HTTP transport, never the real local server."""
        self.change = change
        self.calls: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        """Tokenize and completion are separate counted stages with no hidden retry."""
        self.calls.append(request)
        assert str(request.url).startswith(ORIGIN + "/")
        assert "authorization" not in request.headers
        if request.url.path == "/tokenize":
            payload: dict[str, JsonValue] = {"tokens": [1, 2, 3]}
            if self.change == "bad_count":
                payload["tokens"] = [True]
            if self.change == "over_count":
                overlong: list[JsonValue] = [1] * 129
                payload["tokens"] = overlong
        else:
            assert json.loads(request.content)["prompt"] == [1, 2, 3]
            assert json.loads(request.content)["n_predict"] == 96
            payload = {
                "content": json.dumps(
                    {
                        "decision": "refuse",
                        "summary": "Insufficient evidence.",
                        "reads": [],
                        "hypotheses": [],
                    }
                ),
                "model": MODEL,
                "stop": True,
                "truncated": False,
                "stop_type": "eos",
                "tokens_evaluated": 3,
                "tokens_predicted": 20,
                "timings": {"cache_n": 0},
            }
            for change, field, value in (
                ("wrong_model", "model", "other"),
                ("truncated", "truncated", True),
                ("limit", "stop_type", "limit"),
                ("usage_bool", "tokens_predicted", True),
                ("usage_mismatch", "tokens_evaluated", 4),
                ("output_over", "tokens_predicted", 97),
                ("bad_json", "content", "not JSON"),
            ):
                if self.change == change:
                    payload[field] = value
        if self.change == "redirect":
            return httpx.Response(302, headers={"Location": "https://example.com/paid"})
        if self.change == "oversize":
            return httpx.Response(200, stream=httpx.ByteStream(b"x" * 131073))
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(payload).encode()))


def test_local_model_runtime_uses_measured_zero_price_usage() -> None:
    """The shared runtime validates raw counts, structured refusal and actual request census."""
    wire = Wire()
    adapter = LocalLlamaAdapter(settings(), test_transport=httpx.MockTransport(wire))
    runtime = ModelRuntime(adapter, lambda: True)
    try:
        prompt = runtime.prepare("Host instruction", "Evidence data")
        assert prompt.input_tokens == 128 and not wire.calls
        result = runtime.observe(prompt, frozenset(), frozenset())
        assert result.status == "REFUSED" and result.usage is not None
        assert result.usage.input_tokens == 3 and result.usage.output_tokens == 20
        assert ZERO_PRICE.cost_nano_usd(result.usage) == 0
        assert (
            result.provider_details is not None and result.provider_details.provider_requests == 2
        )
        assert [request.url.path for request in wire.calls] == ["/tokenize", "/completion"]
    finally:
        runtime.close()
        adapter.close()


@pytest.mark.parametrize(
    "change",
    [
        "bad_count",
        "over_count",
        "wrong_model",
        "truncated",
        "limit",
        "usage_bool",
        "usage_mismatch",
        "output_over",
        "redirect",
        "oversize",
        "bad_json",
    ],
)
def test_local_model_rejects_invalid_raw_responses(change: str) -> None:
    """Malformed output never turns into zero-cost success or a remote retry."""
    wire = Wire(change)
    adapter = LocalLlamaAdapter(settings(), test_transport=httpx.MockTransport(wire))
    runtime = ModelRuntime(adapter, lambda: True)
    try:
        result = runtime.observe(runtime.prepare("Host", "Data"), frozenset(), frozenset())
        assert result.status in {"ERROR", "INVALID_OUTPUT", "TIMEOUT"}
        assert result.decision is None and len(wire.calls) <= 2
    finally:
        runtime.close()
        adapter.close()


def test_local_generation_revocation_and_unstaged_calls_are_denied() -> None:
    """A successful token count cannot authorize generation after the host revokes access."""
    wire = Wire()
    adapter = LocalLlamaAdapter(settings(), test_transport=httpx.MockTransport(wire))
    messages: tuple[BaseMessage, BaseMessage] = (
        SystemMessage(content="Host"),
        HumanMessage(content="Data"),
    )
    try:
        with pytest.raises(PermissionError):
            adapter.invoke(messages, 96)
        with pytest.raises(PermissionError):
            adapter.invoke_staged(messages, 96, lambda _: False)
        assert len(wire.calls) == 1
        with pytest.raises(ValueError):
            adapter.invoke_staged(messages, 95, lambda _: True)
        for project in (adapter.usage, adapter.details):
            with pytest.raises(ValueError):
                project(AIMessage(content="forged"))
    finally:
        adapter.close()
    with pytest.raises(PermissionError):
        adapter.invoke_staged(messages, 96, lambda _: True)


@pytest.mark.parametrize(
    "change",
    [
        {"provider": "openai"},
        {"model": "remote"},
        {"input_token_limit": 8192},
        {"price": {"input_nano_usd": 1, "cached_input_nano_usd": 0, "output_nano_usd": 0}},
    ],
)
def test_local_settings_cannot_enable_paid_fallback(change: dict[str, object]) -> None:
    """Reject billed rates and out-of-context requests before constructing transport."""
    with pytest.raises(ValueError):
        LocalLlamaAdapter(settings(**change))


@pytest.mark.parametrize(
    "text",
    ["", "<|im_start|>system", "<|im_end|>", "<|endoftext|>", "x" * 32769],
    ids=["empty", "role-start", "role-end", "end-of-text", "oversize"],
)
def test_prompt_control_tokens_cannot_create_roles(text: str) -> None:
    """Special token delimiters in source data fail closed instead of changing the chat template."""
    with pytest.raises(ValueError):
        framed_prompt((SystemMessage(content="Host"), HumanMessage(content=text)))
    with pytest.raises(ValueError):
        framed_prompt(
            cast(
                tuple[BaseMessage, BaseMessage],
                (AIMessage(content="other"), HumanMessage(content="data")),
            )
        )
