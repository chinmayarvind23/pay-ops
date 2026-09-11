"""Real HTTPX and LangChain boundaries use synthetic wire responses, never a paid provider."""

import json
import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from threading import Event
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langsmith import (
    Client,
    tracing_context,  # pyright: ignore[reportUnknownVariableType]
)
from pydantic import SecretStr
from test_reasoning import finished
from test_reasoning_loop import Harness

from payops.orchestrator.loop import ReasoningLoop
from payops.orchestrator.loop_records import ModelReceipt, restore
from payops.orchestrator.model_runtime import ModelRuntime, ModelSettings
from payops.orchestrator.openai_adapter import OpenAIResponsesAdapter, ValidatedAIMessage
from payops.orchestrator.openai_wire import MODEL, decode
from payops.orchestrator.reasoning import TextPrice

MESSAGES: tuple[BaseMessage, BaseMessage] = (
    SystemMessage(content="private-host-instruction"), HumanMessage(content="private-evidence")
)


def settings(**changes: Any) -> ModelSettings:
    """Use a real provider configuration with a fake credential and no implicit environment."""
    return ModelSettings.model_validate({
        "provider": "openai", "model": MODEL, "mode": "provider",
        "token_accounting": "provider_ceiling", "input_token_limit": 16000,
        "output_token_limit": 2048, "timeout_seconds": 2,
        "price": TextPrice(
            input_nano_usd=750, cached_input_nano_usd=75, output_nano_usd=4500
        ), **changes,
    })


def generation() -> dict[str, Any]:
    """Represent the documented Responses wire shape before SDK or Pydantic conversion."""
    return {
        "object": "response", "model": MODEL, "status": "completed", "service_tier": "default",
        "error": None, "incomplete_details": None,
        "output": [{
            "type": "message", "role": "assistant", "status": "completed",
            "content": [{"type": "output_text", "text": json.dumps(finished())}],
        }],
        "usage": {
            "input_tokens": 100, "output_tokens": 20, "total_tokens": 120,
            "input_tokens_details": {"cached_tokens": 10, "cache_write_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 0},
        },
    }


def response(value: Any, **kwargs: Any) -> httpx.Response:
    """Keep the body streaming so the adapter's raw byte bound is actually exercised."""
    body = value if isinstance(value, bytes) else json.dumps(value).encode()
    return httpx.Response(
        200, headers={"Content-Type": "application/json", **kwargs},
        stream=httpx.ByteStream(body),
    )


class Wire:
    """One local fixture captures exact requests and supplies independently mutable raw replies."""

    def __init__(self) -> None:
        """Two normal Responses calls are the default, with no hidden retries or tools."""
        self.calls: list[httpx.Request] = []
        self.count: Any = {"object": "response.input_tokens", "input_tokens": 100}
        self.result: Any = generation()
        self.adapter = OpenAIResponsesAdapter(
            settings(), SecretStr("synthetic-secret"), test_transport=httpx.MockTransport(self.send)
        )

    def send(self, request: httpx.Request) -> httpx.Response:
        """Dispatch only between the two exact fixed endpoints."""
        self.calls.append(request)
        value = self.count if request.url.path.endswith("/input_tokens") else self.result
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, httpx.Response) else response(value)

    def invoke(self) -> AIMessage:
        """The trusted fixture supplies its stage grant, never an HTTP/model parameter."""
        return self.adapter.invoke_staged(MESSAGES, 2048, lambda count: True)


@pytest.fixture
def wire() -> Iterator[Wire]:
    """Every case closes only its own no-network HTTPX client."""
    value = Wire()
    try:
        yield value
    finally:
        value.adapter.close()


def test_count_create_same_shape_strict_schema_and_validated_usage(wire: Wire) -> None:
    """The ceiling is local; actual counts and request timings are independent returned facts."""
    assert wire.adapter.count_tokens(MESSAGES) == 16000 and not wire.calls
    message = wire.invoke()
    assert isinstance(message, ValidatedAIMessage)
    count_body = json.loads(wire.calls[0].content)
    create_body = json.loads(wire.calls[1].content)
    assert {key: create_body[key] for key in count_body} == count_body
    assert [str(request.url) for request in wire.calls] == [
        "https://api.openai.com/v1/responses/input_tokens", "https://api.openai.com/v1/responses"
    ]
    assert all(request.method == "POST" for request in wire.calls)
    assert create_body["max_output_tokens"] == 2048
    assert create_body["store"] is create_body["stream"] is create_body["background"] is False
    assert create_body["truncation"] == "disabled" and "tools" not in create_body
    assert [entry["role"] for entry in count_body["input"]] == ["system", "user"]
    schema = count_body["text"]["format"]["schema"]
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == set(schema["properties"])
    for definition in schema["$defs"].values():
        if definition.get("type") == "object":
            assert definition["additionalProperties"] is False
            assert set(definition["required"]) == set(definition["properties"])
    usage = wire.adapter.usage(message)
    assert usage is not None and usage.input_tokens == 100 and usage.cached_input_tokens == 10
    details = wire.adapter.details(message)
    assert details.counted_input_tokens == 100 and details.provider_requests == 2
    assert details.count_seconds >= 0 and details.generation_seconds >= 0
    assert len(message.request_shape_sha256) == 64 and not message.normalized_refusal
    assert message.usage_metadata is None and "_validated_usage" not in message.model_dump()
    assert wire.calls[0].extensions["timeout"]["read"] <= 1


@pytest.mark.parametrize("value", [True, -1, 16001, "100", 100.0, None])
def test_bad_count_prevents_generation(wire: Wire, value: Any) -> None:
    """Raw integer and ceiling validation occurs before the second authorized network effect."""
    wire.count["input_tokens"] = value
    with pytest.raises(ValueError, match="provider protocol failure"):
        wire.invoke()
    assert len(wire.calls) == 1


@pytest.mark.parametrize("count_reply", [
    [], {"object": "other", "input_tokens": 100},
    {"object": "response.input_tokens", "input_tokens": 100, "extra": 1},
])
def test_bad_count_envelope_fails_closed(wire: Wire, count_reply: Any) -> None:
    """A different endpoint shape cannot be silently treated as an exact count receipt."""
    wire.count = count_reply
    with pytest.raises(ValueError):
        wire.invoke()
    assert len(wire.calls) == 1


@pytest.mark.parametrize(("path", "value"), [
    (("input_tokens",), True), (("input_tokens",), "100"),
    (("input_tokens",), 100.0), (("input_tokens",), 99),
    (("output_tokens",), 2049), (("total_tokens",), 121),
    (("input_tokens_details", "cached_tokens"), 101),
    (("input_tokens_details", "cached_tokens"), False),
    (("input_tokens_details", "cache_write_tokens"), 1),
    (("input_tokens_details", "new_billing_category"), 1),
    (("output_tokens_details", "reasoning_tokens"), 21),
    (("output_tokens_details", "other"), 1),
    (("extra",), 0), (("input_tokens_details",), []),
])
def test_raw_usage_cannot_be_coerced_or_drop_billable_fields(
    wire: Wire, path: tuple[str, ...], value: Any,
) -> None:
    """Every otherwise valid response retains only usage that obeys the original wire census."""
    target = wire.result["usage"]
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        wire.invoke()
    assert len(wire.calls) == 2


def test_missing_usage_remains_unknown_and_refusal_prose_is_normalized(wire: Wire) -> None:
    """A schema-external provider refusal becomes an explicit host refusal, never invented usage."""
    wire.result["usage"] = None
    wire.result["output"][0]["content"] = [{"type": "refusal", "refusal": "private prose"}]
    message = wire.invoke()
    assert isinstance(message, ValidatedAIMessage) and message.normalized_refusal
    assert wire.adapter.usage(message) is None
    assert isinstance(message.content, str) and json.loads(message.content)["decision"] == "refuse"
    assert "private prose" not in message.model_dump_json()


@pytest.mark.parametrize(("input_tokens", "output_tokens"), [(99, 20), (100, 2049)])
def test_consistent_usage_census_still_must_match_count_and_output_cap(
    wire: Wire, input_tokens: int, output_tokens: int,
) -> None:
    """A correct arithmetic total alone cannot authorize a different measured request or cap."""
    wire.result["usage"].update({
        "input_tokens": input_tokens, "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
    })
    with pytest.raises(ValueError):
        wire.invoke()


@pytest.mark.parametrize("tier", [None, "priority", "flex", "auto", "ultrafast"])
def test_actual_service_tier_must_match_standard_pricing(wire: Wire, tier: str | None) -> None:
    """The response reports actual serving tier, which can differ from the requested tier."""
    if tier is None:
        del wire.result["service_tier"]
    else:
        wire.result["service_tier"] = tier
    with pytest.raises(ValueError):
        wire.invoke()


@pytest.mark.parametrize(("path", "value"), [
    (("object",), "other"), (("model",), "foreign-model"),
    (("status",), "incomplete"), (("error",), {"message": "private"}),
    (("incomplete_details",), {"reason": "max_output_tokens"}),
    (("output",), []), (("output",), [1]),
    (("output", 0, "type"), "function_call"), (("output", 0, "role"), "system"),
    (("output", 0, "status"), "in_progress"), (("output", 0, "content"), []),
    (("output", 0, "content", 0, "type"), "reasoning"),
    (("output", 0, "content", 0, "text"), ""),
    (("output", 0, "content", 0, "text"), "é" * 8193),
    (("output", 0, "content", 0, "text"), 1),
], ids=[
    "object", "model", "incomplete", "error", "incomplete-details", "empty", "nonobject",
    "tool", "role", "message-status", "empty-content", "reasoning", "empty-text",
    "oversized-utf8", "nontext",
])
def test_noncompleted_or_ambiguous_output_never_enters_runtime(
    wire: Wire, path: tuple[str | int, ...], value: Any,
) -> None:
    """Output shape, model identity and byte caps precede any LangChain conversion."""
    target = wire.result
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        wire.invoke()


@pytest.mark.parametrize("body", [
    b'{"x":1,"x":2}', b'{"ignored":NaN}', b'{"ignored":1e400}',
    b'{"x":"\xff"}', b'[]', b'{"a":' * 26 + b'0' + b'}' * 26,
    b'{"a":' * 2000 + b'0' + b'}' * 2000,
])
def test_strict_raw_json_checks_even_discarded_fields(wire: Wire, body: bytes) -> None:
    """Duplicate keys, float overflow, deep objects and malformed UTF-8 cannot hide in metadata."""
    wire.count = body
    with pytest.raises(ValueError):
        wire.invoke()
    assert len(wire.calls) == 1


@pytest.mark.parametrize("status", [301, 302, 401, 429, 500, 503])
def test_http_errors_and_redirects_have_one_attempt(wire: Wire, status: int) -> None:
    """Redirects and transient statuses neither retry nor send the credential to another origin."""
    wire.count = httpx.Response(status, headers={"Location": "https://foreign.invalid"})
    with pytest.raises(ValueError, match="^provider protocol failure$"):
        wire.invoke()
    assert len(wire.calls) == 1


@pytest.mark.parametrize("headers", [
    {"content-encoding": "gzip"}, {"content-type": "text/html"},
    {"content-length": "16385"}, {"content-length": "-1"},
])
def test_transport_header_bounds(wire: Wire, headers: dict[str, str]) -> None:
    """Header validation fails before consuming a provider body outside the fixed wire contract."""
    wire.count = response(wire.count, **headers)
    with pytest.raises(ValueError):
        wire.invoke()


@pytest.mark.parametrize("phase", ["count", "generation"])
def test_accumulated_body_bound_without_content_length(wire: Wire, phase: str) -> None:
    """The actual streamed body remains bounded even when the server omits Content-Length."""
    if phase == "count":
        wire.count = b" " * 16385
    else:
        wire.result = b" " * 131073
    with pytest.raises(ValueError):
        wire.invoke()
    assert len(wire.calls) == (1 if phase == "count" else 2)


@pytest.mark.parametrize("error", [httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ReadError])
def test_transport_failures_are_sanitized_without_retry(wire: Wire, error: type[Exception]) -> None:
    """Neither error bodies nor exception messages are allowed into the runtime's error channel."""
    wire.count = error("synthetic-secret private-evidence")
    expected = TimeoutError if issubclass(error, httpx.TimeoutException) else ValueError
    with pytest.raises(expected) as caught:
        wire.invoke()
    assert "synthetic-secret" not in str(caught.value) and len(wire.calls) == 1


def test_stage_denial_and_late_identity_send_no_generation(
    wire: Wire, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The absolute deadline is rechecked after a fresh lookup, even when it returns true late."""
    with pytest.raises(PermissionError):
        wire.adapter.invoke_staged(MESSAGES, 2048, lambda count: False)
    assert len(wire.calls) == 1
    now = 100.0
    monkeypatch.setattr("payops.orchestrator.openai_adapter.monotonic", lambda: now)

    def late(count: int) -> bool:
        """Advance a deterministic transport clock during the current grant check."""
        nonlocal now
        now += 3
        return True

    with pytest.raises(TimeoutError):
        wire.adapter.invoke_staged(MESSAGES, 2048, late)
    assert len(wire.calls) == 2


def test_count_and_generation_timing_excludes_stage_authority(
    wire: Wire, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generation latency excludes the token-count request and current identity lookup."""
    now = 100.0
    monkeypatch.setattr("payops.orchestrator.openai_adapter.monotonic", lambda: now)
    original = wire.adapter._post  # pyright: ignore[reportPrivateUsage]

    def post(path: str, body: bytes, limit: int, deadline: float) -> dict[str, Any]:
        """Exercise real HTTPX requests while assigning distinct stage durations."""
        nonlocal now
        result = original(path, body, limit, deadline)
        now += 0.2 if path.endswith("/input_tokens") else 0.3
        return result

    def authorize(count: int) -> bool:
        """This duration is not part of either provider request."""
        nonlocal now
        now += 0.7
        return True

    monkeypatch.setattr(wire.adapter, "_post", post)
    message = wire.adapter.invoke_staged(MESSAGES, 2048, authorize)
    details = wire.adapter.details(message)
    assert details.count_seconds == pytest.approx(0.2)
    assert details.generation_seconds == pytest.approx(0.3)


def test_disabled_legacy_closed_and_wrong_cap_never_dispatch(wire: Wire) -> None:
    """There is no direct invoke path that skips stage authorization or widens output allowance."""
    with pytest.raises(PermissionError):
        wire.adapter.invoke(MESSAGES, 2048)
    with pytest.raises(ValueError):
        wire.adapter.invoke_staged(MESSAGES, 2049, lambda count: True)
    wire.adapter.close()
    with pytest.raises(PermissionError):
        wire.invoke()
    assert not wire.calls


@pytest.mark.parametrize("messages", [
    (HumanMessage(content="x"), HumanMessage(content="y")),
    (SystemMessage(content=""), HumanMessage(content="y")),
    (SystemMessage(content="x" * 32769), HumanMessage(content="y")),
    (SystemMessage(content="x"), HumanMessage(content=[{"type": "text", "text": "y"}])),
    (SystemMessage(content="é" * 16000), HumanMessage(content="y")),
])
def test_prompt_roles_content_and_serialized_request_bounds(
    wire: Wire, messages: tuple[BaseMessage, BaseMessage],
) -> None:
    """Local preparation rejects oversized or nontext prompts without any authenticated call."""
    with pytest.raises(ValueError):
        wire.adapter.count_tokens(messages)
    assert not wire.calls


def test_plain_or_uninitialized_ai_messages_cannot_supply_accounting(wire: Wire) -> None:
    """Coerced generic LangChain metadata cannot cross the raw-usage validation boundary."""
    for message in [AIMessage(content="x"), ValidatedAIMessage(content="x")]:
        with pytest.raises(ValueError):
            wire.adapter.details(message)
    with pytest.raises(ValueError):
        wire.adapter.usage(AIMessage(content="x"))


@pytest.mark.parametrize("change", [
    {"provider": "other"}, {"model": "alias"}, {"input_token_limit": 16001},
    {"output_token_limit": 15}, {"output_token_limit": 2049},
    {"mode": "fixture", "token_accounting": "fixture_exact"},
    {"price": {"input_nano_usd": 0, "cached_input_nano_usd": 0, "output_nano_usd": 0}},
    {"price": {"input_nano_usd": 749, "cached_input_nano_usd": 75, "output_nano_usd": 4500}},
    {"price": {"input_nano_usd": 750, "cached_input_nano_usd": 74, "output_nano_usd": 4500}},
    {"price": {"input_nano_usd": 750, "cached_input_nano_usd": 75, "output_nano_usd": 4499}},
])
def test_unapproved_host_settings_fail_before_client_creation(change: dict[str, Any]) -> None:
    """The initial adapter cannot be repurposed into a generic URL/model gateway."""
    with pytest.raises(ValueError):
        OpenAIResponsesAdapter(settings(**change), SecretStr("synthetic-secret"))


@pytest.mark.parametrize("key", ["", "with space", "newline\n", "é", "x" * 4097])
def test_credentials_are_explicit_and_header_safe(key: str) -> None:
    """Invalid secret formats fail without echoing bytes or attempting network work."""
    with pytest.raises(ValueError, match="invalid operator credential format"):
        OpenAIResponsesAdapter(settings(), SecretStr(key))


def test_debug_transport_preflight_rejects_without_mutating_logging(wire: Wire) -> None:
    """Known httpcore debug tracing could export response headers and therefore blocks admission."""
    logger = logging.getLogger("httpcore.http11")
    original_level = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        with pytest.raises(ValueError):
            wire.invoke()
        assert logger.level == logging.DEBUG and not wire.calls
    finally:
        logger.setLevel(original_level)


def test_langsmith_context_and_debug_logs_do_not_receive_prompt(
    wire: Wire, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct transport never enters the LangChain/LangSmith callback pipeline."""
    client = Mock(spec=Client)

    def callbacks(*args: Any, **kwargs: Any) -> None:
        """An enabled callback manager would fail the test before receiving any prompt."""
        raise AssertionError("callback pipeline invoked")

    monkeypatch.setattr("langchain_core.callbacks.manager.CallbackManager.configure", callbacks)
    with caplog.at_level(logging.DEBUG, logger="payops"), tracing_context(
        enabled=True, client=client,
    ):
        wire.invoke()
    assert not client.mock_calls
    assert not any(token in caplog.text for token in (
        "private-host-instruction", "private-evidence", "synthetic-secret"
    ))


@pytest.mark.parametrize("outcome", ["finish", "provider-refusal", "model-json-refusal"])
def test_real_runtime_and_sql_reserve_before_both_wire_requests(
    tmp_path: Path, outcome: str,
) -> None:
    """A transport fixture drives the actual durable loop, then replay makes zero new requests."""
    h = Harness(tmp_path)
    wire = Wire()
    wire.result["output"][0]["content"][0]["text"] = json.dumps(h.finish())
    if outcome == "provider-refusal":
        wire.result["output"][0]["content"] = [
            {"type": "refusal", "refusal": "private provider refusal prose"}
        ]
    elif outcome == "model-json-refusal":
        wire.result["output"][0]["content"][0]["text"] = json.dumps({
            "decision": "refuse", "summary": "Declined in requested schema",
            "reads": [], "hypotheses": [],
        })
    h.runtime.close()
    h.runtime = ModelRuntime(wire.adapter, lambda: True)
    h.limits = h.limits.model_copy(update={"tokens": 80000, "cost_nano_usd": 100000000})
    h.loop = ReasoningLoop(
        h.root / "runs", h.store, h.ledger, h.runtime, h.factory,
        subject="actor", causes=frozenset({"dependency_unavailable"}), limits=h.limits,
    )
    original_send = wire.send

    def charged(request: httpx.Request) -> httpx.Response:
        """Even the token-count endpoint is downstream of the immutable SQL reservation."""
        charges = h.ledger.get("incident").charges
        assert len(charges) == 1 and charges[0].kind == "model"
        assert charges[0].input_tokens == 16000 and charges[0].provider_requests == 2
        return original_send(request)

    wire.adapter.close()
    wire.adapter = OpenAIResponsesAdapter(
        settings(), SecretStr("synthetic-secret"), test_transport=httpx.MockTransport(charged)
    )
    h.runtime.adapter = wire.adapter
    try:
        first = h.run()
        assert first.stop_reason == ("FINISHED" if outcome == "finish" else "REFUSED")
        assert first.mode == "provider"
        assert len(wire.calls) == 2
        assert h.run() == first and len(wire.calls) == 2
        assert not h.read_calls
        completion = h.ledger.get("incident").completions[0]
        receipt = restore(h.store, completion.artifact_sha256, ModelReceipt)
        details = receipt.observation.provider_details
        assert details is not None
        assert details.normalized_refusal is (outcome == "provider-refusal")
        assert details.request_shape_sha256 == sha256(wire.calls[0].content).hexdigest()
        assert "private provider refusal prose" not in receipt.model_dump_json()
    finally:
        h.close()
        wire.adapter.close()


def test_decoder_bound_is_independent_of_transport() -> None:
    """Callers cannot bypass the raw parser cap with an already accumulated oversized body."""
    with pytest.raises(ValueError):
        decode(b" " * 17, 16)
    assert decode(b'{"ignored":1.25,"array":[1]}', 100)["ignored"] == 1.25


def test_response_cookies_never_change_the_second_request(wire: Wire) -> None:
    """A fixed origin alone does not prevent hidden cross-request cookie state."""
    wire.count = response(wire.count, **{"set-cookie": "conversation=opaque; Path=/"})
    wire.invoke()
    assert all("cookie" not in request.headers for request in wire.calls)


def test_httpx_info_preflight_rejects_server_controlled_reason_logging(wire: Wire) -> None:
    """Known INFO logging includes the response reason phrase, not just trusted status integers."""
    logger = logging.getLogger("httpx")
    original_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        with pytest.raises(ValueError):
            wire.invoke()
        assert logger.level == logging.INFO and not wire.calls
    finally:
        logger.setLevel(original_level)


def test_interrupted_first_clock_releases_adapter_capacity(
    wire: Wire, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even an interruption before request preparation must leave the single slot reusable."""
    def interrupt() -> float:
        """Interrupt the first owned operation after acquiring capacity."""
        raise KeyboardInterrupt

    with monkeypatch.context() as patch:
        patch.setattr("payops.orchestrator.openai_adapter.monotonic", interrupt)
        with pytest.raises(KeyboardInterrupt):
            wire.invoke()
    assert not wire.calls
    assert isinstance(wire.invoke(), ValidatedAIMessage)


def test_active_request_has_no_adapter_queue(wire: Wire) -> None:
    """A second direct host call cannot wait behind an existing count request in HTTPX's pool."""
    started, release = Event(), Event()
    body = json.dumps(wire.count).encode()

    class HeldStream(httpx.SyncByteStream):
        """Bounded actual worker blocking stands in for an outstanding transport read."""
        def __iter__(self) -> Iterator[bytes]:
            """Signal readiness before waiting, so the caller never relies on a timing sleep."""
            started.set()
            assert release.wait(3)
            yield body

    wire.count = httpx.Response(
        200, headers={"content-type": "application/json"}, stream=HeldStream()
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(wire.invoke)
        try:
            assert started.wait(1)
            with pytest.raises(PermissionError):
                wire.invoke()
            assert len(wire.calls) == 1
        finally:
            release.set()
        assert isinstance(pending.result(timeout=3), ValidatedAIMessage)


def test_stream_trickle_checks_elapsed_between_transport_chunks(
    wire: Wire, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inactivity can remain short while cumulative transport time exceeds the fixed deadline."""
    now = 100.0
    body = json.dumps(wire.count).encode()
    monkeypatch.setattr("payops.orchestrator.openai_adapter.monotonic", lambda: now)

    class Trickle(httpx.SyncByteStream):
        """No real sleep is needed to exercise the per-transport-chunk elapsed check."""
        def __iter__(self) -> Iterator[bytes]:
            """Advance less than the read-inactivity timeout for each incoming byte."""
            nonlocal now
            for offset in range(0, len(body), 8):
                now += 0.5
                yield body[offset:offset + 8]

    wire.count = httpx.Response(
        200, headers={"content-type": "application/json"}, stream=Trickle()
    )
    with pytest.raises(TimeoutError):
        wire.invoke()
    assert len(wire.calls) == 1 and now == 102.0


def test_only_mock_transport_can_override_production_configuration() -> None:
    """The public test seam does not accept an arbitrary proxy or custom network transport."""
    class ForeignTransport(httpx.MockTransport):
        """Subclass behavior is not part of the explicit MockTransport fixture seam."""

    with pytest.raises(ValueError, match="explicit mock transport"):
        OpenAIResponsesAdapter(
            settings(), SecretStr("synthetic-secret"),
            test_transport=ForeignTransport(lambda request: response({})),
        )


def test_production_transport_is_tls_fixed_retry_zero_and_environment_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Construction inspects the actual installed HTTPX API without making any connection."""
    transport_args: list[dict[str, Any]] = []
    client_args: list[dict[str, Any]] = []
    real_transport, real_client = httpx.HTTPTransport, httpx.Client

    def transport(**kwargs: Any) -> httpx.HTTPTransport:
        """Construct the real transport after retaining its nonsecret configuration."""
        transport_args.append(kwargs)
        return real_transport(**kwargs)

    def client(**kwargs: Any) -> httpx.Client:
        """Keep synthetic-only headers local to this test's assertions."""
        client_args.append(kwargs)
        return real_client(**kwargs)

    monkeypatch.setattr(httpx, "HTTPTransport", transport)
    monkeypatch.setattr(httpx, "Client", client)
    monkeypatch.setenv("OPENAI_API_KEY", "ignored-synthetic-environment-key")
    monkeypatch.setenv("HTTPS_PROXY", "http://unreachable.invalid:1")
    adapter = OpenAIResponsesAdapter(settings(), SecretStr("explicit-synthetic-key"))
    try:
        assert transport_args[0]["verify"] is True
        assert transport_args[0]["trust_env"] is False and transport_args[0]["retries"] == 0
        assert transport_args[0]["limits"].max_connections == 1
        assert client_args[0]["follow_redirects"] is False
        assert client_args[0]["trust_env"] is False
        assert client_args[0]["headers"]["Authorization"] == "Bearer explicit-synthetic-key"
        assert "event_hooks" not in client_args[0] and "mounts" not in client_args[0]
    finally:
        adapter.close()


def test_internal_path_guard_precedes_any_dispatch(wire: Wire) -> None:
    """Even internal accidental path changes cannot turn the fixed transport into generic HTTP."""
    with pytest.raises(ValueError, match="unsupported provider endpoint"):
        wire.adapter._post("/v1/other", b"{}", 16, 0)  # pyright: ignore[reportPrivateUsage]
    assert not wire.calls
