"""Zero-provider-charge Qwen inference through a fixed loopback llama.cpp server."""

import json
from collections.abc import Callable
from hashlib import sha256
from threading import Lock
from time import monotonic

import httpx
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from pydantic import JsonValue

from payops.orchestrator.model_runtime import ModelSettings, ProviderDetails
from payops.orchestrator.openai_adapter import ValidatedAIMessage
from payops.orchestrator.openai_wire import count, decode, object_value
from payops.orchestrator.reasoning import ProviderUsage, ReasoningDecision, TextPrice

ORIGIN = "http://127.0.0.1:18089"
MODEL = "payops-qwen3-1.7b-q4-k-m"
ZERO_PRICE = TextPrice(input_nano_usd=0, cached_input_nano_usd=0, output_nano_usd=0)


def framed_prompt(messages: tuple[BaseMessage, BaseMessage]) -> str:
    """Pin Qwen's observed non-thinking template and reject injected special-token delimiters."""
    if (
        len(messages) != 2
        or type(messages[0]) is not SystemMessage
        or type(messages[1]) is not HumanMessage
    ):
        raise ValueError("local model requires fixed host roles")
    parts: list[str] = []
    for message in messages:
        content = message.content
        if (
            not isinstance(content, str)
            or not content
            or len(content) > 32768
            or any(token in content for token in ("<|im_start|>", "<|im_end|>", "<|endoftext|>"))
        ):
            raise ValueError("invalid local model prompt")
        parts.append(content)
    return (
        "<|im_start|>system\n"
        + parts[0]
        + "<|im_end|>\n<|im_start|>user\n"
        + parts[1]
        + "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
    )


class LocalLlamaAdapter:
    """No credential discovery, remote URL, paid fallback, retries or model-selected tools exist."""

    def __init__(
        self, settings: ModelSettings, *, test_transport: httpx.MockTransport | None = None
    ) -> None:
        """The host owns the pinned model process; accounting remains real provider-mode usage."""
        self.settings = ModelSettings.model_validate_json(settings.model_dump_json())
        if (
            settings.provider != "local_llama"
            or settings.model != MODEL
            or settings.mode != "provider"
            or settings.token_accounting != "provider_ceiling"
            or settings.price != ZERO_PRICE
            or settings.input_token_limit + settings.output_token_limit > 8192
        ):
            raise ValueError("unsupported zero-spend model configuration")
        self._client = httpx.Client(
            transport=test_transport or httpx.HTTPTransport(retries=0),
            trust_env=False,
            follow_redirects=False,
            headers={"Accept-Encoding": "identity"},
        )
        self._slot, self._closed = Lock(), False

    def close(self) -> None:
        """Closing the client leaves the operator-owned model process running."""
        self._closed = True
        self._client.close()

    def count_tokens(self, messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Prepare reserves a ceiling locally; exact server tokenization follows the SQL claim."""
        framed_prompt(messages)
        return self.settings.input_token_limit

    def invoke(self, messages: tuple[BaseMessage, BaseMessage], output_limit: int) -> AIMessage:
        """Unstaged invocation cannot skip the current-authority check before generation."""
        raise PermissionError("local model requires staged invocation")

    def _post(self, path: str, payload: object, deadline: float) -> dict[str, JsonValue]:
        """Bound both request and decoded response bytes on the two fixed local endpoints."""
        remaining = deadline - monotonic()
        raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode()
        if remaining <= 0 or len(raw) > 131072 or path not in {"/tokenize", "/completion"}:
            raise ValueError("local model request exceeds bound")
        with self._client.stream(
            "POST",
            ORIGIN + path,
            content=raw,
            headers={"Content-Type": "application/json"},
            timeout=remaining,
        ) as response:
            if (
                response.status_code != 200
                or response.headers.get("content-encoding", "identity") != "identity"
            ):
                raise ValueError("local model transport failed")
            body = bytearray()
            for chunk in response.iter_raw():
                body.extend(chunk)
                if len(body) > 131072 or monotonic() >= deadline:
                    raise TimeoutError("local model response exceeds bound")
        return dict(decode(bytes(body), 131072))

    def invoke_staged(
        self,
        messages: tuple[BaseMessage, BaseMessage],
        output_limit: int,
        before_generation: Callable[[int], bool],
    ) -> AIMessage:
        """Tokenize once, reauthorize, then generate the exact counted token sequence once."""
        if self._closed or not self._slot.acquire(blocking=False):
            raise PermissionError("local model adapter unavailable")
        try:
            deadline = monotonic() + self.settings.timeout_seconds
            if type(output_limit) is not int or output_limit != self.settings.output_token_limit:
                raise ValueError("local output cap differs from configured bound")
            prompt = framed_prompt(messages)
            started = monotonic()
            raw = self._post(
                "/tokenize",
                {"content": prompt, "add_special": True, "parse_special": True},
                deadline,
            )
            tokens = raw.get("tokens")
            if (
                not isinstance(tokens, list)
                or not tokens
                or len(tokens) > self.settings.input_token_limit
                or any(type(token) is not int or not 0 <= token <= 200000 for token in tokens)
            ):
                raise ValueError("invalid local tokenization")
            count_seconds = monotonic() - started
            if before_generation(len(tokens)) is not True or monotonic() >= deadline:
                raise PermissionError("local generation authority denied")
            started = monotonic()
            result = self._post(
                "/completion",
                {
                    "prompt": tokens,
                    "model": MODEL,
                    "n_predict": output_limit,
                    "temperature": 0,
                    "seed": 0,
                    "stream": False,
                    "cache_prompt": False,
                    "json_schema": ReasoningDecision.model_json_schema(),
                },
                deadline,
            )
            details = ProviderDetails(
                counted_input_tokens=len(tokens),
                provider_requests=2,
                count_seconds=count_seconds,
                generation_seconds=monotonic() - started,
                request_shape_sha256=sha256(prompt.encode()).hexdigest(),
                normalized_refusal=False,
            )
            return validated_message(result, details, output_limit)
        finally:
            self._slot.release()

    @staticmethod
    def usage(message: AIMessage) -> ProviderUsage | None:
        """Only this adapter's raw-validated response envelope can carry measured token usage."""
        if not isinstance(message, ValidatedAIMessage):
            raise ValueError("unvalidated local model message")
        return message.validated_usage()

    @staticmethod
    def details(message: AIMessage) -> ProviderDetails:
        """Per-message census avoids mutable last-response state across investigations."""
        if not isinstance(message, ValidatedAIMessage):
            raise ValueError("unvalidated local model message")
        return message.provider_details()


def validated_message(raw: dict[str, JsonValue], details: ProviderDetails, limit: int) -> AIMessage:
    """Truncated, wrong-model and inconsistent usage cannot count as a completed model result."""
    value = decode(json.dumps(raw).encode(), 131072)
    content = value.get("content")
    if (
        value.get("model") != MODEL
        or value.get("stop") is not True
        or value.get("truncated") is not False
        or value.get("stop_type") != "eos"
        or not isinstance(content, str)
        or not content
        or len(content.encode()) > 16384
    ):
        raise ValueError("incomplete local model response")
    measured = count(value.get("tokens_evaluated"))
    output = count(value.get("tokens_predicted"))
    cached = count(object_value(value.get("timings"))["cache_n"])
    if measured != details.counted_input_tokens or output > limit:
        raise ValueError("local usage disagrees with request")
    usage = ProviderUsage(
        input_tokens=measured,
        output_tokens=output,
        total_tokens=measured + output,
        cached_input_tokens=cached,
    )
    return ValidatedAIMessage.from_validated(
        content, usage, details, details.request_shape_sha256, False
    )
