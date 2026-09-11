"""Fixed Responses transport never discovers credentials or sends requests at import."""

import json
import logging
from collections.abc import Callable
from hashlib import sha256
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy
from threading import Lock
from time import monotonic

import httpx
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from pydantic import JsonValue, PrivateAttr, SecretStr

from payops.orchestrator.model_runtime import ModelSettings, ProviderDetails
from payops.orchestrator.openai_wire import MODEL, decode, raw_usage, response_text, server_count
from payops.orchestrator.reasoning import ProviderUsage, ReasoningDecision, TextPrice

ORIGIN = "https://api.openai.com"
COUNT_PATH = "/v1/responses/input_tokens"
CREATE_PATH = "/v1/responses"
STANDARD_PRICE = TextPrice(
    input_nano_usd=750, cached_input_nano_usd=75, output_nano_usd=4500
)


class ValidatedAIMessage(AIMessage):
    """Per-response validated accounting stays private, outside ordinary message serialization."""

    _validated_usage: ProviderUsage | None = PrivateAttr(default=None)
    _details: ProviderDetails | None = PrivateAttr(default=None)
    _request_shape_sha256: str = PrivateAttr(default="")
    _normalized_refusal: bool = PrivateAttr(default=False)

    @classmethod
    def from_validated(
        cls, content: str, usage: ProviderUsage | None, details: ProviderDetails,
        digest: str, refusal: bool,
    ) -> "ValidatedAIMessage":
        """The internal adapter constructs accounting only after the complete raw validation."""
        result = cls(content=content)
        result._validated_usage = usage
        result._details = details
        result._request_shape_sha256 = digest
        result._normalized_refusal = refusal
        return result

    def validated_usage(self) -> ProviderUsage | None:
        """Return a freshly validated immutable accounting contract, never coerced metadata."""
        usage = self._validated_usage
        return ProviderUsage.model_validate_json(usage.model_dump_json()) if usage else None

    def provider_details(self) -> ProviderDetails:
        """Uninitialized message instances cannot manufacture a measured request census."""
        if self._details is None:
            raise ValueError("provider response details missing")
        return ProviderDetails.model_validate_json(self._details.model_dump_json())

    @property
    def request_shape_sha256(self) -> str:
        """A safe digest binds both API stages to their shared immutable request fields."""
        return self._request_shape_sha256

    @property
    def normalized_refusal(self) -> bool:
        """Explicit provider refusal was mapped to the host's fixed typed refusal envelope."""
        return self._normalized_refusal


def _remaining(deadline: float) -> float:
    """Cooperative deadline checks never reset the absolute invocation budget."""
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError("provider deadline exceeded")
    return remaining


def _logging_safe() -> None:
    """Known transport logs contain server-controlled metadata; do not change global log levels."""
    if logging.getLogger("httpx").isEnabledFor(logging.INFO) or any(
        logging.getLogger(name).isEnabledFor(logging.DEBUG)
        for name in ("httpcore.connection", "httpcore.http11", "httpcore.http2")
    ):
        raise ValueError("provider transport debug logging must be disabled")


class _RejectCookies(DefaultCookiePolicy):
    """Count/create calls cannot establish hidden state through a provider response cookie."""

    def set_ok(self, cookie: Cookie, request: object) -> bool:
        """Reject every attempted cookie insertion into this adapter's private jar."""
        return False


def _shape(messages: tuple[BaseMessage, BaseMessage]) -> bytes:
    """Only the fixed two text roles and host schema can reach either Responses endpoint."""
    if (
        len(messages) != 2
        or type(messages[0]) is not SystemMessage
        or type(messages[1]) is not HumanMessage
    ):
        raise ValueError("provider requires fixed host message roles")
    content = [message.content for message in messages]
    if any(not isinstance(text, str) or not text or len(text) > 32768 for text in content):
        raise ValueError("invalid provider prompt text")
    payload = {
        "model": MODEL,
        "input": [
            {"role": "system", "content": content[0]},
            {"role": "user", "content": content[1]},
        ],
        "reasoning": {"effort": "none"},
        "text": {
            "format": {
                "type": "json_schema", "name": "payops_decision", "strict": True,
                "schema": ReasoningDecision.model_json_schema(),
            }
        },
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    if len(raw) > 65536:
        raise ValueError("provider prompt exceeds request byte bound")
    return raw


class OpenAIResponsesAdapter:
    """Operator-only settings and credentials bind one pinned, text-only, non-retrying provider."""

    def __init__(
        self, settings: ModelSettings, api_key: SecretStr,
        *, test_transport: httpx.MockTransport | None = None,
    ) -> None:
        """Construction configures a private client without loading environment or sending I/O."""
        self.settings = ModelSettings.model_validate_json(settings.model_dump_json())
        if (
            self.settings.provider != "openai"
            or self.settings.model != MODEL
            or self.settings.mode != "provider"
            or self.settings.token_accounting != "provider_ceiling"
            or self.settings.price != STANDARD_PRICE
            or self.settings.input_token_limit > 16000
            or not 16 <= self.settings.output_token_limit <= 2048
        ):
            raise ValueError("unsupported provider configuration")
        key = api_key.get_secret_value()
        if not key or len(key) > 4096 or any(not 33 <= ord(char) <= 126 for char in key):
            raise ValueError("invalid operator credential format")
        if test_transport is not None and type(test_transport) is not httpx.MockTransport:
            raise ValueError("only an explicit mock transport may replace the fixed transport")
        limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
        transport = test_transport or httpx.HTTPTransport(
            verify=True, trust_env=False, retries=0, limits=limits,
        )
        self._client = httpx.Client(
            transport=transport, trust_env=False, follow_redirects=False, limits=limits,
            cookies=CookieJar(policy=_RejectCookies()),
            headers={"Authorization": f"Bearer {key}", "Accept-Encoding": "identity"},
        )
        self._slot = Lock()
        self._closed = False

    def count_tokens(self, messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Return the configured reservation ceiling locally, not a measured token count."""
        _shape(messages)
        return self.settings.input_token_limit

    def invoke(self, messages: tuple[BaseMessage, BaseMessage], output_limit: int) -> AIMessage:
        """Legacy invocation cannot bypass the required current-authority stage hook."""
        raise PermissionError("provider requires staged runtime invocation")

    def invoke_staged(
        self, messages: tuple[BaseMessage, BaseMessage], output_limit: int,
        before_generation: Callable[[int], bool],
    ) -> AIMessage:
        """Call only after durable reservation; authorization and deadline gate generation."""
        if self._closed or not self._slot.acquire(blocking=False):
            raise PermissionError("provider adapter unavailable")
        try:
            deadline = monotonic() + min(25, self.settings.timeout_seconds)
            if type(output_limit) is not int or output_limit != self.settings.output_token_limit:
                raise ValueError("provider output cap differs from configured bound")
            _logging_safe()
            shape = _shape(messages)
            count_started = monotonic()
            measured = server_count(
                self._post(COUNT_PATH, shape, 16384, deadline), self.settings.input_token_limit
            )
            count_seconds = monotonic() - count_started
            _remaining(deadline)
            if before_generation(measured) is not True:
                raise PermissionError("provider stage authority denied")
            _remaining(deadline)
            _logging_safe()
            generation = decode(shape, 65536)
            generation.update({
                "max_output_tokens": output_limit, "stream": False,
                "background": False, "store": False, "service_tier": "default",
                "truncation": "disabled",
            })
            generation_started = monotonic()
            raw = self._post(
                CREATE_PATH, json.dumps(generation, separators=(",", ":")).encode(),
                131072, deadline,
            )
            generation_seconds = monotonic() - generation_started
            usage = raw_usage(raw.get("usage"), measured, output_limit)
            content, refused = response_text(raw)
            _remaining(deadline)
            return ValidatedAIMessage.from_validated(
                content, usage,
                ProviderDetails(
                    counted_input_tokens=measured, provider_requests=2,
                    count_seconds=count_seconds, generation_seconds=generation_seconds,
                    request_shape_sha256=sha256(shape).hexdigest(), normalized_refusal=refused,
                ),
                sha256(shape).hexdigest(), refused,
            )
        except (httpx.TimeoutException, TimeoutError):
            raise TimeoutError("provider transport deadline exceeded") from None
        except PermissionError:
            raise PermissionError("provider authority denied") from None
        except Exception:
            raise ValueError("provider protocol failure") from None
        finally:
            self._slot.release()

    def _post(
        self, path: str, body: bytes, limit: int, deadline: float,
    ) -> dict[str, JsonValue]:
        """Bound accumulated bytes and per-read inactivity; this is not preemptive cancellation."""
        if path not in {COUNT_PATH, CREATE_PATH}:
            raise ValueError("unsupported provider endpoint")
        remaining = _remaining(deadline)
        timeout = httpx.Timeout(
            min(1, remaining), connect=min(3, remaining),
            write=min(3, remaining), pool=min(3, remaining),
        )
        with self._client.stream(
            "POST", ORIGIN + path, content=body,
            headers={"Content-Type": "application/json"}, timeout=timeout,
        ) as response:
            _remaining(deadline)
            if response.status_code != 200:
                raise ValueError("provider request failed")
            if response.headers.get("content-encoding", "identity") != "identity":
                raise ValueError("compressed provider response unsupported")
            if response.headers.get("content-type", "").split(";", 1)[0] != "application/json":
                raise ValueError("provider JSON media type required")
            length = response.headers.get("content-length")
            if length is not None and (not length.isdecimal() or int(length) > limit):
                raise ValueError("provider response exceeds byte bound")
            data = bytearray()
            for chunk in response.iter_raw():
                _remaining(deadline)
                if len(data) + len(chunk) > limit:
                    raise ValueError("provider response exceeds byte bound")
                data.extend(chunk)
            _remaining(deadline)
            return decode(bytes(data), limit)

    def usage(self, message: AIMessage) -> ProviderUsage | None:
        """Only this adapter's explicitly validated message type exposes provider accounting."""
        if type(message) is not ValidatedAIMessage:
            raise ValueError("provider usage requires validated raw response")
        return message.validated_usage()

    def details(self, message: AIMessage) -> ProviderDetails:
        """Return separate count/generation timing without the interstage authorization wait."""
        if type(message) is not ValidatedAIMessage:
            raise ValueError("provider details require validated raw response")
        return message.provider_details()

    def close(self) -> None:
        """Close only this adapter's client; the host must coordinate active invocation shutdown."""
        self._closed = True
        self._client.close()
