"""Provider-neutral LangChain execution limits acceptance without claiming to cancel remote work."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass
from hashlib import sha256
from threading import BoundedSemaphore
from time import monotonic
from typing import Literal, Protocol, Self, runtime_checkable

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from opentelemetry import trace
from opentelemetry.context import Context, get_current
from pydantic import Field, model_validator

from payops.contracts import Contract, Identifier
from payops.orchestrator.reasoning import (
    ProviderUsage,
    ReasoningDecision,
    TextPrice,
    TokenAccounting,
    parse_decision,
)


class ModelSettings(Contract):
    """Host configuration binds provider identity, output limits and verified prices."""

    provider: Identifier
    model: str = Field(min_length=1, max_length=200)
    mode: Literal["fixture", "provider"]
    input_token_limit: int = Field(strict=True, ge=1, le=100000)
    output_token_limit: int = Field(strict=True, ge=1, le=16384)
    timeout_seconds: float = Field(gt=0, le=30, allow_inf_nan=False)
    price: TextPrice
    token_accounting: TokenAccounting = "fixture_exact"

    @model_validator(mode="after")
    def accounting_mode(self) -> Self:
        """Live provider framing is counted remotely after a conservative reservation commits."""
        if (self.mode == "provider") != (self.token_accounting == "provider_ceiling"):
            raise ValueError("model mode and token accounting differ")
        return self


class PromptMessage(Contract):
    """The host supplies roles; evidence strings can never create an additional chat message."""

    role: Literal["system", "user"]
    content: str = Field(min_length=1, max_length=32768)


class ModelPrompt(Contract):
    """An exact prepared two-message prompt is retained before its model budget is reserved."""

    messages: tuple[PromptMessage, PromptMessage]
    input_tokens: int = Field(strict=True, ge=0, le=100000)
    token_accounting: TokenAccounting = "fixture_exact"

    @model_validator(mode="after")
    def roles(self) -> Self:
        """Only the fixed system instruction followed by one evidence-data message is accepted."""
        if tuple(message.role for message in self.messages) != ("system", "user"):
            raise ValueError("prompt roles differ from host contract")
        return self

    def langchain_messages(self) -> tuple[BaseMessage, BaseMessage]:
        """Use real LangChain types instead of mixing provider-specific wire message schemas."""
        return SystemMessage(content=self.messages[0].content), HumanMessage(
            content=self.messages[1].content
        )


class ModelAdapter(Protocol):
    """Concrete providers must disable retries and enforce the supplied output cap at their API."""

    settings: ModelSettings

    def count_tokens(self, messages: tuple[BaseMessage, BaseMessage]) -> int:
        """Return an explicit fixture count or configured provider ceiling without network work."""
        ...

    def invoke(self, messages: tuple[BaseMessage, BaseMessage], output_limit: int) -> AIMessage:
        """Use bounded provider transport and the exact configured model/output limit."""
        ...

    def usage(self, message: AIMessage) -> ProviderUsage | None:
        """Read raw usage validated before SDK coercion; missing usage remains unknown."""
        ...


class ProviderDetails(Contract):
    """Actual count and stage timing are distinct from the conservatively reserved allowance."""

    counted_input_tokens: int = Field(strict=True, ge=0, le=100000)
    provider_requests: Literal[2]
    count_seconds: float = Field(ge=0, allow_inf_nan=False)
    generation_seconds: float = Field(ge=0, allow_inf_nan=False)
    request_shape_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    normalized_refusal: bool = Field(strict=True)


@runtime_checkable
class StagedModelAdapter(ModelAdapter, Protocol):
    """Only the runtime supplies an in-worker authority hook between remote count and generation."""

    def invoke_staged(
        self,
        messages: tuple[BaseMessage, BaseMessage],
        output_limit: int,
        before_generation: Callable[[int], bool],
    ) -> AIMessage:
        """Count and generate once under one reservation, requiring a fresh stage grant."""
        ...

    def details(self, message: AIMessage) -> ProviderDetails:
        """Per-response raw-validated details avoid mutable last-response accounting state."""
        ...


class ModelObservation(Contract):
    """Retain structured decisions and usage without raw reasoning or provider errors."""

    status: Literal["OK", "REFUSED", "INVALID_OUTPUT", "ERROR", "TIMEOUT", "DENIED", "BUSY"]
    decision: ReasoningDecision | None = None
    usage: ProviderUsage | None = None
    provider_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    output_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    provider_details: ProviderDetails | None = None

    @model_validator(mode="after")
    def coherent(self) -> Self:
        """Only successful structured output or explicit refusal may retain a model decision."""
        if (self.status in {"OK", "REFUSED"}) != (self.decision is not None):
            raise ValueError("model outcome and decision disagree")
        if self.decision is not None and (self.decision.decision == "refuse") != (
            self.status == "REFUSED"
        ):
            raise ValueError("refusal status differs from structured decision")
        return self


@dataclass(frozen=True)
class CompletedModel:
    """Finish time distinguishes actual completion from delayed result consumption."""

    observation: ModelObservation
    finished_at: float


class ModelRuntime:
    """One active provider call prevents timed-out work from accumulating a hidden queue."""

    def __init__(self, adapter: ModelAdapter, authorize: Callable[[], bool]) -> None:
        """Budget reservation belongs to the durable loop; this boundary owns provider execution."""
        self.adapter, self.authorize = adapter, authorize
        self.settings = ModelSettings.model_validate_json(adapter.settings.model_dump_json())
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="payops-model")
        self._slot = BoundedSemaphore(1)
        self._closed = False

    def prepare(self, system: str, data: str) -> ModelPrompt:
        """Validate the complete prompt before its durable cost reservation."""
        provisional = ModelPrompt(
            messages=(
                PromptMessage(role="system", content=system),
                PromptMessage(role="user", content=data),
            ),
            input_tokens=0,
            token_accounting=self.settings.token_accounting,
        )
        count = self.adapter.count_tokens(provisional.langchain_messages())
        prepared = ModelPrompt(
            messages=provisional.messages,
            input_tokens=count,
            token_accounting=self.settings.token_accounting,
        )
        if (
            prepared.input_tokens > self.settings.input_token_limit
            or self.settings.token_accounting == "provider_ceiling"
            and prepared.input_tokens != self.settings.input_token_limit
        ):
            raise ValueError("prepared prompt exceeds model input allowance")
        return prepared

    def authority(self) -> Literal["OK", "DENIED", "BUSY", "TIMEOUT", "ERROR"]:
        """Replay publication refresh shares provider capacity and a bounded acceptance deadline."""
        if self._closed or not self._slot.acquire(blocking=False):
            return "BUSY"
        try:
            deadline = monotonic() + min(8, self.settings.timeout_seconds)
            future = self._pool.submit(self._authority)
        except BaseException:
            self._slot.release()
            raise
        try:
            allowed, completed = future.result(timeout=max(0, deadline - monotonic()))
            if completed > deadline:
                return "TIMEOUT"
            return "OK" if allowed else "DENIED"
        except TimeoutError:
            return "TIMEOUT"
        except Exception:
            return "ERROR"

    def _authority(self) -> tuple[bool, float]:
        """Even timed-out authority refreshes retain capacity until their transport finishes."""
        try:
            return self.authorize(), monotonic()
        finally:
            self._slot.release()

    def observe(
        self, prompt: ModelPrompt, evidence_ids: frozenset[str], causes: frozenset[str]
    ) -> ModelObservation:
        """Call only after a new durable reservation; a timeout has unknown remote completion."""
        prompt = ModelPrompt.model_validate_json(prompt.model_dump_json())
        if (
            prompt.input_tokens > self.settings.input_token_limit
            or self._closed
            or prompt.token_accounting != self.settings.token_accounting
        ):
            raise ValueError("model runtime closed or prompt exceeds input allowance")
        if not self._slot.acquire(blocking=False):
            return ModelObservation(status="BUSY")
        try:
            deadline = monotonic() + self.settings.timeout_seconds
            future = self._pool.submit(
                self._call, prompt, evidence_ids, causes, get_current(), deadline
            )
        except BaseException:
            self._slot.release()
            raise
        try:
            completed = future.result(timeout=max(0, deadline - monotonic()))
            return (
                completed.observation
                if completed.finished_at <= deadline
                else ModelObservation(status="TIMEOUT")
            )
        except TimeoutError:
            return ModelObservation(status="TIMEOUT")
        except Exception:
            return ModelObservation(status="ERROR")

    def _call(
        self,
        prompt: ModelPrompt,
        evidence_ids: frozenset[str],
        causes: frozenset[str],
        parent: Context,
        deadline: float,
    ) -> CompletedModel:
        """Carry the investigation trace into its worker without exporting prompts or raw output."""
        try:
            with trace.get_tracer("payops.model").start_as_current_span(
                "invoke_agent", context=parent
            ) as span:
                span.set_attribute("gen_ai.operation.name", "chat")
                span.set_attribute("payops.model", self.settings.model)
                span.set_attribute("payops.mode", self.settings.mode)
                result = self._invoke(prompt, evidence_ids, causes, deadline)
                span.set_attribute("payops.status", result.status)
                if result.usage is not None:
                    span.set_attribute("gen_ai.usage.input_tokens", result.usage.input_tokens)
                    span.set_attribute("gen_ai.usage.output_tokens", result.usage.output_tokens)
                return CompletedModel(result, monotonic())
        finally:
            self._slot.release()

    def _stage_allowed(self, deadline: float) -> bool:
        """A slow identity lookup cannot start another network stage after the original cutoff."""
        if monotonic() >= deadline or self.adapter.settings != self.settings:
            return False
        allowed = self.authorize()
        return allowed and monotonic() < deadline and self.adapter.settings == self.settings

    def _count_allowed(self, count: int, prompt: ModelPrompt, deadline: float) -> bool:
        """Validate remote count before a stage grant can authorize generation spend."""
        if type(count) is not int or not 0 <= count <= prompt.input_tokens:
            raise ValueError("provider count exceeds reservation or has invalid type")
        return self._stage_allowed(deadline)

    def _dispatch(
        self, prompt: ModelPrompt, deadline: float
    ) -> tuple[AIMessage, float, ProviderDetails | None]:
        """Count and generation stay inside one owned slot; fixture timing remains separate."""
        messages = prompt.langchain_messages()
        if self.settings.token_accounting == "fixture_exact":
            started = monotonic()
            message = self.adapter.invoke(messages, self.settings.output_token_limit)
            return message, monotonic() - started, None
        if not isinstance(self.adapter, StagedModelAdapter):
            raise ValueError("provider ceiling requires staged adapter")
        message = self.adapter.invoke_staged(
            messages,
            self.settings.output_token_limit,
            lambda count: self._count_allowed(count, prompt, deadline),
        )
        details = ProviderDetails.model_validate_json(
            self.adapter.details(message).model_dump_json()
        )
        if details.counted_input_tokens > prompt.input_tokens:
            raise ValueError("provider count exceeds reserved ceiling")
        return message, details.generation_seconds, details

    def _invoke(
        self,
        prompt: ModelPrompt,
        evidence_ids: frozenset[str],
        causes: frozenset[str],
        deadline: float,
    ) -> ModelObservation:
        """Retain generation timing separately from token counting and stage authorization."""
        seconds: float | None = None
        usage: ProviderUsage | None = None
        details: ProviderDetails | None = None
        try:
            if not self._stage_allowed(deadline):
                return ModelObservation(status="DENIED")
            message, seconds, details = self._dispatch(prompt, deadline)
            projected = self.adapter.usage(message)
            usage = (
                ProviderUsage.model_validate_json(projected.model_dump_json())
                if projected is not None
                else None
            )
            expected = details.counted_input_tokens if details is not None else prompt.input_tokens
            if usage is not None and (
                usage.input_tokens != expected
                or usage.input_tokens > prompt.input_tokens
                or usage.output_tokens > self.settings.output_token_limit
            ):
                raise ValueError("provider usage violates prepared token contract")
            result = _decision(message, usage, seconds, evidence_ids, causes)
            if details is not None and details.normalized_refusal and result.status != "REFUSED":
                raise ValueError("normalized provider refusal differs from parsed decision")
            if not self._stage_allowed(deadline):
                result = ModelObservation(status="DENIED", usage=usage, provider_seconds=seconds)
            return ModelObservation.model_validate(
                {
                    **result.model_dump(),
                    "provider_details": details,
                }
            )
        except PermissionError:
            return ModelObservation(
                status="DENIED", usage=usage, provider_seconds=seconds, provider_details=details
            )
        except Exception:
            return ModelObservation(
                status="ERROR", usage=usage, provider_seconds=seconds, provider_details=details
            )

    def close(self) -> None:
        """Stop admission; in-flight provider transport must finish under its own timeout."""
        self._closed = True
        self._pool.shutdown(wait=False)


def _decision(
    message: AIMessage,
    usage: ProviderUsage | None,
    seconds: float,
    evidence_ids: frozenset[str],
    causes: frozenset[str],
) -> ModelObservation:
    """Malformed output retains only a bounded digest; arbitrary model reasoning is not logged."""
    content = message.content
    if not isinstance(content, str) or len(content) > 16384 or len(content.encode()) > 16384:
        return ModelObservation(status="INVALID_OUTPUT", usage=usage, provider_seconds=seconds)
    digest = sha256(content.encode()).hexdigest()
    try:
        decision = parse_decision(content, evidence_ids, causes)
    except ValueError:
        return ModelObservation(
            status="INVALID_OUTPUT", usage=usage, provider_seconds=seconds, output_sha256=digest
        )
    return ModelObservation(
        status="REFUSED" if decision.decision == "refuse" else "OK",
        decision=decision,
        usage=usage,
        provider_seconds=seconds,
        output_sha256=digest,
    )
