"""Provider-neutral LangChain execution limits acceptance without claiming to cancel remote work."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass
from hashlib import sha256
from threading import BoundedSemaphore
from time import monotonic
from typing import Literal, Protocol, Self

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from opentelemetry import trace
from opentelemetry.context import Context, get_current
from pydantic import Field, model_validator

from payops.contracts import Contract, Identifier
from payops.orchestrator.reasoning import (
    ProviderUsage,
    ReasoningDecision,
    TextPrice,
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


class PromptMessage(Contract):
    """The host supplies roles; evidence strings can never create an additional chat message."""

    role: Literal["system", "user"]
    content: str = Field(min_length=1, max_length=32768)


class ModelPrompt(Contract):
    """An exact prepared two-message prompt is retained before its model budget is reserved."""

    messages: tuple[PromptMessage, PromptMessage]
    input_tokens: int = Field(strict=True, ge=0, le=100000)

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
        """Count the complete prepared prompt locally with the configured provider tokenizer."""
        ...

    def invoke(self, messages: tuple[BaseMessage, BaseMessage], output_limit: int) -> AIMessage:
        """Use bounded provider transport and the exact configured model/output limit."""
        ...

    def usage(self, message: AIMessage) -> ProviderUsage | None:
        """Read raw usage validated before SDK coercion; missing usage remains unknown."""
        ...


class ModelObservation(Contract):
    """Retain structured decisions and usage without raw reasoning or provider errors."""

    status: Literal["OK", "REFUSED", "INVALID_OUTPUT", "ERROR", "TIMEOUT", "DENIED", "BUSY"]
    decision: ReasoningDecision | None = None
    usage: ProviderUsage | None = None
    provider_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    output_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

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
        )
        count = self.adapter.count_tokens(provisional.langchain_messages())
        prepared = ModelPrompt(messages=provisional.messages, input_tokens=count)
        if prepared.input_tokens > self.settings.input_token_limit:
            raise ValueError("prepared prompt exceeds model input allowance")
        return prepared

    def observe(
        self, prompt: ModelPrompt, evidence_ids: frozenset[str], causes: frozenset[str]
    ) -> ModelObservation:
        """Call only after a new durable reservation; a timeout has unknown remote completion."""
        prompt = ModelPrompt.model_validate_json(prompt.model_dump_json())
        if prompt.input_tokens > self.settings.input_token_limit or self._closed:
            raise ValueError("model runtime closed or prompt exceeds input allowance")
        if not self._slot.acquire(blocking=False):
            return ModelObservation(status="BUSY")
        try:
            deadline = monotonic() + self.settings.timeout_seconds
            future = self._pool.submit(self._call, prompt, evidence_ids, causes, get_current())
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
    ) -> CompletedModel:
        """Carry the investigation trace into its worker without exporting prompts or raw output."""
        try:
            with trace.get_tracer("payops.model").start_as_current_span(
                "invoke_agent", context=parent
            ) as span:
                span.set_attribute("gen_ai.operation.name", "chat")
                span.set_attribute("payops.model", self.settings.model)
                span.set_attribute("payops.mode", self.settings.mode)
                result = self._invoke(prompt, evidence_ids, causes)
                span.set_attribute("payops.status", result.status)
                if result.usage is not None:
                    span.set_attribute("gen_ai.usage.input_tokens", result.usage.input_tokens)
                    span.set_attribute("gen_ai.usage.output_tokens", result.usage.output_tokens)
                return CompletedModel(result, monotonic())
        finally:
            self._slot.release()

    def _invoke(
        self, prompt: ModelPrompt, evidence_ids: frozenset[str], causes: frozenset[str]
    ) -> ModelObservation:
        """Measure provider invocation separately from authorization and output validation."""
        seconds: float | None = None
        usage: ProviderUsage | None = None
        try:
            if self.adapter.settings != self.settings or not self.authorize():
                return ModelObservation(status="DENIED")
            started = monotonic()
            message = self.adapter.invoke(
                prompt.langchain_messages(), self.settings.output_token_limit
            )
            seconds = monotonic() - started
            projected = self.adapter.usage(message)
            usage = (
                ProviderUsage.model_validate_json(projected.model_dump_json())
                if projected is not None
                else None
            )
            if usage is not None and (
                usage.input_tokens != prompt.input_tokens
                or usage.output_tokens > self.settings.output_token_limit
            ):
                raise ValueError("provider usage violates prepared token contract")
            result = _decision(message, usage, seconds, evidence_ids, causes)
            if not self.authorize() or self.adapter.settings != self.settings:
                return ModelObservation(status="DENIED", usage=usage, provider_seconds=seconds)
            return result
        except Exception:
            return ModelObservation(status="ERROR", usage=usage, provider_seconds=seconds)

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
