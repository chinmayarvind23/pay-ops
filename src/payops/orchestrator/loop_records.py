"""Typed reasoning receipts are replayable only through digests anchored in the SQL ledger."""

from hashlib import sha256
from typing import Literal

from pydantic import Field

from payops.contracts import Contract, Identifier
from payops.evidence.artifacts import (
    JSON_OBJECT,
    MAX_ARTIFACT_BYTES,
    ArtifactStore,
    EvidenceIntegrityError,
)
from payops.evidence.context import ReasoningContext
from payops.orchestrator.budget import BudgetLedger, Digest
from payops.orchestrator.model_runtime import ModelObservation, ModelPrompt
from payops.tools.registry import ReadResult


class PreparedTurn(Contract):
    """The complete host prompt and included evidence are fixed before provider dispatch."""

    kind: Literal["prepared_turn"] = "prepared_turn"
    run_id: Identifier
    operation_id: Identifier
    binding_sha256: Digest
    context: ReasoningContext
    prompt: ModelPrompt


class ModelReceipt(Contract):
    """A response belongs to one exact reserved prompt, including explicit failed outcomes."""

    kind: Literal["model_receipt"] = "model_receipt"
    run_id: Identifier
    operation_id: Identifier
    prompt_sha256: Digest
    observation: ModelObservation


class ReadReceipt(Contract):
    """A whole read batch is published together; incomplete writes never authorize replay."""

    kind: Literal["read_receipt"] = "read_receipt"
    run_id: Identifier
    operation_id: Identifier
    results: tuple[ReadResult, ...] = Field(min_length=1, max_length=2)


def retain(store: ArtifactStore, value: Contract) -> str:
    """ArtifactStore fsyncs immutable content before a completion digest can enter SQL."""
    return store.write(JSON_OBJECT.validate_python(value.model_dump(mode="json")))[1]


def restore[T: Contract](store: ArtifactStore, digest: str, schema: type[T]) -> T:
    """Bounded bytes and digest verification precede reconstruction of any saved operation."""
    try:
        with store.path_for(digest).open("rb") as stream:
            content = stream.read(MAX_ARTIFACT_BYTES + 1)
        if len(content) > MAX_ARTIFACT_BYTES or sha256(content).hexdigest() != digest:
            raise EvidenceIntegrityError("reasoning artifact digest differs")
        return schema.model_validate_json(content)
    except (OSError, ValueError) as error:
        raise EvidenceIntegrityError("reasoning artifact unavailable or invalid") from error


def completion(ledger: BudgetLedger, run_id: str, operation_id: str) -> str | None:
    """Absence means completion is unknown; it never grants a second dispatch."""
    return next(
        (
            item.artifact_sha256
            for item in ledger.get(run_id).completions
            if item.operation_id == operation_id
        ),
        None,
    )


def publish(ledger: BudgetLedger, store: ArtifactStore, receipt: ModelReceipt | ReadReceipt) -> str:
    """An interrupted write leaves a charged but unknown operation with no automatic retry."""
    digest = retain(store, receipt)
    ledger.complete(ledger.get(receipt.run_id), receipt.operation_id, digest)
    return digest
