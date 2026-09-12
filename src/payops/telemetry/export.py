"""Export a checksum-verified model receipt without prompts, evidence or actor identifiers."""

import argparse
import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from langsmith import Client
from pydantic import AwareDatetime, Field, model_validator

from payops.contracts import Contract
from payops.evidence.artifacts import ArtifactStore
from payops.orchestrator.loop_records import ModelReceipt, restore


class TraceSummary(Contract):
    """An allowlist prevents future receipt fields from silently becoming hosted telemetry."""

    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    run_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    status: Literal["OK", "REFUSED", "INVALID_OUTPUT", "ERROR", "TIMEOUT", "DENIED", "BUSY"]
    provider_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    input_tokens: int | None = Field(default=None, ge=0, strict=True)
    output_tokens: int | None = Field(default=None, ge=0, strict=True)
    cached_input_tokens: int | None = Field(default=None, ge=0, strict=True)


class ExportWindow(Contract):
    """Wall-clock bounds must be supplied from an operator journal, never invented from latency."""

    started_at: AwareDatetime
    ended_at: AwareDatetime

    @model_validator(mode="after")
    def ordered(self) -> "ExportWindow":
        """Invalid timestamps cannot produce an apparently successful hosted timing trace."""
        if self.ended_at < self.started_at:
            raise ValueError("export window ends before it starts")
        return self


def summarize(store: ArtifactStore, digest: str) -> TraceSummary:
    """Reopen immutable bytes and retain only measured scalars; unknown usage stays unknown."""
    receipt = restore(store, digest, ModelReceipt)
    observation, usage = receipt.observation, receipt.observation.usage
    return TraceSummary(
        receipt_sha256=digest,
        run_sha256=sha256(receipt.run_id.encode()).hexdigest(),
        status=observation.status,
        provider_seconds=observation.provider_seconds,
        input_tokens=usage.input_tokens if usage else None,
        output_tokens=usage.output_tokens if usage else None,
        cached_input_tokens=usage.cached_input_tokens if usage else None,
    )


def export_langsmith(
    client: Client, summary: TraceSummary, window: ExportWindow, project: str
) -> str:
    """One explicit SDK call exports a receipt; a stable ID identifies repeated operator exports."""
    if not project or len(project) > 100:
        raise ValueError("invalid trace project")
    summary = TraceSummary.model_validate_json(summary.model_dump_json())
    window = ExportWindow.model_validate_json(window.model_dump_json())
    run_id = uuid5(NAMESPACE_URL, "payops:receipt:" + summary.receipt_sha256)
    client.create_run(
        "payops.model.receipt",
        {},
        "llm",
        id=run_id,
        project_name=project,
        start_time=window.started_at,
        end_time=window.ended_at,
        outputs={},
        extra={"metadata": summary.model_dump(mode="json")},
        tags=["payops", "receipt-export"],
    )
    return str(run_id)


def main() -> None:
    """Default to a local review file; hosted export requires an explicit credential file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--langsmith-key-file", type=Path)
    parser.add_argument("--project", default="payops")
    parser.add_argument("--started-at", type=datetime.fromisoformat)
    parser.add_argument("--ended-at", type=datetime.fromisoformat)
    args = parser.parse_args()
    summary = summarize(ArtifactStore(args.artifacts), args.receipt)
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(summary.model_dump(mode="json"), indent=2) + "\n")
    if args.langsmith_key_file is not None:
        window = ExportWindow(started_at=args.started_at, ended_at=args.ended_at)
        key = args.langsmith_key_file.read_text(encoding="utf-8").strip()
        if not key or len(key) > 1024:
            raise ValueError("invalid explicit LangSmith credential")
        client = Client(
            api_url="https://api.smith.langchain.com",
            api_key=key,
            auto_batch_tracing=False,
            timeout_ms=5000,
            hide_inputs=True,
            hide_outputs=True,
            omit_traced_runtime_info=True,
        )
        try:
            export_langsmith(client, summary, window, args.project)
        finally:
            client.close()


if __name__ == "__main__":
    main()
