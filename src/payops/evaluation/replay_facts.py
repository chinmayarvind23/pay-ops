"""Source-bound observations keep scorer labels and experiment plans outside model context."""

import json
import re
from hashlib import sha256
from pathlib import Path
from typing import Literal

from pydantic import Field, JsonValue

from payops.contracts import Contract, Identifier
from payops.evaluation.labels import EXPECTED, Case
from payops.orchestrator.openai_wire import decode


class FactReference(Contract):
    """A curator selects source pointers, not a free-written statement of the expected answer."""

    evidence_id: Identifier
    source: Literal["kubernetes", "deployment", "metrics", "logs", "traces", "http"]
    path: str = Field(min_length=1, max_length=500)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    pointer: str = Field(max_length=500)
    line: int | None = Field(default=None, ge=0, le=10000, strict=True)
    transform: Literal["identity", "nonzero_metrics"] = "identity"


class ReplayCase(Contract):
    """Case IDs and support annotations are scorer-only metadata and never serialized to prompts."""

    case_id: Case
    facts: tuple[FactReference, ...] = Field(min_length=1, max_length=12)
    supporting_facts: dict[Identifier, tuple[Identifier, ...]] = Field(default_factory=dict)


def at_pointer(value: JsonValue, pointer: str) -> JsonValue:
    """Resolve strict JSON Pointer syntax without evaluating code or a query language."""
    if not pointer:
        return value
    if not pointer.startswith("/"):
        raise ValueError("invalid source pointer")
    for part in pointer[1:].split("/"):
        if re.search(r"~(?![01])", part):
            raise ValueError("invalid pointer escape")
        key = part.replace("~1", "/").replace("~0", "~")
        if isinstance(value, dict):
            value = value[key]
        elif isinstance(value, list) and re.fullmatch(r"0|[1-9][0-9]*", key):
            value = value[int(key)]
        else:
            raise ValueError("source pointer does not resolve")
    return value


def read_fact(root: Path, reference: FactReference) -> JsonValue:
    """Hash raw bytes before projection; relative source paths cannot escape the evidence root."""
    root = root.resolve(strict=True)
    relative = Path(reference.path)
    path = (root / relative).resolve(strict=True)
    if relative.is_absolute() or root not in path.parents or not path.is_file():
        raise ValueError("source path escapes evidence root")
    with path.open("rb") as stream:
        raw = stream.read(4_194_305)
    if len(raw) > 4_194_304 or sha256(raw).hexdigest() != reference.sha256:
        raise ValueError("source evidence checksum differs")
    value = at_pointer(decode(raw, 4_194_304), reference.pointer)
    if reference.line is not None:
        if not isinstance(value, str):
            raise ValueError("line selection requires source text")
        value = value.splitlines()[reference.line]
    if reference.transform == "nonzero_metrics":
        if not isinstance(value, list) or any(
            not isinstance(row, dict) or type(row.get("value")) not in {int, float} for row in value
        ):
            raise ValueError("nonzero projection requires metric rows")
        filtered: list[JsonValue] = [
            row for row in value if isinstance(row, dict) and row["value"] != 0
        ]
        return filtered
    return value


def prompt_data(
    root: Path, case: ReplayCase, vocabulary: frozenset[str]
) -> tuple[str, frozenset[str]]:
    """Only source type, opaque evidence IDs and mechanically selected values reach the model."""
    ids = frozenset(fact.evidence_id for fact in case.facts)
    if len(ids) != len(case.facts) or not vocabulary:
        raise ValueError("duplicate evidence IDs or empty vocabulary")
    if any(not set(values) <= ids for values in case.supporting_facts.values()):
        raise ValueError("annotation cites unavailable evidence")
    observations = [
        {"id": fact.evidence_id, "source": fact.source, "value": read_fact(root, fact)}
        for fact in case.facts
    ]
    raw_observations = json.dumps(observations, ensure_ascii=True, separators=(",", ":"))
    if re.search(r"(?:OOM|ROLLOUT|SCHED|DEP|TELEM|PAY)-0[1-4]|SANDBOX_[A-Z_]+", raw_observations):
        raise ValueError("experiment identifier or injector configuration in model context")
    data = json.dumps(
        {"cause_codes": sorted(vocabulary), "untrusted_observations": observations},
        ensure_ascii=True,
        separators=(",", ":"),
    )
    if len(data) > 14000:
        raise ValueError("offline context exceeds reviewed bound")
    return data, ids


def load_cases(raw: bytes) -> tuple[ReplayCase, ...]:
    """Require all 24 unique cases before dispatch, retaining unavailable predictions as misses."""
    value = decode(raw, 262144)
    cases = value.get("cases")
    if value.get("version") != 1 or not isinstance(cases, list):
        raise ValueError("invalid replay corpus")
    parsed = tuple(ReplayCase.model_validate(case) for case in cases)
    if len(parsed) != 24 or frozenset(case.case_id for case in parsed) != EXPECTED:
        raise ValueError("replay requires all 24 cases")
    return parsed
