"""Paired human timing keeps incorrect diagnoses and order effects in the reported denominator."""

from collections import Counter
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Literal

from pydantic import Field, JsonValue

from payops.contracts import Contract, Identifier
from payops.evaluation.labels import Case, FrozenLabels, score_causes
from payops.orchestrator.openai_wire import decode


class TimingTrial(Contract):
    """One human trial; source hashes bind the packet presented to the participant."""

    participant_id: Identifier
    case_id: Case
    condition: Literal["baseline", "assisted"]
    order: Literal[1, 2]
    elapsed_seconds: float = Field(strict=True, gt=0, le=7200, allow_inf_nan=False)
    cause_code: Identifier
    evidence_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


def summarize_timing(
    trials: tuple[TimingTrial, ...],
    labels: FrozenLabels,
    planned_pairs: frozenset[tuple[str, str]],
) -> dict[str, object]:
    """No empty, duplicate or unpaired study can produce a speedup; failures remain in medians."""
    paired: dict[tuple[str, str], dict[str, TimingTrial]] = {}
    for trial in trials:
        pair = paired.setdefault((trial.participant_id, trial.case_id), {})
        if trial.condition in pair:
            raise ValueError("duplicate timing condition")
        pair[trial.condition] = trial
    if not paired or frozenset(paired) != planned_pairs:
        raise ValueError("completed timing pairs differ from the frozen plan")
    if any(set(pair) != {"baseline", "assisted"} for pair in paired.values()):
        raise ValueError("timing requires nonempty complete pairs")
    if any({trial.order for trial in pair.values()} != {1, 2} for pair in paired.values()):
        raise ValueError("paired trials must record distinct presentation order")
    baseline = [pair["baseline"] for pair in paired.values()]
    assisted = [pair["assisted"] for pair in paired.values()]
    first = median(trial.elapsed_seconds for trial in baseline)
    second = median(trial.elapsed_seconds for trial in assisted)
    return {
        "pairs": len(paired),
        "participants": len({trial.participant_id for trial in trials}),
        "baseline_median_seconds": first,
        "assisted_median_seconds": second,
        "ratio_of_medians": first / second,
        "baseline_correct": sum(correct(trial, labels) for trial in baseline),
        "assisted_correct": sum(correct(trial, labels) for trial in assisted),
        "baseline_first_pairs": Counter(trial.order for trial in baseline)[1],
        "limitations": [
            "Human participation and source access need researcher verification",
            "Repeated-case learning and order effects require a preregistered protocol",
            "All completed pairs, including wrong diagnoses, remain in timing medians",
        ],
    }


def correct(trial: TimingTrial, labels: FrozenLabels) -> bool:
    """Use frozen release gold after measurement, never the participant's self-rated correctness."""
    score = score_causes(labels, {trial.case_id: (trial.cause_code,)}, 1)
    return score.hits == 1


def load_timing_trial(path: Path) -> TimingTrial:
    """A completed trial must match its original start record and the actual presented packet."""
    trial = TimingTrial.model_validate(read_record(path))
    started = read_record(path.with_name("started.json"))
    fields = ("participant_id", "case_id", "condition", "order", "evidence_sha256")
    if any(started.get(field) != getattr(trial, field) for field in fields):
        raise ValueError("timing trial differs from its start record")
    with path.with_name("source.txt").open("rb") as stream:
        packet = stream.read(262145)
    if len(packet) > 262144 or sha256(packet).hexdigest() != trial.evidence_sha256:
        raise ValueError("timing source packet checksum differs")
    return trial


def read_record(path: Path) -> dict[str, JsonValue]:
    """Bound trial metadata and reject duplicate fields before identity validation."""
    with path.open("rb") as stream:
        return decode(stream.read(16385), 16384)
