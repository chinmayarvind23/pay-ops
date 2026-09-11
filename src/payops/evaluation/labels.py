"""Frozen incident causes and observation conditions use separate, explicit denominators."""

import json
from collections.abc import Mapping
from typing import Annotated, Literal, Self, get_args

from pydantic import Field, StringConstraints, model_validator

from payops.contracts import Contract, Identifier
from payops.evaluation.local import unique_object
from payops.evaluation.metrics import RecallScore, recall_at_k

Case = Annotated[str, StringConstraints(pattern=r"^(OOM|ROLLOUT|SCHED|DEP|TELEM|PAY)-0[1-4]$")]
Causes = Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=3)]
Condition = Literal[
    "UNRELATED_CPU_NOISE", "ARCHIVED_ERROR_DISTRACTOR", "TRACE_SAMPLING_GAP", "DELAYED_METRICS"
]
EXPECTED = frozenset(
    f"{group}-{number:02}"
    for group in ("OOM", "ROLLOUT", "SCHED", "DEP", "TELEM", "PAY")
    for number in range(1, 5)
)
CONDITIONS: dict[str, tuple[Condition, ...]] = {
    "TELEM-01": ("UNRELATED_CPU_NOISE",),
    "TELEM-02": ("ARCHIVED_ERROR_DISTRACTOR",),
    "TELEM-03": ("TRACE_SAMPLING_GAP",),
    "TELEM-04": ("DELAYED_METRICS",),
}


class FrozenLabels(Contract):
    """Version two fixes the cause/challenge distinction before expanded model evaluation."""

    version: Literal[2]
    primary_causes: dict[Case, Causes]
    observation_conditions: dict[Case, tuple[Condition, ...]]

    @model_validator(mode="after")
    def census(self) -> Self:
        """All 24 causes and four declared telemetry conditions survive missing predictions."""
        if frozenset(self.primary_causes) != EXPECTED or set(self.observation_conditions) != {
            f"TELEM-{number:02}" for number in range(1, 5)
        }:
            raise ValueError("release label census differs")
        recall_at_k(self.primary_causes, {}, 1)
        recall_at_k(self.observation_conditions, {}, 1)
        if self.observation_conditions != CONDITIONS:
            raise ValueError("version two observation assignments differ")
        conditions = frozenset(get_args(Condition))
        if conditions.intersection(
            value for values in self.primary_causes.values() for value in values
        ):
            raise ValueError("an observation condition cannot substitute for an incident cause")
        return self

    def cause_vocabulary(self) -> frozenset[str]:
        """Hosts supply the whole release vocabulary, never a case-specific accepted answer."""
        return frozenset(value for values in self.primary_causes.values() for value in values)


def load_labels(content: bytes) -> FrozenLabels:
    """Duplicate keys and oversized input fail before any live scenario or scorer dispatch."""
    if len(content) > 16384:
        raise ValueError("release labels exceed byte bound")
    try:
        return FrozenLabels.model_validate(json.loads(content, object_pairs_hook=unique_object))
    except RecursionError:
        raise ValueError("release label nesting exceeds bounds") from None


def score_causes(
    labels: FrozenLabels, predictions: Mapping[str, tuple[str, ...]], k: int
) -> RecallScore:
    """Main Recall@k covers 24 incident causes, including unavailable or unqualified outputs."""
    labels = load_labels(labels.model_dump_json().encode())
    return recall_at_k(labels.primary_causes, predictions, k)


def score_conditions(
    labels: FrozenLabels, predictions: Mapping[str, tuple[str, ...]], k: int
) -> RecallScore:
    """Observation-condition Recall@k is a separate four-case metric, never a root-cause hit."""
    labels = load_labels(labels.model_dump_json().encode())
    return recall_at_k(labels.observation_conditions, predictions, k)
