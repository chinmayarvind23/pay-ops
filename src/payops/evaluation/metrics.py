"""Exact score denominators and evidence-grounded attribution, outside model control."""

import math
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
from typing import Literal

from payops.contracts import Contract, EvidenceItem, Identifier
from payops.evidence.artifacts import ArtifactStore, EvidenceIntegrityError


@dataclass(frozen=True)
class RecallScore:
    """Retain integers so a rounded target never changes the acceptance boundary."""

    hits: int
    total: int
    k: int

    @property
    def fraction(self) -> Fraction:
        """Expose exact arithmetic for machine-readable release gates."""
        return Fraction(self.hits, self.total)

    def meets(self, required_hits: int, required_total: int) -> bool:
        """A claimed threshold is itself validated before cross-multiplication."""
        if required_total <= 0 or not 0 <= required_hits <= required_total:
            raise ValueError("invalid recall threshold")
        return self.fraction >= Fraction(required_hits, required_total)


def recall_at_k(
    gold: Mapping[str, tuple[str, ...]], predictions: Mapping[str, tuple[str, ...]], k: int
) -> RecallScore:
    """Every frozen case stays in the denominator, including abstentions and missing outputs."""
    if not gold or k <= 0 or set(predictions) - set(gold):
        raise ValueError("invalid recall suite or k")
    if any(not causes or len(causes) != len(set(causes)) for causes in gold.values()):
        raise ValueError("each case requires distinct accepted gold causes")
    if any(len(ranking) != len(set(ranking)) for ranking in predictions.values()):
        raise ValueError("duplicate ranked causes")
    hits = sum(
        bool(set(accepted) & set(predictions.get(case, ())[:k])) for case, accepted in gold.items()
    )
    return RecallScore(hits=hits, total=len(gold), k=k)


class Attribution(Contract):
    """Relation and hypothesis are part of identity; a correct evidence ID alone is insufficient."""

    incident_id: Identifier
    cause_code: Identifier
    evidence_id: Identifier
    relation: Literal["supports", "refutes"]

    def __hash__(self) -> int:
        """Explicit immutable identity also makes hashability visible to strict type checkers."""
        return hash((self.incident_id, self.cause_code, self.evidence_id, self.relation))


@dataclass(frozen=True)
class AttributionScore:
    """Report undefined accuracy for no citations; coverage must be scored separately."""

    correct: int
    total: int
    invalid: int

    @property
    def accuracy(self) -> float | None:
        """No predictions cannot pass as perfectly attributed evidence."""
        return self.correct / self.total if self.total else None


def attribution_accuracy(
    predicted: tuple[Attribution, ...],
    gold: frozenset[Attribution],
    evidence: Mapping[str, EvidenceItem],
    store: ArtifactStore,
) -> AttributionScore:
    """Deduplicate repeated links while retaining invalid links as incorrect predictions."""
    unique = frozenset(predicted)
    correct = invalid = 0
    for link in unique:
        item = evidence.get(link.evidence_id)
        if (
            item is None
            or item.incident_id != link.incident_id
            or item.evidence_id != link.evidence_id
        ):
            invalid += 1
            continue
        try:
            store.verify(item)
        except (EvidenceIntegrityError, ValueError):
            invalid += 1
            continue
        correct += int(link in gold)
    return AttributionScore(correct=correct, total=len(unique), invalid=invalid)


def percentile(samples: tuple[float, ...], quantile: float) -> float:
    """Nearest rank requires actual nonnegative durations and rejects nonfinite samples."""
    if not samples or not 0 < quantile <= 1:
        raise ValueError("percentile requires samples and a quantile in (0, 1]")
    if any(not math.isfinite(sample) or sample < 0 for sample in samples):
        raise ValueError("invalid timing sample")
    return sorted(samples)[math.ceil(quantile * len(samples)) - 1]
