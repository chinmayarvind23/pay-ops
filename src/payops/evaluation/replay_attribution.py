"""Audit replay citations against a separately versioned, source-bound semantic review."""

from collections.abc import Mapping
from pathlib import Path

from pydantic import Field

from payops.contracts import Contract, Identifier
from payops.evaluation.labels import EXPECTED, Case
from payops.evaluation.replay_facts import ReplayCase, read_fact
from payops.orchestrator.model_runtime import ModelObservation


class FactReview(Contract):
    """A reviewer states which cause claims a specific source supports, with a visible rationale."""

    case_id: Case
    evidence_id: Identifier
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    supported_causes: tuple[Identifier, ...] = Field(max_length=24)
    rationale: str = Field(min_length=20, max_length=2000)


class ReplayAttributionScore(Contract):
    """Keep output coverage separate; abstention cannot create a perfect attribution score."""

    cases: int
    cases_without_decision: int
    cases_with_citations: int
    correct: int
    total: int
    invalid: int
    accuracy: float | None


def verify_review(
    root: Path,
    cases: tuple[ReplayCase, ...],
    reviews: tuple[FactReview, ...],
    vocabulary: frozenset[str],
) -> dict[tuple[str, str], FactReview]:
    """Verify all source bytes and exact case/fact coverage before considering any prediction."""
    if len(cases) != 24 or frozenset(case.case_id for case in cases) != EXPECTED:
        raise ValueError("attribution requires all 24 source cases")
    facts = {(case.case_id, fact.evidence_id): fact for case in cases for fact in case.facts}
    indexed = {(review.case_id, review.evidence_id): review for review in reviews}
    if (
        len(facts) != sum(len(case.facts) for case in cases)
        or len(indexed) != len(reviews)
        or facts.keys() != indexed.keys()
    ):
        raise ValueError("review must cover each case-scoped source exactly once")
    for key, fact in facts.items():
        review = indexed[key]
        if (
            review.sha256 != fact.sha256
            or len(set(review.supported_causes)) != len(review.supported_causes)
            or not set(review.supported_causes) <= vocabulary
        ):
            raise ValueError("review source binding or cause vocabulary differs")
        read_fact(root, fact)
    return indexed


def score_replay_attribution(
    root: Path,
    cases: tuple[ReplayCase, ...],
    reviews: tuple[FactReview, ...],
    observations: Mapping[str, ModelObservation],
    vocabulary: frozenset[str],
) -> ReplayAttributionScore:
    """Count every distinct predicted link; unresolved and unreviewed relations remain incorrect."""
    reference = verify_review(root, cases, reviews, vocabulary)
    if frozenset(observations) != EXPECTED:
        raise ValueError("retain an explicit outcome for every case, including failures")
    links: set[tuple[str, str, str, str]] = set()
    missing = 0
    for case_id, observation in observations.items():
        decision = observation.decision
        if observation.status != "OK" or decision is None:
            missing += 1
            continue
        for cause in decision.hypotheses:
            for relation, ids in (
                ("supports", cause.supporting_evidence_ids),
                ("refutes", cause.refuting_evidence_ids),
            ):
                links.update(
                    (case_id, cause.cause_code, evidence_id, relation) for evidence_id in ids
                )
    correct = invalid = 0
    for case_id, cause_code, evidence_id, relation in links:
        review = reference.get((case_id, evidence_id))
        if review is None or cause_code not in vocabulary:
            invalid += 1
        elif relation == "supports" and cause_code in review.supported_causes:
            correct += 1
    return ReplayAttributionScore(
        cases=len(cases), cases_without_decision=missing,
        cases_with_citations=len({link[0] for link in links}),
        correct=correct, total=len(links), invalid=invalid,
        accuracy=correct / len(links) if links else None,
    )
