"""Score retained replay outputs without inference or changes to predictions and corpus."""

import argparse
import json
from hashlib import sha256
from pathlib import Path

from pydantic import TypeAdapter

from payops.evaluation.labels import load_labels
from payops.evaluation.replay_attribution import FactReview, score_replay_attribution
from payops.evaluation.replay_facts import load_cases
from payops.orchestrator.model_runtime import ModelObservation
from payops.orchestrator.reasoning import reject_constant, unique_keys


def load(path: Path) -> tuple[bytes, object]:
    """Bound and reject duplicate keys/nonfinite values before extracting scoring inputs."""
    with path.open("rb") as stream:
        raw = stream.read(1048577)
    if len(raw) > 1048576:
        raise ValueError("scoring input exceeds byte bound")
    return raw, json.loads(raw, object_pairs_hook=unique_keys, parse_constant=reject_constant)


def main() -> None:
    """All output is write-once and binds the exact corpus, review, predictions and scorer bytes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--corpus", type=Path)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    corpus = args.corpus or repo / "evals/replay-v1/corpus.json"
    annotation = corpus.with_name("attribution-review-v1.json")
    labels = repo / "evals/golden/release-v2.json"
    corpus_raw, _ = load(corpus)
    review_raw, review = load(annotation)
    results_raw, rows = load(args.results)
    if not isinstance(review, dict) or review.get("version") != 1 or not isinstance(rows, list):
        raise ValueError("invalid review or prediction document")
    parsed = TypeAdapter(tuple[FactReview, ...]).validate_python(review["reviews"])
    observations = {}
    for row in rows:
        if not isinstance(row, dict) or row["case_id"] in observations:
            raise ValueError("duplicate or invalid prediction case")
        observations[row["case_id"]] = ModelObservation.model_validate(row["observation"])
    score = score_replay_attribution(
        corpus.parent,
        load_cases(corpus_raw),
        parsed,
        observations,
        load_labels(labels.read_bytes()).cause_vocabulary(),
    )
    report = {
        "metric": "post_hoc_support_link_reference_match",
        "reviewer": review["reviewer"],
        "scope": review["scope"],
        "timing": review["timing"],
        "score": score.model_dump(mode="json"),
        "corpus_sha256": sha256(corpus_raw).hexdigest(),
        "review_sha256": sha256(review_raw).hexdigest(),
        "results_sha256": sha256(results_raw).hexdigest(),
        "scorer_sha256": sha256(
            (repo / "src/payops/evaluation/replay_attribution.py").read_bytes()
        ).hexdigest(),
    }
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps(report["score"]))


if __name__ == "__main__":
    main()
