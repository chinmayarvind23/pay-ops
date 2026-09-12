"""The published development corpus must reproduce the same bounded model inputs."""

from pathlib import Path

from payops.evaluation.labels import EXPECTED, load_labels
from payops.evaluation.replay_facts import load_cases, prompt_data


def test_published_replay_has_all_source_bound_cases() -> None:
    """A missing or modified projected fact must fail before any evaluator can dispatch."""
    root = Path(__file__).resolve().parents[2]
    corpus = root / "evals/replay-v1"
    cases = load_cases((corpus / "corpus.json").read_bytes())
    labels = load_labels((root / "evals/golden/release-v2.json").read_bytes())
    assert {case.case_id for case in cases} == EXPECTED
    for case in cases:
        data, ids = prompt_data(corpus, case, labels.cause_vocabulary())
        assert ids and case.case_id not in data
        assert "SANDBOX_" not in data and "PAYOPS_" not in data
