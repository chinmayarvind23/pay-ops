"""Record real interactive human trials and score a complete preregistered paired study."""

import argparse
import json
from hashlib import sha256
from pathlib import Path
from time import monotonic

from pydantic import TypeAdapter

from payops.contracts import Identifier, utc_now
from payops.evaluation.labels import Case, load_labels
from payops.evaluation.timing import TimingTrial, load_timing_trial, summarize_timing
from payops.orchestrator.openai_wire import decode


def save(path: Path, value: object) -> None:
    """Write once; an interrupted trial keeps its start record without inventing an end time."""
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)


def record(args: argparse.Namespace) -> None:
    """The stopwatch covers evidence inspection and answer entry by an actual participant."""
    evidence = args.evidence.read_bytes()
    if len(evidence) > 262144:
        raise ValueError("trial evidence exceeds display bound")
    content = evidence.decode("utf-8")
    args.output.mkdir(parents=True, exist_ok=False)
    input("Human participant: press Enter when ready to inspect the assigned evidence. ")
    started, clock = utc_now().isoformat(), monotonic()
    save(
        args.output / "started.json",
        {
            "started_at": started,
            "participant_id": args.participant,
            "case_id": args.case,
            "condition": args.condition,
            "order": args.order,
            "evidence_sha256": sha256(evidence).hexdigest(),
        },
    )
    with (args.output / "source.txt").open("xb") as stream:
        stream.write(evidence)
    print(content)
    answer = input("Enter the diagnosed cause code: ").strip()
    elapsed = monotonic() - clock
    trial = TimingTrial(
        participant_id=args.participant,
        case_id=args.case,
        condition=args.condition,
        order=args.order,
        elapsed_seconds=elapsed,
        cause_code=answer,
        evidence_sha256=sha256(evidence).hexdigest(),
    )
    save(args.output / "trial.json", trial.model_dump(mode="json"))
    print(f"Recorded {elapsed:.3f} seconds; correctness is scored separately.")


def score(args: argparse.Namespace) -> None:
    """Freeze a pair census before collection; incomplete pairs cannot disappear from the score."""
    plan_raw = args.plan.read_bytes()
    pairs = TypeAdapter(tuple[tuple[Identifier, Case], ...]).validate_python(
        decode(plan_raw, 262144)["pairs"]
    )
    if len(set(pairs)) != len(pairs):
        raise ValueError("duplicate planned pair")
    trials = tuple(load_timing_trial(path) for path in args.trials)
    labels_raw = args.labels.read_bytes()
    report = summarize_timing(trials, load_labels(labels_raw), frozenset(pairs))
    report.update(
        plan_sha256=sha256(plan_raw).hexdigest(),
        labels_sha256=sha256(labels_raw).hexdigest(),
        trial_sha256=[sha256(path.read_bytes()).hexdigest() for path in args.trials],
    )
    save(args.output, report)
    print(json.dumps(report, indent=2))


def main() -> None:
    """No generated baseline or simulated participant is included in the recording command."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("record")
    capture.add_argument("--participant", required=True)
    capture.add_argument("--case", required=True)
    capture.add_argument("--condition", choices=("baseline", "assisted"), required=True)
    capture.add_argument("--order", type=int, choices=(1, 2), required=True)
    capture.add_argument("--evidence", type=Path, required=True)
    capture.add_argument("--output", type=Path, required=True)
    scoring = commands.add_parser("score")
    scoring.add_argument("--plan", type=Path, required=True)
    scoring.add_argument("--trials", type=Path, nargs="+", required=True)
    scoring.add_argument("--labels", type=Path, default=Path("evals/golden/release-v2.json"))
    scoring.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    record(args) if args.command == "record" else score(args)


if __name__ == "__main__":
    main()
