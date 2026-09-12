# Human investigation timing

Scoring reopens each trial's start record and presented source packet. Participant, case,
condition, order and source digest must match; altered or missing packets fail before
statistics are computed. This establishes journal consistency, not proof of participation.

The paired-study collector and scorer are implemented. No human trials have been run, so the 11.8-to-2.9-minute claim remains unmeasured.

Before recruiting participants, freeze a study plan containing every participant/case pair, for example `{"pairs":[["participant-01","DEP-01"]]}`. This is a format example, not a completed study. Assign half the pairs baseline-first and half assisted-first. Specify the same incident intake, available operational tools, stopping rule, training, time budget and a washout interval for both conditions. Record the repeated-case learning limitation. Keep cause labels hidden until scoring.

Baseline participants use the specified observability tools and runbooks without PayOps recommendations. Assisted participants additionally receive the PayOps report. Use the same information-access policy and record which report/source packet was presented. A researcher should verify participation, treatment adherence and retained operator activity; a JSON journal alone cannot prove these.

Record each actual trial interactively:

```bash
uv run python scripts/timing_study.py record --participant participant-01 --case DEP-01 --condition baseline --order 1 --evidence baseline-intake.txt --output /new/baseline-trial
uv run python scripts/timing_study.py record --participant participant-01 --case DEP-01 --condition assisted --order 2 --evidence assisted-intake.txt --output /new/assisted-trial
```

The stopwatch starts before displaying the intake and ends when the participant enters a cause code. The participant may use the assigned investigation tools during that interval. An interrupted trial retains its start record and cannot count as completed. The collector stores the presented packet and its SHA-256 hash. Do not enter simulated participants or scripted waits as human observations.

After all planned pairs finish:

```bash
uv run python scripts/timing_study.py score --plan plan.json --trials /new/baseline-trial/trial.json /new/assisted-trial/trial.json --output /new/study-summary.json
```

The scorer rejects empty, duplicate, incomplete or omitted pairs and repeated presentation positions. It scores answers against frozen cause labels and reports both correctness counts, both medians, the ratio of medians and presentation order. Wrong diagnoses remain in the timing denominator. Report participant/pair counts, accuracy, order and study limitations with any speedup; repeat trials and uncertainty estimates are still needed before a general productivity claim.
