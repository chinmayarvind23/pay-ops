# evals/golden

`local-initial.json` retains the original four-case development labels used by the measured deterministic baseline.

`release-v2.json` declares the 24-case primary incident-cause labels before the expanded evaluation. It separately declares four observation conditions. For example, TELEM-03's incident cause is processor latency; its trace sampling gap is an observation condition. Detecting the gap alone earns no primary Recall credit.

The label contract rejects missing cases, duplicate alternatives, substituted conditions and duplicate JSON keys. Primary Recall always retains 24 cases; observation-condition Recall retains four. Missing predictions are misses. All 24 local scenarios now have qualified reproduction evidence; four have a frozen deterministic diagnosis score. Reproduction does not establish model accuracy.

Host model configuration uses `FrozenLabels.cause_vocabulary()` from the entire release catalog. Constructing a vocabulary from the current case's accepted answer would leak gold even if its scenario ID were hidden.
