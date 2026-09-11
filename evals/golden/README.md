# evals/golden

`local-initial.json` retains the original four-case development labels used by the measured deterministic baseline.

`release-v2.json` declares the 24-case primary incident-cause labels before the expanded evaluation. It separately declares four observation conditions. For example, TELEM-03's incident cause is processor latency; its trace sampling gap is an observation condition. Detecting the gap alone earns no primary Recall credit.

The label contract rejects missing cases, duplicate alternatives, substituted conditions and duplicate JSON keys. Primary Recall always retains 24 cases; observation-condition Recall retains four. Missing predictions are misses. The catalog and these labels describe the intended release suite; they do not establish that every scenario has been reproduced or scored. Eleven local scenarios are qualified and four have a frozen diagnosis score at this revision.

Scoring loads retained labels after predictions are frozen. Scenario IDs, accepted answers, traffic receipts and qualification flags must not enter model context. Freeze the label-file digest with each evaluation; changed labels require a new declared evaluation version. No 24-case or observation-condition model score has been measured yet.
