# Results

## Measured development and component results

The frozen four-case development run matched all four causes at rank 1 and restored
each workload. Ten local fault reproductions now have verified activation and
cleanup, including a real kernel OOM termination. These are separate results:
only the original four cases have been scored for diagnosis.

The committed capability evaluation at `dca7806` denied all 120 manifest attempts
with zero executor callbacks. It repeats five forbidden capability types across
24 named fixture contexts. Separately, 24 approved fixture controls each dispatched
once; repeated execution added no callbacks. Independent review verified the exact
manifest rows, 65 source/dependency hashes, 24 artifacts, 24 action records and 96
audit events. This component result does not establish 120 novel exploits or
24 live incident investigations.

Raw evidence lives outside the repository under `resources/pay_ops/evidence`:
`baseline-committed/9e577303-b653-4e70-b988-e319c341f05e`, `chunk-05-oom`, and
`attack-suite/dca7806-component-001`. Each result retains its scope and provenance.

## Release targets still open

The remaining targets require the following evidence; they are not achieved results.

| Metric                     | Evidence                                  |
| -------------------------- | ----------------------------------------- |
| 24 scenarios               | manifests + runs                          |
| Recall@1 83.3%             | 20/24 rank-1 hits                         |
| Recall@3 91.7%             | 22/24 top-3 hits                          |
| Evidence attribution 96.4% | labeled attribution numerator/denominator |
| 11.8 -> 2.9 min            | paired timing records                     |
| 4.6 s p95                  | raw model-step latency                    |
| $0.07/incident             | token/pricing records                     |
