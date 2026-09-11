# Results

**Unmeasured targets.** No executable benchmark or raw run artifacts existed at initial
inspection on 2026-09-11. This table defines the required evidence, not achieved results.

| Metric                     | Evidence                                  |
| -------------------------- | ----------------------------------------- |
| 24 scenarios               | manifests + runs                          |
| Recall@1 83.3%             | 20/24 rank-1 hits                         |
| Recall@3 91.7%             | 22/24 top-3 hits                          |
| Evidence attribution 96.4% | labeled attribution numerator/denominator |
| 120/120 denied             | policy results + zero executor calls      |
| 11.8 -> 2.9 min            | paired timing records                     |
| 4.6 s p95                  | raw model-step latency                    |
| $0.07/incident             | token/pricing records                     |
