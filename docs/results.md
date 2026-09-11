# Results

## Measured development and component results

The frozen four-case development run matched all four causes at rank 1 and restored
each workload. Twelve local fault reproductions now have verified activation and
cleanup, including a real kernel OOM termination and scheduler rejection of an oversized CPU request. These are separate results:
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
`attack-suite/dca7806-component-001`, and `chunk-06-scheduler/9a4fe93c84ce49eaaf9d4b5d5fafcac5`. Each result retains its scope and provenance.

The committed trace reader at `cb41de9` retained five LOG sources and thirteen verified
TRACE artifacts from an explicit historical five-minute interval. Independent review
found one nine-span path across all five services with eight resolved parent edges;
four additional spans retain unresolved parents. The read used 25 commands under a
separate 40-command reservation and generated no new traffic. This is a bounded
observation sample, not proof of trace completeness or investigation accuracy.
Evidence: `trace-reader/cb41de9-readonly-002`. The earlier empty quiet-window capture
remains retained separately.

The TELEM-03 run at `c9c426b` completed a four-stage sampling experiment with 32
accepted requests. With processor sampling disabled, both bounded captures contained
40 payments spans and zero processor spans. Restoring sampling with the same 600 ms
processor delay returned eight processor spans in each capture. Processor A/B mean
durations were 0.601314/0.601100 seconds while sampling was disabled and
0.601442/0.601372 seconds after sampling returned. Final healthy means were
0.000275/0.000419 seconds after exact original configuration restoration.

Independent review verified 478 retained files, 88 source hashes, 29 operator receipt
hashes, all four observation stages, metric/trace source lineage and the final owned
five-service runtime. The earlier 64 KiB capture failed before injection and remains
recorded; the successful run used a reviewed 128 KiB acquisition cap with unchanged
traffic and acceptance thresholds. Evidence:
`chunk-07-sampling/c1a066c96e9745b4ac759d7cd06dc78e`, run
`b364e918b4944691946c7870f41ff86f`. This qualifies the local sampling scenario;
it does not add a diagnosis score or establish global trace completeness. Its
157.63-second lifecycle duration measures the operator experiment, including controls
and cleanup, rather than agent investigation latency.

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
