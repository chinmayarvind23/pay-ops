# Results

## Measured development and component results

The frozen four-case development run matched all four causes at rank 1 and restored
each workload. Seventeen local fault reproductions now have verified activation and
cleanup, including a real kernel OOM termination and scheduler rejection of oversized CPU and memory requests. These are separate results:
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

The ROLLOUT-04 experiment at `5978c4f` reproduced a real risk request-schema mismatch
while all five services remained ready. Four fresh probes returned 200 for original
v1, 502 for a v1 caller against risk v2, 200 for the matching v2 caller, and 200 after
exact original-spec restoration. The mismatch retained the actual upstream risk 422
access record and two error spans. Each successful control retained the complete
nine-span path across five services.

A separate post-run verification reopened 103 retained files and 96 source hashes,
checked all four observations, verified original Deployment identities/specs and
image-layer/config correspondence, and confirmed the recorded scenario latch was
absent. Evidence: `chunk-08-protocol/db7bf3b7ef444bb5949554da578a333c`, run
`e0129da4358b4180b651a2298212cac4`. The root performed this review without additional
agents. This is local fault qualification, not a new diagnosis score, live-model
measurement or human timing result.

The OOM-03 CPU quota experiment at `56e6337` passed all five stages in the local
Kubernetes cluster. Fifteen fresh payments succeeded, each with a complete nine-span
path. Three equal-work requests per treatment produced these means:

| Treatment | CPU limit | Work duration (s) | Kernel throttled time (s) |
| --- | --- | ---: | ---: |
| Control | 500m | 0.289804 | 0.067121 |
| Restricted | 100m | 1.503687 | 1.126025 |
| Recovered | 500m | 0.274641 | 0.077747 |

The experiment restored the captured disabled-work configuration, verified three
final payments and released its latch. Independent verification reopened 335 files
and 103 source/dependency hashes, checked each stage against its owned runtime,
recomputed the comparisons and matched all 101 Python files in the built image and
installed package to the frozen checkout. Three earlier unqualified attempts remain
recorded; they exposed clock-validation defects and each verified cleanup.

Evidence: `chunk-09-cpu/950b1dbfd70c4615b1f9ec6a2a9078b1`, run
`84e657eadd244cbea21ba5f0ab8c3be3`. These are synthetic Kubernetes workload timings,
not agent latency, diagnosis accuracy or human investigation time. No model diagnosis
callback ran for this qualification.

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


The SCHED-02 run at `e69b814` admitted a 16Gi memory request on nodes reporting
16124080Ki allocatable each. The new owned pod remained Pending with an actual
`Insufficient memory` scheduler event and no container process. All three modified
resource specifications and identities were restored, a synthetic payment returned
200, and all five services were healthy. Independent verification reopened 33
artifact hashes and 104 source/dependency hashes. Evidence:
`chunk-10-scheduler-memory/dbde043cbb0c40a5becafebbd61e01d8`, run
`05baf15280ff466395e32655c94a54e9`. This establishes local reproduction and recovery;
no diagnosis or model timing was measured in this run.


OOM-02 qualified at `7d3e204` with the same 256Mi risk memory limit in both
modes. The release control completed all 40 allocations without restarting.
Retained mode produced two distinct OOMKilled container lifetimes in one owned
pod, with verified restart counts 1 and 2. Peak recorded kernel memory before
termination was 261005312 and 261206016 bytes. Original risk specification and
image were restored, all five services were healthy, and payment traffic was
accepted. Independent review checked 121 artifact hashes, 111 source/dependency
hashes, raw allocation progression, image provenance and exact restoration.
Evidence: `chunk-11-leak/2ba6ab40c6eb415a9034155ecfdaa52e`, run
`d2a14f09ce8349be8fd2c86fab89f44a`. The two failed earlier attempts remain
unqualified. This experiment does not measure model diagnosis or latency.


OOM-04 qualified at `926bbd5` using three fresh payments processes with identical
worker image and 256Mi memory limits. Eight serial requests completed before and
after the parallel treatment. The parallel batch produced eight request failures,
seven recorded admissions, six overlapping allocated blocks, and an owned
OOMKilled/137 termination. Peak recorded kernel memory was 122859520 bytes in the
first control, 244191232 in treatment and 88715264 in recovery. Original deployment
state was restored between stages and at exit; unchanged peers retained their
identities and final synthetic payment health passed.

Separate post-run review checked 74 artifact hashes and 118 source/dependency
hashes, rederived the deployment spec, validated raw plans/receipts and memory
records, joined the OOM to the tested process, and verified image provenance and
exact restoration. Evidence: `chunk-12-concurrency/953e7735aa9546b9b586371586a83536`,
run `79a03f72e74943bb95504b7e6e09ddca`. The operator experiment ran from
23:28:09 to 23:29:55 UTC on September 11, 2026. This duration includes rollouts and
controls; it is not agent latency or a diagnosis measurement. The qualified count
is now 17/24; model quality, timing and cost targets remain unmeasured.
