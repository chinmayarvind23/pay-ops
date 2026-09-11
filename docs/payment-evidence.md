# Payment evidence collection

`PaymentRead.snapshot` issues two fixed Prometheus instant queries for one
allowlisted service. It pins the metric query and scrape-watermark query to the
same evaluation instant. The resulting observation keeps the actual scrape time;
an absent watermark remains absent in its payload.

Each snapshot costs two backend reads. Each response is limited to 128 KiB and
128 series. The reader rejects duplicate JSON keys, compressed responses,
redirects, unknown targets, nonfinite values and mismatched query timestamps.
It checks elapsed time after each raw transport chunk and at completion. The
five-second socket timeout and cooperative elapsed cutoff are not a hard deadline
for the complete two-request snapshot. There are no automatic retries.

`derive_payment_window` combines two verified snapshots from the same service
and incident. Only complete windows from a stable process epoch expose counter
deltas. Missing scrapes, restarts and counter resets remain explicit unavailable
states. The payload retains both full evidence references, the requested interval
and the actual scrape boundaries. These bounds can differ; the result does not
claim that every counted request occurred strictly inside the requested interval.

`verify_payment_window` checks both source artifacts and recomputes the arithmetic.
Live snapshot parsing and retained-artifact parsing share the same validation
rules. This module has no scenario labels, expected traffic census or diagnosis
thresholds.

`WindowCollector` selects payments plus processor for payment/processor alerts,
or webhook plus payments for webhook alerts. Unsupported services require a
separately selected collection profile. It persists a plan before dispatch:
20 logical operations reserve 34 fixed backend commands/queries. The original
instant-query profile reserves 30. These counts do not measure wire HTTP
requests inside kubectl.

The window collector captures initial snapshots, records the measurement start,
reads Kubernetes status/events/logs and target availability, and records the
measurement end. After one five-second scrape interval plus a 0.2-second margin,
it makes one final snapshot attempt per service. A stale response produces a
missing window. Individual stale events do not discard later current events.
An interval longer than 180 seconds retains partial evidence and skips final
reads. An exclusive plan file prevents accidental redispatch into the same output.

The local graph CLI enables this profile with `--payment-windows`. Its checkpoint
records logical and backend reservations independently, and a resumed worker must
use the same profile. Older checkpoints without a backend reservation cannot
dispatch new reads. PAYMENT lineage is verified before checkpointing and again
before ranking after resume. The default CLI profile remains `instant_v1`.
