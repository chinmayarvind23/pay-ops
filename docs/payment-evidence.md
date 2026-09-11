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
thresholds. The current default collector and graph still use the earlier
instant queries; window collection and explicit backend-read reservations are
the next integration step.
