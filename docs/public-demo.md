# Public Demo

The Hugging Face Space replays benchmark incidents, ranked causes/evidence, policy decisions, and eval summaries. It does not operate live infrastructure.

Supabase holds sanitized replay tables with RLS and minimum grants.

The public environment has no GKE, Kubernetes, cloud-admin, Slack, or operational database credentials.

Keep screenshots and sample reports in the repository so the demo remains useful if the live Space is unavailable.

## Current replay export

`uv run python -m payops.evaluation.public_replay --run <frozen-run-directory> --output <new-bundle.json>` exports the reviewed four-case development capture. The exporter pins the score and source-manifest digests, checks each prediction digest and exact case receipt, verifies all retained report observations and every referenced injection artifact, then remaps public citations.

The projection contains fixed numeric/Boolean facts, known service/source/cause labels, timestamps and source digests. It omits raw logs, source summaries, original incident/evidence IDs and local paths. This is a selected presentation of private verified evidence; original artifact bytes are not included. Source hashes alone do not establish truth. Twenty tests and independent offline review accepted the exporter and its actual four-case bundle.

The TypeScript/Bun replay view under `apps/web` consumes this static bundle. It has no operational API calls or credentials. Cloud hosting and Supabase integration remain future work. The original Supabase paragraph above is a target architecture, not a deployment claim.
