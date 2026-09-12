# Deployment

The public [PayOps incident replay](https://huggingface.co/spaces/chinmayarvind/payops-incident-replay)
runs on a **free Hugging Face Static Space**. It contains four sanitized recorded
development investigations and makes no live provider or operational requests.
[Release instructions](../infra/huggingface/README.md) cover building, publishing,
verification and rollback. Static hosting needs no compute hardware or paid plan.

The working investigation environment is local: kind Kubernetes with five synthetic
payment services, PostgreSQL, Redis, Elasticsearch and telemetry services. Scenario
qualification and model benchmarking are separate from public replay hosting.

AWS provisioning is excluded from the current execution scope at the owner's
request. [Optional AWS instructions](../infra/terraform/aws-lightsail/README.md)
let others host the replay and describe the remaining external-processor integration.
No AWS resources were created or billed by this deployment.

GKE, Cloud SQL, Memorystore, Pub/Sub and cloud evidence storage remain unverified
architecture targets. A future deployment must preserve separate evidence-reader,
remediation-executor, scenario-injector and payment-service identities. Vercel,
Supabase and a publicly reachable investigation backend are not prerequisites for
this credential-free static demo.

Verification retained outside the repository: HF revision
`ccdbfdbc1e3473cbbeb1ab2acafecfc1a0b2c23d`, SDK `static`, runtime `RUNNING`, no
hardware request, four exact repository-file comparisons, exact hosted JS/CSS and
HTML matching after the platform metadata insertion. Browser visual verification
and video recording remain pending because no browser surface is connected.
