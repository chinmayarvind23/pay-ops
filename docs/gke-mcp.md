# GKE MCP boundary

`payops.tools.gke_mcp` exposes named-resource, event and container-log reads.
Trusted host configuration binds an incident, project, cluster, namespace, exact
resource names and containers. Requests cannot choose arbitrary resource types,
selectors, shell commands or mutation tools. Before each dispatch, the adapter
compares the three approved upstream input schemas with hashes from pinned source
`a97e99d852c70cf9eb5b85ddf6504a8f0ee8d9d4`.

`payops.tools.mcp_transport` uses MCP SDK2.2.0 over a bounded stdio transport.
The operator supplies an absolute executable, working directory, argv and explicit
environment. Those settings are never accepted as model arguments. The host limits
frame size, cumulative session bytes/messages, catalog pages, tool count and request
duration. Cancellation closes the SDK session and reaps its owned process tree.
No client sampling, elicitation or roots handlers are configured.

Tool responses retain provenance and sanitized artifacts. Upstream errors, partial
logs, malformed results, missing timestamps and size limits remain explicit.
Relative event ages are not converted into invented occurrence timestamps.

The upstream registration probe was run through the actual SDK transport and all
three schema pins matched its12 advertised tools. Real local subprocess tests pass
on Windows and Linux. This establishes protocol/adapter behavior, not authenticated
GKE access. The unmodified upstream server requires ADC; live GKE reads and cloud
diagnosis remain unverified. The current cloud resource projection also needs more
container-termination and event-series detail before it can feed every local rule.

The upstream source pin identifies the reviewed contract. Deployment must separately
verify the executable hash and use narrowly scoped Google IAM and Kubernetes RBAC;
a matching tool schema alone does not prove the identity of a server binary.
