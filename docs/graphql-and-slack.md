# GraphQL and Slack

Both integrations attach to the authenticated FastAPI application created by `create_protected_app`. The default demo server and public Hugging Face replay do not expose them.

## GraphQL

`POST /api/graphql` uses the same bearer identity and incident scope checks as REST. It reads incidents, report state, ranked causes and paginated evidence. There are no mutations, raw artifact paths or remediation resolvers.

```graphql
query Incident($id: ID!) {
  incident(id: $id) {
    id
    title
    service
    namespace
    report {
      terminalState
      mode
      durationSeconds
      causes { code confidence supportingEvidenceIds }
    }
    evidence(offset: 0, limit: 20) { id source summary }
  }
}
```

Send `{"query":"...","variables":{"id":"<incident-id>"}}` with `Authorization: Bearer <token>`. The endpoint accepts one query operation, at most 8,192 query characters and 500 parser tokens, depth five and 50 selected fields. Evidence pages contain 1?50 entries. Fragments, introspection, mutations and HTTP batching are intentionally unsupported. Errors return stable public messages; authentication failure remains HTTP 401. Resolver scope failures produce a generic GraphQL error without incident contents.

The implementation uses [GraphQL-core's reference execution engine](https://graphql-core-3.readthedocs.io/en/stable/modules/graphql.html), with bounded AST validation before execution. See [source](../src/payops/graphql_api.py).

## Slack

Create an incoming webhook for the desired channel using [Slack's official setup](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks). Keep its URL in a private operator-owned secret file outside the repository. Only the host chooses the channel and operator origin.

Add this to your existing authenticated app factory wiring:

```python
from pathlib import Path
from pydantic import SecretStr
from payops.slack_notifications import SlackNotifier
from payops.protected_api import create_protected_app

notifier = SlackNotifier(
    SecretStr(Path("/private/payops/slack-webhook").read_text().strip()),
    "https://your-operator-api.example",
)
app = create_protected_app(
    store, identity, investigate,
    mode="local_kind",
    remediation=broker,
    slack=notifier,
)
```

`store`, `identity`, `investigate` and `broker` are the existing trusted host dependencies. Omitting `slack` disables notification routes. The webhook is restricted to `hooks.slack.com/services/...`; redirects and environment proxies are disabled.

An authenticated responder calls `POST /api/incidents/{id}/notifications/slack`. A successful delivery returns HTTP 202. Viewers cannot send notifications. Messages contain only the incident reference and an authenticated API link?not alert prose, evidence, payment data or approval authority. Approvals and execution still require the existing backend routes and deterministic broker checks.

A timeout, rate limit, oversized response or non-`ok` response returns `SLACK_DELIVERY_UNCONFIRMED`. Delivery is not automatically retried because a timed-out request may already have posted. No inbound Slack commands or interactive approval callbacks are enabled.

## Verification

95 API/remediation tests passed, with 100% statement-and-branch coverage for GraphQL, Slack and the protected API. Tests use real HTTPX request/response handling with a fixture transport; no live workspace delivery is claimed. They cover nested evidence, missing/foreign/revoked identities, query limits, mutation rejection, viewer denial, fixed destinations, no retry, error redaction and preservation of remediation authorization.
