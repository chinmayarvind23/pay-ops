# Backend authentication

`payops.auth` binds a Google Identity Platform project and optional tenant to a
Firebase Admin SDK client. The host must supply an app with the exact project ID
and `httpTimeout=5`. Normal Application Default Credentials or an explicitly
configured service identity belong to the host, never request or model arguments.

The SDK verifies the Google signature, issuer, audience and token lifetime. The
adapter always requests revocation checking and zero clock skew. A second current
account lookup supplies PayOps roles, tenant, enabled state and the latest token
revocation timestamp. Token-carried role claims do not grant permissions: custom
claim changes only reach existing tokens after refresh, so authorization reads
the current account instead. [Firebase Admin token verification](https://firebase.google.com/docs/reference/admin/python/firebase_admin.auth),
[session revocation](https://firebase.google.com/docs/auth/admin/manage-sessions).

The current account's `payops` custom claim has this shape:

```json
{"roles": ["responder"], "namespaces": ["payops-sandbox"]}
```

Roles are `viewer`, `responder`, `approver` or `executor`. Missing, unknown or
malformed grants deny access. Subject and namespace identifiers follow the
repository's bounded identifier contract. The adapter projects no email,
provider profile or password data. A subject passed to the later `principal`
lookup must come from authenticated backend state; that lookup is not a login
method and must not be exposed as a model tool.

An authenticated principal expires at the earlier of token expiry or 60 seconds
from the start of the account lookup. Time spent waiting for the provider consumes
that allowance. Execution-time account lookups use the same cutoff. The policy
and remediation broker independently enforce their own scope and freshness
checks. Account/provider failures deny access with a stable public error rather
than returning provider messages or tokens.

`FIREBASE_AUTH_EMULATOR_HOST` is rejected at construction and before every
verification/account read. The SDK consults that variable dynamically and skips
signature verification in emulator mode. Operational authentication cannot
silently inherit that development setting.

The five-second SDK setting bounds individual HTTP attempts, not the complete
authentication operation. Firebase Admin 7.5.0 retains bounded SDK retries and
certificate caching. Do not label a multi-request authentication sequence as a
five-second hard deadline. Deployment-level concurrency and request deadlines
remain necessary. [SDK app options](https://firebase.google.com/docs/reference/admin/python/firebase_admin),
[pinned HTTP retry implementation](https://github.com/firebase/firebase-admin-python/blob/v7.5.0/firebase_admin/_http_client.py).

## Verification scope

The integration tests run the actual SDK with transient RSA keys and intercepted
Google HTTPS responses. They validate signatures, issuer/audience, expiration,
revocation, disabled accounts and current-grant reads without loading user
credentials or contacting a cloud identity project. Unit tests cover tenant
binding, stale token roles, malformed input, account mismatch, emulator settings
and expiration during a provider lookup.

This is backend authentication code, not a claim of a configured cloud tenant,
SAML provider or deployed login flow. The existing mock API has not yet been
wired to this adapter. Operational API and frontend integration remain separate
release gates.
