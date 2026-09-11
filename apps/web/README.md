# PayOps incident replay

Read-only TypeScript/Bun view of four recorded local development investigations.
Choose an incident, inspect its ranked causes, then follow citations to selected
verified source facts. The view makes no runtime, provider or operational API calls.

```bash
bun install --frozen-lockfile
bun run check
bun test
bun run build
python -m http.server 18123 --bind 127.0.0.1 --directory dist
```

Open `http://127.0.0.1:18123`. Bun 1.4.2 is pinned in `package.json`; the lockfile
pins build dependencies. Production output is static HTML, CSS and JavaScript.

`src/replay.json` is the independently reviewed output of
`payops.evaluation.public_replay`. It contains 11 projected citations from 120
verified observations. The bundle test checks its retained digest and citation
integrity. Raw logs, private incident IDs, local paths and operational credentials
are absent. Confidence remains uncalibrated; timing covers collection and
deterministic ranking only.

The navigation regression runs the actual renderer against a small DOM-method
fixture. It checks initial fallback, case selection, in-page skip fragments and
later case routes. It does not verify browser layout, keyboard events or the
accessibility tree.

Type checking, bundle tests, production build and HTTP serving have passed locally.
Browser visual/click testing is pending because no browser surface was connected
in the build session. The UI is not yet hosted. Public authentication, operational
commands and the broader GraphQL explorer are separate unfinished interfaces.
