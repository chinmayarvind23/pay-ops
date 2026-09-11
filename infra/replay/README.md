# Static replay container

The Docker Space serves the reviewed four-case replay with a pinned unprivileged
Nginx image. It has no Python backend, operational credentials or model endpoint.
Build assets into a fresh external directory so old hashed files cannot enter the
upload. From the repository root, with Bun 1.4.2 installed:

```powershell
$bundlePath = 'C:/absolute/payops-space-release'
if (Test-Path -LiteralPath $bundlePath) { throw 'Use a fresh release directory' }
New-Item -ItemType Directory -Path $bundlePath | Out-Null
Copy-Item -LiteralPath 'infra/replay/Dockerfile','infra/replay/nginx.conf','infra/replay/.dockerignore' -Destination $bundlePath
Copy-Item -LiteralPath 'infra/replay/SPACE_README.md' -Destination (Join-Path $bundlePath 'README.md')
bun build ./apps/web/index.html --outdir (Join-Path $bundlePath 'site') --minify
docker build -t payops-replay:release $bundlePath
docker run --rm --read-only --tmpfs /tmp:rw,noexec,nosuid,size=8m --cap-drop ALL --security-opt no-new-privileges --memory 64m --cpus 0.5 --pids-limit 32 -p 127.0.0.1:18124:7860 payops-replay:release
```

The foreground test server responds at `http://127.0.0.1:18124`, with `/health` for
readiness. Only GET and HEAD are accepted. The server exposes index.html and hashed
JavaScript/CSS files; other paths return 404. CSP blocks runtime connections and
allows embedding by Hugging Face. Inline styles support the existing confidence
bars; scripts must come from the same origin.

The image uses UID 1000, matching [Docker Spaces](https://huggingface.co/docs/hub/spaces-sdks-docker).
Its writable temporary paths follow the [unprivileged Nginx image](https://github.com/nginx/docker-nginx-unprivileged/blob/main/README.md).
The local resource and read-only flags above are verification settings, not claims
about limits enforced by Hugging Face. HTTPS is provided by the hosting platform.

After authenticating with the CLI, upload only this prepared directory:

```bash
hf repos create OWNER/SPACE --type space --space-sdk docker
hf upload OWNER/SPACE C:/absolute/payops-space-release . --type space
```

Use a new Space or inspect an existing repository before updating it. The generated
directory has seven files: Dockerfile, nginx.conf, .dockerignore, README.md and three
site assets. Never upload the project runtime or evidence directory. Retain source,
bundle and hosted-file hashes outside the public Space. Local HTTP checks do not
replace browser layout, interaction or hosted deployment verification.
