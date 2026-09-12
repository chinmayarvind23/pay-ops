# Free Hugging Face deployment

PayOps uses a **Static Space**, which requires no paid plan, compute instance,
model provider, database or persistent storage. It publishes the sanitized
four-case replay. The live Kubernetes investigation backend stays local.

Public Space: https://huggingface.co/spaces/chinmayarvind/payops-incident-replay

## Reproduce the release

From the repository root, use the pinned Bun version in `apps/web/package.json`.
Build into a new empty directory outside the repository:

```powershell
$releasePath = 'C:/absolute/new-payops-static-release'
New-Item -ItemType Directory -Path $releasePath -ErrorAction Stop
bun build ./apps/web/index.html --outdir $releasePath --minify
Copy-Item infra/huggingface/SPACE_README.md (Join-Path $releasePath README.md)
hf auth whoami
hf repos create YOUR_ACCOUNT/payops-incident-replay --type space --space-sdk static
hf upload YOUR_ACCOUNT/payops-incident-replay $releasePath . --type space
hf spaces info YOUR_ACCOUNT/payops-incident-replay
```

The upload contains four files: README, HTML, hashed JS and hashed CSS. Inspect an
existing Space before updating it. Upload only this release directory. Use the
`host` returned by Space information; Static Spaces use a `.static.hf.space` host.
Check that SDK is `static`, visibility is intentional, and runtime is `RUNNING`.
Fetch the HTML and both assets, verify their content, then test incident selection
and citation links in a browser. HF injects its own metadata script into HTML;
compare the application content separately from that platform insertion.

The HTML includes a CSP that blocks application network requests and form
submission. Nginx-specific headers, methods and `/health` from the optional local
Docker image do not apply to Static hosting. No secrets or variables are needed.
To roll back, upload the previously retained four-file release directory.

[Static Spaces documentation](https://huggingface.co/docs/hub/spaces-sdks-static)
confirms that this SDK is free without a paid plan. Do not choose Docker, upgraded
hardware, paid storage or inference endpoints for this deployment.
