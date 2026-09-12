# Local inference

PayOps connects to an operator-owned Qwen model through a loopback llama.cpp server. The local profile requires no inference API key. Configure the host with the matching model alias and explicit runtime paths.

Pinned model: `Qwen/Qwen2.5-3B-Instruct-GGUF`, revision
`7dabda4d13d513e3e842b20f0d435c732f172cbe`, file
`qwen2.5-3b-instruct-q4_k_m.gguf`, SHA256
`626b4a6678b86442240e33df819e00132d3ba7dddfe1cdc4fbb18e0a9615c62d`.
Download with `hf download` as below, launch the same loopback server with this file
and matching alias, and set the operator model name to that alias. Review the
[model license](https://huggingface.co/Qwen/Qwen2.5-3B-Instruct-GGUF) for your use.

The qualified runtime configuration uses Qwen3-1.7B Q4_K_M from the official Qwen repository,
revision `7fb011e9aee6e4dc7adf8430df9ea8de6a466aa3`, and llama.cpp build `b10809`.
Download the model with the Hugging Face CLI:

```bash
hf download Qwen/Qwen3-1.7B-GGUF Qwen3-1.7B-Q4_K_M.gguf \
  --revision 7fb011e9aee6e4dc7adf8430df9ea8de6a466aa3 --local-dir ./models
```

Use the [official llama.cpp release](https://github.com/ggml-org/llama.cpp/releases/tag/b10809)
for your platform and verify its published asset checksum. Start an operator-owned loopback server:

```bash
llama-server -m ./models/Qwen3-1.7B-Q4_K_M.gguf \
  --alias payops-qwen3-1.7b-q4-k-m --host 127.0.0.1 --port 18089 \
  -c 8192 -np 1 -t 4 -tb 8 --threads-http 2 -ngl 0 --reasoning off --metrics
```

In the operator configuration, omit `provider_key`, set `reasoning.cost_nano_usd` to `0`,
and supply this model profile. Other operator paths, identity grant and data credentials
remain explicit as described in [commands](commands.md).

```json
{
  "provider": "local_llama",
  "model": "payops-qwen3-1.7b-q4-k-m",
  "mode": "provider",
  "token_accounting": "provider_ceiling",
  "input_token_limit": 4096,
  "output_token_limit": 512,
  "timeout_seconds": 30,
  "price": {
    "input_nano_usd": 0,
    "cached_input_nano_usd": 0,
    "output_nano_usd": 0
  }
}
```

The input limit is a reservation ceiling, not a measured count. Under the existing durable
budget claim, the adapter calls `/tokenize`, rechecks current authority, and passes that exact
token array to `/completion`. It retains measured input/output usage and both stage timings.
The pinned non-thinking chat frame rejects embedded special-token role delimiters. The model
still returns the same closed decision schema and cannot gain remediation authority.

The adapter accepts only the fixed loopback origin and model alias, disables redirects and
retries, and rejects truncation, incomplete output, wrong-model responses and inconsistent
counts. A timeout remains an unsuccessful investigation; it does not claim cancellation of
the server's work. Stop only your owned server process when finished.
