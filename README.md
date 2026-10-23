# LLM API Router

Ultra-lightweight, zero-dependency multi-account router for OpenAI-, OpenRouter-, Anthropic- and Gemini-compatible APIs. It spreads traffic across many API keys, remembers rate-limited keys for 12 hours, fails over transparently mid-request, and ships a dark-mode control plane that renders entirely in your browser.

| | |
|---|---|
| Runtime | Node.js >= 20 (uses native `fetch`, `node:http`, `node:stream`) |
| Dependencies | **none** â€” `dependencies: {}` |
| Streaming | `Readable.fromWeb(upstream.body).pipe(clientRes)` â€” SSE is never buffered |
| Memory discipline | request/response bodies are never stored; only numeric counters persist |
| State | `config.json` (configuration + keys) Â· `state.json` (counters + cooldown deadlines) |
| Dashboard | `/admin` â€” Tailwind + Alpine.js + Lucide, 100 % client-side |
| Failover | HTTP `429`, `402`, `403` â†’ account marked `limited` for `cooldownHours` (default 12 h) â†’ retry next account in the same provider |
| Container | `docker-compose.yml` caps the service at **128 MB** |

```
client â”€â”€ POST /v1/chat/completions â”€â”€â–¶ router
                                        â”‚ 1. provider: x-router-provider header â–¸ "provider/model" prefix â–¸ routes â–¸ default â–¸ first healthy
                                        â”‚ 2. account : round-robin rotation | sequential first-healthy, skipping cooling accounts
                                        â–¼
                                 fetch() to upstream account
                                        â”‚
                     â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”´â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
             openai-compatible                     anthropic / gemini
     Readable.fromWeb(body).pipe(res)         SSE translated on the fly into
     (zero-copy, nothing buffered)            OpenAI chat.completion.chunk events
```

## Quick start

```bash
git clone https://github.com/mnochernadi/router.git
cd router
ADMIN_KEY=pick-something-strong node server.js
# LLM Router v1.0.0 on node v20+
#   listening: http://0.0.0.0:8787  (dashboard: /admin, API: /v1)
```

Open <http://localhost:8787/admin>, unlock with `ADMIN_KEY`, add a provider account, and point your client at `http://localhost:8787/v1`.

There is no install step and no build step. `npm start` is `node server.js`.

```bash
# streaming, provider chosen explicitly
curl -N http://localhost:8787/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-router-provider: openai-main' \
  -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"hello"}]}'

# provider chosen by the model prefix (prefix is stripped before it reaches upstream)
curl http://localhost:8787/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"anthropic-main/claude-sonnet-4-5","messages":[{"role":"user","content":"hello"}]}'

# any OpenAI-compatible SDK works unchanged
#   base_url = "http://localhost:8787/v1",  api_key = anything (unless proxyKeys is set)
```

## Dashboard (`/admin`)

* **Connect provider** opens a catalogue of 16 presets grouped Free / Free with account / API key / Local. Clicking one opens that provider's own setup page with the credential field it actually needs â€” or none at all â€” plus **Check models**, which probes the upstream before you commit. Clicking a provider card opens its detail page (accounts, models, strategy, proxy, delete).
* **Time-boxed token swapping.** Set *Swap account every (minutes)* on a provider â€” `120` for OpenCode, whose free quota refreshes on a clock â€” and the router keeps one account for that long, then moves to the next, cycling back once every window has closed. The account row counts the live window down. This is the clock-driven path; the 12-hour cooldown stays the reaction to a real `429`.
* **Keyless and deferred credentials.** OpenCode Free, Ollama, LM Studio, vLLM and any custom endpoint save with no credential at all; freebuff saves with an empty token and asks for it later. An empty-key account shows a `no key` badge until a credential is attached, and `POST /admin/api/connect` creates the provider plus its first account in one call.
* **Three tabs** â€” Providers (cards, accounts, strategies, per-provider outbound proxy), Network (exit pool, subscriptions, testing, assignment), Activity (routing events).
* **Top status bar** â€” live RSS (`process.memoryUsage().rss`) with an inline sparkline, uptime, routed-request counter, active vs. rate-limited account counts, failover counter, and a copy button for the router API base URL.
* **Provider cards** â€” type badge, base URL, 1-click **Round-Robin â‡„ Sequential** toggle, per-provider totals, plus *Add Account*, *Edit Provider* and *Delete Provider*.
* **Account rows** â€” name, masked API key, request counter, limit-hit counter, last HTTP status, health badge (green *Active* / red *Limited*), a live `HH:MM:SS` countdown to the automatic reset with a draining progress bar, **Reset now** (clears the cooldown immediately) and **Delete**.
* **Modals** â€” provider add/edit (type, base URL, strategy, cooldown, timeout, max attempts, extra headers), account add/edit, and **bulk paste** (one `name:key` or bare `key` per line).
* **Settings** â€” global cooldown hours, default provider, cooldown status codes, inbound router keys, CORS toggle, rotate the admin key.
* **Backup** â€” *Export backup* downloads `config.json` (with keys) as a JSON envelope; *Import backup* accepts that envelope, a raw config, or an **OmniRoute-style** backup (see below).
* **Auth** â€” the key is kept in `localStorage` and sent as `x-admin-key`; a 401 anywhere reopens the login modal.
* **Activity feed** â€” the last 60 routing events, counters only, never bodies.

All rendering happens in the browser; the server only ever hands out one static file.

## Routing rules

Resolution order for the provider:

1. `x-router-provider: <provider-id>` request header (falls back to a matching `type`).
2. `model` shaped as `providerId/modelName` â€” the prefix is stripped before the upstream call.
3. `defaultProvider` from `config.json`.
4. First provider that currently has a healthy, enabled account.

Account selection per provider `strategy`:

| Strategy | Behaviour |
|---|---|
| `round-robin` | Rotates over healthy accounts in order; the rotation index (`rrIndex`) survives restarts in `state.json`. |
| `sequential` | Always the first healthy account in array order; it only moves on when that account is cooling down or disabled. |

An account is **healthy** when `enabled !== false` and `limitedUntil <= now`.

## Failover and the 12-hour cooldown

1. The router calls the selected account.
2. On `429`, `402` or `403` (configurable via `cooldownStatuses`) the account is marked `limited` until `now + cooldownHours` â€” the deadline is written to `state.json` immediately.
3. The request is retried with the **next healthy account in the same provider**, transparently â€” the client sees a single successful response and the headers `x-router-account` (which key answered) and `x-router-attempts` (how many were tried).
4. When every account is cooling, the client gets `429` with `retryAfterSeconds`, `nextUnlockAt`, `tried` and `healthyRemaining` so it can back off precisely.
5. A sweeper runs every 30 s and unlocks expired accounts automatically â€” no restart, no cron.

`5xx` responses are retried across accounts too (`retryStatuses`) but never trigger a cooldown; if the last attempt is still a `5xx` it is relayed to the client as-is rather than dressed up as a success.

**Why an account is cooling, and for how long.** A `429` carrying `retry-after: 60` cools for 60 seconds, not 12 hours. A `402`/`403` whose body is not quota-related (`free_mode_cli_required`, `model not found`, `account suspended`) gets `errorCooldownMinutes` (default 5) and keeps its real message. The exhausted-provider response reports all of it:

```json
{ "error": {
  "message": "All accounts for provider \"opencode-main\" are rate-limited and cooling down. Tip: set Outbound proxy to \"Rotate the pool\" â€¦",
  "provider": "opencode-main",
  "accounts": [{ "id": "acc-1", "name": "token-1", "status": 429, "kind": "quota",
                 "message": "rate limit exceeded, try again in 2 hours", "unlocksInSeconds": 7150 }],
  "reasons": ["token-1: HTTP 429 rate limit exceeded, try again in 2 hours"],
  "retryAfterSeconds": 7150,
  "resetHint": "Clear it now: POST /admin/api/reset with x-admin-key, or the \"Unlock all\" button in the dashboard."
} }
```

The same reason is shown on the account row and in the Activity tab, so you can tell an IP limit from a bad model handle without reading the provider's docs.

## Provider types

| `type` | Upstream base URL | Auth | Wire style |
|---|---|---|---|
| `openai` | `https://api.openai.com/v1` | `Authorization: Bearer` | passthrough |
| `openrouter` | `https://openrouter.ai/api/v1` | `Authorization: Bearer` + `HTTP-Referer`/`X-Title` | passthrough |
| `anthropic` | `https://api.anthropic.com/v1` | `x-api-key` + `anthropic-version` | OpenAI â‡„ Anthropic messages, incl. SSE |
| `gemini` | `https://generativelanguage.googleapis.com/v1beta` | `x-goog-api-key` | OpenAI â‡„ `generateContent`/`streamGenerateContent` |
| `opencode` | `https://opencode.ai/inference` | `Authorization: Bearer` (+ `x-api-key`/`x-goog-api-key`) | **per model**: chat â‡„ messages / google / responses |
| `opencode-free` | `https://opencode.ai/zen/v1` | none (keyless) | passthrough to the free model list. **Not usable from a server**: the free tier answers `403 FreeTierError` ("can only be used from within OpenCode") from any IP, so it is only useful as a model catalogue. Use `opencode` with a Console key instead. |
| `freebuff` | `https://www.codebuff.com` | Bearer token + **Ed25519 request signature** | OpenAI-shaped, session/run bound (see below) |
| `custom` | whatever you set | `Authorization: Bearer` | passthrough (Ollama, vLLM, LM Studio, LiteLLM, a gatewayâ€¦) |

Translation covers system-message extraction, text and image parts, `max_tokens`, `temperature`, `top_p`, `stop`, usage mapping and finish reasons. Anthropic and Gemini streams are re-emitted as OpenAI `chat.completion.chunk` events terminated by `data: [DONE]`.

## OpenCode (what 9Router calls OpenCode Free, done properly)

One OpenCode account fronts **four different wire formats**, and OpenCode only lists the endpoint per model. The router picks it for you from the model id â€” no separate providers, no manual endpoint juggling:

| Model family | Upstream route | Format |
|---|---|---|
| `claude-*`, `qwen3.5-plus`, `qwen3.6-plus`, `qwen3.7-max`, `qwen3.7-plus`, `qwen3.8-flash` | `/inference/anthropic/v1/messages` | Anthropic messages â‡„ OpenAI |
| `gemini-*` | `/inference/google/v1beta/models/<id>:generateContent` | Google â‡„ OpenAI |
| `gpt-*`, `grok-*`, `muse-*` | `/inference/openai/v1/responses` | Responses API â‡„ OpenAI (incl. SSE events) |
| everything else (`glm-*`, `kimi-*`, `deepseek-*`, `minimax-*`, `qwen3.8-max`, `big-pickle`, `*-free`, â€¦) | `/inference/openai/v1/chat/completions` | native passthrough |

```bash
# Console key (paid + free models): type "opencode", base URL https://opencode.ai/inference
# Free list only, no key at all   : type "opencode-free", base URL https://opencode.ai/zen/v1

curl http://localhost:8787/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-router-provider: opencode-free' \
  -d '{"model":"big-pickle","stream":true,"messages":[{"role":"user","content":"hi"}]}'

curl http://localhost:8787/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"opencode-main/claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}'
```

* **Keyless accounts are legal.** An `opencode-free` account (or any account whose key is empty, `none`, `free`, `no-auth`, `not-needed`) sends no `Authorization` header at all.
* **Model list built in.** Open the provider's **Models** button in the dashboard, or `GET /admin/api/provider/<id>/models` â€” it asks the upstream (`https://opencode.ai/inference/v1/models`, 85 ids at the time of writing, no key required) and shows each id with the route it will take.
* **Escape hatch.** If OpenCode moves a model to another format, override it per provider without touching code:
  ```json
  { "id": "opencode-main", "type": "opencode", "modelStyles": { "gpt-5.5": "openai", "claude-*": "anthropic" } }
  ```
  Keys are exact ids or `glob*` patterns; values are `openai`, `anthropic`, `gemini` or `responses`.
* **Pin one endpoint.** A base URL that already points at a format (`â€¦/inference/openai/v1`) disables per-model routing and is used verbatim â€” handy for a gateway in front of OpenCode.

### The free pool is a daily allowance, and it is not yours to rotate

Once the keyless free tier is used up, `opencode.ai` answers `429 FreeUsageLimitError` with an explicit deadline â€” measured: `retry-after: 57142` (~15.9h).

Rebuilding the client does **not** clear it. Measured against the live service, all with a freshly minted session:

| What was rotated | Result |
|---|---|
| egress IP: direct + three exits (`92.42.100.132`, `57.129.171.129`, `193.29.139.131`) | `429` on all four |
| client version: `2.0.26`, `2.0.31`, `2.1.0` | `429` |
| model: `deepseek-v4.1-flash-free`, `big-pickle`, `glm-5.3-flash-free` | `429` |
| project handle, client name, session id | `429` |
| tool quartet removed (`bash`/`glob`/`grep`/`read`) | `403 FreeTierError` â€” the attestation gate, not a quota |

### The allowance is per model, so a dry model is not a dry provider

The same measurement also shows the allowance is accounted **per model**, not per provider. On one exhausted identity, in the same minute:

| Model | Result |
|---|---|
| `deepseek-v4.1-flash-free`, `glm-5.3-flash-free`, `big-pickle`, `mimo-v2.6-flash-free`, `nemotron-3.5-super-vl-free` | `429 FreeUsageLimitError` |
| `nemotron-3.5-lightning-free`, `beam-free` | `200`, full SSE |
| `flint-40b-free` | `503` (their endpoint, not a quota) |
| `jev-1.13-free` | `400 ModelProtocolUnsupported` on both chat and responses |

So on a quota answer the router moves to the next free model that still has allowance, keeps the identity, and does **not** cool the account down. The client sees a normal `200`; `x-router-model` (and the response's `model`) names the model that actually answered, and the dashboard's Activity tab logs the hop:

```
[proxy] opencode-free/identity-27: deepseek-v4.1-flash-free is out of allowance; answering with nemotron-3.5-lightning-free instead (1/5)
```

Order and behaviour are configurable per provider (`freeFallback: false` disables it, `freeFallbackModels: [...]` overrides the order) or globally (`freeFallbackModels` in `config.json`). Default: the validated list above, in that order. Combined with the daily reset, the provider keeps answering as long as any free model has allowance left.

Three consequences in this router:

* On a free-tier quota error the upstream window is honoured even when it is longer than `cooldownHours` (capped at 24h), so an exhausted pool is not re-probed every 12 hours while a new identity is minted each time â€” upstream reads that churn as abuse.
* `renewManagedIdentity` still rebuilds a whole identity (new account, new session, new project handle) for the limits that *are* per-session; a rebuilt identity presents different values in every header the router controls.
* The only real escape from the free ceiling is a Console key: provider type `opencode`, base URL `https://opencode.ai/inference`. That unlocks the paid models too â€” `minimax-m3`, `qwen3.8-max` and `kimi-k2.6` answer `401 AuthError "Missing API key."` under `Bearer public`.

### Reinstalling the client (automatic, and on demand)

The router reinstalls the client by itself. Whenever a managed provider answers a cooldown status, `reinstallManagedClient` drops that identity and mints one that differs in **every** field the router controls:

| Field | Before -> after |
|---|---|
| account | `identity-N` -> `identity-N+1` |
| session (`x-opencode-session` + 3 aliases) | freshly minted `ses_<12 hex><14 base62>` |
| project (`x-opencode-project`) | `p-<16 hex>`, freshly minted |
| client version (`user-agent`) | next entry of the validated list (`2.0.26` -> `2.0.31` -> `2.1.0`) |
| cooldown | cleared |
| exit IP | rotated when the provider opted into the pool (`proxy: rotate`) |

It runs at most once per request and never more often than `reinstallGapMinutes` (default `10`), so upstream does not read the churn as abuse.

On demand â€” the dashboard's **Reinstall client** button, or the bundled CLI:

```bash
# every managed provider, with a probe that reports the upstream verdict
node scripts/reinstall-client.js --url http://127.0.0.1:8787 --key "$ADMIN_KEY"

# one provider, no request spent, machine-readable
node scripts/reinstall-client.js --provider opencode-free --no-probe --json

# cron-friendly: exits 1 while upstream still refuses the rebuilt client
ADMIN_KEY=... ROUTER_URL=http://127.0.0.1:8787 npm run reinstall-client
```

The probe is the honest part: when upstream answers `429` with a `retry-after` measured in hours, the script prints that deadline and says the ceiling did not move, instead of reporting a fake success. Same action over HTTP: `POST /admin/api/proxies/provider/<id>/reinstall` (`/renew` still works as the historical path).

## API

### Proxy (point your clients here)

Auth accepts anything real clients send: `Authorization: Bearer <router key>`, `x-api-key` (the Anthropic SDK / Claude Code), `x-goog-api-key`, or `?key=`. **The inbound surface is OpenAI- and Anthropic-shaped only** â€” there is no `/v1beta/models/<id>:generateContent` Gemini-shaped inbound; the `gemini` style is an *upstream* adapter.

| Method | Path | Notes |
|---|---|---|
| `POST` | `/v1/chat/completions` | Unified entry point; translated when the target speaks Anthropic/Gemini. |
| `POST` | `/v1/messages` | Anthropic-shaped entry point. |
| `ANY` | `/v1/*` | Passthrough to the chosen OpenAI-compatible provider (append to its base URL). |
| `GET` | `/v1/models` | Lists configured providers as model owners. |
| `GET` | `/health` | `{ ok, version, node, uptimeSeconds, rss, requests, providers, accounts }`. |

Response headers on a routed call: `x-router-provider`, `x-router-account`, `x-router-attempts`.

### Admin (requires `x-admin-key`)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/admin/api/ping` | Unauthenticated liveness probe. |
| `GET` | `/admin/api/state` | Everything the dashboard renders (keys masked). |
| `POST` | `/admin/api/providers` | Create/update a provider (`force: true` overwrites). |
| `PATCH`/`DELETE` | `/admin/api/provider/:id` | Edit / delete a provider. |
| `POST` | `/admin/api/provider/:id/strategy` | `{ strategy: "round-robin" \| "sequential" }`. |
| `POST` | `/admin/api/provider/:id/accounts` | `{ accounts: [...] }` â€” strings, `name:key` lines, or objects. |
| `PATCH`/`DELETE` | `/admin/api/provider/:id/account/:aid` | Edit / delete one account. |
| `POST` | `/admin/api/provider/:id/account/:aid/reset` | Clear the cooldown now. |
| `POST` | `/admin/api/reset` | Unlock every cooling account (`/reset/counters` zeroes counters). |
| `POST` | `/admin/api/settings` | Cooldown hours, default provider, inbound keys, CORS, admin key. |
| `GET` | `/admin/api/provider/:id/models` | Live model list from the upstream (no key needed for OpenCode), with the route each id will take. |
| `POST` | `/admin/api/proxies/{refresh,test,apply,rotate,purge,xray,sources,settings}` | Exit pool: download+parse subscriptions, batch-test, apply to xray, rotate, tune. |
| `GET` | `/admin/api/proxies` Â· `/admin/api/proxies/exit/:id` | Pool snapshot / one exit (full URI). |
| `GET` | `/admin/api/freebuff` | freebuff session state per account (device key, catalog, instance, run). |
| `GET`/`POST` | `/admin/api/export` Â· `/admin/api/import` | Backup out / restore in. |

## Configuration (`config.json`)

`config.json` is created from `config.example.json` on first boot and is git-ignored because it holds real API keys.

| Key | Default | Meaning |
|---|---|---|
| `adminKey` | `changeme` | Dashboard key (`ADMIN_KEY` env wins). Change it. |
| `host` / `port` | `0.0.0.0` / `8787` | Listen address (`HOST`/`PORT` win). |
| `cooldownHours` | `12` | Default cooldown per limited account. |
| `cooldownStatuses` | `[429,402,403]` | Statuses that trigger a cooldown. |
| `retryStatuses` | `[500,502,503,504]` | Statuses that fail over without cooldown. |
| `defaultProvider` | `""` | Fallback provider id. |
| `cors` | `true` | Send permissive CORS headers to proxy clients. |
| `logging.requests` | `false` | Routing lines in the process log. Off by default â€” counters live in the dashboard. |
| `rotateEveryMinutes` | `0` | Time-boxed rotation: keep one account for N minutes, then move to the next. Use it when a provider's free quota resets on a clock â€” OpenCode: `120` for two-hour tokens. Per-account override allowed. `0` = off. |
| `respectRetryAfter` | `true` | When an upstream answers `429` with `retry-after`, cool the account for that long instead of the full `cooldownHours`. |
| `errorCooldownMinutes` | `5` | Cooldown for a `402`/`403` whose body is **not** quota-related (wrong model handle, mode required, suspended account) â€” so a protocol error no longer locks an account for 12 hours. |
| `proxyKeys` | `[]` | Inbound `Bearer` keys the router requires. Empty = open. |
| `providers[]` | â€” | `id`, `name`, `type`, `baseUrl`, `strategy`, `cooldownHours`, `timeoutMs`, `maxAttempts`, `headers`, `modelStyles`, `accounts[]`. |

Account fields: `id`, `name`, `apiKey`, `enabled`. `apiKey` may be empty for keyless providers (OpenCode Free, local servers) â€” the router then sends no auth header.

### Runtime state (`state.json`)

Counters only â€” no prompts, no responses, no keys:

```json
{
  "version": 1,
  "totals": { "requests": 128, "success": 126, "failed": 2, "streamed": 91, "failovers": 3, "limitEvents": 2, "unlocks": 1 },
  "providers": { "openai-main": { "rrIndex": 1, "accounts": { "acc-1": { "requests": 64, "limits": 1, "limitedUntil": 1792900000000, "lastStatus": 429 } } } },
  "events": [{ "t": 1792800000000, "kind": "limited", "pid": "openai-main", "aid": "acc-1", "st": 429 }]
}
```

The smoke test asserts that `state.json` never contains an API key or a prompt body.

## Backups and OmniRoute import

*Export* writes `{ kind: "llm-router-backup", version, exportedAt, config }`.

*Import* understands three shapes, all merged into the current config (deduplicating identical keys, never deleting anything):

1. this router's own export envelope,
2. a raw `config.json`,
3. an **OmniRoute-style** backup â€” providers found under `providers`/`accounts`/`keys`, credential fields `apiKey|key|token|secret|value`, names from `name|label|email`, and type hints from `type|provider|kind` (`openrouter`, `claude`/`anthropic`, `gemini`/`google`, `openai`; anything else becomes `custom`, keeping `baseUrl`). Unrecognised shapes are reported as warnings instead of being guessed silently.

## Docker

```bash
docker compose up -d          # http://localhost:8787/admin
```

* `mem_limit: 128m` (plus a `deploy.resources.limits.memory: 128M` for Swarm), `NODE_OPTIONS=--max-old-space-size=96`.
* Xray-core is baked into the image, so the exit pool works with no host dependency.
* Config, state and the exit pool live in the named volume `router-data` â†’ `/data` (`ROUTER_CONFIG`, `ROUTER_STATE`).
* Runs as the non-root `node` user with `no-new-privileges`, and has a `HEALTHCHECK` on `/health`.

| Env var | Purpose |
|---|---|
| `ADMIN_KEY` | Overrides `adminKey` from config. |
| `PORT` / `HOST` | Listen address. |
| `ROUTER_CONFIG` / `ROUTER_STATE` | Absolute paths for the two JSON files. |
| `ROUTER_PUBLIC_URL` | Value used in the OpenRouter `HTTP-Referer` header. |
| `NODE_OPTIONS` | e.g. `--max-old-space-size=96`. |

## Deploy on your own server

### One command

```bash
curl -fsSL https://raw.githubusercontent.com/mnochernadi/router/main/install.sh \
  | sudo bash -s -- --domain router.example.com --email you@example.com
```

[install.sh](install.sh) does the whole job: detects the distro â†’ installs `git`/`curl`/Node.js 20 â†’ creates a system user â†’ clones into `/opt/llm-router` â†’ generates `ADMIN_KEY` into `/etc/llm-router.env` (mode 600) â†’ creates `config.json` â†’ installs and starts a systemd unit (or `docker compose` if Docker exists) â†’ configures nginx + Let's Encrypt â†’ opens the firewall â†’ waits for `/health` â†’ prints the dashboard URL and the admin key.

Useful flags (full list: `--help`):

| Flag | Effect |
|---|---|
| `--domain <host>` + `--email <addr>` | nginx + Let's Encrypt; router binds `127.0.0.1` behind the proxy |
| `--docker` / `--systemd` | force a layout (default: Docker when present, else systemd) |
| `--port <n>` | router port (default 8787) |
| `--admin-key <key>` | use your own dashboard key instead of generating one |
| `--with-xray` | download Xray-core into the install dir (enables the V2Ray exit pool) |
| `--no-nginx`, `--no-firewall` | skip those steps |
| `--dry-run` | print every action, change nothing |
| `--uninstall [--purge]` | remove the service/unit (and, with `--purge`, the files and keys) |
| `-y` | non-interactive |

Re-running the same command updates the code and restarts the service â€” it never rotates your key and never overwrites `config.json`.

### Manual alternative

Two supported layouts. Both end up at the same place: the router on `127.0.0.1:8787`, TLS in front of it, and clients pointed at `https://your-domain/v1`.

### A. Docker (fastest)

```bash
git clone https://github.com/mnochernadi/router.git /opt/llm-router
cd /opt/llm-router
printf 'ADMIN_KEY=%s\n' "$(openssl rand -hex 24)" | sudo tee /etc/llm-router.env   # keep this key
sudo docker compose --env-file /etc/llm-router.env up -d
curl -s localhost:8787/health
```

Config and cooldown state live in the `router-data` volume (`/data/config.json`, `/data/state.json`), so `docker compose up -d --build` after a `git pull` keeps your keys.

### B. Bare metal with systemd

```bash
sudo useradd --system --home /opt/llm-router --shell /usr/sbin/nologin llmrouter
sudo git clone https://github.com/mnochernadi/router.git /opt/llm-router
sudo mkdir -p /var/lib/llm-router
sudo cp /opt/llm-router/config.example.json /var/lib/llm-router/config.json
sudo chown -R llmrouter:llmrouter /var/lib/llm-router /opt/llm-router

printf 'ADMIN_KEY=%s\n' "$(openssl rand -hex 24)" | sudo tee /etc/llm-router.env
sudo chmod 600 /etc/llm-router.env

sudo cp /opt/llm-router/deploy/llm-router.service /etc/systemd/system/llm-router.service
sudo systemctl daemon-reload
sudo systemctl enable --now llm-router
systemctl status llm-router --no-pager
curl -s localhost:8787/health
journalctl -u llm-router -f          # logs (routing lines only, never bodies)
```

The unit binds to `127.0.0.1`, caps memory at 128 MB and restricts writes to `/var/lib/llm-router`.

### TLS in front (required if the internet can reach it)

```bash
sudo cp /opt/llm-router/deploy/nginx.conf.example /etc/nginx/sites-available/llm-router
sudo sed -i 's/router.example.com/your-domain/' /etc/nginx/sites-available/llm-router
sudo ln -s /etc/nginx/sites-available/llm-router /etc/nginx/sites-enabled/
sudo certbot --nginx -d your-domain            # or use your own certs
sudo nginx -t && sudo systemctl reload nginx
```

The one setting you cannot skip is `proxy_buffering off` â€” it is already in the example, and without it SSE responses arrive in bursts or appear to hang.

Caddy equivalent (`flush_interval -1` keeps SSE un-buffered):

```
your-domain {
    reverse_proxy 127.0.0.1:8787 {
        flush_interval -1
    }
}
```

### Verify what is actually running

A checkout that mixes files from different commits is undebuggable, so the running build reports its own fingerprint:

```bash
curl -s https://your-domain/health | jq .build
# { "commit": "689c4c4", "files": { "server": "3f9c2ab1d4", "dashboard": "9a11c0ffee", "proxies": "77b0e1c2d3", "freebuff": "1c2d3e4f50" } }
```

Until 27e2a68 the installer used `git pull --ff-only`, which leaves hand-edited files in place; it now does `git fetch` + `reset --hard origin/main`, warns when it discards local edits, and prints the resulting commit. If `/health` is not reachable (404), stop â€” that also means an old build, this endpoint used to be `ok/version/node/rss/providers/accounts`.

### Add accounts and use it

1. Open `https://your-domain/admin` and unlock with `ADMIN_KEY`.
2. *Add provider* (type + base URL) â†’ *Account* â†’ paste one key or bulk-paste `name:key` lines.
3. Point any OpenAI-compatible client at `https://your-domain/v1`:

```bash
curl -N https://your-domain/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

```python
from openai import OpenAI
client = OpenAI(base_url="https://your-domain/v1", api_key="anything")
```

```javascript
import OpenAI from "openai";
const client = new OpenAI({ baseURL: "https://your-domain/v1", apiKey: "anything" });
```

Any key works unless you set `proxyKeys` in the dashboard's Settings (then send that key as `Authorization: Bearer â€¦` â€” recommended on a public host).

### Operating it

| Task | Command |
|---|---|
| Health check | `curl -s https://your-domain/health` |
| Live logs | `journalctl -u llm-router -f` / `docker compose logs -f` |
| Update | `git pull && sudo systemctl restart llm-router` / `docker compose up -d --build` |
| Backup keys + state | Dashboard *Export backup* (contains secrets â€” store it encrypted) |
| Restore | Dashboard *Import backup*, or drop `config.json` into `/var/lib/llm-router` and restart |

Firewall: expose only `80`/`443`. `8787` must stay on loopback â€” the router has no TLS of its own, and `proxyKeys` is the only thing standing between a public port and your credits.

## IP rotation: V2Ray/Xray exit pool

Rate limits that follow your **IP** (OpenCode's per-address cap, freebuff's country gate) are not fixed by more accounts â€” they are fixed by leaving through different addresses. The router manages an [Xray-core](https://github.com/XTLS/Xray-core) process and a pool of exits parsed from V2Ray subscriptions:

```
subscription URLs â”€â”€â–¶ parse (vless / vmess / trojan / shadowsocks)
                      â”‚
                      â”œâ”€â–¶ test in batches through a throwaway xray: latency + exit IP + alive/dead, persisted
                      â–¼
                best N exits â”€â”€â–¶ live xray, one local HTTP inbound per exit
                      â”‚
     provider.proxy = rotate â”€â”€â–¶ 429/402/403 arrives
                      â–¼
             rotate to the next exit and retry the SAME account first;
             only if the whole pool fails does the account get cooled down
```

Default sources (editable in the dashboard, stored in `proxies.json`):

* `https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription.txt`
* `https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription1000.txt`

Workflow â€” **Network** tab, or the API:

```bash
K="x-admin-key: $ADMIN_KEY"
curl -X POST localhost:8787/admin/api/proxies/refresh -H "$K"                 # download + parse (~1000 configs)
curl -X POST localhost:8787/admin/api/proxies/test    -H "$K" -d '{}'   # test untested, in batches
curl -X POST localhost:8787/admin/api/proxies/test    -H "$K" -d '{"retest":true,"limit":200}'
curl -X POST localhost:8787/admin/api/proxies/apply   -H "$K"                 # start xray with the best exits
curl -X POST localhost:8787/admin/api/proxies/rotate  -H "$K"                 # rotate now
```

Then assign it per provider â€” **Providers â†’ Edit â†’ Outbound proxy**: `Direct`, `Rotate the pool`, or one pinned exit. OpenCode and freebuff can rotate while OpenAI stays direct. In JSON:

```json
{ "id": "opencode-main", "type": "opencode", "proxy": "rotate" }
```

Notes that save time:

* Every routed response carries `x-router-exit` (exit id or `direct`) and `x-router-rotations`, so `curl -i` shows which address served it.
* Testing walks the list in batches (`testBatchSize`, default 40) through a second xray instance, so 1000 configs never all live at once. The best `maxActive` (default 24) go into the live pool.
* Dead exits are dropped after `markDeadAfter` failures and retried on the next run; `rotateAttempts` (default 3) caps retries per request.
* The `xray` binary: `install.sh --with-xray` installs it, the Docker image already contains it, or install it from your package manager. No binary = no pool, and the router reports that instead of failing requests.
* `testUrl` defaults to `https://api.ipify.org?format=json` because it returns the exit IP; any JSON endpoint with an `ip` field works.

## freebuff (Codebuff) as a provider

freebuff is OpenAI-shaped (`POST /api/v1/chat/completions`) but will not answer a plain API-key call. `freebuff-driver.js` performs the whole handshake, so from the router's point of view it is just another provider:

| Step | What the driver does |
|---|---|
| Device identity | Generates an Ed25519 key, registers it (`POST /api/v1/freebuff/device-keys`), and signs every request as `freebuff-device-v1\n<METHOD>\n<path>\n<ts>\n<sha256(body)>\n<fetchId>` â†’ `x-freebuff-device-{key,ts,sig}` |
| Catalog | `GET /api/v1/freebuff/models` with `x-freebuff-client: cli` â†’ the `fbm1.` **handle** plus `fetchId` (cached 10 min) |
| Session | `POST /api/v1/freebuff/session/admission` with the handle, a fresh instance id, `x-freebuff-wallet-spend-limit: session` |
| Run ledger | `POST /api/v1/agent-runs` `START` â†’ `runId`, echoed as `codebuff_metadata.run_id`; `FINISH` when the response (or stream) ends |
| Request | `codebuff_metadata{cost_mode:"free",surface:"cli",freebuff_instance_id,â€¦}`, `provider.data_collection:"deny"`, the Buffy preamble injected as the first system message, `connection: close`, `x-freebuff-sse-gzip-recovery: 1` |
| Models | `model` may be the catalog key (`m-00032eaeec`), a name, or the raw `fbm1.` handle; the driver resolves it |

Setup: paste the account token (a 36-char UUID from the desktop app vault, or the login flow at `freebuff.com/api/auth/cli/code`) as the account `apiKey`, pick a model with the **Models** button, and set **Outbound proxy: Rotate the pool**:

> The service admits a session and then returns `country_not_allowed` and resets completions for blocked countries (Iran included). The session still reports `active`, so the only reliable fix is a non-blocked egress IP â€” exactly what the exit pool provides. Until then the driver says so plainly: *admitted but country_not_allowed (IR) â€” completions will be reset; assign a proxy exit*.

Per-account freebuff state (device key id, catalog size, fetchId, admitted instance, run id, `needsReauth`) is visible at `GET /admin/api/freebuff`.

## Memory profile (measured, not estimated)

`npm run smoke` measures the running process. On this machine (Node v26.8.1, Windows 11):

| Measurement | Result |
|---|---|
| Idle RSS after boot | **~50 MB** (JS heap ~16 MB) |
| 24 concurrent SSE streams, 5.0 MB relayed | RSS +9.8 MB |
| 24 concurrent SSE streams, **20.2 MB relayed (4Ã—)** | RSS **+2.8 MB** â€” i.e. growth does not track bytes |
| `heapUsed` / `arrayBuffers` during load | 15.7 MB / 1.0 MB |
| Peak RSS (router) | ~92 MB with the exit pool loaded; xray adds ~36 MB |
| Container budget | **128 MB for direct-only**, **512 MB once the exit pool is running** â€” the compose file now ships 512m and `--max-old-space-size=160` |

Consider the 35 MB target honestly: on Node 20+ the V8 isolate alone accounts for roughly 40 MB of RSS before a single byte of your code runs, so ~50 MB idle is the floor on Windows/Node 26 â€” no amount of application tuning reaches 35 MB there (`--max-old-space-size`, `--max-semi-space-size` and `--jitless` were all measured and none helped; a leaner `node:20-alpine` build, which ships small-icu, is the realistic way to get closer). What the design does guarantee, and what the smoke test verifies, is that **memory is flat with respect to traffic**: upstream bodies are piped, never accumulated, and only numeric counters are retained. The compose file enforces the 128 MB cap regardless.

The deployment budget was raised for this workload: 512 MB RAM and 1 vCPU is comfortable â€” xray adds roughly 20â€“40 MB with a 24-exit pool, and the batch tester runs at most `testBatchSize` inbounds at once.

## Security

* `/v1/models` and `/v1/*` require the router key once `proxyKeys` is set; `/health` reports only `ok/version/uptime/rss/requests/build` and no longer enumerates providers or accounts.
* The admin API is **not** CORS-exposed by default (`adminCors: false`): it is reached with `x-admin-key` from the dashboard served by the same origin, or from curl. Set `adminCors: true` only for a separate dashboard origin you control.
* `config.json` (admin key + provider keys) is written `0600`, like `proxies.json` and `xray-config.json`.

* `adminKey` guards every `/admin/api/*` route (compared with `crypto.timingSafeEqual`); `/admin` itself is a static file with no secrets in it.
* Full API keys are **never** sent to the browser â€” only `sk-1â€¢â€¢â€¢â€¢abcd` masks.
* `proxyKeys` adds inbound authentication to `/v1/*`; without it the proxy is open, so put it behind a reverse proxy + TLS if it is reachable from anywhere.
* Keys are stored in plaintext in `config.json` (that is what makes the router readable and portable). `config.json`, `state.json` and exports are git-ignored â€” protect the file and the volume.
* Imported backups are parsed defensively: no code execution, and unparsable input is rejected with a message rather than half-applied.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `503 router_no_provider` | No provider configured, or none has a healthy account. Open `/admin`. |
| `429 router_no_healthy_account` | Every account in that provider is cooling â€” read `retryAfterSeconds`, or hit *Reset now*. |
| Dashboard asks for a key in a loop | Wrong `ADMIN_KEY`; set it via env or `config.json` and re-login. |
| `502 router_translate_error` | Anthropic/Gemini answered with something that is not JSON â€” check the account and `baseUrl`. |
| `401 invalid router API key` | `proxyKeys` is set; send `Authorization: Bearer <key>`. |
| Anthropic returns `max_tokens` errors | The bridge defaults to `4096` when the request omits `max_tokens`. |
| Streams stall behind Nginx | Keep proxy buffering off; responses already send `X-Accel-Buffering: no` and `Cache-Control: no-transform`. |
| `fetch failed (cause: UND_ERR_SOCKET: other side closed)` on requests with a large prompt, while small ones work | The *client's* egress is mangling big plaintext HTTP uploads â€” not the router. Seen with sing-box/v2rayN TUN mode (adapter MTU 9000): a 36KB POST dies with `ECONNRESET` at ~0.70s, while the same body over TLS, or through the local proxy port (`127.0.0.1:10808`), returns 200. Route the client through its proxy instead of the TUN, e.g. for this harness put `HTTP_PROXY`/`HTTPS_PROXY` (plus `NO_PROXY=localhost,127.0.0.1`) in `$DSH_HOME/.env`, and restart it. Diagnosis recipe: POST the same body to the router and to any plain HTTP host, then the same body over TLS â€” plaintext-only failures point at the client, not the server. |
| A non-streaming request that generates for a long time gets cut by the client or an intermediary | The router now opens the response after 7s and writes a JSON-legal newline every 4s while the upstream is still generating, so a buffered answer never leaves the socket idle. Leading whitespace is valid JSON, so `fetch().json()` is unaffected; fast answers still carry `content-length` and no padding. |

## Verification

```bash
npm run smoke     # 109 checks, no network, no API keys, all in a temp dir (SMOKE_STRICT_MEM=1 for tight memory bounds)
```

It boots a mock upstream plus the real `server.js` and asserts, among others: 429 â†’ failover â†’ success in one client-visible response (`x-router-attempts: 2`); the 12-hour deadline persisted to `state.json`; `sequential` sticking to the first healthy account; `round-robin` covering all accounts evenly; `402`/`403` cooling down while `500` does not; 250-chunk SSE passthrough byte-for-byte; Anthropic and Gemini translation for both streaming and non-streaming; admin auth rejection and key masking; strategy changes written to `config.json`; **a provider edit keeping its accounts and custom headers**; OmniRoute import; **OpenCode routing for all four wire formats, keyless accounts and live model discovery**; **a policy `403` that reproduces on every exit: no rotation storm, no 12h ban, the upstream message reaches the client**; **exit telemetry that classifies a self-egress measurement as a leak and keeps it out of the pool**; **inbound auth via `x-api-key`**; **a provider with no enabled account getting its own error type**; **V2Ray exit rotation retrying an IP-bound 429 through a second CONNECT proxy without cooling the account**; **the freebuff Ed25519 handshake verified against a protocol-enforcing mock**; **a client reinstall presenting a different session, project handle and client version while clearing the cooldown**; and the memory behaviour in the table above.

## License

MIT
