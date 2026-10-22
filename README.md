# LLM API Router

Ultra-lightweight, zero-dependency multi-account router for OpenAI-, OpenRouter-, Anthropic- and Gemini-compatible APIs. It spreads traffic across many API keys, remembers rate-limited keys for 12 hours, fails over transparently mid-request, and ships a dark-mode control plane that renders entirely in your browser.

| | |
|---|---|
| Runtime | Node.js >= 20 (uses native `fetch`, `node:http`, `node:stream`) |
| Dependencies | **none** — `dependencies: {}` |
| Streaming | `Readable.fromWeb(upstream.body).pipe(clientRes)` — SSE is never buffered |
| Memory discipline | request/response bodies are never stored; only numeric counters persist |
| State | `config.json` (configuration + keys) · `state.json` (counters + cooldown deadlines) |
| Dashboard | `/admin` — Tailwind + Alpine.js + Lucide, 100 % client-side |
| Failover | HTTP `429`, `402`, `403` → account marked `limited` for `cooldownHours` (default 12 h) → retry next account in the same provider |
| Container | `docker-compose.yml` caps the service at **128 MB** |

```
client ── POST /v1/chat/completions ──▶ router
                                        │ 1. provider: x-router-provider header ▸ "provider/model" prefix ▸ routes ▸ default ▸ first healthy
                                        │ 2. account : round-robin rotation | sequential first-healthy, skipping cooling accounts
                                        ▼
                                 fetch() to upstream account
                                        │
                     ┌──────────────────┴───────────────────┐
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

* **Top status bar** — live RSS (`process.memoryUsage().rss`) with an inline sparkline, uptime, routed-request counter, active vs. rate-limited account counts, failover counter, and a copy button for the router API base URL.
* **Provider cards** — type badge, base URL, 1-click **Round-Robin ⇄ Sequential** toggle, per-provider totals, plus *Add Account*, *Edit Provider* and *Delete Provider*.
* **Account rows** — name, masked API key, request counter, limit-hit counter, last HTTP status, health badge (green *Active* / red *Limited*), a live `HH:MM:SS` countdown to the automatic reset with a draining progress bar, **Reset now** (clears the cooldown immediately) and **Delete**.
* **Modals** — provider add/edit (type, base URL, strategy, cooldown, timeout, max attempts, extra headers), account add/edit, and **bulk paste** (one `name:key` or bare `key` per line).
* **Settings** — global cooldown hours, default provider, cooldown status codes, inbound router keys, CORS toggle, rotate the admin key.
* **Backup** — *Export backup* downloads `config.json` (with keys) as a JSON envelope; *Import backup* accepts that envelope, a raw config, or an **OmniRoute-style** backup (see below).
* **Auth** — the key is kept in `localStorage` and sent as `x-admin-key`; a 401 anywhere reopens the login modal.
* **Activity feed** — the last 60 routing events, counters only, never bodies.

All rendering happens in the browser; the server only ever hands out one static file.

## Routing rules

Resolution order for the provider:

1. `x-router-provider: <provider-id>` request header (falls back to a matching `type`).
2. `model` shaped as `providerId/modelName` — the prefix is stripped before the upstream call.
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
2. On `429`, `402` or `403` (configurable via `cooldownStatuses`) the account is marked `limited` until `now + cooldownHours` — the deadline is written to `state.json` immediately.
3. The request is retried with the **next healthy account in the same provider**, transparently — the client sees a single successful response and the headers `x-router-account` (which key answered) and `x-router-attempts` (how many were tried).
4. When every account is cooling, the client gets `429` with `retryAfterSeconds`, `nextUnlockAt`, `tried` and `healthyRemaining` so it can back off precisely.
5. A sweeper runs every 30 s and unlocks expired accounts automatically — no restart, no cron.

`5xx` responses are retried across accounts too (`retryStatuses`) but never trigger a cooldown; if the last attempt is still a `5xx` it is relayed to the client as-is rather than dressed up as a success.

## Provider types

| `type` | Upstream base URL | Auth | Wire style |
|---|---|---|---|
| `openai` | `https://api.openai.com/v1` | `Authorization: Bearer` | passthrough |
| `openrouter` | `https://openrouter.ai/api/v1` | `Authorization: Bearer` + `HTTP-Referer`/`X-Title` | passthrough |
| `anthropic` | `https://api.anthropic.com/v1` | `x-api-key` + `anthropic-version` | OpenAI ⇄ Anthropic messages, incl. SSE |
| `gemini` | `https://generativelanguage.googleapis.com/v1beta` | `x-goog-api-key` | OpenAI ⇄ `generateContent`/`streamGenerateContent` |
| `opencode` | `https://opencode.ai/inference` | `Authorization: Bearer` (+ `x-api-key`/`x-goog-api-key`) | **per model**: chat ⇄ messages / google / responses |
| `opencode-free` | `https://opencode.ai/zen/v1` | none (keyless) | passthrough to the free model list |
| `custom` | whatever you set | `Authorization: Bearer` | passthrough (Ollama, vLLM, LM Studio, LiteLLM, a gateway…) |

Translation covers system-message extraction, text and image parts, `max_tokens`, `temperature`, `top_p`, `stop`, usage mapping and finish reasons. Anthropic and Gemini streams are re-emitted as OpenAI `chat.completion.chunk` events terminated by `data: [DONE]`.

## OpenCode (what 9Router calls OpenCode Free, done properly)

One OpenCode account fronts **four different wire formats**, and OpenCode only lists the endpoint per model. The router picks it for you from the model id — no separate providers, no manual endpoint juggling:

| Model family | Upstream route | Format |
|---|---|---|
| `claude-*`, `qwen3.5-plus`, `qwen3.6-plus`, `qwen3.7-max`, `qwen3.7-plus`, `qwen3.8-flash` | `/inference/anthropic/v1/messages` | Anthropic messages ⇄ OpenAI |
| `gemini-*` | `/inference/google/v1beta/models/<id>:generateContent` | Google ⇄ OpenAI |
| `gpt-*`, `grok-*`, `muse-*` | `/inference/openai/v1/responses` | Responses API ⇄ OpenAI (incl. SSE events) |
| everything else (`glm-*`, `kimi-*`, `deepseek-*`, `minimax-*`, `qwen3.8-max`, `big-pickle`, `*-free`, …) | `/inference/openai/v1/chat/completions` | native passthrough |

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
* **Model list built in.** Open the provider's **Models** button in the dashboard, or `GET /admin/api/provider/<id>/models` — it asks the upstream (`https://opencode.ai/inference/v1/models`, 85 ids at the time of writing, no key required) and shows each id with the route it will take.
* **Escape hatch.** If OpenCode moves a model to another format, override it per provider without touching code:
  ```json
  { "id": "opencode-main", "type": "opencode", "modelStyles": { "gpt-5.5": "openai", "claude-*": "anthropic" } }
  ```
  Keys are exact ids or `glob*` patterns; values are `openai`, `anthropic`, `gemini` or `responses`.
* **Pin one endpoint.** A base URL that already points at a format (`…/inference/openai/v1`) disables per-model routing and is used verbatim — handy for a gateway in front of OpenCode.

## API

### Proxy (point your clients here)

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
| `POST` | `/admin/api/provider/:id/accounts` | `{ accounts: [...] }` — strings, `name:key` lines, or objects. |
| `PATCH`/`DELETE` | `/admin/api/provider/:id/account/:aid` | Edit / delete one account. |
| `POST` | `/admin/api/provider/:id/account/:aid/reset` | Clear the cooldown now. |
| `POST` | `/admin/api/reset` | Unlock every cooling account (`/reset/counters` zeroes counters). |
| `POST` | `/admin/api/settings` | Cooldown hours, default provider, inbound keys, CORS, admin key. |
| `GET` | `/admin/api/provider/:id/models` | Live model list from the upstream (no key needed for OpenCode), with the route each id will take. |
| `GET`/`POST` | `/admin/api/export` · `/admin/api/import` | Backup out / restore in. |

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
| `proxyKeys` | `[]` | Inbound `Bearer` keys the router requires. Empty = open. |
| `providers[]` | — | `id`, `name`, `type`, `baseUrl`, `strategy`, `cooldownHours`, `timeoutMs`, `maxAttempts`, `headers`, `modelStyles`, `accounts[]`. |

Account fields: `id`, `name`, `apiKey`, `enabled`. `apiKey` may be empty for keyless providers (OpenCode Free, local servers) — the router then sends no auth header.

### Runtime state (`state.json`)

Counters only — no prompts, no responses, no keys:

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
3. an **OmniRoute-style** backup — providers found under `providers`/`accounts`/`keys`, credential fields `apiKey|key|token|secret|value`, names from `name|label|email`, and type hints from `type|provider|kind` (`openrouter`, `claude`/`anthropic`, `gemini`/`google`, `openai`; anything else becomes `custom`, keeping `baseUrl`). Unrecognised shapes are reported as warnings instead of being guessed silently.

## Docker

```bash
docker compose up -d          # http://localhost:8787/admin
```

* `mem_limit: 128m` (plus a `deploy.resources.limits.memory: 128M` for Swarm), `NODE_OPTIONS=--max-old-space-size=96`.
* Config and state live in the named volume `router-data` → `/data` (`ROUTER_CONFIG`, `ROUTER_STATE`).
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

[install.sh](install.sh) does the whole job: detects the distro → installs `git`/`curl`/Node.js 20 → creates a system user → clones into `/opt/llm-router` → generates `ADMIN_KEY` into `/etc/llm-router.env` (mode 600) → creates `config.json` → installs and starts a systemd unit (or `docker compose` if Docker exists) → configures nginx + Let's Encrypt → opens the firewall → waits for `/health` → prints the dashboard URL and the admin key.

Useful flags (full list: `--help`):

| Flag | Effect |
|---|---|
| `--domain <host>` + `--email <addr>` | nginx + Let's Encrypt; router binds `127.0.0.1` behind the proxy |
| `--docker` / `--systemd` | force a layout (default: Docker when present, else systemd) |
| `--port <n>` | router port (default 8787) |
| `--admin-key <key>` | use your own dashboard key instead of generating one |
| `--no-nginx`, `--no-firewall` | skip those steps |
| `--dry-run` | print every action, change nothing |
| `--uninstall [--purge]` | remove the service/unit (and, with `--purge`, the files and keys) |
| `-y` | non-interactive |

Re-running the same command updates the code and restarts the service — it never rotates your key and never overwrites `config.json`.

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

The one setting you cannot skip is `proxy_buffering off` — it is already in the example, and without it SSE responses arrive in bursts or appear to hang.

Caddy equivalent (`flush_interval -1` keeps SSE un-buffered):

```
your-domain {
    reverse_proxy 127.0.0.1:8787 {
        flush_interval -1
    }
}
```

### Add accounts and use it

1. Open `https://your-domain/admin` and unlock with `ADMIN_KEY`.
2. *Add provider* (type + base URL) → *Account* → paste one key or bulk-paste `name:key` lines.
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

Any key works unless you set `proxyKeys` in the dashboard's Settings (then send that key as `Authorization: Bearer …` — recommended on a public host).

### Operating it

| Task | Command |
|---|---|
| Health check | `curl -s https://your-domain/health` |
| Live logs | `journalctl -u llm-router -f` / `docker compose logs -f` |
| Update | `git pull && sudo systemctl restart llm-router` / `docker compose up -d --build` |
| Backup keys + state | Dashboard *Export backup* (contains secrets — store it encrypted) |
| Restore | Dashboard *Import backup*, or drop `config.json` into `/var/lib/llm-router` and restart |

Firewall: expose only `80`/`443`. `8787` must stay on loopback — the router has no TLS of its own, and `proxyKeys` is the only thing standing between a public port and your credits.

## Memory profile (measured, not estimated)

`npm run smoke` measures the running process. On this machine (Node v26.8.1, Windows 11):

| Measurement | Result |
|---|---|
| Idle RSS after boot | **~50 MB** (JS heap ~16 MB) |
| 24 concurrent SSE streams, 5.0 MB relayed | RSS +9.8 MB |
| 24 concurrent SSE streams, **20.2 MB relayed (4×)** | RSS **+2.8 MB** — i.e. growth does not track bytes |
| `heapUsed` / `arrayBuffers` during load | 15.7 MB / 1.0 MB |
| Peak RSS vs. the 128 MB container ceiling | comfortably inside, with headroom |

Consider the 35 MB target honestly: on Node 20+ the V8 isolate alone accounts for roughly 40 MB of RSS before a single byte of your code runs, so ~50 MB idle is the floor on Windows/Node 26 — no amount of application tuning reaches 35 MB there (`--max-old-space-size`, `--max-semi-space-size` and `--jitless` were all measured and none helped; a leaner `node:20-alpine` build, which ships small-icu, is the realistic way to get closer). What the design does guarantee, and what the smoke test verifies, is that **memory is flat with respect to traffic**: upstream bodies are piped, never accumulated, and only numeric counters are retained. The compose file enforces the 128 MB cap regardless.

## Security

* `adminKey` guards every `/admin/api/*` route (compared with `crypto.timingSafeEqual`); `/admin` itself is a static file with no secrets in it.
* Full API keys are **never** sent to the browser — only `sk-1••••abcd` masks.
* `proxyKeys` adds inbound authentication to `/v1/*`; without it the proxy is open, so put it behind a reverse proxy + TLS if it is reachable from anywhere.
* Keys are stored in plaintext in `config.json` (that is what makes the router readable and portable). `config.json`, `state.json` and exports are git-ignored — protect the file and the volume.
* Imported backups are parsed defensively: no code execution, and unparsable input is rejected with a message rather than half-applied.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `503 router_no_provider` | No provider configured, or none has a healthy account. Open `/admin`. |
| `429 router_no_healthy_account` | Every account in that provider is cooling — read `retryAfterSeconds`, or hit *Reset now*. |
| Dashboard asks for a key in a loop | Wrong `ADMIN_KEY`; set it via env or `config.json` and re-login. |
| `502 router_translate_error` | Anthropic/Gemini answered with something that is not JSON — check the account and `baseUrl`. |
| `401 invalid router API key` | `proxyKeys` is set; send `Authorization: Bearer <key>`. |
| Anthropic returns `max_tokens` errors | The bridge defaults to `4096` when the request omits `max_tokens`. |
| Streams stall behind Nginx | Keep proxy buffering off; responses already send `X-Accel-Buffering: no` and `Cache-Control: no-transform`. |

## Verification

```bash
npm run smoke     # 52 checks, no network, no API keys, all in a temp dir
```

It boots a mock upstream plus the real `server.js` and asserts, among others: 429 → failover → success in one client-visible response (`x-router-attempts: 2`); the 12-hour deadline persisted to `state.json`; `sequential` sticking to the first healthy account; `round-robin` covering all accounts evenly; `402`/`403` cooling down while `500` does not; 250-chunk SSE passthrough byte-for-byte; Anthropic and Gemini translation for both streaming and non-streaming; admin auth rejection and key masking; strategy changes written to `config.json`; **a provider edit keeping its accounts and custom headers**; OmniRoute import; **OpenCode routing for all four wire formats, keyless accounts and live model discovery**; and the memory behaviour in the table above.

## License

MIT
