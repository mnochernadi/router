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

* **Connect provider** opens a catalogue of 16 presets grouped Free / Free with account / API key / Local. Clicking one opens that provider's own setup page with the credential field it actually needs — or none at all — plus **Check models**, which probes the upstream before you commit. Clicking a provider card opens its detail page (accounts, models, strategy, proxy, delete).
* **Time-boxed token swapping.** Set *Swap account every (minutes)* on a provider — `120` for OpenCode, whose free quota refreshes on a clock — and the router keeps one account for that long, then moves to the next, cycling back once every window has closed. The account row counts the live window down. This is the clock-driven path; the 12-hour cooldown stays the reaction to a real `429`.
* **Keyless and deferred credentials.** OpenCode Free, Ollama, LM Studio, vLLM and any custom endpoint save with no credential at all; freebuff saves with an empty token and asks for it later. An empty-key account shows a `no key` badge until a credential is attached, and `POST /admin/api/connect` creates the provider plus its first account in one call.
* **Three tabs** — Providers (cards, accounts, strategies, per-provider outbound proxy), Network (exit pool, subscriptions, testing, assignment), Activity (routing events).
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

**Why an account is cooling, and for how long.** A `429` carrying `retry-after: 60` cools for 60 seconds, not 12 hours. A `402`/`403` whose body is not quota-related (`free_mode_cli_required`, `model not found`, `account suspended`) gets `errorCooldownMinutes` (default 5) and keeps its real message. The exhausted-provider response reports all of it:

```json
{ "error": {
  "message": "All accounts for provider \"opencode-main\" are rate-limited and cooling down. Tip: set Outbound proxy to \"Rotate the pool\" …",
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
| `anthropic` | `https://api.anthropic.com/v1` | `x-api-key` + `anthropic-version` | OpenAI ⇄ Anthropic messages, incl. SSE |
| `gemini` | `https://generativelanguage.googleapis.com/v1beta` | `x-goog-api-key` | OpenAI ⇄ `generateContent`/`streamGenerateContent` |
| `opencode` | `https://opencode.ai/inference` | `Authorization: Bearer` (+ `x-api-key`/`x-goog-api-key`) | **per model**: chat ⇄ messages / google / responses |
| `opencode-free` | `https://opencode.ai/inference/v1` | none (keyless) | passthrough to the free model list, presented as the attested OpenCode client (`Bearer public`, the `opencode/latest/<v>/cli` UA, an `x-opencode-session` plus its three aliases, and the built-in `bash`/`glob`/`grep`/`read` tool quartet). The old `/zen/v1` base URL only answers `403 FreeTierError` now. Identities are minted by the router, not by you. |
| `freebuff` | `https://www.codebuff.com` | Bearer token + **Ed25519 request signature** | OpenAI-shaped, session/run bound (see below) |
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

### The free pool is a daily allowance, and it is not yours to rotate

Once the keyless free tier is used up, `opencode.ai` answers `429 FreeUsageLimitError` with an explicit deadline — measured: `retry-after: 57142` (~15.9h).

Rebuilding the client does **not** clear it. Measured against the live service, all with a freshly minted session:

| What was rotated | Result |
|---|---|
| egress IP: direct + three exits (`92.42.100.132`, `57.129.171.129`, `193.29.139.131`) | `429` on all four |
| client version: `2.0.26`, `2.0.31`, `2.1.0` | `429` |
| model: `deepseek-v4.1-flash-free`, `big-pickle`, `glm-5.3-flash-free` | `429` |
| project handle, client name, session id | `429` |
| tool quartet removed (`bash`/`glob`/`grep`/`read`) | `403 FreeTierError` — the attestation gate, not a quota |

### The allowance is per model, so a dry model is not a dry provider

The same measurement also shows the allowance is accounted **per model**, not per provider. On one exhausted identity, in the same minute:

| Model | Result |
|---|---|
| `deepseek-v4.1-flash-free`, `glm-5.3-flash-free`, `big-pickle`, `mimo-v2.6-flash-free`, `nemotron-3.5-super-vl-free` | `429 FreeUsageLimitError` |
| `nemotron-3.5-lightning-free`, `beam-free` | `200`, full SSE |
| `flint-40b-free` | `503` (their endpoint, not a quota) |
| `jev-1.13-free` | `400 ModelProtocolUnsupported` on both chat and responses |

So on a quota answer the router moves, and the order is fixed: **IP first, identity second**. A spent exit is burned (left out of the pool) and the next one takes the request, retrying the model the client actually asked for; only when the fresh IP limits as well is the client reinstalled there; then the model pool on that IP; then the next identity lane. The client sees a normal `200` from somewhere in that ladder, `x-router-model` (and the response's `model`) names the model that actually answered, and the Activity tab logs every hop:

```
[proxy] opencode-free#0/identity-1: 429 on exit 6f1a...; retrying deepseek-v4.1-flash-free on exit b60a... (1/1)
[proxy] opencode-free#0/identity-1: 429 on the new exit too; reinstalled this lane and retrying
[proxy] opencode-free#0/identity-1: 429 [quota] lane 0 spent after 1 rotation(s), 1 reinstall(s); rebuilding it in the background, 3 lane(s) ready; handing over
```

### Identity lanes

One identity on one IP is one allowance, and an allowance that runs out used to mean waiting. So the free tier runs **four identities in parallel** (`identityPool`, 1-6). Each lane is a separate identity pinned to its own exit IP and its own client build; a request picks a ready lane, and which lane it was handed shows up as `#<lane>` in the log.

When a lane answers `429` the request does not sit there being rebuilt - it changes the lane's IP, reinstalls the lane's identity, walks the model pool on that lane, and the moment the lane is spent the request is **handed to the next lane**, whose IP and identity are untouched and which has its own allowance. The spent lane is parked and rebuilt **in the background** (burn its exit, pin a fresh one, new session, new project, new build), then returns to rotation. Only when *every* lane is spent does a request wait for the first rebuild to finish, and only for `LANE_WAIT_MS` (12 s) at most.

A limit that survives that whole ladder is the *provider's* problem, not that lane's. So the moment a lane is spent, the router mints a **new identity on every lane at once** (`queueAllLaneRefreshes`) rather than only on the one that reported it: the next request then starts on a complete set of fresh identities instead of spending itself rediscovering that the other three are spent too. The sweep drives the same per-lane rebuild - new session, new project handle, next client build, and a newly pinned exit - through the background queue, so the reporting request does not pay for four exit re-pins. It is throttled to **one sweep per minute**, `allLanesRenewGapMinutes` in `config.json` (top-level, next to `freeLadderBudgetSeconds`; `0` means every limit, clamped to a day), because a burst of cold requests would otherwise mint a fresh set of identities for every `429` the ladder collects. Whatever that floor says, a single request sweeps **at most once**: a ladder can end "spent" on several lanes in a row, and four new identities per lane-ending is a mint storm rather than a fresh start, while one sweep already leaves the next request a complete set. The dashboard's activity list shows it as a single `reinstall-all` event covering all lanes, and the snapshot exposes the floor as `allLanesRenewGapMinutes` and each provider's own last sweep as `allLanesSweptAt`.

| Lane fingerprint | Rotated per rebuild |
|---|---|
| session id (`x-opencode-session`) | yes |
| project handle (`x-opencode-project`) | yes |
| client build (`user-agent`) | yes, round-robin over the builds that pass attestation |
| exit IP | yes, and the spent one is burned out of the pool |
| lane slot | no - the slot and its state survive, so limits cannot pile up on one lane |

The pins are repaired after every refill, not on the next `429`. A lane that was rebuilt while the pool was empty kept no exit at all, and two lanes could end up sharing one because a burned lane's stale pin still counted as taken - which quietly puts two identities behind one allowance. `reconcileLanes()` runs at the end of `apply()`: a pin that is no longer live, or duplicated, is replaced from the pool. On this server that reads `lane 0..3 → 4 distinct egress IPs`.

The whole ladder is capped at **60 s** (`FREE_LIMIT_BUDGET_MS`); past it the request goes to the configured fallback provider instead of leaving the caller waiting. The budget is checked *before* every attempt, and the remaining budget - never more than the first-byte deadline, 25 s by default - is handed to that attempt, so one silent exit cannot spend the minute on its own. The first-byte deadline covers the CONNECT **and the TLS handshake**; a buffered (non-streaming) answer additionally gets a 20 s idle deadline on the body. Both burn the exit and move on. The cold budget is the operator's dial: `freeLadderBudgetSeconds` in `config.json` (top-level, next to `cooldownHours`) sets it, default 25 s, clamped to 3-120 s; the 60 s ceiling above is fixed and does not move with it. A request that arrives when every identity lane is already spent or rebuilding gets a shorter leash instead - 80 % of the same value, 20 s at the default - because the rungs left are a formality. A nuanced point that matters on this box: the shared subscription exits are themselves spent for the popular free models, so a *fresh identity on a fresh IP* is tried before walking models.

The 25 s default is the right budget while the free tier still answers. When the upstream allowance is spent, the same 25 s is burned on every cold request just to prove the ceiling again, so a deployment that is sitting on a dry free tier sets **6-8 s** and lets the configured fallback provider answer instead - the fallback hop is cheap, proving the ceiling again is not. The key changes nothing else: not the 60 s wall-clock ceiling, not the 20 s body idle deadline, and not the prompt-size extension, which is added on top of whatever the ladder gets.

Order and behaviour are configurable per provider (`freeFallback: false` disables the model walk, `freeFallbackModels: [...]` overrides the order) or globally (`freeFallbackModels` in `config.json`). Default: the validated list above, in that order.

### Context window

The free tier holds roughly **900 KB of context** - measured, and the number the whole design is sized for. A client that is not told the window guesses a small one and truncates the conversation long before the upstream does, so the router advertises a full **1,000,000 tokens** on every model it lists:

```
GET /v1/models
{ "id": "opencode-free/deepseek-v4.1-flash-free", ...
  "limit": { "context": 1000000, "input": 1000000, "output": 32000 },
  "context_length": 1000000, "max_input_tokens": 1000000, "max_output_tokens": 32000 }
```

Both spellings are present because clients disagree: OpenAI-compatible ones read `context_length`/`max_*_tokens`, models.dev-style clients read `limit.context`. Override per provider with `contextWindow` (dashboard: **Context window (tokens)**); `0` or empty means the million default. The window does not have to be honest to be safe - the upstream still 400s with `maximum context length` past ~900 KB, and the router already maps that error to its own message.

The deadlines scale with what is sent, because prefill of a big body is slow *before the first byte*: the first-byte ceiling grows by ~25 ms per KB over 8 KB (capped at +75 s) and the free tier's 60 s ladder budget grows by the same amount, added on top of the base `freeLadderBudgetSeconds` sets. A small chat keeps the default 25 s / 8 s deadlines, a 900 KB conversation gets the prefill time it needs instead of being killed as if its exit were stalled. Request bodies are read at up to 32 MB, which is ~8M tokens of JSON - far past the ceiling the upstream applies.

### Cross-provider last resort

When every model on the first-choice provider is dry — or all of its accounts are cooling — the router can answer from another provider instead. Opt in per provider or globally:

```json
{
  "id": "opencode-free",
  "fallbackProviders": [
    { "provider": "pollinations", "model": "openai" },
    { "provider": "pollinations", "model": "openai-fast" }
  ]
}
```

Only plain OpenAI-compatible targets are used, so the hop never changes the wire format a streaming client is already reading. The substitution is labelled, never disguised: `x-router-provider` and `x-router-model` name the provider and model that actually served the answer, and the Activity tab logs the hop:

```
[proxy] fallback-provider: answering with fallback-model (configured fallback; the first-choice provider had nothing left)
```

**This is exactly what `exclusiveProvider` switches off.** With the lock set the router answers from the locked provider or not at all, because a client that pinned `opencode-free/deepseek-v4.1-flash-free` did not ask for `pollinations/openai-fast`. The same applies to the per-model walk inside the free tier: `freeFallback: false` on the provider keeps every rung of the ladder on the model the client asked for, so a dry model ends in that provider's own `429` (`error.type: router_no_provider`/the upstream quota message) instead of a different model's `200`.

The reasoning is that a substitution is only free while the caller cannot tell. A chat client displays the model it requested, not `x-router-model`, so a swapped answer looks like the model you picked behaving strangely — the wrong model, the wrong price, the wrong context window, and a quota that never seems to apply to you. If you would rather have *an* answer than the *right* answer, leave the fallback list in and the lock off; the two settings are independent.

Two consequences of a locked (and therefore hop-free) free tier:

* A known-dry ceiling no longer skips the ladder. Skipping it was an optimisation for a deployment that had somewhere to hand the request to; with no hops it would be a guaranteed failure. The ladder runs every request (bounded by `freeLadderBudgetSeconds`), so the first request after the allowance returns is the one that succeeds.
* `freeLadderBudgetSeconds` is now purely the caller's patience on a dry tier. `6`-`8` answers a dry request in a few seconds; `25` gives a slow prefill its head start. Without a fallback there is nothing to spend the rest of the budget on.

### Consequences for the free tier

* The free tier carries **no timer**. A quota answer does not cool the account — the upstream's ~16h `retry-after` is deliberately ignored for this provider type — so the very next request tries again at once: it hops IP, reinstalls if the fresh IP is dry too, walks the free-model pool and answers. `freeTierCooldownMinutes` (default `0`, retry immediately) exists only to throttle upstream churn if that is ever needed, and `reinstallGapMinutes: 0` reinstalls on every limit instead of at most every 10 minutes. Every other provider type still honours `retry-after`.
* `renewManagedIdentity` still rebuilds a whole identity (new account, new session, new project handle) for the limits that *are* per-session; a rebuilt identity presents different values in every header the router controls.
* The only real escape from the free ceiling is a Console key: provider type `opencode`, base URL `https://opencode.ai/inference`. That unlocks the paid models too — `minimax-m3`, `qwen3.8-max` and `kimi-k2.6` answer `401 AuthError "Missing API key."` under `Bearer public`.

### Reinstalling the client (automatic, and on demand)

The router reinstalls the client by itself. Whenever a managed provider answers a cooldown status, `reinstallManagedClient` rebuilds **that lane's** identity in place, differing in every field the router controls:

| Field | Before -> after |
|---|---|
| lane slot / account id | unchanged (limits must not pile up on one identity) |
| generation (`gen`) | `N` -> `N+1` |
| session (`x-opencode-session` + 3 aliases) | freshly minted `ses_<12 hex><14 base62>` |
| project (`x-opencode-project`) | `p-<16 hex>`, freshly minted |
| client version (`user-agent`) | next entry of the validated list (`2.0.26` -> `2.0.31` -> `2.1.0`) |
| cooldown | cleared |
| exit IP | the router rotates the exit *before* the reinstall on the free tier, and burns the spent one |

It runs at most once per lane and request and never more often than `reinstallGapMinutes` (default `10`, ignored on the free tier), so upstream does not read the churn as abuse. A lane that keeps limiting is parked and rebuilt off the request path by `queueLaneRefresh`.

On demand — the dashboard's **Reinstall client** button (every lane, or `?lane=N` for one), or the bundled CLI:

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

Auth accepts anything real clients send: `Authorization: Bearer <router key>`, `x-api-key` (the Anthropic SDK / Claude Code), `x-goog-api-key`, or `?key=`. **The inbound surface is OpenAI- and Anthropic-shaped only** — there is no `/v1beta/models/<id>:generateContent` Gemini-shaped inbound; the `gemini` style is an *upstream* adapter.

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
| `POST` | `/admin/api/proxies/{refresh,test,apply,rotate,purge,xray,sources,settings}` | Exit pool: download+parse subscriptions, batch-test, apply to xray, rotate, tune. |
| `GET` | `/admin/api/proxies` · `/admin/api/proxies/exit/:id` | Pool snapshot / one exit (full URI). |
| `GET` | `/admin/api/freebuff` | freebuff session state per account (device key, catalog, instance, run). |
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
| `defaultProvider` | `""` | Fallback provider id. Ignored when `exclusiveProvider` is set (that provider is the default). |
| `exclusiveProvider` | `""` | **Exclusive mode.** The id (or type) of the only provider this router may serve. Set, every other provider is dropped at load, `fallbackProviders` is ignored, a request naming a different provider is refused with `404 router_unknown_provider`, and the admin API rejects adding one back. Use it when a client pins one free tier (`opencode-free/deepseek-v4.1-flash-free`) and must never be answered by another vendor. |
| `freeLadderBudgetSeconds` | `25` | `3`-`120`: time-boxes the free-tier ladder on a cold request (the already-dry leash is 80 % of it - 20 s at the default; the 60 s ceiling is fixed). Set `6`-`8` when the free allowance is spent and the fallback should answer. Unset = 25 s / 20 s, i.e. unchanged. |
| `allLanesRenewGapMinutes` | `1` | `0`-`1440`: floor between "mint a new identity on every lane at once" sweeps. `0` = sweep on every limit (the most aggressive setting: every lane gets a new session, project and client build the moment one of them is spent). One request never sweeps twice whatever this says, so `0` cannot mint four identities per lane-ending. Per provider, stamped in `state.json`, so a restart does not grant a free sweep. |
| `cors` | `true` | Send permissive CORS headers to proxy clients. |
| `logging.requests` | `false` | Routing lines in the process log. Off by default — counters live in the dashboard. |
| `rotateEveryMinutes` | `0` | Time-boxed rotation: keep one account for N minutes, then move to the next. Use it when a provider's free quota resets on a clock — OpenCode: `120` for two-hour tokens. Per-account override allowed. `0` = off. |
| `respectRetryAfter` | `true` | When an upstream answers `429` with `retry-after`, cool the account for that long instead of the full `cooldownHours`. |
| `errorCooldownMinutes` | `5` | Cooldown for a `402`/`403` whose body is **not** quota-related (wrong model handle, mode required, suspended account) — so a protocol error no longer locks an account for 12 hours. |
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
* Xray-core is baked into the image, so the exit pool works with no host dependency.
* Config, state and the exit pool live in the named volume `router-data` → `/data` (`ROUTER_CONFIG`, `ROUTER_STATE`).
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
| `--with-xray` | download Xray-core into the install dir (enables the V2Ray exit pool) |
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

### Verify what is actually running

A checkout that mixes files from different commits is undebuggable, so the running build reports its own fingerprint:

```bash
curl -s https://your-domain/health | jq .build
# { "commit": "689c4c4", "files": { "server": "3f9c2ab1d4", "dashboard": "9a11c0ffee", "proxies": "77b0e1c2d3", "freebuff": "1c2d3e4f50" } }
```

Until 27e2a68 the installer used `git pull --ff-only`, which leaves hand-edited files in place; it now does `git fetch` + `reset --hard origin/main`, warns when it discards local edits, and prints the resulting commit. If `/health` is not reachable (404), stop — that also means an old build, this endpoint used to be `ok/version/node/rss/providers/accounts`.

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

## IP rotation: V2Ray/Xray exit pool

Rate limits that follow your **IP** (OpenCode's per-address cap, freebuff's country gate) are not fixed by more accounts — they are fixed by leaving through different addresses. The router manages an [Xray-core](https://github.com/XTLS/Xray-core) process and a pool of exits parsed from V2Ray subscriptions:

```
subscription URLs ──▶ parse (vless / vmess / trojan / shadowsocks)
                      │
                      ├─▶ test in batches through a throwaway xray: latency + exit IP + alive/dead, persisted
                      ▼
                best N exits ──▶ live xray, one local HTTP inbound per exit
                      │
     provider.proxy = rotate ──▶ 429/402/403 arrives
                      ▼
             burn the spent exit, rotate to the next one and retry the SAME
             account on the new address first; a fresh IP that limits too is
             what triggers the identity reinstall. Only when the pool (and the
             identity) are spent does the account get cooled down
```

Default sources (editable in the dashboard, stored in `proxies.json`):

* `https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription.txt`
* `https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription1000.txt`

Workflow — **Network** tab, or the API:

```bash
K="x-admin-key: $ADMIN_KEY"
curl -X POST localhost:8787/admin/api/proxies/refresh -H "$K"                 # download + parse (~1000 configs)
curl -X POST localhost:8787/admin/api/proxies/test    -H "$K" -d '{}'   # test untested, in batches
curl -X POST localhost:8787/admin/api/proxies/test    -H "$K" -d '{"retest":true,"limit":200}'
curl -X POST localhost:8787/admin/api/proxies/apply   -H "$K"                 # start xray with the best exits
curl -X POST localhost:8787/admin/api/proxies/rotate  -H "$K"                 # rotate now
```

Then assign it per provider — **Providers → Edit → Outbound proxy**: `Direct`, `Rotate the pool`, or one pinned exit. OpenCode and freebuff can rotate while OpenAI stays direct. In JSON:

```json
{ "id": "opencode-main", "type": "opencode", "proxy": "rotate" }
```

Notes that save time:

* Every routed response carries `x-router-exit` (exit id or `direct`) and `x-router-rotations`, so `curl -i` shows which address served it.
* Testing walks the list in batches (`testBatchSize`, default 40) through a second xray instance, so 1000 configs never all live at once. The best `maxActive` (default 15) go into the live pool.
* An exit that answers a rate limit, stalls or resets is **burned** — it leaves the pool at once, whatever its failure count, and is never cycled back into during the same burst. A spare takes its slot immediately (`promote()`, no network: a tested exit is already waiting for it), and once the pool is down to half its depth `replenish()` runs in the background.
* `replenish()` deletes the burn pile, re-fetches the subscriptions if the untested reserve is thin, **retests the exits still in the pool as well**, tests a fresh batch (`maxActive * 3` candidates) and applies the working ones, fresh results first. Fifteen burned exits therefore become fifteen newly tested ones without a human. A request that runs dry mid-ladder waits for a smaller rebuild (`maxActive` candidates) so it can still answer inside its budget; a burn promotes a waiting spare so that case stays rare.
* Dead exits are dropped after `markDeadAfter` failures and retried on the next run; `rotateAttempts` (default 3) caps retries per request.
* The `xray` binary: `install.sh --with-xray` installs it, the Docker image already contains it, or install it from your package manager. No binary = no pool, and the router reports that instead of failing requests.
* `testUrl` defaults to `https://api.ipify.org?format=json` because it returns the exit IP; any JSON endpoint with an `ip` field works.

### Scheduled upkeep (on by default, every 12 h)

The pool refreshes itself, so a subscription that publishes new configs does not need a human:

1. every `refreshIntervalHours` (default `12`) the router re-downloads each enabled source,
2. the **new** exits (status `untested`) are batched through the throwaway xray and measured like a manual test — latency and real exit IP, leaks excluded,
3. the working ones are folded into the pool and `apply()` puts the best `maxActive` live.

The clock is anchored to the last cycle, the last subscription fetch, or the last exit test — a restart or a manual refresh an hour ago does not buy another full interval. A cycle is skipped (and logged) while a manual test is running. Every cycle appends an `auto-refresh` event and stores `lastRefresh`/`lastTest`/`lastApplied` in `proxies.json`, and `GET /admin/api/proxies` reports `auto.nextRun`.

```bash
curl -X POST localhost:8787/admin/api/proxies/settings -H "$K" \
  -d '{"autoRefresh":false}'                    # off
curl -X POST localhost:8787/admin/api/proxies/settings -H "$K" \
  -d '{"autoRefresh":true,"refreshIntervalHours":6}'   # faster
```

`refreshIntervalHours: 0` also disables it. The **Network** tab carries both fields and the next-run countdown.

### Publishing the pool as its own subscription

The pool can feed a subscription instead of only consuming one. `scripts/build-sub.js` writes every exit that currently tests good as a plain URI list under `public/sub/`, which the server already serves statically at `/sub/<file>`:

```bash
node scripts/build-sub.js                       # public/sub/monols.txt, 644
node scripts/build-sub.js --out public/sub/x.txt --min 5   # keep the old file below 5 exits
```

A cron entry every 30 minutes keeps it current (`/etc/cron.d/llm-router-sub`):

```cron
*/30 * * * * root cd /opt/llm-router && /usr/bin/node scripts/build-sub.js --quiet
```

Put that path behind a hostname and point a source at it — that is a self-hosted feed that keeps working when an upstream subscription goes stale. On this server a `cloudflared` ingress rule maps `monols.dpdns.org` to the router, so the URL is `https://monols.dpdns.org/sub/monols.txt`:

```bash
# /etc/cloudflared/config.yml (additive: the existing hostnames keep their rules)
ingress:
  - hostname: omni.monols.dpdns.org
    service: http://localhost:20128
  - hostname: monols.dpdns.org
    service: http://localhost:8787
  - service: http_status:404

cloudflared tunnel route dns omni monols.dpdns.org
systemctl restart cloudflared
curl -s -X POST localhost:8787/admin/api/proxies/sources -H "$K" \
  -d '{"sources":[{"url":"https://monols.dpdns.org/sub/monols.txt"}]}'
```

`sources` is replaced wholesale by that call, so send every URL you want to keep.

### When the ceiling is upstream of the pool

Rotating exits fixes a limit that is accounted per IP. It does nothing when the allowance is accounted per *account*: the same `429 ... Subscribe to Plan` body comes back from every address and every freshly minted identity. The router therefore watches for that specific outcome — a lane that burned its whole ladder without clearing an account-wide body — and then rests the pool for `FREE_DRY_MS` (5 min), answering from the fallback provider in the meantime while the pool rebuilds itself in the background. The burn pile is what pays for the ladder, so this is the difference between "forty seconds and four addresses per request" and "two seconds and none".

Two related defects are fixed alongside it, both visible as "the pool is empty but eleven exits say `ok`":

- A failed test now always clears `ok` from the record. It used to keep `ok` with an empty `exitIp`, which `workingExits()` rejects, so the dashboard counted working exits the pool could never use.
- `apply()` keeps the exits already in the pool in their existing slots and only fills the space behind them. Inbound ports are numbered by position in `active`, so re-ranking a healthy exit moved its port out from under an in-flight request.

## freebuff (Codebuff) as a provider

freebuff is OpenAI-shaped (`POST /api/v1/chat/completions`) but will not answer a plain API-key call. `freebuff-driver.js` performs the whole handshake, so from the router's point of view it is just another provider:

| Step | What the driver does |
|---|---|
| Device identity | Generates an Ed25519 key, registers it (`POST /api/v1/freebuff/device-keys`), and signs every request as `freebuff-device-v1\n<METHOD>\n<path>\n<ts>\n<sha256(body)>\n<fetchId>` → `x-freebuff-device-{key,ts,sig}` |
| Catalog | `GET /api/v1/freebuff/models` with `x-freebuff-client: cli` → the `fbm1.` **handle** plus `fetchId` (cached 10 min) |
| Session | `POST /api/v1/freebuff/session/admission` with the handle, a fresh instance id, `x-freebuff-wallet-spend-limit: session` |
| Run ledger | `POST /api/v1/agent-runs` `START` → `runId`, echoed as `codebuff_metadata.run_id`; `FINISH` when the response (or stream) ends |
| Request | `codebuff_metadata{cost_mode:"free",surface:"cli",freebuff_instance_id,…}`, `provider.data_collection:"deny"`, the Buffy preamble injected as the first system message, `connection: close`, `x-freebuff-sse-gzip-recovery: 1` |
| Models | `model` may be the catalog key (`m-00032eaeec`), a name, or the raw `fbm1.` handle; the driver resolves it |

Setup: paste the account token (a 36-char UUID from the desktop app vault, or the login flow at `freebuff.com/api/auth/cli/code`) as the account `apiKey`, pick a model with the **Models** button, and set **Outbound proxy: Rotate the pool**:

> The service admits a session and then returns `country_not_allowed` and resets completions for blocked countries (Iran included). The session still reports `active`, so the only reliable fix is a non-blocked egress IP — exactly what the exit pool provides. Until then the driver says so plainly: *admitted but country_not_allowed (IR) — completions will be reset; assign a proxy exit*.

Per-account freebuff state (device key id, catalog size, fetchId, admitted instance, run id, `needsReauth`) is visible at `GET /admin/api/freebuff`.

## Memory profile (measured, not estimated)

`npm run smoke` measures the running process. On this machine (Node v26.8.1, Windows 11):

| Measurement | Result |
|---|---|
| Idle RSS after boot | **~50 MB** (JS heap ~16 MB) |
| 24 concurrent SSE streams, 5.0 MB relayed | RSS +9.8 MB |
| 24 concurrent SSE streams, **20.2 MB relayed (4×)** | RSS **+2.8 MB** — i.e. growth does not track bytes |
| `heapUsed` / `arrayBuffers` during load | 15.7 MB / 1.0 MB |
| Peak RSS (router) | ~92 MB with the exit pool loaded; xray adds ~36 MB |
| Container budget | **128 MB for direct-only**, **512 MB once the exit pool is running** — the compose file now ships 512m and `--max-old-space-size=160` |

Consider the 35 MB target honestly: on Node 20+ the V8 isolate alone accounts for roughly 40 MB of RSS before a single byte of your code runs, so ~50 MB idle is the floor on Windows/Node 26 — no amount of application tuning reaches 35 MB there (`--max-old-space-size`, `--max-semi-space-size` and `--jitless` were all measured and none helped; a leaner `node:20-alpine` build, which ships small-icu, is the realistic way to get closer). What the design does guarantee, and what the smoke test verifies, is that **memory is flat with respect to traffic**: upstream bodies are piped, never accumulated, and only numeric counters are retained. The compose file enforces the 128 MB cap regardless.

The deployment budget was raised for this workload: 512 MB RAM and 1 vCPU is comfortable — xray adds roughly 20–40 MB with a 24-exit pool, and the batch tester runs at most `testBatchSize` inbounds at once.

## Security

* `/v1/models` and `/v1/*` require the router key once `proxyKeys` is set; `/health` reports only `ok/version/uptime/rss/requests/build` and no longer enumerates providers or accounts.
* The admin API is **not** CORS-exposed by default (`adminCors: false`): it is reached with `x-admin-key` from the dashboard served by the same origin, or from curl. Set `adminCors: true` only for a separate dashboard origin you control.
* `config.json` (admin key + provider keys) is written `0600`, like `proxies.json` and `xray-config.json`.

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
| `fetch failed (cause: UND_ERR_SOCKET: other side closed)` on requests with a large prompt, while small ones work | The *client's* egress is mangling big plaintext HTTP uploads — not the router. Seen with sing-box/v2rayN TUN mode (adapter MTU 9000): a 36KB POST dies with `ECONNRESET` at ~0.70s, while the same body over TLS, or through the local proxy port (`127.0.0.1:10808`), returns 200. Route the client through its proxy instead of the TUN, e.g. for this harness put `HTTP_PROXY`/`HTTPS_PROXY` (plus `NO_PROXY=localhost,127.0.0.1`) in `$DSH_HOME/.env`, and restart it. Diagnosis recipe: POST the same body to the router and to any plain HTTP host, then the same body over TLS — plaintext-only failures point at the client, not the server. |
| A non-streaming request that generates for a long time gets cut by the client or an intermediary | The router now opens the response after 7s and writes a JSON-legal newline every 4s while the upstream is still generating, so a buffered answer never leaves the socket idle. Leading whitespace is valid JSON, so `fetch().json()` is unaffected; fast answers still carry `content-length` and no padding. |
| `[xray:main] xray exited with code 255` in a loop, pool looks empty, restarts do not help | A stray `xray` from an earlier run (a test batch, or a copy of the repo started from `/tmp`) is still holding the inbound ports, and Xray's `SO_REUSEPORT` lets the new instance bind them and then die. `ps -o pid,args -C xray` shows the offender; `startMain()` now sweeps `llm-router-burn-*`/`llm-router-sched-*` instances on every start, and `test/smoke.js` runs its own manager on ports 23800+ so a test can never squat on the production range. |
| The pool reports `ok` exits but `active` stays 0 and requests log "no working exit to switch to" | Older builds left failed exits marked `ok` with an empty `exitIp`; `workingExits()` ignores those, so nothing could be promoted. Fixed by clearing `ok` on any failed test and by letting a replenish retest a few of the least-failed `dead` entries instead of only `untested` ones. |

## Verification

```bash
npm run smoke     # 111 checks, no network, no API keys, all in a temp dir (SMOKE_STRICT_MEM=1 for tight memory bounds)
```

It boots a mock upstream plus the real `server.js` and asserts, among others: 429 → failover → success in one client-visible response (`x-router-attempts: 2`); the 12-hour deadline persisted to `state.json`; `sequential` sticking to the first healthy account; `round-robin` covering all accounts evenly; `402`/`403` cooling down while `500` does not; 250-chunk SSE passthrough byte-for-byte; Anthropic and Gemini translation for both streaming and non-streaming; admin auth rejection and key masking; strategy changes written to `config.json`; **a provider edit keeping its accounts and custom headers**; OmniRoute import; **OpenCode routing for all four wire formats, keyless accounts and live model discovery**; **a policy `403` that reproduces on every exit: no rotation storm, no 12h ban, the upstream message reaches the client**; **exit telemetry that classifies a self-egress measurement as a leak and keeps it out of the pool**; **inbound auth via `x-api-key`**; **a provider with no enabled account getting its own error type**; **V2Ray exit rotation retrying an IP-bound 429 through a second CONNECT proxy without cooling the account**; **the freebuff Ed25519 handshake verified against a protocol-enforcing mock**; **a client reinstall presenting a different session, project handle and client version while clearing the cooldown**; **a dry free model answered by the next free model without cooling the account**; **an exhausted provider answered by the configured fallback provider**; and the memory behaviour in the table above.

## License

MIT
