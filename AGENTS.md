# AGENTS.md — hard rules for this workspace

## 0. HANDS OFF THE OPERATOR'S OTHER SERVICES (standing order, no exceptions)

These run on the same VPS as this project. They are **not** part of this repo, they
are **not** in scope for any task, and they must never be stopped, started,
restarted, removed, reconfigured, or inspected-with-intent:

| Service | What it is | Where |
|---|---|---|
| `omniroute` (docker) | the operator's own LLM router — **live, in use, publicly exposed** | container `omniroute`, `diegosouzapw/omniroute:latest`, `0.0.0.0:20128`, data at `/root/omniroute-data`, published through `/etc/cloudflared/config.yml` → `localhost:20128` |
| `marzban-marzban-1` (docker) | the operator's VPN panel | container `marzban-marzban-1`, `gozargah/marzban:latest` |

`docker stop omniroute` was executed once by an agent reading "no other providers in
the router" as "no other router on the box". It cut a live, publicly-served endpoint.
The order is now explicit: **never touch them, in any wording of any task.**
If a task seems to require it, it does not — the task is about `llm-router` only.
Reading `docker ps` for context is fine; anything that changes state is not.

## 1. What this project is

`llm-router` — the service on `65.109.211.84`, `systemd` unit `llm-router.service`,
working dir `/opt/llm-router`, listening `0.0.0.0:8787`, config
`/var/lib/llm-router/config.json`, state `/var/lib/llm-router/state.json`,
admin key in `/etc/llm-router.env`.

## 2. Serving policy: opencode-free only

The operator's rule: **only and only OpenCode Free.** The deployed config carries a
single provider, `opencode-free`, with `exclusiveProvider: "opencode-free"` — no
`pollinations`, no `omni`, no `omniroute` provider, no cross-provider fallback. Any
request naming another provider is refused with `404 router_unknown_provider`.

Consequence to state plainly, every time it matters: with the lock on there is no
substitution, so when the free allowance is spent the client gets that `429` — there
is nothing else to answer with. That is the chosen trade, not a bug.

## 3. The free-tier ceiling (measured, do not re-litigate)

The anonymous OpenCode free tier is **one daily bucket that resets at 00:00 UTC**.
It is not keyed on anything the client controls. Measured 2026-10-24 12:30–12:36 UTC
on this server, with the bucket spent:

* 14 distinct egress IPs (host + 13 pool exits) → `429 FreeUsageLimitError`, every one
* client builds `2.0.26`, `2.0.31`, `2.1.0` → `429`
* fresh session vs. reused session, fresh project handle → `429`
* `deepseek-v4.1-flash-free`, `glm-5.3-flash-free`, `big-pickle`, `beam-free` → `429`
* `/v1/chat/completions`, `/v1/messages`, `/openai/v1/chat/completions`,
  `/anthropic/v1/messages`, `/zen/v1/chat/completions` → `429`
* every `retry-after` counted down to the **same absolute instant**, `2026-10-25T00:00:00Z`

So: IP rotation, identity rotation and a new build buy nothing once the bucket is dry.
Re-verify any time with `bash .tools/live-final-matrix.sh` on the server (see
`.tools/` for the SSH driver). `oc-reset-probe.timer` logs the recovery to
`/var/log/oc-free-reset.log`.

### 3.1 Two regimes — the rotation is not useless in the other one

Do not read §3 as "rotation never helps". **While the day still has allowance** the
free allowance moves by IP and by model — recorded on this box: *the same request
answered 200 from a fresh exit in the minute the old one answered 429*. That is what
the ladder (`exit → identity → model → lane → wait → fallback`) exists for, and it
works. The discriminator is `FREE_DAY_HORIZON_MS = 30 min` in `server.js`: under the
horizon a spent address or model is cleared by moving; at or over it the *day* is
spent and the router parks the tier until the instant the upstream named.

Re-confirmed live 2026-10-24 12:59:20–12:59:39 UTC with the bucket spent: 46 rows,
`200=0 · 429=40 · 403=3 (gate) · 404=2 (unrouted) · 426=1 (build floor)`, every
`retry-after` in lockstep to `2026-10-25T00:00:00Z`. The load-bearing row is
auth-style `E5`: **a request with no credential at all still returns
`429 FreeUsageLimitError`**, so the bucket hangs on the anonymous principal the edge
derives from the connection — not on the bearer, build, session, project, model or
path. Full run: `.tools/FREE-TIER-FINAL-VERDICT.md`.

The `proxies.json` sweep is void: all 1,647 entries carry `exitIp: ""` /
`exit produced no response head in 8s`. The **xray inbounds are a different story** —
re-measured 2026-10-24 13:01:54Z, `127.0.0.1:20801-20815` egress fine and resolve to
14 live exits across 10 ASNs / 7 countries (20807–20810 share one Scaleway /24, so 10
distinct networks). They answer `403 FreeTierError` on a gate-failing shape and `429`
to the same `2026-10-25T00:00:00Z` on the gate-passing matrix shape. Do not read the
dead registry as "the pool is dead".

### 3.2 The client-side question is closed — real client, not a forgery

Ran the genuine binary (`opencode-linux-x64@1.19.4`, fresh `HOME`, 0 credentials,
13:04Z): `AI_APICallError: Free usage limit reached`. The real client on a clean
install hits the same wall as every forged row. Cause, from the bundle itself: with
no key set the client does `if (!g) y.request.body.apiKey = "public"` — the anonymous
tier is addressed with a literal public credential, so there is no per-install token
to rotate. Sign-in is an OAuth device flow to `/auth/device/token`
(`client_id=opencode-cli`) and is the only lever that changes principal. Burn
timeline 2026-10-24: no traffic 00:00–05:00Z, first `429` 05:51:24Z, day spent
05:51:47Z → usable window ≈ 00:00–06:00Z. Evidence and the live exit table:
`.tools/FREE-TIER-REALCLIENT-PROOF.md`.

### 3.3 CORRECTION, 2026-10-25 — the genuine client works; the forged shape is what is refused

§3.2 is true for 2026-10-24 and false as a general conclusion. Re-measured
2026-10-25 09:37–09:49Z:

* **The genuine client answers.** `opencode-linux-x64@1.19.4`, fresh `HOME`, 0
  credentials → **10/10 successful completions** of `deepseek-v4.1-flash-free`,
  1.9–11 s each, `tokens.output=2`, text `OK`. Six via `opencode run`, three via the
  `opencode serve` session API, one initial CLI probe.
* **Every forged shape is refused.** Host egress, all 15 pooled exits, all nine
  `*-free` models → `429 FreeUsageLimitError`, every `retry-after` landing on
  `2026-10-26T00:00:00Z`. `minimax-m3.1-flash-free` is `410 ModelDeprecated`.
* **The deployed router earned zero successes.** `journalctl -u llm-router` since
  `2026-10-25 00:00Z` contains **no `200`**. At `00:00:17Z` `oc-window-open.timer`
  recycled 4 lanes onto fresh exits with new identities; at `00:00:18Z` — one second
  later — the router logged *"free tier is spent for the day (1440m to the reset)"*.
  The 40-request `/admin/api/proxies/test` burst in that script is **not** the cause:
  the very first probe after the reset was already refused.

So the allowance is **not** keyed on the connection the way §3.1 concludes. It is
granted to the genuine-client principal and withheld from the forged-header principal,
and the 00:00 UTC reset restored it to the former only. The 2026-10-24 `429` from the
real client was a genuinely spent bucket, not evidence that the client is refused.

**Working path, installed and verified 2026-10-25:**

```bash
# create a session pinned to the free model
SID=$(curl -s -X POST http://127.0.0.1:47999/api/session \
  -H 'content-type: application/json' \
  -d '{"model":{"id":"deepseek-v4.1-flash-free","providerID":"opencode"}}' \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])')

# message it -- note: NO /api prefix on this route
curl -s -X POST "http://127.0.0.1:47999/session/$SID/message" \
  -H 'content-type: application/json' \
  -d '{"parts":[{"type":"text","text":"say OK"}]}'
# -> {"info":{...,"modelID":"deepseek-v4.1-flash-free"},"parts":[{...,"text":"OK"}]}
```

Backed by `oc-serve.service` (`deploy/oc-serve.service`), enabled, bound to
`127.0.0.1:47999`, binary at `/opt/oc/bin/opencode`, `HOME=/var/lib/oc-serve`.
`GET /api/health` → `{"healthy":true}`. Two route traps: session creation wraps its
reply in `{"data":{...}}`, and the message route is `/session/{id}/message` while
`/api/session/{id}/message` returns the web UI HTML with `200`.

The router now uses it. Provider type `opencode-cli` with `cliMode: "serve"` and
`cliServeUrl: "http://127.0.0.1:47999"` solves the two route traps in the driver
(`opencode-cli-driver.js`) and presents the answer as an OpenAI `chat.completion` (or
an SSE stream of the same text). `cliMode: "spawn"` runs `opencode run` per request
instead, and a serve failure falls back to it automatically.

Measured through `:8787` on 2026-10-25 09:56Z, `exclusiveProvider: "opencode-cli"`:
three consecutive non-stream requests `200` in 1.73/1.29/1.38s with the model's own
text, a stream `200` with the first byte at 1.47s and a proper `[DONE]`, router
memory flat at 26M while `oc-serve` holds ~589M. Two bugs had to be fixed first, both
of which looked exactly like a hang:

* `MemoryMax=128M` on `llm-router.service` starved the bun child into state `D`
  (memory reclaim, ~180M RSS) with no output at all. The unit now ships
  `MemoryMax=512M` / `MemoryHigh=384M` (a drop-in does the same on this box).
* the child inherited the service environment, including `PORT=8787`/`HOST=0.0.0.0`.
  The CLI boots local services of its own and hung on the port the router already
  owns. The driver now passes a minimal env (HOME/XDG/TERM/`NO_COLOR`, PATH/LANG/TZ,
  plus the exit proxy when the pool is in use).

`cliDry` in `state.json` is this transport's own window: the CLI's limit parks it and
the router answers `429` with the `00:00 UTC` clock in `Retry-After`. It is kept
separate from the anonymous API's `freeDry` on purpose — the genuine client is the one
the gate serves when every forged shape is refused, so a spent forged window must not
mask it (that bug cost a debugging round: `handleOpenCodeCli` answered `429` without
ever running the binary).

Full evidence: `.tools/LIVE-VERDICT-2026-10-25.md`.
