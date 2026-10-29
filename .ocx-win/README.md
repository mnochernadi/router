# ocx-win — anonymous OpenCode CLI lane with automatic exit rotation

A second OpenCode lane that is **always anonymous**: its own profile, its own store,
no credential ever. It runs on the anonymous bucket, which is a different bucket from
the account your desktop app uses, and it **changes egress IP by itself** when the
limit lands.

Nothing system-wide is touched: no WinINET proxy, no `netsh`, no PATH change.
Isolation is per-process only.

```powershell
cd C:\router\.ocx-win
.\ocx.ps1 run "say OK"     # completion; rotates exit on the free-usage limit
.\ocx.ps1 refresh          # rebuild the exit pool now
.\ocx.ps1 status           # pool size, age, parked exits, credential state
```

## Rotation

The free allowance is refused per egress IP: measured 2026-10-29, the same minute
answered `429` from one IP and `200` from another, and swapped back. So the lane
keeps a pool of working exits built from the subscription and, on a limit, parks the
exit that was refused and retries from the next one — repeatedly, until one answers.

The pool is rebuilt from the subscriptions **every 12 hours** (scheduled task
`ocx-pool-refresh`) and immediately whenever every exit in it is parked.

Exits are probed before they enter the pool, and the per-attempt xray config carries
**only** the chosen outbound — no `direct` fallback, so a dead exit fails loudly
instead of silently answering from the local IP.

## Isolation

| Layer | Mechanism |
|---|---|
| Profile | own `USERPROFILE` + `XDG_*` under `home/`, `data/`, `config/`, `cache/` |
| Service | `--standalone` on every call, so the desktop app's background service is never used |
| Credential | `strip.py` runs before every `run` and deletes any credential row |
| Egress | the project's own xray on `127.0.0.1:18080`, one exit per attempt |
| Ports | nothing of the operator's is bound; v2rayN on 10808 is not used |

## No auth, by construction

There is no sign-in step. `status` must print `anonymous lane`; if it ever prints
`WARNING`, a credential crept in and `strip.py` clears it on the next run.

## Files

| File | Role |
|---|---|
| `ocx.ps1` | entry point, sets the isolated environment |
| `lane.py` | pool build, rotation, run loop |
| `build_exits.py` | one-shot pool build from the subs |
| `pick.py` | single-exit config writer |
| `strip.py` | clears credential rows |
| `seed.py` | copies the model catalog from the default profile, then clears credentials |
| `relay.py` | fallback pipe to a local v2ray listener |
| `subs/` | last fetched subscriptions |
| `exits-ok.json` | working pool |
| `bad.json` | parked exits, reset on every refresh |

## Subscriptions

```
https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription.txt
https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription1000.txt
```

Both are read on every refresh; the second is the fallback pool when the first is
thin. `parse()` handles this subscription's malformed URIs (the `path` parameter
swallows the following keys, e.g. `path=/fp=chrome&security=tls`) and keeps the
WebSocket `Host` header separate from the TLS SNI.

## Model catalog

A fresh profile has no model list, so `seed.py` copies `opencode.db` from the default
profile (that carries the catalog) and clears the credential tables. Re-run it if the
catalog goes stale:

```powershell
python seed.py
```
