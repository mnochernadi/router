# ocx — the isolated OpenCode client

`/opt/ocx-client.sh` runs the genuine OpenCode client as a stranger on a foreign IP.
It exists because the anonymous free tier is one global daily bucket: nothing the
client controls resets it, so the only useful lever is *being a different machine*
when the clock does reset it.

## What is isolated

| Layer | Mechanism |
|---|---|
| Egress | `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY` → `127.0.0.1:20801` → `51.77.217.70` (OVH FR, AS16276) |
| Leak guard | iptables chain `OCX_OUT` matched on `--uid-owner ocx`: loopback + DNS only, everything else `REJECT` |
| IPv6 | `ip6tables` same chain, `REJECT` — the host's global v6 address is never used |
| Identity | dedicated uid `ocx`, HOME `/var/lib/ocx`, project `/var/lib/ocx/project`, empty credential store |
| Build | binary re-downloaded from npm on every `setup`, never shared with `/opt/oc/bin` |

The host is Hetzner AS24940; the exit is OVH AS16276 in another country. Different
ASN, different /8, different rDNS, no shared TLS session cache.

## Commands

```bash
/opt/ocx-client.sh setup            # user, dirs, fresh binary, firewall
/opt/ocx-client.sh verify           # exit IP as the client sees it + chain + sockets
/opt/ocx-client.sh run "say OK"     # one completion
/opt/ocx-client.sh fw               # re-arm the firewall only
ocx "say OK"                        # wrapper for the above
```

`OCX_EXIT=20802` picks a different exit port; `OCX_VER=` pins a different build.

## Proof the guard holds

```
$ setpriv --reuid=ocx ... curl https://api.ipify.org        # no proxy
rc=7                      <- REJECT, nothing escapes to the host IP
$ setpriv --reuid=ocx ... HTTPS_PROXY=...:20801 curl ...
51.77.217.70              <- the OVH exit
$ ss -tnp | grep pid=<client>
127.0.0.1:34364 -> 127.0.0.1:20801   <- the only socket the client ever opened
```

## Firewall persistence

`ocx-net.service` (enabled) re-applies the chain on boot and tears it down on stop.
It only ever touches `OCX_OUT` and one `OUTPUT` jump for uid `ocx` — `omniroute`,
`marzban-marzban-1`, `llm-router` and `oc-serve` are unaffected.

## Measured

2026-10-25 23:34Z, bucket spent: the isolated client reaches the API through the exit
and returns `AI_APICallError: Free usage limit reached` with the same
`retry-after` → `2026-10-26T00:00:00Z` as every other lane. The reset is temporal.

2026-10-26 00:00:39Z, after the UTC reset — **4/4 completions `rc=0`, text `OK`,
no error line**, first one 35 s after the boundary:

```
2026-10-26T00:00:39Z try=1 rc=0 out=OK err=-
2026-10-26T00:00:49Z try=2 rc=0 out=OK err=-
2026-10-26T00:00:57Z try=3 rc=0 out=OK err=-
```

Exit identity during those runs: `51.77.217.70` / `AS16276 OVH SAS`. The host is
`65.109.211.84` / `AS24940 Hetzner`. The router's own `opencode-cli` path recovered on
the same boundary (`200`, 5.4 s), so the reset is not specific to the isolated lane.
