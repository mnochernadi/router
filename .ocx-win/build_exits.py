"""build_exits.py -- build a rotating exit pool from the operator's subscriptions.

Reads the sub files, parses every vless URI (they carry malformed ones: the `path`
parameter swallows the following keys, e.g. "path=/fp=chrome&security=tls"), tests
each candidate through the bundled xray, and keeps only the ones that carry traffic.

Output:
  exits-ok.json   the working outbounds
  exits-rot.json  a config with a round-robin balancer over them

The lane then rotates exits per connection, which is what beats a per-IP allowance.
"""
import base64, json, os, re, subprocess, sys, time, urllib.parse, urllib.request

ROOT = r"C:\router\.ocx-win"
XRAY = os.path.join(ROOT, "bin", "xray.exe")
CFG = os.path.join(ROOT, "xray.json")
PORT = 18200
WANT = int(sys.argv[1]) if len(sys.argv) > 1 else 8
TEST = "https://api.ipify.org"
SUBS = [
    r"C:\router\.ocx-win\sub-new.txt",
    r"C:\Users\MONO\Desktop\flash mahdi\.w\papa.txt",
    r"C:\Users\MONO\Desktop\flash mahdi\.w\sub2.txt",
    r"C:\Users\MONO\Desktop\flash mahdi\.w\sub.txt",
]
KEYS = ("fp", "security", "type", "sni", "host", "alpn", "flow")


def read_links():
    out = []
    for f in SUBS:
        if not os.path.exists(f):
            continue
        raw = open(f, encoding="utf-8", errors="replace").read()
        if "://" not in raw:
            try:
                raw = base64.b64decode(re.sub(r"\s", "", raw)).decode("utf-8", "replace")
            except Exception:
                continue
        out += [l.strip() for l in raw.splitlines() if l.strip().startswith("vless://")]
    return out


def parse(link, idx):
    u = urllib.parse.urlsplit(link)
    q = urllib.parse.parse_qs(u.query)
    g = lambda k, d="": (q.get(k) or [d])[0]

    raw_path = g("path", "/")
    real = raw_path
    for k in KEYS:
        at = real.find(k + "=")
        if at > 0:
            tail = real[at:]
            real = real[:at]
            for part in re.split(r"(?=(?:" + "|".join(KEYS) + r")=)", tail):
                if "=" in part:
                    kk, vv = part.split("=", 1)
                    if kk in KEYS and kk not in q:
                        q[kk] = [vv]
            break
    real = real or "/"

    host = (u.hostname or "").rstrip(".")
    if not host or not u.port:
        return None
    sec = g("security", "")
    net = g("type", "tcp")
    if sec not in ("tls", "reality") or net not in ("ws", "xhttp", "grpc"):
        return None
    sni = (g("sni") or g("host") or host).rstrip(".")
    ws_host = (g("host") or sni).rstrip(".")

    o = {"protocol": "vless", "tag": f"ex-{idx}",
         "settings": {"vnext": [{"address": host, "port": u.port,
                                 "users": [{"id": u.username,
                                            "encryption": g("encryption", "none") or "none"}]}]}}
    ss = {"network": net}
    if sec == "tls":
        ss["security"] = "tls"
        ss["tlsSettings"] = {"serverName": sni, "fingerprint": g("fp") or "chrome"}
        if g("alpn"):
            ss["tlsSettings"]["alpn"] = g("alpn").split(",")
    else:
        ss["security"] = "reality"
        ss["realitySettings"] = {"serverName": sni, "fingerprint": g("fp") or "chrome",
                                 "publicKey": g("pbk"), "shortId": g("sid"),
                                 "spiderX": g("spx", "/") or "/"}
    if net == "ws":
        ss["wsSettings"] = {"path": real, "host": ws_host}
    elif net == "xhttp":
        ss["xhttpSettings"] = {"path": real, "host": ws_host, "mode": g("mode", "auto") or "auto"}
    else:
        ss["grpcSettings"] = {"serviceName": g("serviceName") or real.strip("/")}
    o["streamSettings"] = ss
    return o


def probe(o, port):
    cfg = {"log": {"loglevel": "warning"},
           "inbounds": [{"listen": "127.0.0.1", "port": port, "protocol": "http"}],
           "outbounds": [o, {"protocol": "freedom", "tag": "direct"}]}
    json.dump(cfg, open(CFG, "w"), indent=1)
    log = open(os.path.join(ROOT, "cache", "ex.log"), "w")
    p = subprocess.Popen([XRAY, "run", "-c", CFG], stdout=log, stderr=log,
                         creationflags=0x08000000)
    try:
        time.sleep(1.4)
        if p.poll() is not None:
            return None
        op = urllib.request.build_opener(urllib.request.ProxyHandler(
            {"http": f"http://127.0.0.1:{port}", "https": f"http://127.0.0.1:{port}"}))
        return op.open(TEST, timeout=9).read().decode().strip()
    except Exception:
        return None
    finally:
        p.terminate()
        try: p.wait(3)
        except Exception: p.kill()
        log.close()
        time.sleep(0.15)


links = read_links()
seen, cands = set(), []
for l in links:
    o = parse(l, 0)
    if not o:
        continue
    v = o["settings"]["vnext"][0]
    k = (v["address"], v["port"])
    if k in seen:
        continue
    seen.add(k)
    cands.append(l)

print(f"{len(cands)} distinct TLS/reality candidates", flush=True)
ok = []
for i, l in enumerate(cands):
    if len(ok) >= WANT:
        break
    o = parse(l, len(ok))
    ip = probe(o, PORT + (i % 30))
    v = o["settings"]["vnext"][0]
    if ip:
        ok.append(o)
        print(f"  OK {v['address']}:{v['port']} -> {ip}", flush=True)
    else:
        print(f"  -- {v['address']}:{v['port']}", flush=True)

json.dump(ok, open(os.path.join(ROOT, "exits-ok.json"), "w"), indent=1)
rot = {
    "log": {"loglevel": "warning"},
    "inbounds": [{"listen": "127.0.0.1", "port": 18080, "protocol": "http", "tag": "in"}],
    "outbounds": ok + [{"protocol": "freedom", "tag": "direct"}],
    "routing": {"balancers": [{"tag": "bal", "selector": ["ex-"],
                               "strategy": {"type": "roundRobin"}}],
                "rules": [{"type": "field", "network": "tcp,udp", "balancerTag": "bal"}]},
}
json.dump(rot, open(os.path.join(ROOT, "exits-rot.json"), "w"), indent=1)
print(f"\nkept {len(ok)} working exits -> exits-rot.json", flush=True)
