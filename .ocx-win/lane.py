"""lane.py -- anonymous OpenCode lane with automatic exit rotation.

Behaviour:
  * the exit pool is rebuilt from the subscriptions every POOL_TTL (12 h) and
    whenever it runs dry
  * a run that hits the free-usage limit advances to the next working exit and
    retries, and keeps doing that until one answers or the pool is exhausted
  * exits that answered 429 are parked in bad.json until the next refresh

  lane.py run "say OK"     one completion, rotating on limit
  lane.py refresh          force a pool rebuild now
  lane.py status           pool size, age, parked exits

Environment is supplied by ocx.ps1 (isolated profile + HTTPS_PROXY to 127.0.0.1:18080).
"""
import base64, concurrent.futures as cf, json, os, re, subprocess, sys, threading, time, urllib.parse, urllib.request

ROOT = r"C:\router\.ocx-win"
XRAY = os.path.join(ROOT, "bin", "xray.exe")
CFG = os.path.join(ROOT, "xray.json")
POOL = os.path.join(ROOT, "exits-ok.json")
BAD = os.path.join(ROOT, "bad.json")
META = os.path.join(ROOT, "pool.meta")
SUBDIR = os.path.join(ROOT, "subs")
CLI = r"C:\Users\MONO\AppData\Local\Programs\@opencodedesktop\resources\opencode-cli.exe"
PORT = 18080
MODEL = "opencode/deepseek-v4.1-flash-free"
POOL_TTL = 12 * 3600
SUBS = [
    "https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription.txt",
    "https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription1000.txt",
]
KEYS = ("fp", "security", "type", "sni", "host", "alpn", "flow")


# ---------------------------------------------------------------- pool building
def fetch_subs():
    os.makedirs(SUBDIR, exist_ok=True)
    links = []
    for i, url in enumerate(SUBS):
        try:
            req = urllib.request.Request(url, headers={"user-agent": "curl/8"})
            raw = urllib.request.urlopen(req, timeout=40).read().decode("utf-8", "replace")
        except Exception as e:
            print(f"  sub {i + 1} fetch failed: {type(e).__name__}", flush=True)
            continue
        open(os.path.join(SUBDIR, f"sub{i + 1}.txt"), "w", encoding="utf-8").write(raw)
        if "://" not in raw:
            try:
                raw = base64.b64decode(re.sub(r"\s", "", raw)).decode("utf-8", "replace")
            except Exception:
                pass
        got = [l.strip() for l in raw.splitlines() if l.strip().startswith("vless://")]
        print(f"  sub {i + 1}: {len(got)} links", flush=True)
        links += got
    return links


def parse(link, idx):
    u = urllib.parse.urlsplit(link)
    q = urllib.parse.parse_qs(u.query)
    g = lambda k, d="": (q.get(k) or [d])[0]

    real = g("path", "/")
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
    sec, net = g("security", ""), g("type", "tcp")
    if sec not in ("tls", "reality") or net not in ("ws", "xhttp", "grpc"):
        return None
    sni = (g("sni") or g("host") or host).rstrip(".")
    wh = (g("host") or sni).rstrip(".")

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
        ss["wsSettings"] = {"path": real, "host": wh}
    elif net == "xhttp":
        ss["xhttpSettings"] = {"path": real, "host": wh, "mode": g("mode", "auto") or "auto"}
    else:
        ss["grpcSettings"] = {"serviceName": g("serviceName") or real.strip("/")}
    o["streamSettings"] = ss
    return o


def probe(o, port):
    cfg = {"log": {"loglevel": "warning"},
           "inbounds": [{"listen": "127.0.0.1", "port": port, "protocol": "http"}],
           "outbounds": [o]}
    json.dump(cfg, open(CFG, "w"), indent=1)
    log = open(os.path.join(ROOT, "cache", "probe.log"), "w")
    p = subprocess.Popen([XRAY, "run", "-c", CFG], stdout=log, stderr=log,
                         creationflags=0x08000000)
    try:
        time.sleep(1.4)
        if p.poll() is not None:
            return False
        op = urllib.request.build_opener(urllib.request.ProxyHandler(
            {"http": f"http://127.0.0.1:{port}", "https": f"http://127.0.0.1:{port}"}))
        return bool(op.open("https://api.ipify.org", timeout=9).read())
    except Exception:
        return False
    finally:
        p.terminate()
        try:
            p.wait(3)
        except Exception:
            p.kill()
        log.close()
        time.sleep(0.12)


def refresh(limit=12):
    print("refreshing exit pool from the subscriptions", flush=True)
    links = fetch_subs()
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
    print(f"  {len(cands)} distinct candidates", flush=True)

    ok, lock = [], threading.Lock()
    slots = threading.Semaphore(8)

    def work(item):
        i, l = item
        if len(ok) >= limit:
            return
        with slots:
            if len(ok) >= limit:
                return
            o = parse(l, 0)
            if not o:
                return
            v = o["settings"]["vnext"][0]
            if probe(o, PORT + 1 + (i % 25)):
                with lock:
                    if len(ok) < limit:
                        o["tag"] = f"ex-{len(ok)}"
                        ok.append(o)
                        print(f"  OK {v['address']}:{v['port']}", flush=True)

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(work, enumerate(cands)))

    json.dump(ok, open(POOL, "w"), indent=1)
    json.dump([], open(BAD, "w"))
    open(META, "w").write(str(int(time.time())))
    print(f"pool: {len(ok)} working exits", flush=True)
    return ok


def ensure_pool():
    fresh = os.path.exists(POOL) and os.path.exists(META) and \
        (time.time() - int(open(META).read().strip() or 0)) < POOL_TTL
    if fresh:
        return json.load(open(POOL))
    return refresh()


def parked():
    try:
        return set(json.load(open(BAD)))
    except Exception:
        return set()


def park(tag):
    b = parked() | {tag}
    json.dump(sorted(b), open(BAD, "w"))


# ---------------------------------------------------------------------- running
def run_once(prompt, timeout=180):
    out = os.path.join(ROOT, "cache", "run.out")
    err = os.path.join(ROOT, "cache", "run.err")
    p = subprocess.Popen([CLI, "run", "--standalone", "-m", MODEL, prompt],
                         cwd=os.path.join(ROOT, "proj"),
                         stdout=open(out, "w"), stderr=open(err, "w"),
                         creationflags=0x08000000)
    try:
        p.wait(timeout)
    except subprocess.TimeoutExpired:
        p.kill()
    o = open(out, encoding="utf-8", errors="replace").read()
    e = open(err, encoding="utf-8", errors="replace").read()
    return p.returncode, o, e


def run(prompt):
    pool = ensure_pool()
    if not pool:
        print("no working exit in the pool", flush=True)
        return 1
    tried = 0
    while True:
        bad = parked()
        live = [o for o in pool if o["tag"] not in bad]
        if not live:
            print("every exit in the pool is parked -- refreshing", flush=True)
            pool = refresh()
            if not pool:
                print("still nothing working", flush=True)
                return 1
            continue
        o = live[0]
        v = o["settings"]["vnext"][0]
        cfg = {"log": {"loglevel": "warning"},
               "inbounds": [{"listen": "127.0.0.1", "port": PORT, "protocol": "http"}],
               "outbounds": [o]}
        json.dump(cfg, open(CFG, "w"), indent=1)
        x = subprocess.Popen([XRAY, "run", "-c", CFG],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             creationflags=0x08000000)
        time.sleep(1.3)
        try:
            rc, out, err = run_once(prompt)
        finally:
            x.terminate()
            try:
                x.wait(3)
            except Exception:
                x.kill()
        blob = out + err
        tried += 1
        if "Free usage limit" in blob or "429" in blob:
            park(o["tag"])
            print(f"  limit on {v['address']}:{v['port']} -> next exit", flush=True)
            continue
        if rc == 0:
            lines = [l for l in out.splitlines() if l.strip()]
            print(lines[-1] if lines else "(empty)", flush=True)
            print(f"  exit {v['address']}:{v['port']} (attempt {tried})", flush=True)
            return 0
        print(f"  error on {v['address']}:{v['port']}", flush=True)
        park(o["tag"])
        if tried > len(pool) + 2:
            return 1


def status():
    pool = json.load(open(POOL)) if os.path.exists(POOL) else []
    age = "-"
    if os.path.exists(META):
        age = f"{(time.time() - int(open(META).read().strip() or 0)) / 3600:.1f} h"
    print(f"pool: {len(pool)} exits, age {age}, parked {len(parked())}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "refresh":
        sys.exit(0 if refresh() else 1)
    if cmd == "status":
        status()
    else:
        sys.exit(run(sys.argv[2] if len(sys.argv) > 2 else "say OK"))
