"""pick.py -- write a single-exit xray config, rotating the exit on every run.

A balancer depends on observatory health data and silently fell through to `direct`;
a counter is deterministic. One run = one exit = one egress IP.
"""
import json, os, sys

ROOT = r"C:\router\.ocx-win"
POOL = os.path.join(ROOT, "exits-ok.json")
CFG = os.path.join(ROOT, "xray.json")
CTR = os.path.join(ROOT, "cache", "exit.ctr")
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18080

pool = json.load(open(POOL))
if not pool:
    sys.exit("empty exit pool")

n = 0
if os.path.exists(CTR):
    try:
        n = int(open(CTR).read().strip() or 0)
    except ValueError:
        n = 0
idx = n % len(pool)
open(CTR, "w").write(str(n + 1))

cfg = {
    "log": {"loglevel": "warning"},
    "inbounds": [{"listen": "127.0.0.1", "port": PORT, "protocol": "http", "tag": "in"}],
    "outbounds": [pool[idx], {"protocol": "freedom", "tag": "direct"}],
}
json.dump(cfg, open(CFG, "w"), indent=1)
v = pool[idx]["settings"]["vnext"][0]
print(f"exit {idx + 1}/{len(pool)}: {v['address']}:{v['port']}")
