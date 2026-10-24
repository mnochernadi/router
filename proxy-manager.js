'use strict';

/**
 * ============================================================================
 *  proxy-manager.js -- V2Ray/Xray exit pools for the router
 * ============================================================================
 *
 *  Problem it solves: providers such as OpenCode rate-limit by source IP. One
 *  server IP burns through the limit; a pool of rotating exits spreads the
 *  traffic across many addresses.
 *
 *  How it works
 *   1. subscription URLs (vless:// vmess:// trojan:// ss:// links, plain or
 *      base64-wrapped) are downloaded and parsed into Xray outbound objects;
 *   2. every candidate is tested in batches through a throwaway Xray instance
 *      (latency + exit IP + alive/dead) and the results are persisted;
 *   3. the best N working exits are loaded into the long-running Xray instance,
 *      each on its own local HTTP inbound port;
 *   4. providers ask for `direct`, `rotate` (round-robin over the pool) or one
 *      pinned exit, and the router retries a rate-limited request through the
 *      next exit before it gives up on the account.
 *
 *  Requires the `xray` binary (ROUTER_XRAY env var or settings.xrayPath). With
 *  no binary the router keeps working, direct-only, and says so.
 */

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const net = require('node:net');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const { proxiedGet } = require('./proxy-client');

const DEFAULT_SOURCES = [
  { url: 'https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription.txt', enabled: true },
  { url: 'https://raw.githubusercontent.com/papa8274/sub-mirror/main/subscription1000.txt', enabled: true },
];

const DEFAULT_SETTINGS = {
  enabled: true,
  xrayPath: process.env.ROUTER_XRAY || 'xray',
  inboundBasePort: 20801,
  testBasePort: 30801,
  // Deep enough to hop fifteen times before anything has to be re-tested, small
  // enough that a full burn pile is cheap to rebuild.
  maxActive: 15,
  testBatchSize: 40,
  testConcurrency: 8,
  testUrl: 'https://api.ipify.org?format=json',
  testTimeoutMs: 8000,
  rotateOnStatuses: [429, 402, 403],
  rotateAttempts: 3,
  markDeadAfter: 3,
  insecure: false,
  // Keep the pool fresh on its own: re-fetch the subscriptions, test whatever is
  // new, and fold the working ones into the pool. 0 disables the timer.
  autoRefresh: true,
  refreshIntervalHours: 12,
};

const EVENT_LIMIT = 40;
// Bumped whenever the way we measure an exit changes, so stale telemetry from
// an older build cannot keep a wrong exitIp in the active pool.
const TELEMETRY_SCHEMA = 3;

// ---------------------------------------------------------------------------
// subscription parsing
// ---------------------------------------------------------------------------

const b64decode = (s) => {
  try { return Buffer.from(String(s).replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8'); }
  catch { return ''; }
};

function b64json(s) {
  try { return JSON.parse(b64decode(s)); } catch { return null; }
}

function splitFirst(s, ch) {
  const i = s.indexOf(ch);
  return i === -1 ? [s, ''] : [s.slice(0, i), s.slice(i + 1)];
}

function streamSettings({ net: rawNet, security, q, host }) {
  const net = rawNet === 'splithttp' ? 'xhttp' : rawNet;
  const s = { network: net || 'tcp' };
  const sni = q.get('sni') || q.get('host') || host;
  const insecure = q.get('allowInsecure') === '1' || q.get('insecure') === '1';
  const fp = q.get('fp') || undefined;
  const alpn = q.get('alpn') ? q.get('alpn').split(',').filter(Boolean) : undefined;

  if (security === 'tls') {
    s.security = 'tls';
    s.tlsSettings = { serverName: sni, allowInsecure: insecure, fingerprint: fp, alpn };
  } else if (security === 'reality') {
    s.security = 'reality';
    s.realitySettings = {
      serverName: sni,
      publicKey: q.get('pbk') || '',
      shortId: q.get('sid') || '',
      fingerprint: fp || 'chrome',
      spiderX: q.get('spx') || '/',
      show: false,
    };
  } else {
    s.security = 'none';
  }

  const path = q.get('path') || undefined;
  const headerHost = q.get('host') || undefined;
  if (net === 'ws') s.wsSettings = { path: path || '/', headers: headerHost ? { Host: headerHost } : undefined };
  else if (net === 'grpc') s.grpcSettings = { serviceName: q.get('serviceName') || path || '' };
  else if (net === 'http' || net === 'h2') s.httpSettings = { path: path || '/', host: headerHost ? [headerHost] : undefined };
  else if (net === 'xhttp') s.xhttpSettings = { path: path || '/', host: headerHost, mode: q.get('mode') || 'auto' };
  else if (net === 'tcp' && q.get('headerType') === 'http') {
    s.tcpSettings = { header: { type: 'http', request: { path: [path || '/'], headers: headerHost ? { Host: [headerHost] } : {} } } };
  }
  return s;
}

function newExit(uri, fields) {
  const id = crypto.createHash('sha1').update(uri).digest('hex').slice(0, 12);
  return { id, uri, status: 'untested', latencyMs: 0, exitIp: '', lastTested: 0, failures: 0, ...fields };
}

function parseVless(uri) {
  const u = new URL(uri);
  const q = u.searchParams;
  const flow = q.get('flow') || '';
  const id = decodeURIComponent(u.username || '');
  if (!id) throw new Error('vless without uuid');
  return newExit(uri, {
    protocol: 'vless',
    name: decodeURIComponent(u.hash.replace(/^#/, '')) || `${u.hostname}:${u.port}`,
    host: u.hostname,
    port: Number(u.port || 443),
    outbound: {
      protocol: 'vless',
      settings: {
        vnext: [{
          address: u.hostname,
          port: Number(u.port || 443),
          users: [{ id, encryption: q.get('encryption') || 'none', ...(flow ? { flow } : {}) }],
        }],
      },
      streamSettings: streamSettings({ net: q.get('type') || 'tcp', security: q.get('security') || 'none', q, host: u.hostname }),
    },
  });
}

function parseVmess(uri) {
  const j = b64json(uri.slice('vmess://'.length));
  if (!j || !j.add || !j.id) throw new Error('bad vmess payload');
  const q = new URLSearchParams();
  if (j.sni) q.set('sni', j.sni);
  if (j.host) q.set('host', j.host);
  if (j.path) q.set('path', j.path);
  if (j.alpn) q.set('alpn', j.alpn);
  if (j.fp) q.set('fp', j.fp);
  const net = j.net || 'tcp';
  const security = j.tls === 'tls' ? 'tls' : 'none';
  return newExit(uri, {
    protocol: 'vmess',
    name: j.ps || `${j.add}:${j.port}`,
    host: j.add,
    port: Number(j.port || 443),
    outbound: {
      protocol: 'vmess',
      settings: {
        vnext: [{
          address: j.add,
          port: Number(j.port || 443),
          users: [{ id: j.id, alterId: Number(j.aid || 0), security: j.scy || 'auto' }],
        }],
      },
      streamSettings: streamSettings({ net, security, q, host: j.add }),
    },
  });
}

function parseTrojan(uri) {
  const u = new URL(uri);
  const q = u.searchParams;
  const password = decodeURIComponent(u.username || '');
  if (!password) throw new Error('trojan without password');
  return newExit(uri, {
    protocol: 'trojan',
    name: decodeURIComponent(u.hash.replace(/^#/, '')) || `${u.hostname}:${u.port}`,
    host: u.hostname,
    port: Number(u.port || 443),
    outbound: {
      protocol: 'trojan',
      settings: { servers: [{ address: u.hostname, port: Number(u.port || 443), password }] },
      streamSettings: streamSettings({ net: q.get('type') || 'tcp', security: q.get('security') || 'tls', q, host: u.hostname }),
    },
  });
}

function parseShadowsocks(uri) {
  let s = uri.slice('ss://'.length);
  let name = '';
  const hash = s.indexOf('#');
  if (hash !== -1) { name = decodeURIComponent(s.slice(hash + 1)); s = s.slice(0, hash); }
  const qIdx = s.indexOf('?');
  if (qIdx !== -1) s = s.slice(0, qIdx);

  let method;
  let password;
  let hostport;
  const at = s.lastIndexOf('@');
  if (at !== -1) {
    const credPart = s.slice(0, at);
    const cred = credPart.includes(':') ? decodeURIComponent(credPart) : b64decode(credPart);
    [method, password] = splitFirst(cred, ':');
    hostport = s.slice(at + 1);
  } else {
    const dec = b64decode(s);
    const at2 = dec.lastIndexOf('@');
    [method, password] = splitFirst(dec.slice(0, at2), ':');
    hostport = dec.slice(at2 + 1);
  }
  const portIdx = hostport.lastIndexOf(':');
  const address = hostport.slice(0, portIdx);
  const port = Number(hostport.slice(portIdx + 1));
  if (!address || !Number.isFinite(port) || !method) throw new Error('bad ss payload');
  return newExit(uri, {
    protocol: 'shadowsocks',
    name: name || `${address}:${port}`,
    host: address,
    port,
    outbound: {
      protocol: 'shadowsocks',
      settings: { servers: [{ address, port, method, password }] },
      streamSettings: { network: 'tcp', security: 'none' },
    },
  });
}

function parseSubscriptionText(text) {
  let body = String(text || '').trim();
  if (!/(vless|vmess|trojan|ss):\/\//.test(body)) {
    const dec = b64decode(body.replace(/\s+/g, ''));
    if (/(vless|vmess|trojan|ss):\/\//.test(dec)) body = dec;
  }
  const lines = body.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const exits = [];
  const skipped = {};
  for (const line of lines) {
    const scheme = (/^([a-z0-9]+):\/\//i.exec(line) || [])[1];
    const key = (scheme || 'unknown').toLowerCase();
    try {
      if (key === 'vless') exits.push(parseVless(line));
      else if (key === 'vmess') exits.push(parseVmess(line));
      else if (key === 'trojan') exits.push(parseTrojan(line));
      else if (key === 'ss') exits.push(parseShadowsocks(line));
      else { skipped[key] = (skipped[key] || 0) + 1; }
    } catch {
      skipped[key] = (skipped[key] || 0) + 1;
    }
  }
  return { exits, skipped, lines: lines.length };
}

/** Xray config: one local HTTP inbound per exit, routed to its own outbound. */
function buildXrayConfig(exits, settings, { basePort } = {}) {
  const base = basePort || settings.inboundBasePort;
  const inbounds = [];
  const outbounds = [];
  const rules = [];
  exits.forEach((exit, i) => {
    const port = base + i;
    inbounds.push({
      tag: `in-${i}`,
      listen: '127.0.0.1',
      port,
      protocol: 'http',
      settings: { timeout: 0, allowTransparent: false },
    });
    outbounds.push({ tag: `out-${i}`, ...exit.outbound });
    rules.push({ type: 'field', inboundTag: [`in-${i}`], outboundTag: `out-${i}` });
  });
  outbounds.push({ tag: 'direct', protocol: 'freedom' });
  outbounds.push({ tag: 'block', protocol: 'blackhole' });
  return {
    log: { loglevel: 'warning' },
    inbounds,
    outbounds,
    routing: { domainStrategy: 'AsIs', rules },
  };
}

const waitPort = (port, timeoutMs = 10_000) => new Promise((resolve) => {
  const deadline = Date.now() + timeoutMs;
  const attempt = () => {
    const sock = net.connect({ host: '127.0.0.1', port });
    sock.once('connect', () => { sock.destroy(); resolve(true); });
    sock.once('error', () => {
      sock.destroy();
      if (Date.now() > deadline) resolve(false);
      else setTimeout(attempt, 150);
    });
  };
  attempt();
});

/**
 * Wait until every port stops answering, i.e. the previous throwaway xray has
 * really let go of its inbounds. SIGTERM plus a fixed sleep is not enough: the
 * next batch then binds into a socket the dying process still owns, xray logs
 * `failed to listen` and moves on, and every probe on that port ends as a
 * bogus ECONNREFUSED -- a working exit gets marked dead.
 */
const portFree = (port) => new Promise((resolve) => {
  const sock = net.connect({ host: '127.0.0.1', port });
  sock.once('connect', () => { sock.destroy(); resolve(false); });
  sock.once('error', () => { sock.destroy(); resolve(true); });
});

const waitPortsFree = async (ports, timeoutMs = 3000) => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const res = await Promise.all(ports.map(portFree));
    if (res.every(Boolean) || Date.now() > deadline) return;
    await new Promise((r) => setTimeout(r, 150));
  }
};

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------

class ProxyManager {
  constructor({ dataDir, log = () => {} } = {}) {
    this.dataDir = dataDir;
    this.log = log;
    this.file = path.join(dataDir, 'proxies.json');
    this.configPath = path.join(dataDir, 'xray-config.json');
    this.state = {
      version: 1,
      settings: { ...DEFAULT_SETTINGS },
      sources: DEFAULT_SOURCES.map((s, i) => ({ id: `s${i + 1}`, url: s.url, enabled: s.enabled, lastFetched: 0, count: 0, error: '' })),
      exits: [],
      active: [],
      // Lane pins: one exit per free-tier identity lane, per provider. Kept in
      // the persisted state so a restart does not collapse four identities onto
      // the same IP (which would hand them one shared quota).
      lanes: {},
      // Scheduled-upkeep bookkeeping (last run, what it found) survives restarts.
      auto: { lastRun: 0 },
      updatedAt: 0,
    };
    this.xray = { running: false, pid: 0, startedAt: 0, error: '', version: '', ports: [] };
    this.tester = { running: false, pid: 0 };
    this.test = { running: false, total: 0, done: 0, ok: 0, dead: 0, startedAt: 0, finishedAt: 0, batch: 0, batches: 0, error: '' };
    this.events = [];
    this.index = 0;
    // One cursor per provider: a limit hit on OpenCode must not move the exit
    // that OpenAI is using.
    this.cursors = new Map();
    this.byId = new Map();
    this._saveTimer = null;
    this._dirty = false;
  }

  // ---- persistence -------------------------------------------------------

  init() {
    try {
      const raw = JSON.parse(fs.readFileSync(this.file, 'utf8'));
      this.state.settings = { ...DEFAULT_SETTINGS, ...(raw.settings || {}) };
      if (Array.isArray(raw.sources) && raw.sources.length) this.state.sources = raw.sources;
      if (Array.isArray(raw.exits)) this.state.exits = raw.exits;
      if (Array.isArray(raw.active)) this.state.active = raw.active;
      if (raw.lanes && typeof raw.lanes === 'object') this.state.lanes = raw.lanes;
      if (raw.auto && typeof raw.auto === 'object') this.state.auto = raw.auto;
      this.state.updatedAt = raw.updatedAt || 0;
    } catch { /* first boot */ }
    // Anything measured by an older schema is untested, not 'ok'.
    for (const e of this.state.exits) {
      if ((e.status === 'ok' || e.status === 'leak') && e.testedBy !== TELEMETRY_SCHEMA) e.status = 'untested';
    }
    this.state.active = this.state.active.filter((id) => {
      const e = this.state.exits.find((x) => x.id === id);
      return e && e.status === 'ok' && e.testedBy === TELEMETRY_SCHEMA;
    });
    this.reindex();
  }

  reindex() {
    this.byId = new Map(this.state.exits.map((e) => [e.id, e]));
  }

  save() {
    this._dirty = true;
    if (this._saveTimer) return;
    this._saveTimer = setTimeout(() => {
      this._saveTimer = null;
      if (!this._dirty) return;
      this._dirty = false;
      this.state.updatedAt = Date.now();
      const tmp = `${this.file}.${process.pid}.tmp`;
      fsp.writeFile(tmp, JSON.stringify(this.state), { mode: 0o600 })
        .then(() => fsp.rename(tmp, this.file))
        .catch((err) => this.log(`[proxies] save failed: ${err.message}`));
    }, 500);
  }

  pushEvent(ev) {
    this.events.unshift({ t: Date.now(), ...ev });
    if (this.events.length > EVENT_LIMIT) this.events.length = EVENT_LIMIT;
  }

  // ---- settings / sources ------------------------------------------------

  updateSettings(patch = {}) {
    const s = this.state.settings;
    if (patch.xrayPath != null) s.xrayPath = String(patch.xrayPath);
    if (patch.enabled != null) s.enabled = !!patch.enabled;
    for (const key of ['maxActive', 'testBatchSize', 'testConcurrency', 'testTimeoutMs', 'rotateAttempts', 'markDeadAfter', 'inboundBasePort', 'testBasePort']) {
      if (patch[key] != null && Number(patch[key]) > 0) s[key] = Number(patch[key]);
    }
    if (patch.testUrl) s.testUrl = String(patch.testUrl);
    if (Array.isArray(patch.rotateOnStatuses) && patch.rotateOnStatuses.length) s.rotateOnStatuses = patch.rotateOnStatuses.map(Number);
    if (patch.insecure != null) s.insecure = !!patch.insecure;
    if (patch.autoRefresh != null) s.autoRefresh = !!patch.autoRefresh;
    if (patch.refreshIntervalHours != null) s.refreshIntervalHours = Math.max(0, Number(patch.refreshIntervalHours) || 0);
    this.save();
    return s;
  }

  updateSources(list) {
    if (!Array.isArray(list)) return this.state.sources;
    const merged = [];
    list.forEach((raw, i) => {
      const url = String((raw && raw.url) || '').trim();
      if (!url) return;
      const existing = this.state.sources.find((s) => s.url === url);
      merged.push({
        id: (raw && raw.id) || (existing && existing.id) || `s${i + 1}`,
        url,
        enabled: raw && raw.enabled === false ? false : true,
        lastFetched: (existing && existing.lastFetched) || 0,
        count: (existing && existing.count) || 0,
        error: '',
      });
    });
    if (merged.length) this.state.sources = merged;
    this.save();
    return this.state.sources;
  }

  async refresh({ sourceIds } = {}) {
    const wanted = this.state.sources.filter((s) => s.enabled && (!sourceIds || sourceIds.includes(s.id)));
    const summary = { sources: wanted.length, fetched: 0, parsed: 0, skipped: {}, added: 0, updated: 0, errors: [] };
    const seen = new Set();
    for (const source of wanted) {
      try {
        const res = await fetch(source.url, { headers: { 'user-agent': 'llm-router' }, signal: AbortSignal.timeout(30_000) });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const text = await res.text();
        const parsed = parseSubscriptionText(text);
        source.lastFetched = Date.now();
        source.count = parsed.exits.length;
        source.error = '';
        summary.fetched += 1;
        summary.parsed += parsed.exits.length;
        for (const [k, v] of Object.entries(parsed.skipped)) summary.skipped[k] = (summary.skipped[k] || 0) + v;
        for (const exit of parsed.exits) {
          if (seen.has(exit.id)) continue;
          seen.add(exit.id);
          const existing = this.byId.get(exit.id);
          if (existing) {
            existing.name = exit.name;
            existing.uri = exit.uri;
            existing.outbound = exit.outbound;
            existing.sourceId = source.id;
            summary.updated += 1;
          } else {
            exit.sourceId = source.id;
            this.state.exits.push(exit);
            summary.added += 1;
          }
          this.byId.set(exit.id, existing || exit);
        }
      } catch (err) {
        source.error = err.message;
        summary.errors.push(`${source.id}: ${err.message}`);
      }
    }
    this.save();
    this.pushEvent({ kind: 'refresh', ok: summary.fetched, added: summary.added, parsed: summary.parsed });
    return summary;
  }

  // ---- xray process ------------------------------------------------------

  async version() {
    if (this.xray.version) return this.xray.version;
    const bin = this.state.settings.xrayPath;
    const out = await new Promise((resolve) => {
      let child;
      try { child = spawn(bin, ['-version'], { stdio: ['ignore', 'pipe', 'pipe'] }); }
      catch (err) { return resolve({ error: err.message }); }
      let text = '';
      child.stdout.on('data', (d) => { text += d.toString(); });
      child.stderr.on('data', (d) => { text += d.toString(); });
      child.once('error', (err) => resolve({ error: err.message }));
      child.once('close', () => resolve({ text }));
      setTimeout(() => { try { child.kill(); } catch { /* noop */ } resolve({ error: 'xray -version timed out' }); }, 5000).unref?.();
    });
    if (out.error) return { error: out.error };
    const m = /(\d+\.\d+\.\d+)/.exec(out.text || '');
    this.xray.version = m ? m[1] : 'unknown';
    return { version: this.xray.version };
  }

  async startProcess(exits, settings, { basePort, tag }) {
    const config = buildXrayConfig(exits, settings, { basePort });
    const file = tag === 'test' ? path.join(this.dataDir, 'xray-test-config.json') : this.configPath;
    await fsp.writeFile(file, JSON.stringify(config, null, 2), { mode: 0o600 });
    const bin = settings.xrayPath;
    const child = spawn(bin, ['run', '-config', file], { stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    let exited = false;
    let resolveDied;
    const died = new Promise((resolve) => { resolveDied = resolve; });
    // A missing or non-executable binary emits 'error' and never 'exit', so both
    // paths must settle the race -- otherwise every batch waits out the full
    // port timeout for a process that was never started.
    const markDead = () => { exited = true; resolveDied(null); };
    child.once('error', (err) => { stderr += `spawn error: ${err.message}\n`; markDead(); });
    child.stderr.on('data', (d) => { stderr += d.toString(); if (stderr.length > 8000) stderr = stderr.slice(-8000); });
    child.once('exit', (code) => {
      const target = tag === 'test' ? this.tester : this.xray;
      target.running = false;
      target.pid = 0;
      if (code !== 0 && code !== null) {
        target.error = (stderr.split('\n').filter(Boolean).pop() || `xray exited with code ${code}`).slice(0, 400);
        this.log(`[xray:${tag}] ${target.error}`);
        this.pushEvent({ kind: 'xray-exit', tag, code });
      }
      markDead();
    });
    const ports = exits.map((_, i) => (basePort || settings.inboundBasePort) + i);
    const ready = await Promise.race([Promise.all(ports.map((p) => waitPort(p, 8000))), died]);
    const up = Array.isArray(ready) && ready.some(Boolean) && !exited;
    return { child, ports, up, stderr, exited, done: died };
  }

  primePort(port) {
    return new Promise((resolve) => {
      const sock = net.connect({ host: '127.0.0.1', port });
      sock.once('connect', () => { sock.destroy(); resolve(true); });
      sock.once('error', () => { sock.destroy(); resolve(false); });
    });
  }

  /**
   * Kill xray processes that a previous run left behind on throwaway configs.
   *
   * Every test batch writes its own config into a temp directory and runs its own
   * xray on the test ports. If such a process outlives the run that spawned it, it
   * keeps holding ports - and because Xray sets SO_REUSEPORT, the *next* main
   * instance binds the same numbers "successfully" and then dies with exit 255 as
   * soon as the two disagree about who owns them. That is the "xray down" loop:
   * the pool looks empty, restarts do nothing, and the real cause is a stray
   * listener from yesterday. Sweeping at startup costs one directory read.
   */
  sweepStrays() {
    if (process.platform !== 'linux') return 0;
    let killed = 0;
    let entries = [];
    try { entries = fs.readdirSync('/proc'); } catch { return 0; }
    const liveTest = this.tester && this.tester.child ? this.tester.child.pid : 0;
    for (const entry of entries) {
      if (!/^\d+$/.test(entry)) continue;
      if (Number(entry) === process.pid) continue;
      if (liveTest && Number(entry) === liveTest) continue;
      try {
        const comm = fs.readFileSync(`/proc/${entry}/comm`, 'utf8').trim();
        if (comm !== 'xray') continue;
        const cmd = fs.readFileSync(`/proc/${entry}/cmdline`, 'utf8').replace(/\0/g, ' ');
        // The test instance writes `xray-test-config.json` in the data dir, not a
        // temp dir, so the `(burn|sched)` pattern never matched it. A test xray
        // that outlived its run held 30801.. forever; every later batch then died
        // on `bind: address already in use` (exit 23), and because a failed start
        // splits the batch down to single exits, the whole run condemned good
        // exits as dead - 591 of 1647 on 2026-10-24. Match it here too.
        if (!/llm-router-(burn|sched)-|xray-test-config\.json/.test(cmd)) continue;
        process.kill(Number(entry), 'SIGTERM');
        killed += 1;
      } catch { /* already gone, or not ours to signal */ }
    }
    if (killed) {
      this.log(`[xray] killed ${killed} stray test instance(s) holding the pool ports`);
      this.pushEvent({ kind: 'xray-sweep', killed });
    }
    return killed;
  }

  async startMain() {
    await this.stopMain();
    const exits = this.activeExits();
    if (!this.state.settings.enabled) { this.xray.error = 'proxies disabled'; return { running: false, reason: 'disabled' }; }
    if (!exits.length) { this.xray.error = 'no working exits yet - run a test'; return { running: false, reason: 'no-exits' }; }
    const { version, error } = await this.version();
    if (error) { this.xray.error = `xray binary not usable (${error})`; this.pushEvent({ kind: 'xray-missing', error }); return { running: false, reason: 'no-binary', error }; }
    this.sweepStrays();
    const started = await this.startProcess(exits, this.state.settings, { basePort: this.state.settings.inboundBasePort, tag: 'main' });
    this.xray.running = started.up;
    this.xray.pid = started.child.pid || 0;
    this.xray.startedAt = Date.now();
    this.xray.ports = started.ports;
    this.xray.child = started.child;
    this.xray.error = started.up ? '' : (started.stderr.split('\n').filter(Boolean).pop() || 'xray did not open its inbounds').slice(0, 400);
    this.pushEvent({ kind: 'xray-start', exits: exits.length, ok: started.up, version });
    this.log(`[xray] started with ${exits.length} exits on ports ${started.ports[0] || '-'}..${started.ports[started.ports.length - 1] || '-'} (v${version})`);
    return { running: this.xray.running, exits: exits.length, ports: started.ports, error: this.xray.error };
  }

  async stopMain() {
    const child = this.xray.child;
    this.xray.child = null;
    this.xray.running = false;
    if (child) {
      try { child.kill('SIGTERM'); } catch { /* noop */ }
      await new Promise((r) => setTimeout(r, 150));
    }
  }

  activeExits() {
    const ids = new Set(this.state.active);
    const picked = this.state.exits.filter((e) => ids.has(e.id) && e.status === 'ok'
      && e.testedBy === TELEMETRY_SCHEMA && e.exitIp && e.exitIp !== this.localIp);
    if (picked.length) return picked;
    // fall back to the best working exits if `active` was never set
    return this.workingExits().slice(0, this.state.settings.maxActive);
  }

  workingExits() {
    return this.state.exits
      .filter((e) => e.status === 'ok' && e.testedBy === TELEMETRY_SCHEMA && e.exitIp && e.exitIp !== this.localIp)
      .sort((a, b) => (a.latencyMs || 1e9) - (b.latencyMs || 1e9));
  }

  portForExit(exit) {
    const active = this.activeExits();
    const i = active.findIndex((e) => e.id === exit.id);
    if (i === -1) return 0;
    return this.state.settings.inboundBasePort + i;
  }

  // ---- testing -----------------------------------------------------------

  testStatus() {
    return {
      ...this.test,
      xray: { running: this.xray.running, pid: this.xray.pid, version: this.xray.version, error: this.xray.error, ports: this.xray.ports.length },
    };
  }

  async testAll({ limit = 0, protocols = null, retest = false, sourceIds = null, ids = null } = {}) {
    if (this.test.running) return { error: 'a test run is already in progress' };

    // A test xray left behind by an earlier run still owns the test ports; every
    // batch of this run would then die on `address already in use`. Clear them
    // before the first spawn (the guard above keeps a live run safe).
    this.sweepStrays();

    // Pre-flight: without a usable binary every batch would burn its port
    // timeout and report the exits dead for the wrong reason.
    const bin = await this.version();
    if (bin.error) {
      const msg = `xray is not usable (${bin.error}) - install it (install.sh --with-xray) or set the path in the Network tab`;
      this.test.error = msg;
      this.pushEvent({ kind: 'test-blocked', error: msg.slice(0, 160) });
      return { error: msg };
    }

    const settings = this.state.settings;
    let candidates = this.state.exits.filter((e) => (!sourceIds || sourceIds.includes(e.sourceId)));
    if (protocols && protocols.length) candidates = candidates.filter((e) => protocols.includes(e.protocol));
    if (ids && ids.length) {
      // An explicit list means the caller wants these retested whatever their
      // current status is (a refill retests the exits that are still in the pool).
      const want = new Set(ids);
      candidates = candidates.filter((e) => want.has(e.id));
    } else if (!retest) {
      candidates = candidates.filter((e) => e.status === 'untested');
    }
    if (limit > 0) candidates = candidates.slice(0, limit);

    if (!candidates.length) {
      candidates = this.workingExits().slice(0, limit > 0 ? limit : 20);
      if (!candidates.length) return { error: 'nothing to test - refresh the subscriptions first' };
    }

    Object.assign(this.test, {
      running: true, total: candidates.length, done: 0, ok: 0, dead: 0,
      startedAt: Date.now(), finishedAt: 0, batch: 0,
      batches: Math.ceil(candidates.length / settings.testBatchSize), error: '',
    });
    this.pushEvent({ kind: 'test-start', total: candidates.length });

    this.runTestLoop(candidates).catch((err) => {
      this.test.running = false;
      this.test.error = err.message;
      this.pushEvent({ kind: 'test-error', error: err.message });
    });
    return { started: true, total: candidates.length, batches: this.test.batches };
  }

  async runTestLoop(candidates) {
    const settings = this.state.settings;
    // Baseline: our own egress IP, so an exit that does not actually route is
    // detected instead of being recorded as working.
    if (!this.localIp) {
      try {
        const res = await fetch(settings.testUrl, { headers: { 'user-agent': 'llm-router' }, signal: AbortSignal.timeout(8000) });
        const text = await res.text();
        try { this.localIp = JSON.parse(text).ip || ''; } catch { this.localIp = text.trim().slice(0, 64); }
        this.log(`[proxy] local egress IP baseline: ${this.localIp || 'unknown'}`);
      } catch { this.localIp = ''; }
    }
    const size = Math.max(1, settings.testBatchSize);
    for (let start = 0; start < candidates.length; start += size) {
      const batch = candidates.slice(start, start + size);
      this.test.batch += 1;
      await this.testBatch(batch, settings);
    }
    this.test.running = false;
    this.test.finishedAt = Date.now();
    this.save();
    this.pushEvent({ kind: 'test-done', ok: this.test.ok, dead: this.test.dead });
    await this.apply();
  }

  /**
   * Test one batch. Xray validates the whole file before it opens a single
   * inbound, so one unusable exit (`vless without TLS ... is prohibited`,
   * `unknown transport protocol: xhttp`, a bad REALITY key) rejects all 40.
   * Halve the batch until the offender stands alone instead of condemning it.
   */
  async testBatch(batch, settings) {
    if (!batch.length) return;
    const { child, up, stderr, done } = await this.startProcess(batch, settings, { basePort: settings.testBasePort, tag: 'test' });
    this.tester.child = child;
    const ports = batch.map((_, i) => settings.testBasePort + i);
    const release = async () => {
      try { child.kill('SIGTERM'); } catch { /* noop */ }
      const exited = await Promise.race([done.then(() => true), delay(2500).then(() => false)]);
      if (!exited) {
        try { child.kill('SIGKILL'); } catch { /* noop */ }
        await Promise.race([done, delay(1000)]);
      }
      this.tester.child = null;
      await waitPortsFree(ports, 2000);
    };
    if (!up) {
      await release();
      const reason = `test instance failed to start: ${(stderr || '').split('\n').filter(Boolean).pop() || 'unknown'}`;
      if (batch.length > 1) {
        const mid = Math.ceil(batch.length / 2);
        this.pushEvent({ kind: 'test-split', size: batch.length, error: reason.slice(0, 160) });
        await this.testBatch(batch.slice(0, mid), settings);
        await this.testBatch(batch.slice(mid), settings);
        return;
      }
      this.test.error = reason;
      this.recordTest(batch[0], false, 0, '', reason);
      this.test.done += 1;
      return;
    }
    const queue = ports.map((port, i) => ({ exit: batch[i], port }));
    const workers = Array.from({ length: Math.min(Math.max(1, settings.testConcurrency), queue.length) }, async () => {
      for (;;) {
        const job = queue.shift();
        if (!job) return;
        await this.testOne(job.exit, job.port);
        this.test.done += 1;
      }
    });
    // A batch must not be able to stop the run: every probe has its own deadline,
    // but a stuck one used to hold `Promise.all` open forever and left the pool at
    // zero active with the run "in progress". Cap the whole batch.
    const batchDeadline = Math.ceil(queue.length / Math.max(1, settings.testConcurrency)) * settings.testTimeoutMs * 3 + 10_000;
    const finished = await Promise.race([Promise.all(workers).then(() => true), delay(batchDeadline).then(() => false)]);
    if (!finished) {
      this.log(`[pool] batch of ${queue.length} candidate(s) did not settle in ${Math.round(batchDeadline / 1000)}s - moving on`);
      this.pushEvent({ kind: 'test-stall', stuck: queue.length });
      this.test.done += queue.length;
      queue.length = 0;
    }
    await release();
  }

  async testOne(exit, port) {
    const settings = this.state.settings;
    const proxy = { host: '127.0.0.1', port };
    // Hard cap on the probe itself. The tunnel has its own deadlines, but a
    // cancelled read or a half-open socket can still leave the promise pending;
    // this guarantees the worker moves on.
    const cap = settings.testTimeoutMs * 3 + 2_000;
    const verdict = await Promise.race([
      this.probeExit(exit, proxy, settings),
      delay(cap).then(() => ({ ok: false, reason: `probe hung past ${Math.round(cap / 1000)}s` })),
    ]);
    if (verdict.ok) this.recordTest(exit, true, verdict.ms, verdict.ip, '');
    else this.recordTest(exit, false, 0, '', verdict.reason);
  }

  async probeExit(exit, proxy, settings) {
    let last = null;
    // A refused inbound is ambiguous: the exit may be fine and the local listener
    // merely late. One short retry before condemning it.
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const res = await proxiedGet(settings.testUrl, proxy, {
          timeoutMs: settings.testTimeoutMs,
          insecure: settings.insecure,
        });
        if (!res.ok || res.status === 0) throw new Error(`HTTP ${res.status}`);
        let ip = '';
        try { const j = JSON.parse(res.text); ip = j.ip || j.origin || j.query || ''; } catch { ip = (res.text || '').trim().slice(0, 64); }
        return { ok: true, ms: res.ms, ip };
      } catch (err) {
        last = err;
        if (attempt === 0 && /ECONNREFUSED|ECONNRESET|EPIPE/.test(err.message)) await delay(600);
        else break;
      }
    }
    return { ok: false, reason: last.message };
  }

  recordTest(exit, ok, latencyMs, exitIp, error) {
    const target = this.byId.get(exit.id) || exit;
    target.lastTested = Date.now();
    target.testedBy = TELEMETRY_SCHEMA;
    // Egress on our own public IP means the request never used the exit
    // (routing miss, or a node that forwards without NAT). That is a leak:
    // never 'ok', and never admitted to the pool.
    const leak = ok && this.localIp && exitIp && exitIp === this.localIp;
    if (leak) {
      target.status = 'leak';
      target.exitIp = '';
      target.latencyMs = 0;
      target.error = `egress equals this host's public IP (${this.localIp}) - the exit was not used`;
      target.failures = 0;
      this.state.active = this.state.active.filter((id) => id !== target.id);
      this.pushEvent({ kind: 'exit-leak', exit: target.id, ip: this.localIp });
      this.log(`[proxy] exit ${target.id} egresses as this host (${this.localIp}) - marked leak, kept out of the pool`);
      this.save();
      return;
    }
    if (ok) {
      target.status = 'ok';
      target.latencyMs = latencyMs;
      target.exitIp = exitIp;
      target.failures = 0;
      target.error = '';
      this.test.ok += 1;
    } else {
      target.failures = (target.failures || 0) + 1;
      target.error = (error || '').slice(0, 200);
      // An exit a lane is holding right now may be failing the probe only because
      // it is busy carrying the request this test runs beside: the probe competes
      // with it for the same node. Such an exit keeps its status, its egress IP and
      // its slot in the pool, so a refill triggered by a request cannot knock the
      // pool over while the answer is still in flight.
      const pinned = Object.values(this.state.lanes || {})
        .some((pins) => Object.values(pins || {}).includes(target.id));
      if (pinned) {
        target.retryAfter = 0;
        this.test.dead += 1;
        this.save();
        return;
      }
      target.exitIp = '';
      // A failed test always clears `ok` from the record: the entry has no egress
      // IP behind it any more, so calling it good would only put a dead address in
      // front of `workingExits()` (which requires an IP) and leave the pool reading
      // "eleven working, zero usable". The failure count still decides how quickly
      // it drops out of the retry queue, not what the record claims.
      const wasActive = this.state.active.includes(target.id);
      if (wasActive) this.state.active = this.state.active.filter((id) => id !== target.id);
      target.status = 'dead';
      this.test.dead += 1;
      if (target.failures < this.state.settings.markDeadAfter) {
        // Below the threshold it is worth another look sooner than the dead pile.
        target.retryAfter = Date.now() + 10 * 60_000;
      } else {
        target.retryAfter = 0;
      }
    }
    this.save();
  }

  // ---- pool / rotation ---------------------------------------------------

  /**
   * Point xray at a pool.
   *
   * The exits already in the pool keep their slots: inbounds are numbered by
   * position in `active`, so reordering a healthy entry moves its port and breaks
   * the socket a request is using at that very moment. Whatever is in the pool and
   * still good stays at the front in the order it already has; the candidates only
   * fill the space behind it. A refill therefore tops the pool up instead of
   * reshuffling it under the caller.
   */
  async apply({ preferFresh = false } = {}) {
    const maxActive = this.state.settings.maxActive;
    const usable = (e) => e && e.status === 'ok' && e.testedBy === TELEMETRY_SCHEMA && e.exitIp && e.exitIp !== this.localIp;
    const held = this.state.active.filter((id) => usable(this.byId.get(id)));
    const heldSet = new Set(held);
    const ranked = (preferFresh ? this.freshFirst() : this.workingExits()).map((e) => e.id).filter((id) => !heldSet.has(id));
    const best = [...held, ...ranked].slice(0, maxActive);
    this.state.active = best;
    this.index = 0;
    this.cursors.clear();
    this.save();
    this.pushEvent({ kind: 'apply', active: best.length });
    const moved = this.reconcileLanes();
    if (moved) this.log(`[pool] re-pinned ${moved} lane(s) onto their own exit after the refill`);
    return this.startMain();
  }

  /**
   * Give every identity lane its own live exit again.
   *
   * A lane only picks a new exit when a request rotates, and a lane rebuilt while
   * the pool was empty keeps whatever it had - nothing, or an address another lane
   * already holds. Four identities sharing one egress IP share one allowance, so
   * the pool's own health is not enough: the pins have to be repaired after every
   * refill, not on the next 429.
   */
  reconcileLanes() {
    const pool = this.activeExits();
    if (!pool.length) return 0;
    const aliveId = (id) => {
      const e = this.byId.get(id);
      return !!e && e.status === 'ok' && this.state.active.includes(id);
    };
    let moved = 0;
    for (const pid of Object.keys(this.state.lanes || {})) {
      const pins = this.state.lanes[pid] || {};
      const claimed = new Set();
      for (const laneKey of Object.keys(pins)) {
        const id = pins[laneKey];
        if (id && aliveId(id) && !claimed.has(id)) { claimed.add(id); continue; }
        const fresh = this.laneExit(pid, laneKey, pool);
        if (!fresh) continue;
        pins[laneKey] = fresh.id;
        claimed.add(fresh.id);
        moved += 1;
        this.pushEvent({ kind: 'lane-repin', pid, lane: laneKey, exit: fresh.id, ip: fresh.exitIp || '' });
      }
    }
    return moved;
  }

  /**
   * Working exits with the just-tested ones first. A refill must not hand the
   * pool back to stale entries that only look good because their latency was
   * measured hours ago; within each group latency still decides.
   */
  freshFirst(windowMs = 15 * 60_000) {
    const cutoff = Date.now() - windowMs;
    return this.workingExits().sort((a, b) => {
      const fresh = (e) => (Number(e.lastTested) > cutoff ? 1 : 0);
      return fresh(b) - fresh(a) || (a.latencyMs || 1e9) - (b.latencyMs || 1e9);
    });
  }

  // ---- scheduled upkeep: fetch -> test what is new -> fold into the pool -----

  /**
   * Epoch ms of the next scheduled cycle (0 = disabled). The clock is anchored
   * to the newest thing that actually happened — the last cycle, the last
   * subscription fetch, or the last exit test — so a deploy or a manual
   * refresh an hour ago does not buy the box another full interval. The
   * generic `updatedAt` is deliberately not used: the process saves state on
   * boot, and every save would push the next run out.
   */
  nextAutoRunAt() {
    const s = this.state.settings;
    if (!s.autoRefresh) return 0;
    const hours = Number(s.refreshIntervalHours);
    if (!(hours > 0)) return 0;
    let last = Number((this.state.auto || {}).lastRun) || 0;
    for (const src of this.state.sources) last = Math.max(last, Number(src.lastFetched) || 0);
    for (const exit of this.state.exits) last = Math.max(last, Number(exit.lastTested) || 0);
    return (last || Date.now()) + hours * 3600_000;
  }

  /** Sources that a cycle would actually fetch from. */
  enabledSources() {
    return this.state.sources.filter((s) => s.enabled);
  }

  isAutoRunDue(nowMs = Date.now()) {
    const next = this.nextAutoRunAt();
    return !!next && nowMs >= next;
  }

  /**
   * One scheduled cycle: re-fetch every enabled subscription, test only the
   * exits that have never been tested (so the pool keeps growing as the
   * subscription publishes new configs), then fold the working ones into the
   * active pool. Refuses to run while a manual test is in flight.
   */
  async runAutoRefresh(reason = 'scheduled') {
    if (this.test.running) return { skipped: 'a test run is already in progress' };
    const sources = this.enabledSources();
    if (!sources.length) {
      this.state.auto = { ...(this.state.auto || {}), lastRun: Date.now(), lastReason: reason, lastSkipped: 'no enabled subscription source' };
      this.save();
      return { skipped: 'no enabled subscription source' };
    }
    const started = Date.now();
    const untestedBefore = this.state.exits.filter((e) => e.status === 'untested').length;
    this.log(`[pool] auto refresh (${reason}): fetching ${sources.length} source(s)`);
    const refresh = await this.refresh({});
    const untested = this.state.exits.filter((e) => e.status === 'untested').length;
    let applied = null;
    if (untested > 0) {
      this.log(`[pool] auto refresh: testing ${untested} new exit(s)`);
      const startedTest = await this.testAll({ retest: false });
      if (startedTest && startedTest.started) {
        // testAll schedules the loop and returns; wait for it so the pool is
        // applied only once the results are in. The loop applies by itself.
        const deadline = Date.now() + 2 * 3600_000;
        while (this.test.running && Date.now() < deadline) await new Promise((r) => setTimeout(r, 2000));
        applied = { running: !!this.xray.running, exits: this.state.active.length, reason: 'test-loop' };
      } else {
        if (startedTest && startedTest.error) this.log(`[pool] auto refresh: test refused (${startedTest.error})`);
        applied = await this.apply();
      }
    } else {
      this.log('[pool] auto refresh: no untested exits, nothing to test');
      applied = await this.apply();
    }
    this.state.auto = {
      lastRun: Date.now(),
      lastDurationMs: Date.now() - started,
      lastReason: reason,
      lastRefresh: { parsed: refresh.parsed, added: refresh.added, updated: refresh.updated, errors: (refresh.errors || []).length },
      lastTest: { total: this.test.total, ok: this.test.ok, dead: this.test.dead, error: this.test.error || '', newCandidates: untestedBefore },
      lastApplied: { running: !!applied.running, exits: (applied.exits != null ? applied.exits : this.state.active.length), reason: applied.reason || '' },
    };
    this.save();
    this.pushEvent({ kind: 'auto-refresh', parsed: refresh.parsed, added: refresh.added, ok: this.test.ok, dead: this.test.dead });
    this.log(`[pool] auto refresh done in ${Math.round((Date.now() - started) / 1000)}s: +${refresh.added} new, ${this.test.ok} working, ${this.state.active.length} active, next in ${this.state.settings.refreshIntervalHours}h`);
    return { ok: true, ...this.state.auto };
  }

  async maybeAutoRefresh() {
    if (!this.isAutoRunDue()) return false;
    await this.runAutoRefresh('scheduled');
    return true;
  }

  /** Cheap check every 5 minutes; real work only when a cycle is due. */
  startScheduler({ checkMs = 5 * 60_000 } = {}) {
    if (this._sched) return;
    if (!this.state.auto) this.state.auto = { lastRun: 0 };
    this._sched = setInterval(() => {
      this.maybeAutoRefresh().catch((err) => this.log(`[pool] auto refresh failed: ${err.message}`));
    }, checkMs);
    if (this._sched.unref) this._sched.unref();
    const s = this.state.settings;
    if (!(s.autoRefresh && Number(s.refreshIntervalHours) > 0)) {
      this.log('[pool] auto refresh disabled');
      return;
    }
    if (!this.enabledSources().length) {
      this.log('[pool] auto refresh armed, but no subscription source is enabled');
      return;
    }
    const next = this.nextAutoRunAt();
    const mins = Math.max(0, Math.round((next - Date.now()) / 60_000));
    this.log(`[pool] auto refresh every ${s.refreshIntervalHours}h - next in ${mins}m (${new Date(next).toISOString()})`);
  }

  stopScheduler() {
    if (this._sched) { clearInterval(this._sched); this._sched = null; }
  }

  /** Choose the local proxy for a provider, honouring direct / rotate / pinned. */
  pick(provider) {
    const mode = (provider && provider.proxy) || 'direct';
    if (mode === 'direct' || !this.state.settings.enabled) return null;
    const pool = this.activeExits();
    if (!pool.length) return null;
    if (mode === 'rotate') {
      const key = (provider && provider.id) || 'default';
      const at = this.cursors.get(key) || 0;
      const exit = pool[at % pool.length];
      return { mode, exit, index: at % pool.length, proxy: { host: '127.0.0.1', port: this.portForExit(exit) } };
    }
    const exit = this.byId.get(mode);
    if (!exit || exit.status !== 'ok') return null;
    return { mode: 'fixed', exit, proxy: { host: '127.0.0.1', port: this.portForExit(exit) } };
  }

  /**
   * Sticky exit per lane. A lane is one self-managed free-tier identity: it must
   * keep its own egress IP, because the quota is accounted per source IP and four
   * identities behind one address share one allowance. Two lanes therefore never
   * get the same exit while the pool has room, and a lane keeps its pin across
   * requests and restarts.
   */
  laneExit(pid, laneKey, pool, { avoid = '' } = {}) {
    const pins = this.state.lanes[pid] || (this.state.lanes[pid] = {});
    const mine = pins[laneKey] || '';
    const aliveId = (id) => {
      const e = this.byId.get(id);
      return !!e && e.status === 'ok' && this.state.active.includes(id);
    };
    // Only a live pin held by another lane blocks a candidate. A lane whose exit
    // burned is not holding anything any more, and treating its stale pin as taken
    // is what left two lanes sharing one address while a third had none.
    const takenBy = (id) => Object.entries(pins)
      .some(([k, v]) => k !== laneKey && v === id && aliveId(v));
    const alive = aliveId;
    if (!avoid && mine && alive(mine) && !takenBy(mine)) return this.byId.get(mine);
    const candidates = pool.filter((e) => e.id !== mine && e.id !== avoid && !takenBy(e.id));
    const free = candidates.length ? candidates : pool.filter((e) => e.id !== mine);
    if (!free.length) return this.byId.get(mine) || pool[0];
    // Prefer an exit whose egress IP no other lane already holds.
    const held = new Set(Object.entries(pins).filter(([k]) => k !== laneKey).map(([, v]) => (this.byId.get(v) || {}).exitIp));
    const distinct = free.find((e) => !e.exitIp || !held.has(e.exitIp));
    const at = this.cursors.get(`lane:${pid}`) || 0;
    const exit = distinct || free[at % free.length];
    this.cursors.set(`lane:${pid}`, (at + 1) % Math.max(1, free.length));
    pins[laneKey] = exit.id;
    this.save();
    return exit;
  }

  /** Sticky (or newly pinned) proxy for a lane. */
  pickLane(provider, laneKey) {
    if (!this.state.settings.enabled) return null;
    const pid = (provider && provider.id) || 'default';
    const pool = this.activeExits();
    if (!pool.length) return null;
    const exit = this.laneExit(pid, laneKey, pool);
    if (!exit) return null;
    return { mode: 'rotate', exit, proxy: { host: '127.0.0.1', port: this.portForExit(exit) } };
  }

  /** Move a lane to a different exit and keep the pin there. */
  rotateLane(provider, laneKey, reason = 'rotate') {
    if (!this.state.settings.enabled) return null;
    const pid = (provider && provider.id) || 'default';
    const pool = this.activeExits();
    if (!pool.length) return null;
    const pins = this.state.lanes[pid] || (this.state.lanes[pid] = {});
    const exit = this.laneExit(pid, laneKey, pool, { avoid: pins[laneKey] || '' });
    if (!exit) return null;
    pins[laneKey] = exit.id;
    this.save();
    this.pushEvent({ kind: 'rotate', reason, exit: exit.id, host: exit.host, ip: exit.exitIp || '', provider: pid, lane: laneKey });
    this.log(`[proxy] ${pid}#${laneKey} switched to exit=${exit.id} host=${exit.host}${exit.exitIp ? ` egress=${exit.exitIp}` : ' egress=unknown'} - ${reason}`);
    return exit;
  }

  releaseLane(providerId, laneKey) {
    const pid = providerId || 'default';
    const pins = this.state.lanes[pid];
    if (!pins || !(laneKey in pins)) return;
    delete pins[laneKey];
    this.save();
  }

  /** Exits currently pinned by any lane, for the admin page. */
  laneSummary(providerId) {
    const pins = this.state.lanes[providerId || 'default'] || {};
    return Object.entries(pins).map(([lane, id]) => ({ lane, exit: id }));
  }

  rotate(reason = 'manual', providerId = null) {
    const pool = this.activeExits();
    if (!pool.length) return null;
    if (providerId) {
      const key = providerId || 'default';
      const at = ((this.cursors.get(key) || 0) + 1) % pool.length;
      this.cursors.set(key, at);
      const picked = pool[at];
      this.pushEvent({ kind: 'rotate', reason, exit: picked.id, host: picked.host, ip: picked.exitIp || '', leak: !!(this.localIp && picked.exitIp === this.localIp), provider: providerId });
      this.log(`[proxy] ${providerId} switched to exit=${picked.id} host=${picked.host}${picked.exitIp ? ` egress=${picked.exitIp}` : ' egress=unknown'} - ${reason}`);
      return picked;
    }
    this.index = (this.index + 1) % pool.length;
    const exit = pool[this.index];
    this.pushEvent({ kind: 'rotate', reason, exit: exit.id, host: exit.host, ip: exit.exitIp || '', leak: !!(this.localIp && exit.exitIp === this.localIp) });
    this.log(`[proxy] rotated to exit=${exit.id} host=${exit.host}${exit.exitIp ? ` egress=${exit.exitIp}` : ' egress=unknown'}${this.localIp && exit.exitIp === this.localIp ? ' [LEAK]' : ''} - ${reason}`);
    return exit;
  }

  /**
   * Record the outcome of a real request through an exit.
   *
   * `burn` marks the exit spent rather than merely unlucky: the caller knows the
   * exit answered a quota limit (or produced no headers at all), which is the
   * exit's fault and not something a retry on the same exit can fix. A burned
   * exit leaves the pool at once regardless of `markDeadAfter`, and shows up as
   * burned in the UI so the pile is visible before it is rebuilt.
   */
  noteResult(exitId, ok, { burn = false } = {}) {
    const exit = this.byId.get(exitId);
    if (!exit) return;
    if (ok) { exit.failures = 0; return; }
    exit.failures = (exit.failures || 0) + 1;
    const spent = burn || exit.failures >= this.state.settings.markDeadAfter;
    if (spent) {
      exit.status = burn ? 'burned' : 'dead';
      exit.burnedAt = burn ? Date.now() : 0;
      exit.error = burn ? 'spent on a rate limit - leaves the pool until the next test' : exit.error;
      this.state.active = this.state.active.filter((id) => id !== exit.id);
      this.pushEvent({ kind: burn ? 'exit-burned' : 'exit-dead', exit: exit.id });
      this.log(`[proxy] exit ${exit.id} ${burn ? 'burned (spent on a rate limit)' : 'marked dead'} - ${this.state.active.length} left in the pool`);
    }
    this.save();
    // Keep the pool at strength without waiting for a request to run dry: an
    // exit leaving takes a spare's place, and if there is no spare the pool is
    // rebuilt in the background so the next request does not have to wait.
    const filled = this.promote();
    if (this.needsRebuild()) {
      setImmediate(() => { this.replenish(`${this.state.active.length} exits left in the pool`).catch(() => {}); });
    }
    void filled;
  }

  /**
   * Top the active list back up to `maxActive` from exits that already tested
   * good. Cheap (no network) and it is what keeps "15 in the pool" true between
   * burns instead of letting a spare sit idle while the pool shrinks.
   */
  promote() {
    const maxActive = Math.max(1, Number(this.state.settings.maxActive) || 1);
    const active = new Set(this.state.active);
    if (active.size >= maxActive) return 0;
    let added = 0;
    for (const exit of this.workingExits()) {
      if (active.size >= maxActive) break;
      if (active.has(exit.id)) continue;
      active.add(exit.id);
      this.state.active.push(exit.id);
      added += 1;
    }
    if (!added) return 0;
    // Ports are assigned by position in `active`, so the inbound set has to be
    // rebuilt for the new exits to have listeners. Existing exits keep their
    // index (append-only), so their ports do not move; `settle()` keeps callers
    // from dialling a port while that restart is in flight.
    this.save();
    this.log(`[pool] promoted ${added} spare exit(s) into the pool - ${this.state.active.length} active`);
    this.pushEvent({ kind: 'promote', added, active: this.state.active.length });
    this._restarting = this.startMain()
      .catch(() => {})
      .finally(() => { this._restarting = null; });
    return added;
  }

  /** True while xray is being re-pointed at a changed pool. */
  get restarting() { return !!this._restarting; }

  /** Wait (bounded) for a pool restart so a hop never dials a dead port. */
  async settle(ms = 4000) {
    if (!this._restarting) return;
    await Promise.race([this._restarting, new Promise((r) => setTimeout(r, ms))]);
  }

  /** True when the pool has burned/dead through most of its depth. */
  needsRebuild() {
    if (this._replenishing) return false;
    const maxActive = Math.max(1, Number(this.state.settings.maxActive) || 1);
    const floor = Math.max(2, Math.ceil(maxActive / 2));
    if (this.state.active.length > floor) return false;
    return this.state.exits.some((e) => e.status === 'untested') || this.state.exits.some((e) => e.status === 'burned');
  }

  /**
   * Rebuild the pool after the burn pile has eaten it.
   *
   * The spent exits are deleted outright (the subscriptions re-publish the same
   * URIs on the next fetch, so nothing is lost), the exits that are still in the
   * pool are retested together with a fresh batch, and the working results are
   * applied - fresh ones first. The point is a pool of fifteen *new* addresses,
   * not fifteen entries that were good two hours ago, so the current active set
   * is retested as part of the same run.
   *
   * `maxActive` replacements are wanted, so `maxActive * 3` candidates are tried
   * (roughly a third of shared subscription exits survive a test). A rebuild
   * triggered from a request tests a smaller batch so it can finish inside the
   * caller's budget; the background path takes the full batch.
   */
  async replenish(reason = 'pool empty', { maxCandidates = 0 } = {}) {
    if (this._replenishing) return { skipped: 'a replenish is already running' };
    this._replenishing = true;
    try {
      this.pushEvent({ kind: 'replenish', reason });
      this.log(`[pool] replenishing the exit pool (${reason})`);
      const spent = this.state.exits.filter((e) => e.status === 'burned');
      if (spent.length) {
        const gone = new Set(spent.map((e) => e.id));
        this.state.exits = this.state.exits.filter((e) => !gone.has(e.id));
        for (const id of gone) this.byId.delete(id);
        this.state.active = this.state.active.filter((id) => !gone.has(id));
        this.log(`[pool] dropped ${spent.length} spent exit(s) out of the pool`);
        this.save();
      }
      const maxActive = Math.max(1, Number(this.state.settings.maxActive) || 1);
      const wanted = Math.max(maxActive, maxCandidates || maxActive * 3);
      // Retest what is still active (it may have aged) plus fresh candidates.
      const retest = this.state.active.slice(0, maxActive);
      let untested = this.state.exits.filter((e) => e.status === 'untested').length;
      if (untested + retest.length < wanted) {
        this.log(`[pool] only ${untested} untested exit(s) on hand - re-fetching the subscriptions first`);
        await this.refresh({});
        untested = this.state.exits.filter((e) => e.status === 'untested').length;
      }
      const fresh = this.state.exits.filter((e) => e.status === 'untested').slice(0, Math.max(0, wanted - retest.length)).map((e) => e.id);
      // Dead is not a life sentence: most of that pile failed once on a timeout
      // that says nothing about the node, and their URIs are already known. With
      // room left in the batch, retest the ones that failed least.
      const room = Math.max(0, wanted - retest.length - fresh.length);
      const revisit = room > 0
        ? this.state.exits
          .filter((e) => e.status === 'dead')
          .sort((a, b) => ((a.failures || 0) - (b.failures || 0))
            || ((b.lastTested || 0) - (a.lastTested || 0)))
          .slice(0, Math.min(room, maxActive))
          .map((e) => e.id)
        : [];
      const ids = [...retest, ...fresh, ...revisit];
      if (!ids.length) {
        this.log('[pool] nothing left to test - the pool stays as it is');
        return { ok: false, reason: 'no untested exits left' };
      }
      this.log(`[pool] testing ${ids.length} candidate(s): ${retest.length} current + ${fresh.length} new + ${revisit.length} retry`);
      const started = await this.testAll({ ids });
      // Wait for whichever run is actually doing the work - ours, or one that was
      // already in flight (testAll refuses a second run) - and then apply.
      if ((started && started.started) || (started && started.error && this.test.running)) {
        const deadline = Date.now() + 2 * 3600_000;
        while (this.test.running && Date.now() < deadline) await new Promise((r) => setTimeout(r, 1000));
      }
      const applied = await this.apply({ preferFresh: true });
      const working = this.state.active.length;
      this.pushEvent({ kind: 'replenished', active: working, ok: this.test.ok });
      this.log(`[pool] replenished: ${this.test.ok} working found, ${working} active${working < maxActive ? ' (below target - the next cycle tops it up)' : ''}`);
      return { ok: true, tested: started ? started.total : 0, working: this.test.ok, active: working, running: !!applied.running };
    } finally {
      this._replenishing = false;
    }
  }


  snapshot({ includeUri = false } = {}) {
    const counts = { total: this.state.exits.length, ok: 0, dead: 0, untested: 0, burned: 0, byProtocol: {} };
    for (const e of this.state.exits) {
      if (e.status === 'ok') counts.ok += 1;
      else if (e.status === 'dead') counts.dead += 1;
      else if (e.status === 'burned') counts.burned += 1;
      else if (e.status === 'leak') counts.leak = (counts.leak || 0) + 1;
      else counts.untested += 1;
      counts.byProtocol[e.protocol] = (counts.byProtocol[e.protocol] || 0) + 1;
    }
    const activeIds = new Set(this.state.active);
    return {
      settings: { ...this.state.settings, xrayPath: undefined },
      xrayPath: this.state.settings.xrayPath,
      sources: this.state.sources,
      counts,
      active: this.state.active.length,
      index: this.index,
      cursors: Object.fromEntries(this.cursors),
      xray: { running: this.xray.running, pid: this.xray.pid, version: this.xray.version, error: this.xray.error, ports: this.xray.ports.length },
      test: this.testStatus(),
      auto: {
        enabled: !!this.state.settings.autoRefresh && Number(this.state.settings.refreshIntervalHours) > 0,
        intervalHours: Number(this.state.settings.refreshIntervalHours) || 0,
        lastRun: (this.state.auto && this.state.auto.lastRun) || 0,
        nextRun: this.nextAutoRunAt(),
        summary: (this.state.auto && this.state.auto.lastRefresh) || null,
      },
      events: this.events.slice(0, 20),
      updatedAt: this.state.updatedAt,
      exits: this.state.exits.slice(0, 400).map((e) => ({
        id: e.id,
        name: e.name,
        protocol: e.protocol,
        host: e.host,
        port: e.port,
        status: e.status,
        latencyMs: e.latencyMs,
        exitIp: e.exitIp,
        lastTested: e.lastTested,
        failures: e.failures || 0,
        error: e.error || '',
        sourceId: e.sourceId,
        active: activeIds.has(e.id),
        uri: includeUri ? e.uri : undefined,
      })),
      truncated: this.state.exits.length > 400,
    };
  }
}

module.exports = {
  ProxyManager,
  TELEMETRY_SCHEMA,
  parseSubscriptionText,
  buildXrayConfig,
  DEFAULT_SOURCES,
  DEFAULT_SETTINGS,
};
