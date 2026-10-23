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
  maxActive: 24,
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
    return { child, ports, up, stderr, exited };
  }

  primePort(port) {
    return new Promise((resolve) => {
      const sock = net.connect({ host: '127.0.0.1', port });
      sock.once('connect', () => { sock.destroy(); resolve(true); });
      sock.once('error', () => { sock.destroy(); resolve(false); });
    });
  }

  async startMain() {
    await this.stopMain();
    const exits = this.activeExits();
    if (!this.state.settings.enabled) { this.xray.error = 'proxies disabled'; return { running: false, reason: 'disabled' }; }
    if (!exits.length) { this.xray.error = 'no working exits yet - run a test'; return { running: false, reason: 'no-exits' }; }
    const { version, error } = await this.version();
    if (error) { this.xray.error = `xray binary not usable (${error})`; this.pushEvent({ kind: 'xray-missing', error }); return { running: false, reason: 'no-binary', error }; }
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

  async testAll({ limit = 0, protocols = null, retest = false, sourceIds = null } = {}) {
    if (this.test.running) return { error: 'a test run is already in progress' };

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
    if (!retest) candidates = candidates.filter((e) => e.status === 'untested');
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
    const { child, up, stderr } = await this.startProcess(batch, settings, { basePort: settings.testBasePort, tag: 'test' });
    this.tester.child = child;
    if (!up) {
      try { child.kill(); } catch { /* noop */ }
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
    const queue = batch.map((exit, i) => ({ exit, port: settings.testBasePort + i }));
    const workers = Array.from({ length: Math.min(Math.max(1, settings.testConcurrency), queue.length) }, async () => {
      for (;;) {
        const job = queue.shift();
        if (!job) return;
        await this.testOne(job.exit, job.port);
        this.test.done += 1;
      }
    });
    await Promise.all(workers);
    try { child.kill('SIGTERM'); } catch { /* noop */ }
    await new Promise((r) => setTimeout(r, 200));
  }

  async testOne(exit, port) {
    const settings = this.state.settings;
    const proxy = { host: '127.0.0.1', port };
    try {
      const res = await proxiedGet(settings.testUrl, proxy, {
        timeoutMs: settings.testTimeoutMs,
        insecure: settings.insecure,
      });
      if (!res.ok || res.status === 0) throw new Error(`HTTP ${res.status}`);
      let ip = '';
      try { const j = JSON.parse(res.text); ip = j.ip || j.origin || j.query || ''; } catch { ip = (res.text || '').trim().slice(0, 64); }
      this.recordTest(exit, true, res.ms, ip, '');
    } catch (err) {
      this.recordTest(exit, false, 0, '', err.message);
    }
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
      target.exitIp = '';
      if (target.failures >= this.state.settings.markDeadAfter) target.status = 'dead';
      this.test.dead += 1;
    }
    this.save();
  }

  // ---- pool / rotation ---------------------------------------------------

  async apply() {
    const best = this.workingExits().slice(0, this.state.settings.maxActive);
    this.state.active = best.map((e) => e.id);
    this.index = 0;
    this.cursors.clear();
    this.save();
    this.pushEvent({ kind: 'apply', active: best.length });
    return this.startMain();
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

  noteResult(exitId, ok) {
    const exit = this.byId.get(exitId);
    if (!exit) return;
    if (ok) { exit.failures = 0; return; }
    exit.failures = (exit.failures || 0) + 1;
    if (exit.failures >= this.state.settings.markDeadAfter) {
      exit.status = 'dead';
      this.state.active = this.state.active.filter((id) => id !== exit.id);
      this.pushEvent({ kind: 'exit-dead', exit: exit.id });
      this.log(`[proxy] exit ${exit.id} marked dead after ${exit.failures} failures`);
    }
    this.save();
  }

  snapshot({ includeUri = false } = {}) {
    const counts = { total: this.state.exits.length, ok: 0, dead: 0, untested: 0, byProtocol: {} };
    for (const e of this.state.exits) {
      if (e.status === 'ok') counts.ok += 1;
      else if (e.status === 'dead') counts.dead += 1;
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
