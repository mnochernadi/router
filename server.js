#!/usr/bin/env node
'use strict';

/**
 * ============================================================================
 *  LLM API Router  --  zero-dependency, multi-account, streaming proxy
 * ============================================================================
 *
 *  Design constraints (intentional, do not "improve" them away):
 *
 *   - Only native modules: node:http, global fetch, node:stream.
 *   - Upstream response bodies are NEVER buffered: they are piped with
 *     Readable.fromWeb(upstream.body).pipe(clientRes). The only exception is a
 *     non-streaming request that needs *format translation* (anthropic/gemini),
 *     which is read through a hard byte cap and released immediately.
 *   - Prompt/response bodies are never written to state.json, never kept in a
 *     module-level variable, never logged. Persistent memory holds counters only.
 *   - configuration lives in config.json (secrets), runtime counters and
 *     cooldown deadlines live in state.json. Both are written atomically.
 *
 *  Endpoints:
 *    POST /v1/chat/completions        unified OpenAI-compatible entry point
 *    POST /v1/messages                anthropic-shaped entry point
 *    ANY  /v1/*                       passthrough to an OpenAI-compatible provider
 *    GET  /health                     liveness + rss
 *    GET  /admin                      client-side dashboard (static, 0 RAM)
 *    GET  /admin/api/state            everything the dashboard renders
 *    ...  /admin/api/*                provider/account CRUD, reset, import/export
 *
 *  Licence: MIT
 * ============================================================================
 */

const http = require('node:http');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { Readable, Transform } = require('node:stream');
const { ProxyManager } = require('./proxy-manager');
const { proxiedRequest } = require('./proxy-client');
const { FreebuffDriver } = require('./freebuff-driver');

// ---------------------------------------------------------------------------
// Paths and static defaults
// ---------------------------------------------------------------------------

const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, 'public');
const CONFIG_PATH = process.env.ROUTER_CONFIG || path.join(ROOT, 'config.json');
const CONFIG_EXAMPLE_PATH = path.join(ROOT, 'config.example.json');
const STATE_PATH = process.env.ROUTER_STATE || path.join(ROOT, 'state.json');
const PKG_VERSION = (() => {
  try { return JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).version || '0.0.0'; }
  catch { return '0.0.0'; }
})();

const PROVIDER_TYPES = {
  openai: { label: 'OpenAI', baseUrl: 'https://api.openai.com/v1', style: 'openai' },
  openrouter: {
    label: 'OpenRouter',
    baseUrl: 'https://openrouter.ai/api/v1',
    style: 'openai',
    headers: { 'HTTP-Referer': 'https://github.com/mnochernadi/router', 'X-Title': 'LLM Router' },
  },
  anthropic: { label: 'Anthropic', baseUrl: 'https://api.anthropic.com/v1', style: 'anthropic' },
  gemini: { label: 'Google Gemini', baseUrl: 'https://generativelanguage.googleapis.com/v1beta', style: 'gemini' },
  // OpenCode (opencode.ai): one account fronts four wire formats. The router
  // picks the right sub-endpoint per model id -- see openCodeKind().
  opencode: { label: 'OpenCode (Console)', baseUrl: 'https://opencode.ai/inference', style: 'opencode' },
  // Free tier lives on /inference/v1 (v2). It needs the attested client
  // identity: Bearer public, the v2 UA, x-opencode-session and the built-in
  // tool quartet -- see the adapter below, ported from 9Router's opencode.js.
  'opencode-free': {
    label: 'OpenCode Free (no key)',
    baseUrl: 'https://opencode.ai/inference/v1',
    style: 'opencode-free',
    keyless: true,
    // Accounts here are identities, not credentials: the router creates,
    // replaces and deletes them itself. Manual add is refused.
    manageAccounts: true,
    defaultTimeoutMs: 600_000, // big contexts are slow before the first byte
  },
  // freebuff / Codebuff: OpenAI-shaped but signed, session-bound and geo-gated.
  // The driver owns the whole handshake, so this style always wants a proxy exit.
  freebuff: { label: 'freebuff (Codebuff)', baseUrl: 'https://www.codebuff.com', style: 'freebuff', keyOptional: true },
  custom: { label: 'OpenAI-compatible', baseUrl: '', style: 'openai' },
};

// Model families that OpenCode serves over a non-OpenAI wire format.
// Source: opencode.ai/v2/docs/console/models endpoint table.
const OPENCODE_ANTHROPIC_RE = [
  /^claude-/, /^qwen3\.5-plus$/, /^qwen3\.6-plus$/, /^qwen3\.7-plus$/, /^qwen3\.7-max$/, /^qwen3\.8-flash$/,
];
const OPENCODE_RESPONSES_RE = [/^gpt-/, /^grok-/, /^muse-/];
const KEYS_MEANINGLESS = /^(none|no-?auth|free|not-?needed|dummy|placeholder|public)$/i;

function usableKey(apiKey) {
  const k = String(apiKey ?? '').trim();
  return !k || KEYS_MEANINGLESS.test(k) ? '' : k;
}

function normalizeKind(kind) {
  const k = String(kind || '').toLowerCase();
  if (k === 'google' || k === 'gemini') return 'gemini';
  if (k === 'anthropic' || k === 'claude') return 'anthropic';
  if (k === 'responses' || k === 'openai-responses') return 'responses';
  return 'openai';
}

/** Which upstream wire format serves this model on OpenCode. */
function openCodeKind(provider, model) {
  const m = String(model || '').trim();
  const overrides = provider && provider.modelStyles;
  if (overrides && typeof overrides === 'object') {
    if (overrides[m]) return normalizeKind(overrides[m]);
    for (const [pattern, kind] of Object.entries(overrides)) {
      if (pattern.includes('*') && new RegExp(`^${pattern.split('*').map((s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('.*')}$`).test(m)) {
        return normalizeKind(kind);
      }
    }
  }
  if (/^gemini-/.test(m)) return 'gemini';
  if (OPENCODE_ANTHROPIC_RE.some((re) => re.test(m))) return 'anthropic';
  if (OPENCODE_RESPONSES_RE.some((re) => re.test(m))) return 'responses';
  return 'openai';
}

const DEFAULT_COOLDOWN_STATUSES = [429, 402, 403];
const DEFAULT_RETRY_STATUSES = [500, 502, 503, 504];
const DEFAULT_COOLDOWN_HOURS = 12;
const MAX_BODY_BYTES = 32 * 1024 * 1024;        // request bodies in flight
const MAX_TRANSLATE_BYTES = 8 * 1024 * 1024;    // buffered only for non-stream translation
const EVENT_LIMIT = 60;                          // ring buffer of numeric event summaries
const MEM_SAMPLES = 30;
const MEM_SAMPLE_MS = 10_000;
const SWEEP_MS = 30_000;

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

const now = () => Date.now();
const uid = (p = 'id') => `${p}_${crypto.randomBytes(6).toString('hex')}`;
const clampHours = (v, fb = DEFAULT_COOLDOWN_HOURS) => {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 && n <= 24 * 30 ? n : fb;
};

function maskKey(key) {
  const k = String(key || '');
  if (k.length <= 10) return k ? '••••••' : '';
  return `${k.slice(0, 4)}••••${k.slice(-4)}`;
}

function timingSafeEqual(a, b) {
  const ba = Buffer.from(String(a ?? ''));
  const bb = Buffer.from(String(b ?? ''));
  if (ba.length !== bb.length) return false;
  try { return crypto.timingSafeEqual(ba, bb); } catch { return false; }
}

function sendJson(res, status, obj) {
  const payload = JSON.stringify(obj);
  // Proxy answers are CORS-open (clients call them from browsers); the admin
  // API is not, unless the operator opts in -- it takes the x-admin-key header.
  const cors = res.__admin ? !!config.adminCors : !!config.cors;
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
    ...(cors ? { 'access-control-allow-origin': '*' } : {}),
  });
  res.end(payload);
}

function readBody(req, limit = MAX_BODY_BYTES) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(Object.assign(new Error('request body too large'), { statusCode: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

async function readJsonBody(req, limit) {
  const buf = await readBody(req, limit);
  if (!buf.length) return {};
  try { return JSON.parse(buf.toString('utf8')); }
  catch { throw Object.assign(new Error('invalid JSON body'), { statusCode: 400 }); }
}

let tmpSeq = 0;
async function renameWithRetry(from, to, attempts = 6) {
  for (let i = 1; ; i += 1) {
    try { return await fsp.rename(from, to); }
    catch (err) {
      // Windows: an indexer/AV can hold the target briefly (EPERM/EBUSY/EACCES).
      if (i >= attempts || !['EPERM', 'EBUSY', 'EACCES', 'ENOENT'].includes(err.code)) throw err;
      await new Promise((r) => setTimeout(r, 25 * i));
    }
  }
}

function atomicWrite(file, data) {
  // Unique temp name per write: a debounced state flush and a shutdown flush can
  // otherwise target the same path and the second rename fails with ENOENT.
  const tmp = `${file}.${process.pid}.${(tmpSeq = (tmpSeq + 1) % 1e6).toString(36)}.tmp`;
  return fsp.writeFile(tmp, data).then(() => renameWithRetry(tmp, file));
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

function defaultConfig() {
  return {
    adminKey: 'changeme',
    host: '0.0.0.0',
    port: 8787,
    cooldownHours: DEFAULT_COOLDOWN_HOURS,
    cooldownStatuses: DEFAULT_COOLDOWN_STATUSES.slice(),
    retryStatuses: DEFAULT_RETRY_STATUSES.slice(),
    defaultProvider: '',
    cors: true,
    proxyKeys: [],
    logging: { requests: false },
    respectRetryAfter: true,
    adminCors: false,
    errorCooldownMinutes: 5,
    providers: [
      { id: 'openai-main', name: 'OpenAI', type: 'openai', strategy: 'round-robin', accounts: [] },
      { id: 'openrouter-main', name: 'OpenRouter', type: 'openrouter', strategy: 'round-robin', accounts: [] },
      { id: 'anthropic-main', name: 'Anthropic', type: 'anthropic', strategy: 'sequential', accounts: [] },
      { id: 'gemini-main', name: 'Google Gemini', type: 'gemini', strategy: 'round-robin', accounts: [] },
    ],
  };
}

function normalizeAccount(a, fallbackName = 'account') {
  const out = {
    id: String(a.id || uid('acc')),
    name: String(a.name || a.label || fallbackName),
    apiKey: String(a.apiKey ?? a.key ?? a.token ?? ''),
    enabled: a.enabled !== false,
  };
  if (a.note) out.note = String(a.note);
  if (Number(a.rotateEveryMinutes) > 0) out.rotateEveryMinutes = Number(a.rotateEveryMinutes);
  if (a.keyless) out.keyless = true;
  return out;
}

function normalizeProvider(p) {
  const type = PROVIDER_TYPES[p.type] ? p.type : 'custom';
  const def = PROVIDER_TYPES[type];
  // Session-quota providers are never routed through the exit pool.
  const forceDirect = def.style === 'opencode-free';
  const accounts = Array.isArray(p.accounts) ? p.accounts.map((a, i) => normalizeAccount(a, `account-${i + 1}`)) : [];
  return {
    id: String(p.id || uid('prov')),
    name: String(p.name || def.label),
    type,
    baseUrl: String(p.baseUrl || def.baseUrl || ''),
    strategy: p.strategy === 'sequential' ? 'sequential' : 'round-robin',
    cooldownHours: clampHours(p.cooldownHours),
    timeoutMs: Number(p.timeoutMs) > 0 ? Number(p.timeoutMs) : (Number(def.defaultTimeoutMs) > 0 ? Number(def.defaultTimeoutMs) : 30_000),
    maxAttempts: Number(p.maxAttempts) > 0 ? Math.min(Number(p.maxAttempts), 32) : 0,
    headers: p.headers && typeof p.headers === 'object' ? { ...p.headers } : undefined,
    modelStyles: p.modelStyles && typeof p.modelStyles === 'object' ? { ...p.modelStyles } : undefined,
    proxy: forceDirect ? 'direct' : String(p.proxy || 'direct'),
    // Per-model fallback inside the free tier (see OPENCODE_FREE_FALLBACK_MODELS).
    // Default on for opencode-free; `freeFallback: false` turns it off, and
    // `freeFallbackModels` overrides the order.
    freeFallback: p.freeFallback === false ? false : undefined,
    freeFallbackModels: Array.isArray(p.freeFallbackModels) && p.freeFallbackModels.length
      ? p.freeFallbackModels.map(String).filter(Boolean)
      : undefined,
    // Cross-provider last resort: [{ provider: 'pollinations', model: 'openai' }]
    fallbackProviders: Array.isArray(p.fallbackProviders) && p.fallbackProviders.length ? p.fallbackProviders : undefined,
    // Time-boxed rotation: keep one account for N minutes, then switch. 0 = off.
    rotateEveryMinutes: Number(p.rotateEveryMinutes) > 0 ? Number(p.rotateEveryMinutes) : 0,
    accounts,
  };
}

function normalizeConfig(cfg) {
  const base = defaultConfig();
  const out = { ...base, ...(cfg && typeof cfg === 'object' ? cfg : {}) };
  out.port = Number(process.env.PORT || out.port || 8787);
  out.host = process.env.HOST || out.host || '0.0.0.0';
  out.cooldownHours = clampHours(out.cooldownHours);
  out.cooldownStatuses = Array.isArray(out.cooldownStatuses) && out.cooldownStatuses.length
    ? out.cooldownStatuses.map(Number).filter(Number.isFinite)
    : DEFAULT_COOLDOWN_STATUSES.slice();
  out.retryStatuses = Array.isArray(out.retryStatuses) && out.retryStatuses.length
    ? out.retryStatuses.map(Number).filter(Number.isFinite)
    : DEFAULT_RETRY_STATUSES.slice();
  out.proxyKeys = Array.isArray(out.proxyKeys) ? out.proxyKeys.map(String) : [];
  out.logging = { requests: !!(out.logging && out.logging.requests) };
  out.adminCors = out.adminCors === true;
  out.respectRetryAfter = out.respectRetryAfter !== false;
  out.errorCooldownMinutes = Number(out.errorCooldownMinutes) > 0 ? Number(out.errorCooldownMinutes) : 5;
  out.providers = Array.isArray(out.providers) ? out.providers.map(normalizeProvider) : [];
  if (process.env.ADMIN_KEY) out.adminKey = process.env.ADMIN_KEY;
  return out;
}

let config = defaultConfig();

// V2Ray/Xray exit pool + freebuff session bookkeeping. Both are optional: with
// no xray binary and no freebuff accounts the router behaves exactly as before.
let proxies = null;
let freebuff = null;
const DATA_DIR = path.dirname(STATE_PATH);

// Fingerprint of what is actually running: a "Frankenstein" checkout (files
// from different commits) is otherwise invisible until something breaks.
const buildInfo = (() => {
  const hash = (f) => {
    try { return crypto.createHash('sha1').update(fs.readFileSync(path.join(ROOT, f))).digest('hex').slice(0, 10); }
    catch { return 'missing'; }
  };
  let commit = 'unknown';
  try {
    const head = fs.readFileSync(path.join(ROOT, '.git', 'HEAD'), 'utf8').trim();
    commit = head.startsWith('ref:')
      ? fs.readFileSync(path.join(ROOT, '.git', head.slice(4).trim()), 'utf8').trim().slice(0, 12)
      : head.slice(0, 12);
  } catch { /* not a checkout */ }
  return {
    commit,
    files: { server: hash('server.js'), dashboard: hash('public/index.html'), proxies: hash('proxy-manager.js'), freebuff: hash('freebuff-driver.js') },
    startedAt: Date.now(),
  };
})();

function reqLog(...args) {
  if (config.logging && config.logging.requests) console.log(...args);
}

function loadConfigSync() {
  for (const file of [CONFIG_PATH, CONFIG_EXAMPLE_PATH]) {
    try {
      const raw = fs.readFileSync(file, 'utf8');
      config = normalizeConfig(JSON.parse(raw));
      return { file, created: false };
    } catch (err) {
      if (err.code === 'ENOENT') continue;
      console.error(`[config] ${file} unreadable/invalid: ${err.message}`);
    }
  }
  config = normalizeConfig(defaultConfig());
  return { file: null, created: true };
}

async function saveConfig() {
  const serializable = { ...config };
  serializable.providers = config.providers.map((p) => ({
    ...p,
    accounts: (p.accounts || []).map((a) => ({ id: a.id, name: a.name, apiKey: a.apiKey, enabled: a.enabled !== false })),
  }));
  await atomicWrite(CONFIG_PATH, `${JSON.stringify(serializable, null, 2)}\n`);
  try { await fsp.chmod(CONFIG_PATH, 0o600); } catch { /* best effort (windows) */ }
}

function findProvider(id) {
  return config.providers.find((p) => p.id === id) || null;
}

// ---------------------------------------------------------------------------
// Runtime state (counters only -- never prompt/response content)
// ---------------------------------------------------------------------------

let state = {
  version: 1,
  startedAt: now(),
  totals: { requests: 0, success: 0, failed: 0, streamed: 0, failovers: 0, limitEvents: 0, unlocks: 0, bytesOut: 0, renewals: 0, reinstalls: 0, modelFallbacks: 0, providerFallbacks: 0 },
  providers: {},
  events: [],
  mem: { rss: 0, heapUsed: 0, samples: [] },
};

function loadStateSync() {
  try {
    const raw = JSON.parse(fs.readFileSync(STATE_PATH, 'utf8'));
    state = {
      ...state,
      ...raw,
      startedAt: now(),
      totals: { ...state.totals, ...(raw.totals || {}) },
      providers: raw.providers && typeof raw.providers === 'object' ? raw.providers : {},
      events: Array.isArray(raw.events) ? raw.events.slice(0, EVENT_LIMIT) : [],
      mem: { rss: 0, heapUsed: 0, samples: [] },
    };
  } catch { /* first boot: keep defaults */ }
}

let persistTimer = null;
let persistDirty = false;
let lastStateWrite = Promise.resolve();
function persistState() {
  persistDirty = true;
  if (persistTimer) return;
  persistTimer = setTimeout(() => {
    persistTimer = null;
    if (!persistDirty) return;
    persistDirty = false;
    lastStateWrite = atomicWrite(STATE_PATH, `${JSON.stringify(state)}\n`).catch((err) => {
      console.error(`[state] persist failed: ${err.message}`);
    });
  }, 400);
  if (persistTimer.unref) persistTimer.unref();
}

function providerState(pid) {
  let ps = state.providers[pid];
  if (!ps) ps = state.providers[pid] = { rrIndex: 0, accounts: {} };
  if (!ps.accounts) ps.accounts = {};
  return ps;
}

function accountState(pid, aid) {
  const ps = providerState(pid);
  let as = ps.accounts[aid];
  if (!as) as = ps.accounts[aid] = { requests: 0, failures: 0, limits: 0, limitedUntil: 0, lastStatus: 0, lastUsed: 0 };
  return as;
}

function pushEvent(ev) {
  state.events.unshift({ t: now(), ...ev });
  if (state.events.length > EVENT_LIMIT) state.events.length = EVENT_LIMIT;
}

function dropProviderState(pid) { delete state.providers[pid]; }

const isLimited = (as) => Number(as.limitedUntil) > now();

function cooldownMs(provider) {
  return Math.round(clampHours(provider?.cooldownHours ?? config.cooldownHours) * 3600 * 1000);
}

const QUOTA_HINT = /quota|credit|rate.?limit|too many|exhaust|usage|spend|balance|insufficient|limit reached|out of/i;
// A client-side policy gate (OpenCode's FreeTierError, a country block, a
// suspended account) reproduces on every exit: rotating cannot fix it and a
// long cooldown only hides the real message.
const POLICY_HINT = /free.?tier|policy|not allowed|country|region|geo|banned|suspended|unavailable in|download it at|client.?required/i;

function upstreamHint(status, message) {
  const m = String(message || '');
  if (/missing api key|autherror/i.test(m)) {
    return ' � this model is not in OpenCode\'s free tier. Add an OpenCode Console key (provider type "opencode") to use it; its *-free models still cost nothing.';
  }
  if (/maximum context length/i.test(m)) {
    return ' � the free tier window is smaller than 1M tokens (measured: ~900KB of context passes, 2MB is refused). Compact the conversation or use a Console key for the 1M-context models.';
  }
  return '';
}

function classifyFailure(status, message) {
  const msg = String(message || '');
  if (POLICY_HINT.test(msg)) return 'policy';
  if (QUOTA_HINT.test(msg)) return 'quota';
  if (status === 429 || status === 402) return 'quota';
  return 'error';
}

/** Parse retry-after / retry-after-ms; returns ms or 0. */
function retryAfterMs(headers) {
  try {
    const ms = Number(headers.get('retry-after-ms'));
    if (Number.isFinite(ms) && ms > 0) return Math.round(ms);
    const raw = headers.get('retry-after');
    if (!raw) return 0;
    const secs = Number(raw);
    if (Number.isFinite(secs) && secs >= 0) return Math.round(secs * 1000);
    const at = Date.parse(raw);
    if (Number.isFinite(at)) return Math.max(0, at - Date.now());
  } catch { /* noop */ }
  return 0;
}

/**
 * Read at most `maxBytes` of an error body so the reason can be reported, then
 * release the stream. Nothing is retained beyond a short message.
 */
async function peekError(upstream, maxBytes = 1024) {
  if (!upstream || !upstream.body || typeof upstream.body.getReader !== 'function') return '';
  try {
    const reader = upstream.body.getReader();
    let text = '';
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      text += Buffer.from(value).toString('utf8');
      if (text.length >= maxBytes) break;
    }
    try { await reader.cancel(); } catch { /* noop */ }
    const m = /"message"\s*:\s*"((?:[^"\\]|\\.)*)"|"error"\s*:\s*"((?:[^"\\]|\\.)*)"/.exec(text);
    const raw = (m && (m[1] || m[2])) || text;
    return String(raw).replace(/\\[nrt]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 180);
  } catch {
    try { await upstream.body.cancel(); } catch { /* noop */ }
    return '';
  }
}

/**
 * Cool an account down.
 *  - a provider-supplied retry-after shortens the wait (freebuff answers 60s,
 *    OpenCode often minutes) instead of burning the full cooldownHours;
 *  - a 402/403 whose body is not quota-related is not a rate limit at all
 *    (wrong model handle, mode required, suspended account), so it gets the
 *    short errorCooldownMinutes and keeps its real message for the dashboard.
 */
function markLimited(provider, account, status, { ms = 0, message = '' } = {}) {
  const as = accountState(provider.id, account.id);
  const configured = cooldownMs(provider);
  const freeTier = (PROVIDER_TYPES[provider.type] || {}).style === 'opencode-free';
  let wait = configured;
  if (freeTier) {
    // The free tier gets no timer. Its allowance is accounted per model, so the
    // router answers from the next free model immediately (and reinstalls the
    // client) instead of sitting out the upstream's ~16h retry-after - a window
    // that no rebuild clears anyway. `freeTierCooldownMinutes` (default 0, i.e.
    // retry at once) exists only to throttle upstream churn if that is ever
    // needed; it is deliberately not derived from the upstream retry-after.
    const minutes = config.freeTierCooldownMinutes === undefined || config.freeTierCooldownMinutes === null
      ? 0 : Math.max(0, Number(config.freeTierCooldownMinutes) || 0);
    wait = minutes * 60_000;
  } else if (config.respectRetryAfter !== false && ms > 0) {
    // Everywhere else a provider-supplied retry-after shortens the wait
    // (freebuff answers 60s, OpenCode often minutes) instead of burning the full
    // cooldownHours.
    wait = Math.min(configured, ms);
  }
  const kind = classifyFailure(status, message);
  const notQuota = kind !== 'quota';
  if (notQuota) wait = Math.min(wait, Math.max(1, Number(config.errorCooldownMinutes) || 5) * 60_000);
  as.limitedUntil = now() + Math.max(1_000, wait);
  as.lastStatus = status;
  as.lastMessage = message.slice(0, 180);
  as.lastLimitedKind = kind;
  as.limits = (as.limits || 0) + 1;
  state.totals.limitEvents += 1;
  state.totals.failovers += 1;
  pushEvent({ kind: 'limited', pid: provider.id, aid: account.id, st: status, mins: Math.round(Math.max(1_000, wait) / 60000), msg: as.lastMessage.slice(0, 80) });
  persistState();
  return as;
}

function markUnlocked(provider, account, mode) {
  const as = accountState(provider.id, account.id);
  as.limitedUntil = 0;
  as.lastStatus = 0;
  state.totals.unlocks += 1;
  pushEvent({ kind: 'unlocked', pid: provider.id, aid: account.id, mode });
  persistState();
}

// ---------------------------------------------------------------------------
// Account selection: round-robin + sequential, healthy accounts only
// ---------------------------------------------------------------------------

/**
 * Time-boxed rotation window for one account, in ms (0 = no window).
 * Providers such as OpenCode refresh their free quota on a clock, so swapping
 * the token every N minutes beats waiting for a 429 that costs a 12h cooldown.
 */
function windowMs(provider, account) {
  const mins = Number((account && account.rotateEveryMinutes) || (provider && provider.rotateEveryMinutes) || 0);
  return mins > 0 ? Math.round(mins * 60_000) : 0;
}

function windowInfo(provider, account) {
  const ms = windowMs(provider, account);
  if (!ms) return { ms: 0, startedAt: 0, expiresAt: 0, open: true };
  const as = accountState(provider.id, account.id);
  if (!as.activeSince) return { ms, startedAt: 0, expiresAt: 0, open: true };
  return { ms, startedAt: as.activeSince, expiresAt: as.activeSince + ms, open: Date.now() - as.activeSince < ms };
}

function selectAccount(provider, exclude) {
  const ps = providerState(provider.id);
  const list = provider.accounts || [];
  const n = list.length;
  if (!n) return null;

  const eligible = (i) => {
    const a = list[i];
    if (!a || a.enabled === false) return false;
    if (exclude.has(a.id)) return false;
    return !isLimited(accountState(provider.id, a.id));
  };

  const healthy = [];
  for (let i = 0; i < n; i += 1) if (eligible(i)) healthy.push(i);
  if (!healthy.length) return null;

  // Windows: stay on the account that is currently inside its window, otherwise
  // move to one whose window is still open, and only when every window has
  // closed start the cycle again from the first account.
  let pool = healthy;
  const timeBoxed = healthy.some((i) => windowMs(provider, list[i]) > 0);
  if (timeBoxed) {
    const open = healthy.filter((i) => windowInfo(provider, list[i]).open);
    if (open.length) {
      const started = open.filter((i) => accountState(provider.id, list[i].id).activeSince);
      pool = started.length ? started : open;
    } else {
      for (const i of healthy) accountState(provider.id, list[i].id).activeSince = 0;
      pushEvent({ kind: 'window-reset', pid: provider.id });
      persistState();
      pool = healthy;
    }
    if (pool.length === 1) return list[pool[0]];
  }

  if (provider.strategy === 'sequential') return list[pool[0]];

  for (let k = 0; k < pool.length; k += 1) {
    const i = pool[(ps.rrIndex + k) % pool.length];
    ps.rrIndex = (pool.indexOf(i) + 1) % pool.length;
    return list[i];
  }
  return null;
}

/** Called when an account is actually handed a request. */
function openWindow(provider, account) {
  const ms = windowMs(provider, account);
  if (!ms) return 0;
  const as = accountState(provider.id, account.id);
  if (!as.activeSince) {
    as.activeSince = Date.now();
    pushEvent({ kind: 'window-open', pid: provider.id, aid: account.id, mins: Math.round(ms / 60000) });
  }
  return as.activeSince + ms;
}

function accountSummary(provider, account) {
  const as = accountState(provider.id, account.id);
  return {
    id: account.id,
    name: account.name,
    maskedKey: maskKey(account.apiKey),
    keyless: !usableKey(account.apiKey),
    enabled: account.enabled !== false,
    requests: as.requests || 0,
    failures: as.failures || 0,
    limits: as.limits || 0,
    limitedUntil: isLimited(as) ? as.limitedUntil : 0,
    lastMessage: as.lastMessage || '',
    rotateEveryMinutes: Number(account.rotateEveryMinutes) || 0,
    limitedKind: as.lastLimitedKind || '',
    windowStartedAt: as.activeSince || 0,
    windowExpiresAt: as.activeSince && windowMs(provider, account) ? as.activeSince + windowMs(provider, account) : 0,
    lastStatus: as.lastStatus || 0,
    lastUsed: as.lastUsed || 0,
  };
}

function providerSummary(provider) {
  const accounts = (provider.accounts || []).map((a) => accountSummary(provider, a));
  return {
    id: provider.id,
    name: provider.name,
    type: provider.type,
    baseUrl: provider.baseUrl,
    strategy: provider.strategy,
    cooldownHours: clampHours(provider.cooldownHours ?? config.cooldownHours),
    timeoutMs: provider.timeoutMs,
    maxAttempts: provider.maxAttempts || 0,
    hasCustomHeaders: !!provider.headers,
    hasModelStyles: !!provider.modelStyles,
    rotateEveryMinutes: Number(provider.rotateEveryMinutes) || 0,
    managed: isManagedProvider(provider),
    // The dashboard reads this to render the "rotating exits" pill and to seed
    // the edit form; without it an edit silently saved `proxy: 'direct'`.
    proxy: provider.proxy || 'direct',
    accounts,
    accountCount: accounts.length,
    activeCount: accounts.filter((a) => a.limitedUntil === 0 && a.enabled).length,
    limitedCount: accounts.filter((a) => a.limitedUntil > 0).length,
    requests: accounts.reduce((s, a) => s + a.requests, 0),
  };
}

// ---------------------------------------------------------------------------
// Provider detection + upstream request construction
// ---------------------------------------------------------------------------

function resolveProvider(body, req) {
  const hinted = req.headers['x-router-provider'];
  if (hinted) {
    const byId = findProvider(String(hinted));
    if (byId) return { provider: byId, model: body?.model };
    const byType = config.providers.find((p) => p.type === hinted);
    if (byType) return { provider: byType, model: body?.model };
  }
  const model = typeof body?.model === 'string' ? body.model : '';
  if (model.includes('/')) {
    const [prefix, ...rest] = model.split('/');
    const byId = findProvider(prefix);
    if (byId) return { provider: byId, model: rest.join('/') };
  }
  if (config.defaultProvider) {
    const byId = findProvider(config.defaultProvider);
    if (byId) return { provider: byId, model };
  }
  for (const p of config.providers) {
    const ps = providerState(p.id);
    const healthy = (p.accounts || []).some((a) => a.enabled !== false && !isLimited(accountState(p.id, a.id)));
    if (healthy && (p.accounts || []).length) return { provider: p, model };
    void ps;
  }
  return { provider: null, model };
}

function joinUrl(base, suffix) {
  if (!base) return suffix;
  return `${base.replace(/\/+$/, '')}${suffix}`;
}

function buildUpstream(provider, account, ctx) {
  const type = PROVIDER_TYPES[provider.type] ? provider.type : 'custom';
  const def = PROVIDER_TYPES[type];
  const baseUrl = provider.baseUrl || def.baseUrl;
  const style = def.style;

  if (style === 'freebuff') {
    // URL/headers/body are produced by freebuff-driver inside callUpstream.
    return { style, responseStyle: 'openai', url: '', headers: {}, payload: {} };
  }

  if (style === 'opencode-free') {
    const session = openCodeSession(provider, account, { fresh: !!ctx.freshSession });
    if (ctx.freshSession) ctx.freshSession = false;
    const baseModel = stripThinkingSuffix(ctx.model || (ctx.body || {}).model);
  // Answer with the id the model actually has, not the UI's "(high)" variant.
  ctx.model = baseModel;
  const payload = ensureFingerprintTools({ ...(ctx.body || {}), model: baseModel });
    return {
      style, responseStyle: 'openai',
      url: `${String(baseUrl || '').replace(/\/+$/, '')}${ctx.suffix || '/chat/completions'}`,
      headers: {
        'content-type': 'application/json',
        accept: 'text/event-stream',
        authorization: 'Bearer public',
        'user-agent': openCodeUA(provider, account),
        'x-opencode-client': 'cli',
        'x-opencode-session': session,
        'x-opencode-session-id': session,
        'x-session-affinity': session,
        'x-session-id': session,
        'x-opencode-project': openCodeProject(provider, account),
        ...(provider.headers || {}),
      },
      payload,
    };
  }

  if (style === 'opencode') {
    const key = usableKey(account.apiKey);
    const headers = { 'content-type': 'application/json', ...(provider.headers || {}) };
    if (key) headers.authorization = `Bearer ${key}`;

    const root = String(baseUrl || '').replace(/\/+$/, '');
    const kind = style === 'opencode-free' ? 'openai' : (ctx.responseStyle || openCodeKind(provider, ctx.model));

    // A base URL that already points at one wire format (/v1, /openai/v1, ...)
    // means the caller wants exactly that endpoint, no per-model routing.
    const pinned = /\/(openai|anthropic|google)\/v\d/.test(root) || /\/v\d$/.test(root);

    if (!pinned && kind === 'anthropic') {
      if (key) headers['x-api-key'] = key;
      headers['anthropic-version'] = '2023-06-01';
      return {
        style, responseStyle: 'anthropic',
        url: `${root}/anthropic/v1/messages`,
        headers,
        payload: ctx.anthropicInbound ? ctx.body : toAnthropicBody(ctx.body, ctx.model),
      };
    }
    if (!pinned && kind === 'gemini') {
      const model = String(ctx.model || 'gemini-3.1-pro');
      if (key) headers['x-goog-api-key'] = key;
      const suffix2 = ctx.stream
        ? `/models/${encodeURIComponent(model)}:streamGenerateContent?alt=sse`
        : `/models/${encodeURIComponent(model)}:generateContent`;
      return { style, responseStyle: 'gemini', url: `${root}/google/v1beta${suffix2}`, headers, payload: toGeminiBody(ctx.body) };
    }
    if (!pinned && kind === 'responses') {
      return { style, responseStyle: 'responses', url: `${root}/openai/v1/responses`, headers, payload: toResponsesBody(ctx.body, ctx.model) };
    }
    const payload = { ...(ctx.body || {}) };
    if (ctx.model) payload.model = ctx.model;
    const suffix2 = pinned ? (ctx.suffix || '/chat/completions') : '/openai/v1/chat/completions';
    return { style, responseStyle: 'openai', url: joinUrl(root, suffix2), headers, payload };
  }

  if (style === 'anthropic') {
    return {
      style,
      url: joinUrl(baseUrl, '/messages'),
      headers: {
        'content-type': 'application/json',
        'x-api-key': account.apiKey,
        'anthropic-version': '2023-06-01',
        ...(provider.headers || {}),
      },
      payload: toAnthropicBody(ctx.body, ctx.model),
    };
  }

  if (style === 'gemini') {
    const model = String(ctx.model || 'gemini-2.0-flash');
    const suffix = ctx.stream
      ? `/models/${encodeURIComponent(model)}:streamGenerateContent?alt=sse`
      : `/models/${encodeURIComponent(model)}:generateContent`;
    return {
      style,
      url: joinUrl(baseUrl, suffix),
      headers: {
        'content-type': 'application/json',
        'x-goog-api-key': account.apiKey,
        ...(provider.headers || {}),
      },
      payload: toGeminiBody(ctx.body),
    };
  }

  const payload = { ...ctx.body };
  if (ctx.model && ctx.model !== payload.model) payload.model = ctx.model;
  return {
    style: 'openai',
    url: joinUrl(baseUrl, ctx.suffix || '/chat/completions'),
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${account.apiKey}`,
      ...(def.headers || {}),
      ...(provider.headers || {}),
    },
    payload,
  };
}

async function callUpstream(provider, account, ctx) {
  const spec = buildUpstream(provider, account, ctx);
  const controller = new AbortController();
  const timeoutMs = Number(provider.timeoutMs) > 0 ? Number(provider.timeoutMs) : 30_000;
  const timer = setTimeout(() => controller.abort(new Error('upstream timeout')), timeoutMs);
  if (timer.unref) timer.unref();

  let url = spec.url;
  let headers = spec.headers;
  let payload = ctx.method === 'GET' ? null : JSON.stringify(spec.payload);
  let meta = null;

  try {
    if (spec.style === 'freebuff') {
      // The driver performs the catalog/admission/run handshake and returns a
      // request that is already signed; we only choose the exit it travels over.
      if (!freebuff) throw new Error('freebuff driver unavailable');
      const built = await freebuff.prepare({ provider, account, body: ctx.body, model: ctx.model, stream: ctx.stream });
      url = built.url;
      headers = built.headers;
      payload = built.raw;
      meta = built.meta;
    }
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }

  const freeTierStyle = (PROVIDER_TYPES[provider.type] || {}).style === 'opencode-free';
  const picked = proxies && !freeTierStyle ? proxies.pick(provider) : null;
  let upstream;
  try {
    if (picked) {
      upstream = await proxiedRequest(url, {
        method: ctx.method === 'GET' ? 'GET' : 'POST',
        headers,
        body: payload,
        proxy: picked.proxy,
        timeoutMs,
        insecure: !!proxies.state.settings.insecure,
        signal: controller.signal,
      });
    } else {
      upstream = await fetch(url, {
        method: ctx.method === 'GET' ? 'GET' : 'POST',
        headers,
        body: payload,
        signal: controller.signal,
        redirect: 'follow',
      });
    }
  } finally {
    clearTimeout(timer);
  }
  return {
    upstream,
    controller,
    style: spec.style,
    responseStyle: spec.responseStyle,
    exit: picked ? picked.exit : null,
    mode: picked ? picked.mode : 'direct',
    meta,
  };
}

// ---------------------------------------------------------------------------
// Format translation: OpenAI <-> Anthropic <-> Gemini
// (transient per request; nothing is retained after the response completes)
// ---------------------------------------------------------------------------

function textFromContent(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.map((p) => (typeof p === 'string' ? p : p?.text || '')).join('');
  }
  return '';
}

function toAnthropicBody(body, model) {
  const src = Array.isArray(body?.messages) ? body.messages : [];
  const system = [];
  const messages = [];
  for (const m of src) {
    if (m.role === 'system' || m.role === 'developer') {
      const text = textFromContent(m.content);
      if (text) system.push(text);
      continue;
    }
    const role = m.role === 'assistant' ? 'assistant' : 'user';
    const parts = Array.isArray(m.content) ? m.content : [{ type: 'text', text: textFromContent(m.content) }];
    const blocks = [];
    for (const p of parts) {
      if (typeof p === 'string') { blocks.push({ type: 'text', text: p }); continue; }
      if (p?.type === 'image_url' && p.image_url?.url) {
        const url = String(p.image_url.url);
        const m2 = /^data:([^;]+);base64,(.*)$/.exec(url);
        blocks.push(m2
          ? { type: 'image', source: { type: 'base64', media_type: m2[1], data: m2[2] } }
          : { type: 'image', source: { type: 'url', url } });
        continue;
      }
      if (p?.type === 'image' && p.source) { blocks.push(p); continue; }
      if (typeof p?.text === 'string') blocks.push({ type: 'text', text: p.text });
    }
    messages.push({ role, content: blocks.length ? blocks : [{ type: 'text', text: '' }] });
  }
  const out = {
    model: model || body?.model,
    messages,
    max_tokens: Number(body?.max_tokens) > 0 ? Number(body.max_tokens) : 4096,
    stream: !!body?.stream,
  };
  if (system.length) out.system = system.join('\n\n');
  if (body?.temperature != null) out.temperature = body.temperature;
  if (body?.top_p != null) out.top_p = body.top_p;
  if (Array.isArray(body?.stop)) out.stop_sequences = body.stop;
  else if (typeof body?.stop === 'string') out.stop_sequences = [body.stop];
  return out;
}

function fromAnthropicResponse(json, model) {
  const text = Array.isArray(json?.content) ? textFromContent(json.content) : '';
  const finish = { end_turn: 'stop', stop_sequence: 'stop', max_tokens: 'length', tool_use: 'tool_calls' }[json?.stop_reason] || 'stop';
  return {
    id: json?.id || uid('chatcmpl'),
    object: 'chat.completion',
    created: Math.floor(now() / 1000),
    model: json?.model || model,
    choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: finish }],
    usage: {
      prompt_tokens: json?.usage?.input_tokens ?? 0,
      completion_tokens: json?.usage?.output_tokens ?? 0,
      total_tokens: (json?.usage?.input_tokens ?? 0) + (json?.usage?.output_tokens ?? 0),
    },
  };
}

function toGeminiBody(body) {
  const src = Array.isArray(body?.messages) ? body.messages : [];
  const contents = [];
  const systemParts = [];
  for (const m of src) {
    if (m.role === 'system' || m.role === 'developer') {
      const text = textFromContent(m.content);
      if (text) systemParts.push({ text });
      continue;
    }
    const role = m.role === 'assistant' ? 'model' : 'user';
    const parts = Array.isArray(m.content) ? m.content : [{ type: 'text', text: textFromContent(m.content) }];
    const gparts = [];
    for (const p of parts) {
      if (typeof p === 'string') { gparts.push({ text: p }); continue; }
      if (p?.type === 'image_url' && p.image_url?.url) {
        const url = String(p.image_url.url);
        const m2 = /^data:([^;]+);base64,(.*)$/.exec(url);
        if (m2) gparts.push({ inlineData: { mimeType: m2[1], data: m2[2] } });
        else gparts.push({ fileData: { fileUri: url } });
        continue;
      }
      if (typeof p?.text === 'string') gparts.push({ text: p.text });
    }
    contents.push({ role, parts: gparts.length ? gparts : [{ text: '' }] });
  }
  const generationConfig = {};
  if (body?.temperature != null) generationConfig.temperature = body.temperature;
  if (body?.top_p != null) generationConfig.topP = body.top_p;
  if (Number(body?.max_tokens) > 0) generationConfig.maxOutputTokens = Number(body.max_tokens);
  if (Array.isArray(body?.stop)) generationConfig.stopSequences = body.stop;
  else if (typeof body?.stop === 'string') generationConfig.stopSequences = [body.stop];
  const out = { contents };
  if (systemParts.length) out.systemInstruction = { parts: systemParts };
  if (Object.keys(generationConfig).length) out.generationConfig = generationConfig;
  return out;
}

function geminiText(json) {
  const parts = json?.candidates?.[0]?.content?.parts;
  if (!Array.isArray(parts)) return '';
  return parts.map((p) => p?.text || '').join('');
}

function geminiFinish(reason) {
  return { STOP: 'stop', MAX_TOKENS: 'length', SAFETY: 'content_filter', RECITATION: 'content_filter' }[reason] || (reason ? 'stop' : null);
}

function fromGeminiResponse(json, model) {
  const usage = json?.usageMetadata || {};
  return {
    id: uid('chatcmpl'),
    object: 'chat.completion',
    created: Math.floor(now() / 1000),
    model,
    choices: [{
      index: 0,
      message: { role: 'assistant', content: geminiText(json) },
      finish_reason: geminiFinish(json?.candidates?.[0]?.finishReason) || 'stop',
    }],
    usage: {
      prompt_tokens: usage.promptTokenCount ?? 0,
      completion_tokens: usage.candidatesTokenCount ?? 0,
      total_tokens: usage.totalTokenCount ?? 0,
    },
  };
}

// ---------------------------------------------------------------------------
// OpenAI Responses API  <->  chat.completions
// OpenCode exposes its GPT/Grok/Muse models only on /openai/v1/responses.
// ---------------------------------------------------------------------------

function toResponsesBody(body, model) {
  const src = Array.isArray(body?.messages) ? body.messages : [];
  const input = [];
  const instructions = [];
  for (const m of src) {
    if (m.role === 'system' || m.role === 'developer') {
      const t = textFromContent(m.content);
      if (t) instructions.push(t);
      continue;
    }
    const role = m.role === 'assistant' ? 'assistant' : 'user';
    const parts = Array.isArray(m.content) ? m.content : [{ type: 'text', text: textFromContent(m.content) }];
    const content = [];
    for (const p of parts) {
      if (typeof p === 'string') {
        content.push({ type: role === 'assistant' ? 'output_text' : 'input_text', text: p });
        continue;
      }
      if (p?.type === 'image_url' && p.image_url?.url) {
        content.push({ type: 'input_image', image_url: String(p.image_url.url) });
        continue;
      }
      if (typeof p?.text === 'string') content.push({ type: role === 'assistant' ? 'output_text' : 'input_text', text: p.text });
    }
    input.push({ role, content: content.length ? content : [{ type: 'input_text', text: '' }] });
  }
  const out = { model: model || body?.model, input, stream: !!body?.stream };
  if (instructions.length) out.instructions = instructions.join('\n\n');
  if (Number(body?.max_tokens) > 0) out.max_output_tokens = Number(body.max_tokens);
  if (body?.temperature != null) out.temperature = body.temperature;
  if (body?.top_p != null) out.top_p = body.top_p;
  return out;
}

function responsesText(json) {
  if (typeof json?.output_text === 'string' && json.output_text) return json.output_text;
  const parts = [];
  for (const item of json?.output || []) {
    for (const c of item?.content || []) {
      if (typeof c?.text === 'string') parts.push(c.text);
    }
  }
  return parts.join('');
}

function fromResponsesResponse(json, model) {
  const u = json?.usage || {};
  const prompt = u.input_tokens ?? 0;
  const completion = u.output_tokens ?? 0;
  return {
    id: json?.id || uid('chatcmpl'),
    object: 'chat.completion',
    created: Math.floor(now() / 1000),
    model: json?.model || model,
    choices: [{
      index: 0,
      message: { role: 'assistant', content: responsesText(json) },
      finish_reason: json?.status === 'incomplete' ? 'length' : 'stop',
    }],
    usage: { prompt_tokens: prompt, completion_tokens: completion, total_tokens: u.total_tokens ?? (prompt + completion) },
  };
}

/** Single entry point for "upstream JSON -> OpenAI chat.completion". */
function fromUpstreamResponse(style, json, model) {
  if (style === 'anthropic') return fromAnthropicResponse(json, model);
  if (style === 'responses') return fromResponsesResponse(json, model);
  return fromGeminiResponse(json, model);
}

/**
 * Streaming guard: pass frames through untouched, except a usage-only frame
 * (empty choices) that the client never asked for.
 */
function usageFrameFilter(includeUsage) {
  let buf = '';
  const write = (push, line) => { push(`${line}\n`); };
  return new Transform({
    transform(chunk, _enc, cb) {
      buf += chunk.toString('utf8');
      let i;
      while ((i = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, i);
        buf = buf.slice(i + 1);
        const trimmed = line.trim();
        if (!includeUsage && trimmed.startsWith('data:')) {
          const d = trimmed.slice(5).trim();
          if (d && d !== '[DONE]') {
            try {
              const j = JSON.parse(d);
              if (Array.isArray(j.choices) && j.choices.length === 0) continue;
            } catch { /* not our business */ }
          }
        }
        write((l) => this.push(l), line);
      }
      cb();
    },
    flush(cb) { if (buf) this.push(buf); cb(); },
  });
}

function openAiChunk(model, delta, finishReason, extra) {
  const payload = {
    id: extra?.id || chunkId(),
    object: 'chat.completion.chunk',
    created: Math.floor(now() / 1000),
    model,
    choices: [{ index: 0, delta, finish_reason: finishReason ?? null }],
  };
  if (extra?.usage) payload.usage = extra.usage;
  return `data: ${JSON.stringify(payload)}\n\n`;
}

let chunkSeq = 0;
const chunkId = () => `chatcmpl-${(chunkSeq = (chunkSeq + 1) % 1e9).toString(36)}${crypto.randomBytes(3).toString('hex')}`;

/**
 * Streaming translator: consumes upstream SSE (anthropic events or gemini
 * chunks) and emits OpenAI-compatible `chat.completion.chunk` SSE.
 * Parses one line at a time; holds at most one partial line in memory.
 */
function translateStream(style, model) {
  let buf = '';
  let usage = null;
  let finish = null;
  let finished = false;

  const emit = (delta, fr) => {
    if (fr) finished = true;
    return openAiChunk(model, delta, fr || null, usage ? { usage } : undefined);
  };

  const handleLine = (line) => {
    const out = [];
    if (!line || line.startsWith(':') || line.startsWith('event:')) return out;
    if (!line.startsWith('data:')) return out;
    const data = line.slice(5).trim();
    if (!data || data === '[DONE]') return out;
    let ev;
    try { ev = JSON.parse(data); } catch { return out; }

    if (style === 'anthropic') {
      switch (ev.type) {
        case 'content_block_delta': {
          const text = ev.delta?.text ?? ev.delta?.thinking ?? '';
          if (text) out.push(emit({ content: text }));
          break;
        }
        case 'message_delta':
          if (ev.usage) usage = { prompt_tokens: usage?.prompt_tokens ?? 0, completion_tokens: ev.usage.output_tokens ?? 0, total_tokens: 0 };
          if (ev.delta?.stop_reason) finish = { end_turn: 'stop', stop_sequence: 'stop', max_tokens: 'length' }[ev.delta.stop_reason] || 'stop';
          break;
        case 'message_start':
          if (ev.message?.usage) usage = { prompt_tokens: ev.message.usage.input_tokens ?? 0, completion_tokens: 0, total_tokens: 0 };
          break;
        case 'message_stop':
          out.push(emit({}, finish || 'stop'));
          break;
        case 'error':
          out.push(`data: ${JSON.stringify({ error: { message: ev.error?.message || 'upstream error', type: 'upstream_error' } })}\n\n`);
          break;
        default:
          break;
      }
      return out;
    }

    if (style === 'responses') {
      switch (ev.type) {
        case 'response.output_text.delta':
          if (ev.delta) out.push(emit({ content: ev.delta }));
          break;
        case 'response.completed': {
          const u = ev.response?.usage || {};
          usage = {
            prompt_tokens: u.input_tokens ?? 0,
            completion_tokens: u.output_tokens ?? 0,
            total_tokens: u.total_tokens ?? ((u.input_tokens ?? 0) + (u.output_tokens ?? 0)),
          };
          out.push(emit({}, ev.response?.status === 'incomplete' ? 'length' : 'stop'));
          break;
        }
        case 'response.failed':
        case 'response.incomplete':
        case 'error':
          out.push(`data: ${JSON.stringify({ error: { message: ev.response?.error?.message || ev.error?.message || ev.type, type: 'upstream_error' } })}\n\n`);
          out.push(emit({}, 'stop'));
          break;
        default:
          break;
      }
      return out;
    }

    // gemini
    const text = geminiText(ev);
    if (text) out.push(emit({ content: text }));
    if (ev.usageMetadata) {
      usage = {
        prompt_tokens: ev.usageMetadata.promptTokenCount ?? 0,
        completion_tokens: ev.usageMetadata.candidatesTokenCount ?? 0,
        total_tokens: ev.usageMetadata.totalTokenCount ?? 0,
      };
    }
    const fr = geminiFinish(ev.candidates?.[0]?.finishReason);
    if (fr) out.push(emit({}, fr));
    return out;
  };

  return new Transform({
    transform(chunk, _enc, cb) {
      buf += chunk.toString('utf8');
      let idx;
      while ((idx = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        for (const piece of handleLine(line.trim())) this.push(piece);
      }
      cb();
    },
    flush(cb) {
      if (buf.trim()) for (const piece of handleLine(buf.trim())) this.push(piece);
      if (!finished) this.push(openAiChunk(model, {}, 'stop'));
      this.push('data: [DONE]\n\n');
      cb();
    },
  });
}

/** Read an upstream body through a hard byte cap (translation path only). */
async function readCapped(upstream, max = MAX_TRANSLATE_BYTES) {
  if (!upstream.body) return '';
  const chunks = [];
  let size = 0;
  for await (const c of Readable.fromWeb(upstream.body)) {
    size += c.length;
    if (size > max) throw Object.assign(new Error('upstream response exceeded translation buffer cap'), { statusCode: 502 });
    chunks.push(c);
  }
  return Buffer.concat(chunks).toString('utf8');
}

// ---------------------------------------------------------------------------
// Streaming to the client
// ---------------------------------------------------------------------------

function baseResponseHeaders(status, contentType, provider) {
  const headers = {
    'content-type': contentType,
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-accel-buffering': 'no',
  };
  if (config.cors) {
    headers['access-control-allow-origin'] = '*';
    headers['access-control-expose-headers'] = 'x-router-provider,x-router-account,x-router-attempts,x-router-model';
  }
  headers['x-router-provider'] = String(provider?.id || '');
  return headers;
}

async function relay(provider, account, upstream, controller, ctx, res, attempts) {
  const contentType = upstream.headers.get('content-type') || (ctx.stream ? 'text/event-stream' : 'application/json');

  // ---- translated, non-streaming: buffer (capped), convert, then answer ------
  if (ctx.translate && !ctx.stream) {
    let payload;
    let status = upstream.status;
    try {
      const raw = await readCapped(upstream, MAX_TRANSLATE_BYTES);
      if (!upstream.ok) {
        // never dress an upstream error up as a success
        status = upstream.status;
        payload = raw || JSON.stringify({ error: { message: 'upstream error', type: 'upstream_error' } });
      } else {
        const json = JSON.parse(raw);
        payload = JSON.stringify(fromUpstreamResponse(ctx.style, json, ctx.model));
      }
    } catch (err) {
      if (!res.headersSent) {
        sendJson(res, 502, { error: { message: `upstream translation failed: ${err.message}`, type: 'router_translate_error' } });
      } else if (!res.writableEnded) res.destroy();
      return;
    }
    const headers = baseResponseHeaders(status, 'application/json; charset=utf-8', provider);
    headers['x-router-account'] = String(account?.id || '');
    headers['x-router-model'] = String(ctx.model || '');
    headers['x-router-attempts'] = String(attempts);
    headers['content-length'] = Buffer.byteLength(payload);
    res.writeHead(status, headers);
    res.end(payload);
    return;
  }

  // ---- upstream refused: pass the reason through, plus what to do about it --
  if (!upstream.ok && ctx.style === 'opencode-free' && upstream.body) {
    let raw = '';
    try { raw = (await peekError(upstream, 4096)) || ''; } catch { /* noop */ }
    let message = raw.slice(0, 300);
    let type = 'upstream_error';
    try {
      const j = JSON.parse(raw);
      message = j.error?.message || j.message || message;
      type = j.error?.type || j.type || type;
    } catch { /* keep the raw head */ }
    const hint = upstreamHint(upstream.status, message);
    const headers = baseResponseHeaders(upstream.status, 'application/json; charset=utf-8', provider);
    headers['x-router-account'] = String(account?.id || '');
    headers['x-router-model'] = String(ctx.model || '');
    headers['x-router-attempts'] = String(attempts);
    headers['x-router-exit'] = 'direct';
    res.writeHead(upstream.status, headers);
    res.end(JSON.stringify({ error: { message: `${message}${hint}`, type, upstreamStatus: upstream.status, provider: provider.id } }));
    return;
  }

  // ---- upstream streams, client asked for one JSON object ------------------
  if (ctx.aggregate && upstream.body && upstream.status < 300) {
    let payload = '';
    const early = baseResponseHeaders(upstream.status, 'application/json; charset=utf-8', provider);
    early['x-router-account'] = String(account?.id || '');
    early['x-router-model'] = String(ctx.model || '');
    early['x-router-attempts'] = String(attempts);
    early['x-router-exit'] = ctx.exitId || 'direct';
    early['x-router-rotations'] = String(ctx.rotations || 0);
    const keepWarm = armSlowResponse(res, upstream.status, early);
    try {
      const agg = sseAggregator(ctx.model);
      const done = new Promise((resolve, reject) => {
        agg.on('data', (c) => { payload += c.toString('utf8'); });
        agg.on('end', resolve);
        agg.on('error', reject);
      });
      Readable.fromWeb(upstream.body).pipe(agg);
      await done;
    } catch (err) {
      keepWarm.stop();
      if (!res.headersSent) sendJson(res, 502, { error: { message: `aggregating the upstream stream failed: ${err.message}`, type: 'router_relay_error' } });
      else if (!res.writableEnded) res.end(JSON.stringify({ error: { message: `aggregating the upstream stream failed: ${err.message}`, type: 'router_relay_error' } }));
      return;
    }
    keepWarm.stop();
    const headers2 = baseResponseHeaders(upstream.status, 'application/json; charset=utf-8', provider);
    headers2['x-router-account'] = String(account?.id || '');
    headers2['x-router-model'] = String(ctx.model || '');
    headers2['x-router-attempts'] = String(attempts);
    headers2['x-router-exit'] = ctx.exitId || 'direct';
    headers2['x-router-rotations'] = String(ctx.rotations || 0);
    if (!keepWarm.opened()) {
      headers2['content-length'] = Buffer.byteLength(payload);
      res.writeHead(upstream.status, headers2);
    }
    res.end(payload);
    return;
  }

  // ---- streaming / passthrough: zero-copy, never buffered -------------------
  const headers = baseResponseHeaders(upstream.status, contentType, provider);
  headers['x-router-account'] = String(account?.id || '');
  headers['x-router-attempts'] = String(attempts);
  headers['x-router-exit'] = ctx.exitId || 'direct';
  headers['x-router-rotations'] = String(ctx.rotations || 0);
  const len = upstream.headers.get('content-length');
  if (!ctx.stream && len) headers['content-length'] = len;
  res.writeHead(upstream.status, headers);
  if (res.socket && res.socket.setNoDelay) res.socket.setNoDelay(true);

  if (!upstream.body) { res.end(); return; }

  let torndown = false;
  const teardown = () => {
    if (torndown) return;
    torndown = true;
    try { controller.abort(); } catch { /* noop */ }
  };
  // Client hung up: release the upstream socket instead of streaming into a void.
  res.on('close', () => { if (!res.writableEnded) teardown(); });

  const fail = (err) => {
    if (!err || err.name === 'AbortError' || err.code === 'ERR_STREAM_PREMATURE_CLOSE') return; // expected on disconnect
    console.error(`[proxy] stream error: ${err.message}`);
    teardown();
    if (!res.writableEnded) res.destroy();
  };
  res.on('error', fail);

  const source = Readable.fromWeb(upstream.body);
  source.on('error', fail); // pipe() does not forward errors: without this a client
                            // disconnect would surface as an uncaughtException

  if (ctx.translate) {
    const translator = translateStream(ctx.style, ctx.model);
    translator.on('error', fail);
    source.pipe(translator).pipe(res);
  } else {
    // Strict clients index choices[0] on every frame, so a usage-only frame is
    // only forwarded when it was requested.
    const filtered = ctx.stream ? source.pipe(usageFrameFilter(!!ctx.includeUsage)) : source;
    filtered.on('error', fail);
    filtered.pipe(res);
  }
}

// ---------------------------------------------------------------------------
// Routing with automatic failover
// ---------------------------------------------------------------------------

function allExhausted(provider, tried, dueToLimit) {
  const accounts = (provider.accounts || []).filter((a) => a.enabled !== false);
  const healthyLeft = accounts.filter((a) => !tried.has(a.id) && !isLimited(accountState(provider.id, a.id)));
  const nextUnlock = accounts
    .map((a) => accountState(provider.id, a.id).limitedUntil)
    .filter((t) => t > now())
    .sort((a, b) => a - b)[0] || 0;
  // Say which account answered what, and when it frees up: without this the
  // client only sees "all accounts are limited" and cannot tell an IP limit
  // from a bad model handle or a suspended account.
  const detail = accounts.map((a) => {
    const as = accountState(provider.id, a.id);
    return {
      id: a.id,
      name: a.name,
      status: as.lastStatus || 0,
      kind: as.lastLimitedKind || '',
      message: as.lastMessage || '',
      unlocksInSeconds: as.limitedUntil > now() ? Math.round((as.limitedUntil - now()) / 1000) : 0,
    };
  });
  const reasons = detail.filter((d) => d.message).map((d) => `${d.name}: HTTP ${d.status} ${d.message}`);
  const rotateHint = provider.proxy === 'rotate'
    ? (proxies && proxies.activeExits().length ? '' : ' Rotation is configured but the exit pool has no working exit - run Test/Apply in the Network tab.')
    : ' Tip: set Outbound proxy to "Rotate the pool" so an IP-bound limit retries on a new exit first.';
  const enabled = accounts.length;
  const lastTried = [...tried].pop();
  const last = detail.find((d) => d.id === lastTried) || detail.find((d) => d.status) || null;
  const notEnabled = enabled === 0;
  const policy = last && (last.kind === 'policy' || last.kind === 'error');
  return {
    status: notEnabled ? 503 : (policy ? last.status : (dueToLimit ? 429 : 503)),
    error: {
      message: notEnabled
        ? `Provider "${provider.id}" has no enabled account. Enable one (or add one) in the dashboard; retrying will not help.`
        : (policy
          ? `Provider "${provider.id}" refused the request upstream: HTTP ${last.status} ${last.message || ''}`.trim() + ' This is not an account or exit limit, so nothing was rotated.'
          : (dueToLimit
            ? `All accounts for provider "${provider.id}" are rate-limited and cooling down.`
            : `No healthy account available for provider "${provider.id}".`)) + rotateHint,
      type: notEnabled ? 'router_no_enabled_accounts' : (policy ? 'upstream_error' : 'router_no_healthy_account'),
      upstreamStatus: last ? last.status : 0,
      provider: provider.id,
      tried: [...tried],
      accounts: detail,
      reasons,
      healthyRemaining: healthyLeft.length,
      // never advertise 0: a 5xx with retryAfterSeconds 0 invites a hot loop
      ...(nextUnlock ? { retryAfterSeconds: Math.max(1, Math.round((nextUnlock - now()) / 1000)), nextUnlockAt: nextUnlock } : {}),
      resetHint: `Clear it now: POST /admin/api/reset with x-admin-key, or the "Unlock all" button in the dashboard.`,
    },
  };
}

/** The router key from any header the real clients use. */
function inboundKey(req, url) {
  const header = req.headers['x-api-key']
    || req.headers['x-goog-api-key']
    || req.headers['api-key']
    || '';
  if (header) return String(header).trim();
  const auth = String(req.headers.authorization || '').trim();
  if (auth) return auth.replace(/^Bearer\s+/i, '').trim();
  try { return String((url && url.searchParams.get('key')) || '').trim(); } catch { return ''; }
}

// ---------------------------------------------------------------------------
// Self-managed provider identities (opencode-free)
// ---------------------------------------------------------------------------
// The free tier has no credential: an "account" is an identity holder whose
// quota lives in its session id and its source IP. When one is exhausted the
// router drops it, mints a new one and retries - the operator never adds an
// account and never waits for a cooldown.
const MANAGED_MAX_RENEWALS = 4;
// Minimum spacing between identity replacements for a managed provider.
const MANAGED_RENEW_GAP_MS = 10 * 60_000;
const MANAGED_BACKOFF_MS = 60_000;

function isManagedProvider(provider) {
  return !!(PROVIDER_TYPES[provider && provider.type] || {}).manageAccounts;
}

function newManagedAccount(provider) {
  const n = (state.totals.renewals || 0) + 1;
  const acc = normalizeAccount({ name: `identity-${n}`, apiKey: '' });
  acc.keyless = true;
  return acc;
}

function ensureManagedAccount(provider) {
  if (!isManagedProvider(provider)) return null;
  provider.accounts = provider.accounts || [];
  // One identity is the model: it is replaced, not accumulated. Extra rows
  // from an older build are pruned so the provider page stays honest.
  if (provider.accounts.length > 1) {
    const keep = provider.accounts.find((a) => a.enabled !== false) || provider.accounts[0];
    const ps = providerState(provider.id);
    for (const a of provider.accounts) if (a.id !== keep.id && ps.accounts) delete ps.accounts[a.id];
    provider.accounts = [keep];
    saveConfig().catch(() => {});
  }
  const live = provider.accounts.find((a) => a.enabled !== false);
  if (live) return live;
  const created = provider.accounts[0] || newManagedAccount(provider);
  if (!provider.accounts.length) provider.accounts.push(created);
  created.enabled = true;
  saveConfig().catch(() => {});
  pushEvent({ kind: 'identity-created', pid: provider.id, aid: created.id });
  return created;
}

/**
 * Reinstall the client: drop a spent identity and mint a replacement that
 * presents a *completely* different client, not just a new token.
 *
 * What changes: account id and name, session id, `x-opencode-project` handle and
 * the client version (`user-agent`), plus the previous cooldown and (when the
 * provider opted into the pool) the exit IP. A quota that is accounted per
 * session/instance is cleared by this; a quota accounted upstream against the
 * whole free pool is not - measured against the live service, a freshly minted
 * session with a fresh project handle, three client versions, three models and
 * four egress IPs all answer `429 FreeUsageLimitError` with
 * `retry-after: 57142` (~15.9h). That is why the caller also honours the
 * upstream window instead of reinstalling in a loop.
 */
function reinstallManagedClient(provider, oldAccount, reason, { rotateExit = false } = {}) {
  const prevAs = accountState(provider.id, oldAccount.id);
  const prevUa = Number(prevAs.uaIndex || 0);
  const idx = (provider.accounts || []).findIndex((a) => a.id === oldAccount.id);
  if (idx !== -1) provider.accounts.splice(idx, 1);
  const ps = providerState(provider.id);
  if (ps.accounts) delete ps.accounts[oldAccount.id];
  const fresh = newManagedAccount(provider);
  provider.accounts.push(fresh);
  state.totals.renewals = (state.totals.renewals || 0) + 1;
  state.totals.reinstalls = (state.totals.reinstalls || 0) + 1;
  // The old session is gone with its account, so the next request signs with a
  // new one; every other fingerprint the router controls is rotated here too.
  const as = accountState(provider.id, fresh.id);
  as.uaIndex = (prevUa + 1) % OPENCODE_FREE_UAS.length;
  as.lastRenewAt = Date.now();
  as.reinstalledAt = Date.now();
  as.reinstallReason = String(reason || '').slice(0, 120);
  as.limitedUntil = 0;
  // Mint the session now instead of lazily on the next request, so the rebuilt
  // client is complete and observable the moment this returns (and the admin
  // API can hand back the session it will actually use).
  as.sessionId = '';
  openCodeSession(provider, fresh);
  if (rotateExit && provider.proxy === 'rotate' && proxies && proxies.activeExits().length > 1) {
    proxies.rotate(`reinstall after ${reason}`, provider.id);
  }
  pushEvent({ kind: 'reinstall', pid: provider.id, aid: fresh.id, reason });
  console.warn(`[proxy] ${provider.id}: client reinstalled (${reason}); ${oldAccount.name} -> ${fresh.name}, new session, new project, client ${OPENCODE_FREE_UAS[as.uaIndex]}`);
  saveConfig().catch(() => {});
  persistState();
  return fresh;
}

// The historical name: the request path, the admin API and the tests use it.
const renewManagedIdentity = reinstallManagedClient;

/**
 * Last resort: answer from another provider when the chosen one has nothing left.
 *
 * Configured as `fallbackProviders: [{ provider: "pollinations", model: "openai" }, ...]`
 * in config.json (or per provider with `fallbackProviders`). Only plain
 * OpenAI-compatible targets are used, because the hop must not change the wire
 * format the client is already streaming in. The response carries the real
 * provider and model in `x-router-provider` / `x-router-model`, so a substituted
 * answer is never silently mislabelled.
 */
function providerFallbackHops(provider) {
  const list = (provider && Array.isArray(provider.fallbackProviders) && provider.fallbackProviders.length)
    ? provider.fallbackProviders
    : (Array.isArray(config.fallbackProviders) ? config.fallbackProviders : []);
  return list
    .map((h) => (typeof h === 'string'
      ? { provider: String(h).split('/')[0], model: String(h).split('/').slice(1).join('/') }
      : { provider: String(h && h.provider || ''), model: String(h && h.model || '') }))
    .filter((h) => h.provider && h.model && h.provider !== (provider && provider.id));
}

async function relayViaFallback(hop, body, res) {
  const fb = findProvider(hop.provider);
  if (!fb) return false;
  const account = (fb.accounts || []).find((a) => a.enabled !== false);
  if (!account) return false;
  if ((PROVIDER_TYPES[fb.type] || PROVIDER_TYPES.custom).style !== 'openai') return false;
  const ctx2 = {
    model: hop.model,
    body: { ...(body || {}), model: hop.model },
    stream: !!(body && body.stream),
    suffix: '/chat/completions',
    style: 'openai',
    responseStyle: 'openai',
    translate: false,
    aggregate: !(body && body.stream),
    includeUsage: !!(body && body.stream_options && body.stream_options.include_usage),
  };
  try {
    const call = await callUpstream(fb, account, ctx2);
    if (!call.upstream.ok) {
      try { if (call.upstream.body && call.upstream.body.cancel) await call.upstream.body.cancel(); } catch { /* noop */ }
      try { if (call.controller && call.controller.abort) call.controller.abort(); } catch { /* noop */ }
      return false;
    }
    ctx2.exitId = call.exit ? call.exit.id : 'direct';
    state.totals.providerFallbacks = (state.totals.providerFallbacks || 0) + 1;
    pushEvent({ kind: 'provider-fallback', pid: fb.id, to: hop.model });
    console.warn(`[proxy] ${fb.id}: answering with ${hop.model} (configured fallback; the first-choice provider had nothing left)`);
    await relay(fb, account, call.upstream, call.controller, ctx2, res, 1);
    return true;
  } catch (err) {
    console.warn(`[proxy] ${fb.id}: fallback hop failed: ${err.message}`);
    return false;
  }
}

async function handleProxy(req, res, url) {
  const t0 = now();
  state.totals.requests += 1;

  if (config.proxyKeys.length) {
    const key = inboundKey(req, url);
    if (!config.proxyKeys.includes(key)) {
      state.totals.failed += 1;
      return sendJson(res, 401, { error: { message: 'invalid router API key', type: 'invalid_request_error' } });
    }
  }

  let body = {};
  const isChat = url.pathname === '/v1/chat/completions' || url.pathname === '/v1/messages';
  if (req.method !== 'GET') {
    try { body = await readJsonBody(req); }
    catch (err) { state.totals.failed += 1; return sendJson(res, err.statusCode || 400, { error: { message: err.message } }); }
  }

  const { provider, model } = resolveProvider(body, req);
  if (!provider) {
    state.totals.failed += 1;
    return sendJson(res, 503, {
      error: {
        message: config.providers.length
          ? 'No provider has a healthy account. Add or reset accounts in the dashboard at /admin.'
          : 'No providers configured. Open /admin to add one.',
        type: 'router_no_provider',
      },
    });
  }
  if (!(provider.accounts || []).length) {
    state.totals.failed += 1;
    return sendJson(res, 503, { error: { message: `Provider "${provider.id}" has no accounts configured.`, type: 'router_no_account' } });
  }

  const style = (PROVIDER_TYPES[provider.type] || PROVIDER_TYPES.custom).style;
  // Per-model fallback inside the free tier: the requested model is tried first,
  // then the next free model that still has allowance (see the constant above).
  const freeFallbacks = style === 'opencode-free' && provider.freeFallback !== false
    ? (provider.freeFallbackModels
      || (Array.isArray(config.freeFallbackModels) && config.freeFallbackModels.length ? config.freeFallbackModels : OPENCODE_FREE_FALLBACK_MODELS))
    : [];
  const triedModels = new Set([String(model || '')]);
  let modelFallbacks = 0;
  // /v1/messages is anthropic-shaped: only translated when the target speaks OpenAI/Gemini.
  const wantAnthropicIn = url.pathname === '/v1/messages';
  const suffix = isChat ? '/chat/completions' : url.pathname;
  const ctx = {
    body,
    model,
    method: req.method,
    stream: !!body?.stream,
    suffix: wantAnthropicIn && (style === 'openai' || style === 'opencode' || style === 'opencode-free') ? '/chat/completions' : suffix,
    style,
    anthropicInbound: wantAnthropicIn,
    translate: isChat && style !== 'openai' && style !== 'opencode-free' && style !== 'freebuff',
  };
  if (style === 'freebuff') ctx.responseStyle = 'openai';
  if (style === 'anthropic' && wantAnthropicIn) {
    ctx.suffix = '/messages';
    ctx.translate = false;
  }
  if (style === 'opencode') {
    // One OpenCode account fronts four wire formats; pick by model id.
    const kind = openCodeKind(provider, model);
    ctx.responseStyle = kind;
    ctx.style = kind; // selects the translator (anthropic | gemini | responses)
    ctx.translate = isChat && kind !== 'openai';
    if (kind === 'anthropic' && wantAnthropicIn) ctx.translate = false;
  } else if (style === 'opencode-free') {
    ctx.responseStyle = 'openai';
    // Upstream always streams on the free tier; a non-stream client is served
    // by aggregating the SSE here instead of asking for stream:false.
    ctx.aggregate = !ctx.stream;
  } else if (style === 'anthropic' || style === 'gemini') {
    ctx.responseStyle = style;
  } else {
    ctx.responseStyle = 'openai';
  }

  if (isManagedProvider(provider)) ensureManagedAccount(provider);
  ctx.includeUsage = !!(body && body.stream_options && body.stream_options.include_usage);
  const tried = new Set();
  const maxAttempts = provider.maxAttempts > 0 ? provider.maxAttempts : Math.max(1, (provider.accounts || []).length);
  let sawLimit = false;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    let account = selectAccount(provider, tried);
    if (!account) break;
    tried.add(account.id);
    let renewals = 0;

    const rotateWanted = provider.proxy === 'rotate' && !!proxies && proxies.state.settings.enabled;
    const rotateMax = rotateWanted ? Math.max(0, Number(proxies.state.settings.rotateAttempts) || 0) : 0;
    let rotations = 0;
    let lastExitId = '';
    let relayed = false;
    let lastFailure = null;
    let lastFailureKey = '';
    let netRetries = 0;

    // Inner loop: same account, new exit IP. A rate limit here is usually
    // IP-bound, so rotating the exit beats burning the account for 12 hours.
    for (;;) {
      const as = accountState(provider.id, account.id);
      as.requests = (as.requests || 0) + 1;
      as.lastUsed = now();
      openWindow(provider, account);

      let upstream;
      let controller;
      let call;
      try {
        call = await callUpstream(provider, account, ctx);
        ({ upstream, controller } = call);
      } catch (err) {
        as.failures = (as.failures || 0) + 1;
        as.lastStatus = 0;
        as.lastError = err.message.slice(0, 120);
        pushEvent({ kind: 'error', pid: provider.id, aid: account.id, st: 0 });
        persistState();
        console.warn(`[proxy] ${provider.id}/${account.name} network error: ${err.message}`);
        if (call && call.exit && proxies) proxies.noteResult(call.exit.id, false);
        // Transient socket drops (TLS reset, upstream idle close) are worth one
        // retry on the same identity before the provider is called unhealthy.
        if (netRetries < 2) {
          netRetries += 1;
          await new Promise((r) => setTimeout(r, 400 * netRetries));
          continue;
        }
        break;
      }

      lastExitId = call.exit ? call.exit.id : '';
      as.lastStatus = upstream.status;
      const cooldownStatuses = config.cooldownStatuses;

      if (cooldownStatuses.includes(upstream.status)) {
        sawLimit = true;
        // Read a bounded reason + retry-after BEFORE touching the body: a 403
        // saying "mode required" is not a rate limit, and a 60s retry-after
        // must not cost 12 hours.
        const retryMs = retryAfterMs(upstream.headers);
        const snippet = await peekError(upstream);
        const kind = classifyFailure(upstream.status, snippet);
        lastFailure = { status: upstream.status, kind, message: snippet };
        // The same answer from a second exit means the condition is not the
        // exit (policy gate, bad model handle): stop burning the pool.
        const repeated = rotations > 0 && lastFailureKey === `${upstream.status}|${snippet}`;
        lastFailureKey = `${upstream.status}|${snippet}`;
        if (isManagedProvider(provider) && renewals < 1) {
          // One reinstall per request, and not more often than every 10 minutes:
          // minting an identity on every 429 looked like abuse upstream and got
          // our TLS handshakes dropped. Between reinstalls the provider backs off.
          const rt = accountState(provider.id, account.id);
          // 0 means "reinstall at once"; unset keeps the conservative spacing
          // that stops an identity being minted on every single 429.
          const rawGap = config.reinstallGapMinutes;
          const gapMs = (rawGap === undefined || rawGap === null || rawGap === '')
            ? MANAGED_RENEW_GAP_MS
            : Math.max(0, Number(rawGap) || 0) * 60_000;
          const since = Date.now() - (rt.lastRenewAt || 0);
          if (since >= gapMs) {
            const fresh = reinstallManagedClient(provider, account, `HTTP ${upstream.status}`, { rotateExit: true });
            renewals += 1;
            account = fresh;
            continue;
          }
        }
        const freeTier = (PROVIDER_TYPES[provider.type] || {}).style === 'opencode-free';
        if (freeTier && kind === 'quota' && rotations < Math.max(1, rotateMax)) {
          // The free tier accounts quota per session, so a new session is a new
          // allowance; that beats sitting out a cooldown.
          ctx.freshSession = true;
          rotations += 1;
          console.warn(`[proxy] ${provider.id}/${account.name} -> ${upstream.status} on the free tier; minting a new session and retrying (${rotations}/${Math.max(1, rotateMax)})`);
          continue;
        }
        // The account is fine - only this model is dry. Move to the next free
        // model that has allowance and keep the identity (nothing is cooled).
        if (freeTier && kind === 'quota') {
          const nextModel = freeFallbacks.find((m) => !triedModels.has(m));
          if (nextModel && modelFallbacks < freeFallbacks.length) {
            const dryModel = ctx.model;
            triedModels.add(nextModel);
            modelFallbacks += 1;
            ctx.model = nextModel;
            state.totals.modelFallbacks = (state.totals.modelFallbacks || 0) + 1;
            pushEvent({ kind: 'model-fallback', pid: provider.id, aid: account.id, from: dryModel, to: nextModel });
            console.warn(`[proxy] ${provider.id}/${account.name}: ${dryModel} is out of allowance; answering with ${nextModel} instead (${modelFallbacks}/${freeFallbacks.length})`);
            continue;
          }
        }
        if (kind === 'quota' && !repeated && rotateWanted && rotations < rotateMax && proxies.activeExits().length > 1) {
          if (call.exit) proxies.noteResult(call.exit.id, false);
          const next = proxies.rotate(`HTTP ${upstream.status} on ${provider.id}`, provider.id);
          rotations += 1;
          console.warn(`[proxy] ${provider.id}/${account.name} -> ${upstream.status}${snippet ? ` (${snippet.slice(0, 80)})` : ''}; rotating exit to ${next ? next.id : '?'} and retrying (${rotations}/${rotateMax})`);
          continue;
        }
        const cooled = markLimited(provider, account, upstream.status, { ms: retryMs, message: snippet });
        console.warn(`[proxy] ${provider.id}/${account.name} -> HTTP ${upstream.status} [${kind}] ${snippet ? `"${snippet}" ` : ''}cooling down ${Math.round((cooled.limitedUntil - now()) / 60000)}m${retryMs ? ` (retry-after ${Math.round(retryMs / 1000)}s)` : ''}${kind === 'policy' ? ' - not an exit problem, rotation skipped' : ''}, failing over`);
        break;
      }

      if (config.retryStatuses.includes(upstream.status) && attempt < maxAttempts) {
        as.failures = (as.failures || 0) + 1;
        pushEvent({ kind: 'retry', pid: provider.id, aid: account.id, st: upstream.status });
        persistState();
        try { upstream.body && upstream.body.cancel && await upstream.body.cancel(); } catch { /* noop */ }
        break;
      }

      const ok = upstream.status >= 200 && upstream.status < 300;
      if (ok) {
        state.totals.success += 1;
        if (ctx.stream) state.totals.streamed += 1;
        if (call.exit && proxies) proxies.noteResult(call.exit.id, true);
      } else {
        state.totals.failed += 1;
        if (call.exit && proxies) proxies.noteResult(call.exit.id, false);
      }
      pushEvent({ kind: ok ? 'ok' : 'http', pid: provider.id, aid: account.id, st: upstream.status, ms: now() - t0, s: ctx.stream ? 1 : 0 });
      persistState();

      if (!ctx.stream && !ctx.translate) {
        const len = Number(upstream.headers.get('content-length'));
        if (Number.isFinite(len)) state.totals.bytesOut += len;
      }

      res.on('finish', () => {
        reqLog(`[proxy] ${req.method} ${url.pathname} -> ${provider.id}/${account.name} ${upstream.status} ${now() - t0}ms attempts=${attempt}${ctx.stream ? ' stream' : ''}${call.mode === 'direct' ? '' : ` via=${call.exit ? call.exit.id : call.mode}`}`);
      });

      if (call.meta) {
        // freebuff: close the agent run after the response (or the stream) ends
        res.on('close', () => {
          freebuff.onCallFinished({ provider, account, status: upstream.status, ok }).catch((err) => reqLog(`[freebuff] finish failed: ${err.message}`));
        });
      }

      ctx.exitId = call.mode === 'direct' ? 'direct' : (call.exit ? call.exit.id : call.mode);
      ctx.rotations = rotations;

      try {
        await relay(provider, account, upstream, controller, ctx, res, attempt);
      } catch (err) {
        console.error(`[proxy] relay error after ${now() - t0}ms: ${err.message}`);
        if (!res.headersSent) sendJson(res, 502, { error: { message: `upstream relay failed: ${err.message}`, type: 'router_relay_error' } });
        else res.destroy();
      }
      relayed = true;
      break;
    }

    if (relayed) return;
    if (rotations) pushEvent({ kind: 'rotated', pid: provider.id, aid: account.id, n: rotations });
    void lastExitId;
  }

  // Configured cross-provider fallback, tried only when nothing on this provider
  // could answer (every model out of allowance or every account cooling).
  if (!res.headersSent) {
    for (const hop of providerFallbackHops(provider)) {
      if (res.headersSent) break;
      if (await relayViaFallback(hop, body, res)) return;
    }
  }

  state.totals.failed += 1;
  if (!res.headersSent) {
    const { error, status } = allExhausted(provider, tried, sawLimit);
    sendJson(res, status || (sawLimit ? 429 : 503), { error });
  }
}

// ---------------------------------------------------------------------------
// Admin API
// ---------------------------------------------------------------------------

function requireAdmin(req, res) {
  const provided = req.headers['x-admin-key'] || (String(req.headers.authorization || '').replace(/^Bearer\s+/i, '')) || '';
  if (!timingSafeEqual(provided, config.adminKey)) {
    sendJson(res, 401, { error: { message: 'invalid admin key', type: 'unauthorized' } });
    return false;
  }
  return true;
}

function snapshot() {
  const providers = config.providers.map(providerSummary);
  const mem = process.memoryUsage();
  const accounts = providers.flatMap((p) => p.accounts);
  return {
    ok: true,
    version: PKG_VERSION,
    node: process.version,
    build: buildInfo,
    now: now(),
    uptimeSeconds: Math.round((now() - state.startedAt) / 1000),
    host: config.host,
    port: config.port,
    publicBaseUrl: process.env.ROUTER_PUBLIC_URL || '',
    defaultProvider: config.defaultProvider,
    cooldownHours: clampHours(config.cooldownHours),
    cooldownStatuses: config.cooldownStatuses,
    cors: !!config.cors,
    hasProxyKeys: config.proxyKeys.length > 0,
    memory: {
      rss: mem.rss,
      heapUsed: mem.heapUsed,
      heapTotal: mem.heapTotal,
      external: mem.external,
      arrayBuffers: mem.arrayBuffers,
      samples: state.mem.samples,
    },
    totals: { ...state.totals, accounts: accounts.length, active: accounts.filter((a) => !a.limitedUntil && a.enabled).length, limited: accounts.filter((a) => a.limitedUntil > 0).length },
    logging: { requests: !!(config.logging && config.logging.requests) },
    proxies: proxies ? {
      enabled: proxies.state.settings.enabled,
      running: proxies.xray.running,
      version: proxies.xray.version,
      error: proxies.xray.error,
      active: proxies.state.active.length,
      working: proxies.workingExits().length,
      total: proxies.state.exits.length,
      index: proxies.index,
      testing: proxies.test.running,
      test: proxies.testStatus(),
      sources: proxies.state.sources.length,
      // the dashboard only needs counts here; /admin/api/proxies has the detail
    } : { enabled: false, total: 0 },
    freebuff: freebuff ? freebuff.snapshot().filter((a) => a.lastError || a.needsReauth) : [],
    providers,
    events: state.events,
  };
}

function touchProvider(provider) {
  const existing = findProvider(provider.id);
  if (existing) {
    Object.assign(existing, provider);
    return { provider: existing, created: false };
  }
  config.providers.push(provider);
  return { provider, created: true };
}

// ---------------------------------------------------------------------------
// OpenCode free tier (ported from 9Router's opencode executor)
// ---------------------------------------------------------------------------
// The gate is client attestation, not an IP or an account:
//   * POST /inference/v1/chat/completions  (the old /zen/v1 now always 403s)
//   * Authorization: Bearer public
//   * User-Agent: opencode/latest/<v>/cli  (the v1 'opencode/1.x' form is refused)
//   * x-opencode-session plus its three aliases, shaped ses_<12 hex><14 base62>
//     where the hex field is ~(timeMs*4096 + counter) truncated to 48 bits
//   * tools must contain bash, glob, grep and read
//   * stream is always true upstream
// Quota is accounted per session, so a fresh session is a fresh allowance.
const OPENCODE_FREE_UA = 'opencode/latest/2.0.26/cli';
// A dry model is not a dry provider: the free allowance is accounted per model.
// Measured on the same exhausted identity within one minute - deepseek-v4.1-flash-free,
// glm-5.3-flash-free, big-pickle, mimo-v2.6-flash-free and nemotron-3.5-super-vl-free
// answered 429 (retry-after ~16h) while beam-free and nemotron-3.5-lightning-free
// answered 200. So when the requested free model is out of allowance the router
// answers with the next free model that still has some, instead of cooling the
// account and failing the client.
const OPENCODE_FREE_FALLBACK_MODELS = [
  'nemotron-3.5-lightning-free',
  'beam-free',
  'flint-40b-free',
  'mimo-v2.6-flash-free',
  'nemotron-3.5-super-vl-free',
];
// Client versions that pass the free tier's attestation gate (measured: each
// answers a quota error rather than a 403 "only from within OpenCode"). A
// reinstalled identity steps to the next one.
const OPENCODE_FREE_UAS = [
  'opencode/latest/2.0.26/cli',
  'opencode/latest/2.0.31/cli',
  'opencode/latest/2.1.0/cli',
];
const OPENCODE_FINGERPRINT_TOOLS = ['bash', 'glob', 'grep', 'read'];
const BASE62 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

function openCodeSessionId(timestamp = Date.now(), counter = 1) {
  const mask = (1n << 48n) - 1n;
  const hex = (~(BigInt(timestamp) * 0x1000n + BigInt(counter)) & mask).toString(16).padStart(12, '0');
  const bytes = crypto.randomBytes(14);
  let tail = '';
  for (let i = 0; i < 14; i += 1) tail += BASE62[bytes[i] % 62];
  return `ses_${hex}${tail}`;
}

/** One stable session per provider+account, so quota is not burned per request. */
function openCodeSession(provider, account, { fresh = false } = {}) {
  const as = accountState(provider.id, account.id);
  if (fresh || !as.sessionId) {
    as.sessionId = openCodeSessionId();
    as.sessionStartedAt = Date.now();
    // Rebuilding the client means every field we control changes, not just the
    // session id: opencode fingerprints the free tier on the client headers.
    as.projectId = `p-${crypto.randomBytes(8).toString('hex')}`;
    persistState();
  }
  return as.sessionId;
}

/** The project handle this identity presents; a rebuilt identity gets a new one. */
function openCodeProject(provider, account) {
  return accountState(provider.id, account.id).projectId || 'global';
}

/** The client version this identity presents; a rebuilt identity steps to the next. */
function openCodeUA(provider, account) {
  const i = Number(accountState(provider.id, account.id).uaIndex || 0);
  return OPENCODE_FREE_UAS[i % OPENCODE_FREE_UAS.length];
}

/** "model (high)" -> "model": the upstream knows only the base id. */
function stripThinkingSuffix(model) {
  return String(model || '').replace(/\([^()]+\)\s*$/, '').trim();
}

function ensureFingerprintTools(body) {
  const out = { ...(body || {}) };
  const tools = Array.isArray(out.tools) ? out.tools.slice() : [];
  const present = new Set(tools.map((t) => String(t?.function?.name || t?.name || '').trim()));
  for (const name of OPENCODE_FINGERPRINT_TOOLS) {
    if (present.has(name)) continue;
    tools.push({
      type: 'function',
      function: { name, description: `OpenCode built-in ${name} tool`, parameters: { type: 'object', properties: {} } },
    });
  }
  out.tools = tools;
  out.stream = true; // stream:false is refused on the free tier
  // Measured against the live service: `metadata` is rejected with 400 while
  // user, n, logprobs, response_format, penalties, seed and top_p are accepted.
  delete out.metadata;
  return out;
}

/** Aggregate an upstream SSE stream into one chat.completion (for non-stream clients). */
/**
 * If a buffered response is going to be slow, open it early and send a
 * newline every few seconds: JSON allows leading whitespace, and an idle
 * socket is what gets reset by proxies on the client's side of the world.
 */
function armSlowResponse(res, status, headers, delayMs = 7000, everyMs = 4000) {
  const state = { opened: false, timer: null };
  const arm = setTimeout(() => {
    if (res.writableEnded || res.headersSent) return;
    res.writeHead(status, headers);
    state.opened = true;
    res.write(' \n');
    state.timer = setInterval(() => { if (!res.writableEnded) { try { res.write(' \n'); } catch { /* closed */ } } }, everyMs);
    if (state.timer.unref) state.timer.unref();
  }, delayMs);
  if (arm.unref) arm.unref();
  return {
    opened: () => state.opened,
    stop() { clearTimeout(arm); if (state.timer) clearInterval(state.timer); },
  };
}

function sseAggregator(model) {
  let buf = '';
  let content = '';
  let reasoning = '';
  let usage = null;
  let finish = 'stop';
  let id = '';
  let created = 0;
  return new Transform({
    transform(chunk, _enc, cb) {
      buf += chunk.toString('utf8');
      let i;
      while ((i = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        if (!line.startsWith('data:')) continue;
        const data = line.slice(5).trim();
        if (!data || data === '[DONE]') continue;
        try {
          const j = JSON.parse(data);
          if (j.id) id = j.id;
          if (j.created) created = j.created;
          const ch = j.choices && j.choices[0];
          if (ch) {
            if (ch.delta && typeof ch.delta.content === 'string') content += ch.delta.content;
            if (ch.delta && typeof ch.delta.reasoning_content === 'string') reasoning += ch.delta.reasoning_content;
            if (ch.finish_reason) finish = ch.finish_reason;
          }
          if (j.usage) usage = j.usage;
        } catch { /* partial frame */ }
      }
      cb();
    },
    flush(cb) {
      this.push(JSON.stringify({
        id: id || uid('chatcmpl'),
        object: 'chat.completion',
        created: created || Math.floor(now() / 1000),
        model,
        choices: [{ index: 0, message: { role: 'assistant', content, ...(reasoning ? { reasoning_content: reasoning } : {}) }, finish_reason: finish }],
        ...(usage ? { usage } : {}),
      }));
      cb();
    },
  });
}

/**
 * Model discovery: ask the upstream what it serves. Works for OpenAI-compatible
 * /models, Anthropic, Gemini and OpenCode (which lists 80+ ids at
 * https://opencode.ai/inference/v1/models, no key required).
 */
function modelListRequest(provider, account) {
  const def = PROVIDER_TYPES[provider.type] || PROVIDER_TYPES.custom;
  const base = String(provider.baseUrl || def.baseUrl || '').replace(/\/+$/, '');
  const key = usableKey(account && account.apiKey);
  const headers = { accept: 'application/json' };
  if (key) headers.authorization = `Bearer ${key}`;
  if (def.style === 'anthropic') {
    if (key) headers['x-api-key'] = key;
    headers['anthropic-version'] = '2023-06-01';
  } else if (def.style === 'gemini') {
    if (key) headers['x-goog-api-key'] = key;
  }
  if (def.style === 'opencode') {
    if (key) headers['x-api-key'] = key;
    return { url: /\/v\d+$/.test(base) ? `${base}/models` : `${base}/v1/models`, headers };
  }
  return { url: `${base}/models`, headers };
}

function extractModelIds(json) {
  if (!json) return [];
  const arr = Array.isArray(json.data) ? json.data
    : Array.isArray(json.models) ? json.models
      : Array.isArray(json) ? json
        : [];
  return [...new Set(arr.map((m) => (typeof m === 'string' ? m : (m && (m.id || m.name || m.model)) || ''))
    .map((s) => String(s).replace(/^models\//, ''))
    .filter(Boolean))].sort();
}

/**
 * /v1/models: ask each usable provider what it serves and expose the ids as
 * `providerId/modelId`, which is exactly the form the router accepts back and
 * what a client model picker needs. Cached, because a picker refresh must not
 * hammer every upstream; discovery failures degrade to `providerId/*` instead
 * of making the endpoint empty or slow.
 */
const MODEL_CACHE_TTL_MS = 10 * 60 * 1000;
const modelCache = new Map();

function providerIsReachable(provider, def) {
  // Same condition the dispatcher uses: only an enabled account can serve a
  // request, so advertising models from a provider with none is a lie. The
  // keyless styles just need no key for discovery.
  return (provider.accounts || []).some((a) => a.enabled !== false);
}

async function providerModelIds(provider) {
  const def = PROVIDER_TYPES[provider.type] || PROVIDER_TYPES.custom;
  if (!providerIsReachable(provider, def)) return null;
  const hit = modelCache.get(provider.id);
  if (hit && Date.now() - hit.at < MODEL_CACHE_TTL_MS) return hit.ids;
  const account = (provider.accounts || []).find((a) => a.enabled !== false) || { apiKey: '' };
  let ids = null;
  try {
    if (provider.type === 'freebuff') {
      ids = (await freebuff.listModels({ provider, account })).map(String);
    } else {
      const spec = modelListRequest(provider, account);
      const upstream = await fetch(spec.url, { headers: spec.headers, signal: AbortSignal.timeout(6000) });
      const text = await upstream.text();
      let json = null;
      try { json = JSON.parse(text); } catch { /* non-JSON body: no ids */ }
      ids = extractModelIds(json);
    }
  } catch { ids = null; }
  if (ids && !ids.length) ids = null;
  modelCache.set(provider.id, { at: Date.now(), ids });
  return ids;
}

async function routerModelList() {
  const per = await Promise.all(config.providers.map(async (p) => {
    const ids = await providerModelIds(p);
    if (ids) return ids.map((m) => ({ id: `${p.id}/${m}`, object: 'model', owned_by: p.id, provider: p.id, type: p.type }));
    const def = PROVIDER_TYPES[p.type] || PROVIDER_TYPES.custom;
    if (!providerIsReachable(p, def)) return [];
    return [{ id: `${p.id}/*`, object: 'model', owned_by: p.id, provider: p.id, type: p.type }];
  }));
  return per.flat();
}

async function fetchModelList(provider, account) {
  const spec = modelListRequest(provider, account);
  const upstream = await fetch(spec.url, { headers: spec.headers, signal: AbortSignal.timeout(15_000) });
  const text = await upstream.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* non-JSON: report the raw head */ }
  const models = extractModelIds(json);
  return {
    ok: upstream.ok,
    status: upstream.status,
    source: spec.url,
    provider: provider.id,
    count: models.length,
    models: models.length ? models : undefined,
    raw: models.length ? undefined : text.slice(0, 2000),
  };
}

async function handleAdmin(req, res, url) {
  if (url.pathname === '/admin/api/ping') return sendJson(res, 200, { ok: true, version: PKG_VERSION });
  if (!requireAdmin(req, res)) return;

  const seg = url.pathname.split('/').filter(Boolean); // ['admin','api',...]
  const [, , a, b, c, d, e] = seg;
  const method = req.method;

  if (method === 'GET' && a === 'state') return sendJson(res, 200, snapshot());

  if (method === 'GET' && a === 'export') {
    const payload = { kind: 'llm-router-backup', version: 1, exportedAt: new Date().toISOString(), config };
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'content-disposition': `attachment; filename="router-backup-${Date.now()}.json"`,
      'cache-control': 'no-store',
    });
    return res.end(JSON.stringify(payload, null, 2));
  }

  if (method === 'POST' && a === 'import') {
    const body = await readJsonBody(req, 12 * 1024 * 1024);
    const converted = convertBackup(body);
    if (converted.errors.length) return sendJson(res, 400, { error: { message: converted.errors.join('; '), type: 'import_failed' } });
    if (converted.replace) config = normalizeConfig({ ...config, ...converted.config });
    else mergeImported(converted.config);
    await saveConfig();
    persistState();
    return sendJson(res, 200, { ok: true, imported: converted.summary, warnings: converted.warnings });
  }

  if (method === 'POST' && a === 'providers') {
    const body = await readJsonBody(req);
    const existing = findProvider(String(body.id || ''));
    // A UI form sends only the fields it shows: anything absent keeps its stored
    // value (accounts above all — an "edit provider" must never orphan keys).
    const seeded = {};
    if (existing) {
      for (const key of ['accounts', 'headers', 'modelStyles', 'maxAttempts', 'cooldownHours', 'timeoutMs', 'strategy', 'baseUrl', 'name', 'type', 'proxy', 'rotateEveryMinutes']) {
        if (body[key] === undefined) seeded[key] = existing[key];
      }
    }
    const provider = normalizeProvider({ ...seeded, ...body });
    if (existing && !body.force) {
      return sendJson(res, 409, { error: { message: `provider "${provider.id}" already exists`, type: 'conflict' } });
    }
    const { created } = touchProvider(provider);
    await saveConfig();
    return sendJson(res, created ? 201 : 200, { ok: true, provider: providerSummary(provider) });
  }

  if (method === 'POST' && a === 'provider' && b && c === 'strategy') {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
    const body = await readJsonBody(req);
    provider.strategy = body.strategy === 'sequential' ? 'sequential' : 'round-robin';
    await saveConfig();
    pushEvent({ kind: 'strategy', pid: provider.id, s: provider.strategy === 'sequential' ? 1 : 0 });
    persistState();
    return sendJson(res, 200, { ok: true, provider: providerSummary(provider) });
  }

  if (method === 'POST' && a === 'provider' && b && c === 'accounts') {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
    if (isManagedProvider(provider)) {
      return sendJson(res, 409, { error: { message: `provider "${provider.id}" manages its own identities; it replaces them automatically when one is exhausted`, type: 'accounts_managed' } });
    }
    const body = await readJsonBody(req);
    const incoming = Array.isArray(body.accounts) ? body.accounts : [body];
    // Keyless providers (OpenCode Free, freebuff before its token is pasted,
    // local servers) legitimately store an account without a credential, and
    // the connect flow is allowed to save first and ask for the key later.
    const keyless = (PROVIDER_TYPES[provider.type] || PROVIDER_TYPES.custom).style === 'opencode-free'
      || (PROVIDER_TYPES[provider.type] || {}).keyless === true
      || body.allowEmptyKey === true;
    const added = [];
    for (const raw of incoming) {
      const acc = typeof raw === 'string' ? parseAccountLine(raw) : normalizeAccount(raw, `account-${provider.accounts.length + 1}`);
      const genericName = /^account-\d+$/.test(acc.name || '');
      if (!acc.apiKey && !keyless && genericName) continue;
      if (!acc.name || genericName) acc.name = acc.apiKey ? acc.name : (keyless ? 'keyless' : `account-${provider.accounts.length + 1}`);
      if (!acc.apiKey) acc.keyless = true;
      provider.accounts.push(acc);
      added.push(accountSummary(provider, acc));
    }
    await saveConfig();
    return sendJson(res, 200, { ok: true, added: added.length, accounts: added });
  }

  if (method === 'GET' && a === 'provider' && b && c === 'models') {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
    const accounts = provider.accounts || [];
    const account = accounts.find((x) => x.enabled !== false && !isLimited(accountState(provider.id, x.id))) || accounts[0] || { apiKey: '' };
    if (provider.type === 'freebuff') {
      const token = usableKey(account.apiKey);
      const st = freebuff.accountState(provider.id, account.id);
      if (!token || st.needsReauth) {
        return sendJson(res, 200, {
          ok: true, status: 200, source: 'freebuff (no session yet)', provider: provider.id,
          count: 0, models: [],
          raw: st.needsReauth
            ? 'The stored freebuff token was rejected. Press "Authorize freebuff" to sign in again � nothing to paste.'
            : 'Not authorized yet. Press "Authorize freebuff" on this provider: a freebuff tab opens, you approve it, and the account is created here automatically.',
        });
      }
      try {
        const models = await freebuff.listModels({ provider, account });
        return sendJson(res, 200, { ok: true, status: 200, source: 'freebuff catalog (signed)', provider: provider.id, count: models.length, models });
      } catch (err) {
        return sendJson(res, 502, { error: { message: `freebuff catalog failed: ${err.message}`, type: 'upstream_error' } });
      }
    }
    const spec = modelListRequest(provider, account);
    try {
      return sendJson(res, 200, await fetchModelList(provider, account));
    } catch (err) {
      return sendJson(res, 502, { error: { message: `model list fetch failed: ${err.message}`, type: 'upstream_error' } });
    }
  }

  // "Check models" in the connect flow: probe a provider that is not saved yet.
  if (method === 'POST' && a === 'probe-models') {
    const body = await readJsonBody(req);
    const incoming = body.provider || {};
    const provider = normalizeProvider({
      id: String(incoming.id || 'probe'),
      name: String(incoming.name || 'probe'),
      type: incoming.type,
      baseUrl: incoming.baseUrl,
      accounts: [],
    });
    const account = { apiKey: String((body.account && body.account.apiKey) || ''), id: 'probe', name: 'probe' };
    if (provider.type === 'freebuff') {
      return sendJson(res, 200, {
        ok: true, status: 200, source: 'freebuff catalog (signed, needs a saved account)', provider: provider.id,
        count: 0, models: [], raw: 'Connect first: the freebuff catalog needs a registered device key, then press Models.',
      });
    }
    try {
      return sendJson(res, 200, await fetchModelList(provider, account));
    } catch (err) {
      return sendJson(res, 502, { error: { message: `model list fetch failed: ${err.message}`, type: 'upstream_error' } });
    }
  }

  // ---- V2Ray / Xray exit pool -------------------------------------------
  if (a === 'proxies') {
    if (method === 'GET' && !b) return sendJson(res, 200, proxies.snapshot());
    if (method === 'GET' && b === 'events') return sendJson(res, 200, { events: proxies.events });
    if (method === 'POST' && b === 'sources') {
      const body = await readJsonBody(req);
      return sendJson(res, 200, { ok: true, sources: proxies.updateSources(body.sources) });
    }
    if (method === 'POST' && b === 'settings') {
      const body = await readJsonBody(req);
      return sendJson(res, 200, { ok: true, settings: proxies.updateSettings(body) });
    }
    if (method === 'POST' && b === 'refresh') {
      const body = await readJsonBody(req);
      const summary = await proxies.refresh({ sourceIds: body.sourceIds });
      return sendJson(res, 200, { ok: true, ...summary, snapshot: proxies.snapshot() });
    }
    if (method === 'POST' && b === 'test') {
      const body = await readJsonBody(req);
      const started = await proxies.testAll({
        limit: Number(body.limit) || 0,
        protocols: Array.isArray(body.protocols) ? body.protocols : null,
        retest: !!body.retest,
        sourceIds: Array.isArray(body.sourceIds) ? body.sourceIds : null,
      });
      return sendJson(res, started.error ? 409 : 200, started.error ? { error: { message: started.error } } : { ok: true, ...started });
    }
    if (method === 'POST' && b === 'apply') {
      const res2 = await proxies.apply();
      return sendJson(res, 200, { ok: true, ...res2, snapshot: proxies.snapshot() });
    }
    // Reinstall the client: new identity, session, project handle and client
    // version, with any cooldown cleared. `renew` stays as the historical path.
    if (method === 'POST' && b === 'provider' && c && (d === 'reinstall' || d === 'renew')) {
      const provider = findProvider(c);
      if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
      if (!isManagedProvider(provider)) return sendJson(res, 409, { error: { message: `provider "${provider.id}" does not manage its identities`, type: 'not_managed' } });
      const old = (provider.accounts || [])[0];
      const fresh = old ? reinstallManagedClient(provider, old, 'manual') : ensureManagedAccount(provider);
      await saveConfig();
      const as = fresh ? accountState(provider.id, fresh.id) : {};
      return sendJson(res, 200, {
        ok: true,
        account: fresh ? accountSummary(provider, fresh) : null,
        client: fresh ? {
          name: fresh.name,
          session: maskKey(as.sessionId || ''),
          project: as.projectId || 'global',
          userAgent: openCodeUA(provider, fresh),
          cooldownCleared: !(as.limitedUntil > now()),
        } : null,
        reinstalls: state.totals.reinstalls || 0,
      });
    }

    if (method === 'POST' && b === 'provider' && c && d === 'rotate-exit') {
      const provider = findProvider(c);
      if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
      if (provider.proxy !== 'rotate') return sendJson(res, 409, { error: { message: `provider "${provider.id}" is not using the rotating pool (proxy=${provider.proxy})`, type: 'not_rotating' } });
      const exit = proxies.rotate('manual (dashboard)', provider.id);
      return sendJson(res, 200, { ok: !!exit, exit: exit ? { id: exit.id, host: exit.host, exitIp: exit.exitIp, latencyMs: exit.latencyMs } : null });
    }

    if (method === 'POST' && b === 'rotate') {
      const exit = proxies.rotate('manual');
      return sendJson(res, 200, { ok: true, exit: exit ? { id: exit.id, host: exit.host, exitIp: exit.exitIp, latencyMs: exit.latencyMs } : null, index: proxies.index });
    }
    if (method === 'POST' && b === 'xray') {
      const body = await readJsonBody(req);
      if (body.xrayPath) proxies.updateSettings({ xrayPath: body.xrayPath });
      if (body.action === 'stop') { await proxies.stopMain(); return sendJson(res, 200, { ok: true, xray: proxies.xray }); }
      const started = await proxies.startMain();
      return sendJson(res, 200, { ok: true, ...started, xray: { running: proxies.xray.running, version: proxies.xray.version, error: proxies.xray.error } });
    }
    if (method === 'POST' && b === 'version') {
      const v = await proxies.version();
      return sendJson(res, 200, { ok: !v.error, ...v, path: proxies.state.settings.xrayPath });
    }
    if (method === 'GET' && b === 'exit' && c) {
      const exit = proxies.byId.get(c);
      if (!exit) return sendJson(res, 404, { error: { message: 'exit not found', type: 'not_found' } });
      return sendJson(res, 200, { ok: true, exit });
    }
    if (method === 'DELETE' && b === 'exit' && c) {
      proxies.state.exits = proxies.state.exits.filter((e) => e.id !== c);
      proxies.state.active = proxies.state.active.filter((id) => id !== c);
      proxies.reindex();
      proxies.save();
      return sendJson(res, 200, { ok: true });
    }
    if (method === 'POST' && b === 'purge') {
      const body = await readJsonBody(req);
      const status = body.status || 'dead';
      const before = proxies.state.exits.length;
      proxies.state.exits = proxies.state.exits.filter((e) => e.status !== status);
      proxies.state.active = proxies.state.active.filter((id) => proxies.byId.has(id));
      proxies.reindex();
      proxies.save();
      return sendJson(res, 200, { ok: true, removed: before - proxies.state.exits.length });
    }
  }

  // Exact path only: /freebuff/login/:id is the polling endpoint below, and the
  // snapshot route used to swallow it (the dashboard never saw an approval).
  if (method === 'GET' && a === 'freebuff' && !b) {
    return sendJson(res, 200, { ok: true, accounts: freebuff.snapshot() });
  }

  // freebuff has no API key to paste: the operator approves a device code.
  if (method === 'POST' && a === 'freebuff' && b === 'login') {
    const body = await readJsonBody(req);
    try {
      const session = await freebuff.startLogin();
      return sendJson(res, 200, { ok: true, session: { id: session.id, loginUrl: session.loginUrl, expiresAt: session.expiresAt, pollMs: 4000 } });
    } catch (err) {
      return sendJson(res, 502, { error: { message: err.message, type: 'login_failed' } });
    }
  }

  if (method === 'GET' && a === 'freebuff' && b === 'login' && c) {
    const session = freebuff.logins && freebuff.logins.get(c);
    if (!session) return sendJson(res, 404, { error: { message: 'login session not found (restart the flow)', type: 'not_found' } });
    try {
      const r = await freebuff.pollLogin(session);
      if (r.token) {
        const providerId = url.searchParams.get('provider') || '';
        const accountId = url.searchParams.get('account') || '';
        const provider = findProvider(providerId);
        const account = provider && (provider.accounts.find((x) => x.id === accountId) || provider.accounts[0]);
        if (!provider) return sendJson(res, 400, { error: { message: 'provider query parameter is required', type: 'bad_request' } });
        const label = (r.user && (r.user.email || r.user.name)) || '';
        if (account) {
          account.apiKey = r.token;
          delete account.keyless;
          if (label) account.name = label;
        } else {
          provider.accounts.push(normalizeAccount({ name: label || 'freebuff-login', apiKey: r.token }));
        }
        const st = freebuff.accountState(provider.id, (account || provider.accounts[provider.accounts.length - 1]).id);
        st.needsReauth = false;
        st.lastError = '';
        freebuff.logins.delete(c);
        await saveConfig();
        await freebuff.save();
        pushEvent({ kind: 'freebuff-login', pid: provider.id, s: 1 });
        return sendJson(res, 200, { ok: true, authorized: true, user: r.user || null, provider: providerSummary(provider) });
      }
      return sendJson(res, 200, { ok: true, authorized: false, pending: true, status: r.status, raw: r.raw });
    } catch (err) {
      return sendJson(res, 502, { error: { message: err.message, type: 'login_poll_failed' } });
    }
  }

  // One-shot connect flow for the dashboard catalogue: create/update the
  // provider and (optionally) its first account in a single call.
  if (method === 'POST' && a === 'connect') {
    const body = await readJsonBody(req);
    const incoming = body.provider && typeof body.provider === 'object' ? body.provider : {};
    const existing = findProvider(String(incoming.id || ''));
    const p = normalizeProvider(existing ? { ...existing, ...incoming, accounts: existing.accounts, id: existing.id } : incoming);
    touchProvider(p);
    const def = PROVIDER_TYPES[p.type] || PROVIDER_TYPES.custom;
    if (def.manageAccounts) {
      if (!(p.accounts || []).length) { const acc = newManagedAccount(p); p.accounts.push(acc); }
      await saveConfig();
      return sendJson(res, existing ? 200 : 201, { ok: true, provider: providerSummary(p), account: p.accounts[0] ? accountSummary(p, p.accounts[0]) : null });
    }
    const cred = String((body.account && body.account.apiKey) || '').trim();
    const givenName = String((body.account && body.account.name) || '').trim();
    let account = null;
    // The catalogue always sends an account object (with an empty key when the
    // provider needs no credential), so its presence is the intent to create one.
    if (body.account && typeof body.account === 'object') {
      const acc = normalizeAccount({
        name: givenName || (cred ? `${p.id}-1` : 'keyless'),
        apiKey: cred,
      });
      if (!cred) acc.keyless = true;
      p.accounts.push(acc);
      account = accountSummary(p, acc);
    }
    await saveConfig();
    persistState();
    pushEvent({ kind: 'connect', pid: p.id, s: 1 });
    return sendJson(res, existing ? 200 : 201, { ok: true, provider: providerSummary(p), account });
  }

  if (a === 'provider' && b && c === 'account' && d) {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
    const idx = provider.accounts.findIndex((x) => x.id === d);
    if (idx === -1) return sendJson(res, 404, { error: { message: 'account not found', type: 'not_found' } });

    if (method === 'POST' && e === 'reset') {
      markUnlocked(provider, provider.accounts[idx], 'manual');
      return sendJson(res, 200, { ok: true, account: accountSummary(provider, provider.accounts[idx]) });
    }
    if (method === 'PATCH') {
      const body = await readJsonBody(req);
      const acc = provider.accounts[idx];
      if (body.name != null) acc.name = String(body.name);
      if (body.apiKey) acc.apiKey = String(body.apiKey);
      if (body.enabled != null) acc.enabled = body.enabled !== false;
      if (body.rotateEveryMinutes != null) {
        const mins = Number(body.rotateEveryMinutes);
        if (mins > 0) acc.rotateEveryMinutes = mins; else delete acc.rotateEveryMinutes;
      }
      await saveConfig();
      return sendJson(res, 200, { ok: true, account: accountSummary(provider, acc) });
    }
    if (method === 'DELETE') {
      provider.accounts.splice(idx, 1);
      delete providerState(provider.id).accounts[d];
      await saveConfig();
      persistState();
      return sendJson(res, 200, { ok: true });
    }
  }

  if (a === 'provider' && b) {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
    if (method === 'PATCH') {
      const body = await readJsonBody(req);
      const merged = normalizeProvider({ ...provider, ...body, accounts: provider.accounts, id: provider.id });
      Object.assign(provider, merged);
      await saveConfig();
      return sendJson(res, 200, { ok: true, provider: providerSummary(provider) });
    }
    if (method === 'DELETE') {
      config.providers = config.providers.filter((p) => p.id !== provider.id);
      dropProviderState(provider.id);
      if (config.defaultProvider === provider.id) config.defaultProvider = '';
      await saveConfig();
      persistState();
      return sendJson(res, 200, { ok: true });
    }
  }

  if (method === 'POST' && a === 'reset') {
    let count = 0;
    for (const p of config.providers) {
      for (const acc of p.accounts) {
        const as = accountState(p.id, acc.id);
        if (isLimited(as)) { markUnlocked(p, acc, 'manual-all'); count += 1; }
        else if (b === 'counters') { as.requests = 0; as.failures = 0; as.limits = 0; }
      }
    }
    await saveConfig();
    return sendJson(res, 200, { ok: true, unlocked: count });
  }

  if (method === 'POST' && a === 'settings') {
    const body = await readJsonBody(req);
    if (body.adminKey) config.adminKey = String(body.adminKey);
    if (body.cooldownHours != null) config.cooldownHours = clampHours(body.cooldownHours);
    if (body.defaultProvider != null) config.defaultProvider = String(body.defaultProvider);
    if (body.cors != null) config.cors = !!body.cors;
    // Clearing inbound auth must be explicit: the dashboard never receives the
    // stored keys, so an empty list means "nothing typed", not "remove them".
    if (Array.isArray(body.proxyKeys) && (body.proxyKeys.length || body.clearProxyKeys === true)) {
      config.proxyKeys = body.proxyKeys.map(String).filter(Boolean);
    }
    if (body.loggingRequests != null) config.logging = { requests: !!body.loggingRequests };
    if (body.respectRetryAfter != null) config.respectRetryAfter = !!body.respectRetryAfter;
    if (body.errorCooldownMinutes != null) config.errorCooldownMinutes = Math.max(1, Number(body.errorCooldownMinutes) || 5);
    if (Array.isArray(body.cooldownStatuses) && body.cooldownStatuses.length) config.cooldownStatuses = body.cooldownStatuses.map(Number);
    await saveConfig();
    return sendJson(res, 200, { ok: true, settings: { cooldownHours: config.cooldownHours, defaultProvider: config.defaultProvider, cors: config.cors, cooldownStatuses: config.cooldownStatuses, hasProxyKeys: config.proxyKeys.length > 0, loggingRequests: !!(config.logging && config.logging.requests) } });
  }

  return sendJson(res, 404, { error: { message: `unknown admin route ${method} ${url.pathname}`, type: 'not_found' } });
}

function parseAccountLine(line) {
  const s = String(line).trim();
  if (!s) return { apiKey: '' };
  const m = /^([^:=\t]{1,64})[:\t|]\s*(\S{8,})$/.exec(s);
  if (m && !/^https?$/i.test(m[1].trim())) return normalizeAccount({ name: m[1].trim(), apiKey: m[2].trim() });
  return normalizeAccount({ name: `account-${crypto.randomBytes(3).toString('hex')}`, apiKey: s });
}

/**
 * Backup importer. Accepts this router's own export, a raw config object, or an
 * OmniRoute-style backup (providers/accounts with apiKey|key|token fields).
 * Best-effort by design: unknown shapes are reported, never guessed silently.
 */
function convertBackup(body) {
  const errors = [];
  const warnings = [];
  let root = body;
  if (root && typeof root === 'object' && root.config && typeof root.config === 'object') root = root.config;
  if (typeof root === 'string') { try { root = JSON.parse(root); } catch { errors.push('backup is not valid JSON'); return { errors, warnings }; } }
  if (!root || typeof root !== 'object') { errors.push('backup has no object payload'); return { errors, warnings }; }

  const rawProviders = Array.isArray(root.providers) ? root.providers
    : Array.isArray(root.accounts) ? [{ id: 'imported', name: 'Imported', type: 'openai', accounts: root.accounts }]
      : [];

  if (!rawProviders.length && Array.isArray(root.keys)) {
    rawProviders.push({ id: 'imported', name: 'Imported', type: 'openai', accounts: root.keys.map((k) => (typeof k === 'string' ? { apiKey: k } : k)) });
  }
  if (!rawProviders.length) { errors.push('no provider or account arrays found in backup'); return { errors, warnings }; }

  const providers = rawProviders.map((p, i) => {
    const typeHint = String(p.type || p.provider || p.kind || '').toLowerCase();
    let type = PROVIDER_TYPES[typeHint] ? typeHint : 'openai';
    let baseUrl = p.baseUrl || p.base_url || p.url || '';
    if (!PROVIDER_TYPES[typeHint]) {
      if (/openrouter/.test(typeHint)) type = 'openrouter';
      else if (/anthropic|claude/.test(typeHint)) type = 'anthropic';
      else if (/gemini|google/.test(typeHint)) type = 'gemini';
      else if (/openai/.test(typeHint)) type = 'openai';
      else { type = 'custom'; if (!baseUrl) warnings.push(`provider ${i + 1}: no baseUrl recognised, marked custom`); }
    }
    const rawAccounts = [].concat(p.accounts || p.keys || p.apiKeys || p.tokens || p.credentials || []).filter(Boolean);
    const accounts = rawAccounts.map((a, j) => {
      if (typeof a === 'string') return normalizeAccount({ name: `${p.name || type}-${j + 1}`, apiKey: a });
      return normalizeAccount({
        id: a.id,
        name: a.name || a.label || a.email || `${p.name || type}-${j + 1}`,
        apiKey: a.apiKey || a.key || a.token || a.secret || a.value || '',
        enabled: a.enabled ?? a.active ?? true,
      });
    }).filter((a) => a.apiKey);
    return normalizeProvider({
      id: p.id || p.slug || `${type}-${i + 1}`,
      name: p.name || PROVIDER_TYPES[type].label,
      type,
      baseUrl,
      strategy: p.strategy || p.routing || 'round-robin',
      cooldownHours: p.cooldownHours || root.cooldownHours,
      accounts,
    });
  }).filter((p) => p.accounts.length);

  if (!providers.length) { errors.push('backup contained providers but no usable API keys'); return { errors, warnings }; }

  const cfg = { providers };
  if (root.defaultProvider) cfg.defaultProvider = root.defaultProvider;
  if (root.cooldownHours) cfg.cooldownHours = clampHours(root.cooldownHours);
  if (root.adminKey) cfg.adminKey = root.adminKey;

  const summary = {
    mode: 'merge',
    providers: providers.length,
    accounts: providers.reduce((s, p) => s + p.accounts.length, 0),
    names: providers.map((p) => `${p.id}(${p.accounts.length})`),
  };
  return { errors, warnings, config: cfg, summary, replace: false };
}

function mergeImported(incoming) {
  for (const p of incoming.providers || []) {
    const existing = findProvider(p.id);
    if (!existing) { config.providers.push(p); continue; }
    const seen = new Set(existing.accounts.map((a) => a.apiKey));
    for (const acc of p.accounts) {
      if (seen.has(acc.apiKey)) continue;
      existing.accounts.push({ ...acc, id: uid('acc') });
      seen.add(acc.apiKey);
    }
  }
  if (incoming.cooldownHours) config.cooldownHours = clampHours(incoming.cooldownHours);
}

// ---------------------------------------------------------------------------
// Static dashboard
// ---------------------------------------------------------------------------

const STATIC_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.json': 'application/json; charset=utf-8',
  '.webmanifest': 'application/manifest+json',
};

function serveStatic(req, res, url) {
  const rel = url.pathname === '/' || url.pathname === '/admin' || url.pathname === '/admin/' ? 'index.html' : url.pathname.replace(/^\/+/, '');
  const target = path.join(PUBLIC_DIR, path.normalize(rel).replace(/^(\.\.[/\\])+/, ''));
  if (!target.startsWith(PUBLIC_DIR)) return sendJson(res, 403, { error: { message: 'forbidden', type: 'forbidden' } });
  const stream = fs.createReadStream(target);
  stream.once('open', () => {
    res.writeHead(200, {
      'content-type': STATIC_TYPES[path.extname(target).toLowerCase()] || 'application/octet-stream',
      'cache-control': 'no-cache',
    });
    stream.pipe(res);
  });
  stream.once('error', () => {
    if (!res.headersSent) sendJson(res, 404, { error: { message: 'not found', type: 'not_found' } });
    else res.end();
  });
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

const server = http.createServer(async (req, res) => {
  let url;
  try { url = new URL(req.url, 'http://localhost'); }
  catch { return sendJson(res, 400, { error: { message: 'bad request url', type: 'bad_request' } }); }

  if (req.method === 'OPTIONS') {
    const clientHeaders = 'authorization,content-type,x-api-key,x-goog-api-key,x-router-provider';
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,PATCH,DELETE,OPTIONS',
      'access-control-allow-headers': config.adminCors ? `${clientHeaders},x-admin-key` : clientHeaders,
      'access-control-max-age': '86400',
    });
    return res.end();
  }

  if (url.pathname === '/health') {
    const mem = process.memoryUsage();
    return sendJson(res, 200, {
      ok: true,
      version: PKG_VERSION,
      uptimeSeconds: Math.round((now() - state.startedAt) / 1000),
      rss: mem.rss,
      requests: state.totals.requests,
      build: buildInfo,
    });
  }

  if (url.pathname.startsWith('/admin/api/')) {
    res.__admin = true;
    handleAdmin(req, res, url).catch((err) => {
      console.error(`[admin] ${err.message}`);
      if (!res.headersSent) sendJson(res, err.statusCode || 500, { error: { message: err.message, type: 'admin_error' } });
      else res.end();
    });
    return;
  }

  if (url.pathname === '/admin' || url.pathname === '/admin/' || url.pathname === '/' || url.pathname.startsWith('/assets/')) {
    return serveStatic(req, res, url);
  }

  if (url.pathname === '/v1/models') {
    if (config.proxyKeys.length && !config.proxyKeys.includes(inboundKey(req, url))) {
      return sendJson(res, 401, { error: { message: 'invalid router API key', type: 'invalid_request_error' } });
    }
    let data = [];
    try { data = await routerModelList(); } catch { data = []; }
    if (!data.length) {
      data = config.providers.flatMap((p) => (p.accounts || []).filter((a) => a.enabled !== false).map((a) => ({
        id: `${p.id}/*`, object: 'model', owned_by: p.id, provider: p.id, type: p.type,
      })));
    }
    return sendJson(res, 200, { object: 'list', data });
  }

  if (url.pathname.startsWith('/v1/')) {
    handleProxy(req, res, url).catch((err) => {
      console.error(`[proxy] fatal: ${err.stack || err.message}`);
      if (!res.headersSent) sendJson(res, 500, { error: { message: 'internal router error', type: 'router_error' } });
      else res.destroy();
    });
    return;
  }

  return sendJson(res, 404, { error: { message: `no route for ${req.method} ${url.pathname}`, type: 'not_found' } });
});

server.keepAliveTimeout = 65_000;
server.headersTimeout = 70_000;
server.requestTimeout = 0; // streaming responses may run long

// ---------------------------------------------------------------------------
// Background maintenance: cooldown sweeper + memory sampler
// ---------------------------------------------------------------------------

function sweepCooldowns() {
  let unlocked = 0;
  for (const p of config.providers) {
    for (const acc of p.accounts) {
      const as = providerState(p.id).accounts[acc.id];
      if (as && as.limitedUntil && as.limitedUntil <= now()) {
        as.limitedUntil = 0;
        as.lastStatus = 0;
        state.totals.unlocks += 1;
        pushEvent({ kind: 'unlocked', pid: p.id, aid: acc.id, mode: 'expired' });
        unlocked += 1;
      }
    }
  }
  if (unlocked) {
    console.log(`[cooldown] auto-unlocked ${unlocked} account(s) after cooldown expiry`);
    persistState();
  }
}

function sampleMemory() {
  const mem = process.memoryUsage();
  state.mem.rss = mem.rss;
  state.mem.heapUsed = mem.heapUsed;
  state.mem.samples.push(Math.round(mem.rss / 1024));
  if (state.mem.samples.length > MEM_SAMPLES) state.mem.samples.shift();
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

function boot() {
  loadStateSync();
  const { file, created } = loadConfigSync();
  if (created) {
    saveConfig().catch((err) => console.error(`[config] could not write config.json: ${err.message}`));
    console.warn('[config] no config.json found - wrote defaults. Open /admin and add accounts.');
  }
  sampleMemory();
  proxies = new ProxyManager({ dataDir: DATA_DIR, log: (m) => console.warn(m) });
  proxies.init();
  freebuff = new FreebuffDriver({ stateFile: path.join(DATA_DIR, 'freebuff-state.json'), log: (m) => console.warn(m) });
  freebuff.init();
  server.listen(config.port, config.host, () => {
    const mem = process.memoryUsage();
    const where = `${config.host}:${config.port}`;
    console.log(`LLM Router v${PKG_VERSION} on node ${process.version}`);
    console.log(`  config:    ${file || CONFIG_PATH}`);
    console.log(`  state:     ${STATE_PATH}`);
    console.log(`  listening: http://${where}  (dashboard: /admin, API: /v1)`);
    console.log(`  providers: ${config.providers.length}, accounts: ${config.providers.reduce((s, p) => s + (p.accounts || []).length, 0)}`);
    console.log(`  cooldown:  ${clampHours(config.cooldownHours)}h on ${config.cooldownStatuses.join('/')}`);
    console.log(`  rss:       ${(mem.rss / 1048576).toFixed(1)} MB`);
    if (config.adminKey === 'changeme') console.warn('  WARNING: adminKey is the default "changeme" - set ADMIN_KEY or edit config.json');
  });

  const sweeper = setInterval(sweepCooldowns, SWEEP_MS);
  const sampler = setInterval(sampleMemory, MEM_SAMPLE_MS);
  if (sweeper.unref) sweeper.unref();
  if (sampler.unref) sampler.unref();

  // Bring the exit pool up in the background; a missing xray binary must never
  // stop the router from serving direct traffic.
  if (proxies.state.settings.enabled && proxies.activeExits().length) {
    proxies.startMain()
      .then((r) => { if (!r.running) console.warn(`[proxy] pool not started: ${r.error || r.reason}`); })
      .catch((err) => console.warn(`[proxy] pool start failed: ${err.message}`));
  } else if (proxies.state.settings.enabled) {
    console.warn('[proxy] no working exits yet - open the dashboard Network tab and run a proxy test');
  }
}

async function shutdown(signal) {
  console.log(`\n[shutdown] ${signal} - flushing state`);
  try {
    if (persistTimer) clearTimeout(persistTimer);
    persistTimer = null;
    await lastStateWrite;
    await atomicWrite(STATE_PATH, `${JSON.stringify(state)}\n`);
  } catch (err) {
    console.error(`[shutdown] state flush failed: ${err.message}`);
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2_000).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', (err) => console.error(`[unhandledRejection] ${err && err.stack ? err.stack : err}`));
process.on('uncaughtException', (err) => console.error(`[uncaughtException] ${err && err.stack ? err.stack : err}`));

if (require.main === module) boot();

module.exports = { server, config, state, selectAccount, convertBackup, normalizeConfig, markLimited, normalizeProvider };
