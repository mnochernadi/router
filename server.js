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
const { OpenCodeCliDriver } = require('./opencode-cli-driver');

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
  // The genuine CLI as the transport. The free tier's gate is on the *client*: the
  // exact request the real binary sends answers 429 from node/bun/curl/browser
  // fingerprints and 200 from the binary itself (measured on this box), so the only
  // keyless way through is to let the binary make the call. No key, no account.
  'opencode-cli': {
    label: 'OpenCode CLI (keyless, uses the real client)',
    baseUrl: '',
    style: 'opencode-cli',
    keyless: true,
    defaultTimeoutMs: 300_000,
  },
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

function sendJson(res, status, obj, extra = null) {
  const payload = JSON.stringify(obj);
  // Proxy answers are CORS-open (clients call them from browsers); the admin
  // API is not, unless the operator opts in -- it takes the x-admin-key header.
  const cors = res.__admin ? !!config.adminCors : !!config.cors;
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
    ...(cors ? { 'access-control-allow-origin': '*' } : {}),
    ...(extra || {}),
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
    // Exclusive mode: the id (or type) of the only provider this router serves.
    // See `exclusiveProvider()` -- empty means the normal multi-provider router.
    exclusiveProvider: '',
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
  // Lane identity is positional state, not operator input, and it must survive a
  // save/load cycle: it is what gives an identity its own slot and its own exit
  // pin. Dropping it collapsed every lane onto lane 0 on the next restart (one
  // exit, one slot, and a pool that rotates nothing), which is the opposite of
  // what `identityPool` is for. `gen` rides along so the dashboard can show how
  // many identities a lane has been through.
  if (Number.isFinite(Number(a.lane)) && Number(a.lane) >= 0) out.lane = Number(a.lane);
  if (Number(a.gen) > 0) out.gen = Number(a.gen);
  // A signed-in lane's credential and its refresh material are the whole reason it
  // exists (its own principal, its own daily allowance), so they must round-trip.
  if (a.signedIn) {
    out.signedIn = true;
    if (a.refreshToken) out.refreshToken = String(a.refreshToken);
    if (Number(a.tokenExpiresAt) > 0) out.tokenExpiresAt = Number(a.tokenExpiresAt);
    if (a.email) out.email = String(a.email);
    if (a.orgId) out.orgId = String(a.orgId);
  }
  return out;
}

function normalizeProvider(p) {
  const type = PROVIDER_TYPES[p.type] ? p.type : 'custom';
  const def = PROVIDER_TYPES[type];
  // The free tier's allowance is accounted per model *and* per IP (measured: a
  // fresh server IP answered deepseek-v4.1-flash-free while the old IP was 429
  // for hours), so the operator may route it through the exit pool on purpose.
  // Default stays 'direct': rotating this provider costs its session affinity
  // and previously ended in TLS resets, so it is an explicit choice, not a
  // default. `proxy: 'rotate'` plus the dashboard's Next IP does the rest.
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
    proxy: String(p.proxy || 'direct'),
    // Per-model fallback inside the free tier (see OPENCODE_FREE_FALLBACK_MODELS).
    // Default on for opencode-free; `freeFallback: false` turns it off, and
    // `freeFallbackModels` overrides the order.
    freeFallback: p.freeFallback === false ? false : undefined,
    freeFallbackModels: Array.isArray(p.freeFallbackModels) && p.freeFallbackModels.length
      ? p.freeFallbackModels.map(String).filter(Boolean)
      : undefined,
    // Cross-provider last resort: [{ provider: 'pollinations', model: 'openai' }]
    fallbackProviders: Array.isArray(p.fallbackProviders) && p.fallbackProviders.length ? p.fallbackProviders : undefined,
    // opencode-cli transport: where the genuine binary lives and how to run it.
    cliPath: p.cliPath ? String(p.cliPath) : undefined,
    cliHome: p.cliHome ? String(p.cliHome) : undefined,
    cliArgs: Array.isArray(p.cliArgs) && p.cliArgs.length ? p.cliArgs.map(String) : undefined,
    // 'serve' prefers a long-lived `opencode serve`; 'spawn' runs a fresh binary
    // per request. Both are the genuine client as far as the gate is concerned.
    cliMode: p.cliMode === 'serve' || p.cliMode === 'spawn' ? p.cliMode : undefined,
    cliServeUrl: p.cliServeUrl ? String(p.cliServeUrl) : undefined,
    // Time-boxed rotation: keep one account for N minutes, then switch. 0 = off.
    rotateEveryMinutes: Number(p.rotateEveryMinutes) > 0 ? Number(p.rotateEveryMinutes) : 0,
    // Free-tier identity lanes: how many identities run in parallel, each with
    // its own exit IP. Clamped to 1..6; 0/unset means the default (4).
    identityPool: Number(p.identityPool) > 0 ? Math.min(6, Number(p.identityPool)) : undefined,
    // Context window advertised to clients for this provider (tokens). The free
    // tier holds ~900KB, so the default is a full million.
    contextWindow: Number(p.contextWindow) > 0 ? Math.round(Number(p.contextWindow)) : undefined,
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
  // Operator dial on the free-tier ladder, in seconds (see freeProviderBudgetMs()).
  // Absent, 0 or nonsense keeps the built-in 25s cold / 20s dry pair; anything else
  // is clamped to 3..120s so a fat-fingered value cannot disable the ladder outright
  // or hold a caller for ten minutes.
  const ladderSeconds = clampLadderSeconds(out.freeLadderBudgetSeconds);
  if (ladderSeconds) out.freeLadderBudgetSeconds = ladderSeconds;
  else delete out.freeLadderBudgetSeconds;
  // Floor between "renew every lane" sweeps, in minutes. Absent keeps the built-in
  // minute; 0 is meaningful ("sweep on every limit") and so is kept, unlike a zero
  // ladder dial. Clamped to a day so a typo cannot park the sweep for a week.
  if (out.allLanesRenewGapMinutes === undefined || out.allLanesRenewGapMinutes === null || out.allLanesRenewGapMinutes === '') {
    delete out.allLanesRenewGapMinutes;
  } else {
    const sweepMinutes = Number(out.allLanesRenewGapMinutes);
    if (Number.isFinite(sweepMinutes) && sweepMinutes >= 0) out.allLanesRenewGapMinutes = Math.min(1440, sweepMinutes);
    else delete out.allLanesRenewGapMinutes;
  }
  // Identity burst: while several requests are in the free tier's ladder, each
  // gets its own identity instead of queueing behind a lane parked for a rebuild.
  // Default on; `false` restores the strictly one-request-at-a-time ladder (every
  // limit sweeps all lanes and a spent lane waits out its park).
  out.identityBurst = out.identityBurst !== false;
  // What counts as "the day is spent" rather than "this exit/identity is spent".
  // Absent keeps the measured 30 minutes; 0 is meaningful and means *never park* -
  // every limit runs the ladder (rotate the exit, mint an identity, walk the pool)
  // instead of being parked until the upstream's reset instant. Clamped to a day.
  if (out.freeDayHorizonMinutes === undefined || out.freeDayHorizonMinutes === null || out.freeDayHorizonMinutes === '') {
    delete out.freeDayHorizonMinutes;
  } else {
    const horizonMinutes = Number(out.freeDayHorizonMinutes);
    if (Number.isFinite(horizonMinutes) && horizonMinutes >= 0) out.freeDayHorizonMinutes = Math.min(1440, horizonMinutes);
    else delete out.freeDayHorizonMinutes;
  }
  // How long the pool rests after a ladder that could not clear a ceiling.
  // Absent keeps the escalating 5m -> 2h backoff; 0 means *never rest* - the next
  // request runs the whole ladder again instead of being told to wait. A positive
  // value pins a fixed window. Clamped to a day.
  if (out.freePoolRestMinutes === undefined || out.freePoolRestMinutes === null || out.freePoolRestMinutes === '') {
    delete out.freePoolRestMinutes;
  } else {
    const restMinutes = Number(out.freePoolRestMinutes);
    if (Number.isFinite(restMinutes) && restMinutes >= 0) out.freePoolRestMinutes = Math.min(1440, restMinutes);
    else delete out.freePoolRestMinutes;
  }
  // How long the last resort keeps retrying before the caller is told the truth.
  // Unset/nonsense keeps the built-in 30s; clamped to 1s..5min so a typo cannot hold
  // a request for an hour (or give up before a single retry).
  if (out.fallbackRetrySeconds === undefined || out.fallbackRetrySeconds === null || out.fallbackRetrySeconds === '') {
    delete out.fallbackRetrySeconds;
  } else {
    const retrySeconds = Number(out.fallbackRetrySeconds);
    if (Number.isFinite(retrySeconds) && retrySeconds > 0) out.fallbackRetrySeconds = Math.min(300, Math.max(1, retrySeconds));
    else delete out.fallbackRetrySeconds;
  }
  out.providers = Array.isArray(out.providers) ? out.providers.map(normalizeProvider) : [];
  // Exclusive mode: one provider, and nothing else in the router.
  //
  // `exclusiveProvider` is a hard lock, not a preference. A deployment whose
  // client asks for one free tier (`opencode-free/deepseek-v4.1-flash-free`)
  // must never be answered by a different vendor, so at load time every other
  // provider is dropped and the cross-provider fallback list is thrown away.
  // The request path enforces the same rule again (resolveProvider refuses an
  // unknown provider prefix instead of picking a healthy one), so a config that
  // is edited behind the router's back still cannot switch the answer.
  const locked = String(out.exclusiveProvider || '').trim();
  if (locked) {
    const target = out.providers.find((p) => p.id === locked) || out.providers.find((p) => p.type === locked);
    if (!target) {
      console.warn(`[config] exclusiveProvider "${locked}" is not a configured provider; ignoring the lock`);
      delete out.exclusiveProvider;
    } else {
      const dropped = out.providers.filter((p) => p !== target).map((p) => p.id);
      if (dropped.length) console.warn(`[config] exclusive mode on "${target.id}": ignoring provider(s) ${dropped.join(', ')}`);
      if (target.fallbackProviders) console.warn(`[config] exclusive mode on "${target.id}": cross-provider fallbacks are not used`);
      out.exclusiveProvider = target.id;
      out.providers = [target];
      delete target.fallbackProviders;
      out.defaultProvider = target.id;
    }
  }
  if (process.env.ADMIN_KEY) out.adminKey = process.env.ADMIN_KEY;
  return out;
}

let config = defaultConfig();

// V2Ray/Xray exit pool + freebuff session bookkeeping. Both are optional: with
// no xray binary and no freebuff accounts the router behaves exactly as before.
let proxies = null;
let freebuff = null;
// The genuine CLI, when a provider uses the `opencode-cli` transport.
let opencodeCli = null;
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
    // Round-trip the accounts through the loader's own shape instead of a hand
    // picked subset: the subset dropped `lane`, `gen` and `keyless`, so the identity
    // pool write-back lost the lane indices on every save and a restart collapsed
    // four lanes into lane 0 - one exit pin, one slot, a pool that rotates nothing.
    accounts: (p.accounts || []).map((a) => normalizeAccount(a, a.name || 'account')),
  }));
  await atomicWrite(CONFIG_PATH, `${JSON.stringify(serializable, null, 2)}\n`);
  try { await fsp.chmod(CONFIG_PATH, 0o600); } catch { /* best effort (windows) */ }
}

function findProvider(id) {
  return config.providers.find((p) => p.id === id) || null;
}

/**
 * The one provider this router is locked to, or null.
 *
 * Set by `exclusiveProvider` in config.json. When it is set the router serves
 * that provider and only that provider: the loader drops the others, a request
 * naming another provider is refused, the cross-provider fallback is off, and
 * the admin API will not add one back. The point is a client that pins
 * `opencode-free/<model>` and must never be answered by another vendor.
 */
function exclusiveProvider() {
  return config.exclusiveProvider ? findProvider(config.exclusiveProvider) : null;
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
    const as = accountState(provider.id, a.id);
    // A lane under rebuild has no valid identity yet.
    if (as.refreshing) return false;
    return !isLimited(as);
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
    lane: account.lane === undefined ? 0 : Number(account.lane),
    gen: Number(as.gen) || 1,
    build: openCodeUA(provider, account),
    refreshing: !!as.refreshing,
    exit: (proxies && proxies.state.lanes[provider.id] ? proxies.state.lanes[provider.id][laneKeyOf(account)] : '') || '',
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

// The `opencode serve` health, cached: the dashboard asks for state on every poll
// and this transport is a process that is either there or not.
const cliServeHealth = { at: 0, ok: false, url: '' };

async function cliServeStatus(provider) {
  const url = String(provider.cliServeUrl || defaultCliServeUrl());
  if (Date.now() - cliServeHealth.at < 15_000 && cliServeHealth.url === url) return cliServeHealth.ok;
  let ok = false;
  try {
    const res = await fetch(`${url.replace(/\/+$/, '')}/api/health`, { signal: AbortSignal.timeout(2500) });
    ok = res.ok;
  } catch { ok = false; }
  cliServeHealth.at = Date.now();
  cliServeHealth.url = url;
  cliServeHealth.ok = ok;
  return ok;
}
/**
 * What the dashboard shows for a CLI-transport provider.
 *
 * The provider has no accounts and no base URL, so the generic card renders empty -
 * which is exactly what an operator saw after this transport landed: "there is
 * nothing in the dashboard". This block is the transport's own panel.
 */
function cliSummary(provider) {
  const style = (PROVIDER_TYPES[provider.type] || {}).style;
  if (style !== 'opencode-cli') return undefined;
  const pool = cliIdentityPool(provider);
  const identities = [];
  for (let i = 1; i <= pool; i += 1) {
    const home = cliIdentityHome(provider, i);
    const warm = fs.existsSync(path.join(home, '.local', 'share', 'opencode', 'opencode.db'));
    identities.push({ index: i, home, warm });
  }
  const mode = provider.cliMode === 'serve' || (provider.cliMode !== 'spawn' && provider.cliServeUrl) ? 'serve' : 'spawn';
  const serveUrl = String(provider.cliServeUrl || defaultCliServeUrl());
  const known = cliServeHealth.url === serveUrl && cliServeHealth.at > 0;
  // Keep the dashboard's answer fresh without making the state payload wait on a
  // network call: refresh in the background, report what is known right now.
  if (!known || Date.now() - cliServeHealth.at > 15_000) {
    cliServeStatus(provider).catch(() => {});
  }
  return {
    mode,
    serveUrl,
    serveHome: String(provider.cliServeHome || defaultCliServeHome()),
    serveOk: known ? cliServeHealth.ok : null,
    cliPath: String(provider.cliPath || defaultCliPath()),
    cliPathPresent: fs.existsSync(String(provider.cliPath || defaultCliPath())),
    identityPool: pool,
    identities,
    warmCount: identities.filter((x) => x.warm).length,
    cooldownUntil: cliDry.until > now() ? cliDry.until : 0,
    cooldownSeconds: cliDry.until > now() ? Math.round((cliDry.until - now()) / 1000) : 0,
    cooldownMinutes: Math.round(cliLimitCooldownMs() / 60_000),
    ladderBudgetMs: (() => {
      const raw = Number(config.cliLadderBudgetMs);
      return Number.isFinite(raw) && raw > 0 ? raw : 45_000;
    })(),
    attempts: cliDry.hits || 0,
  };
}

function providerSummary(provider) {
  const accounts = (provider.accounts || []).map((a) => accountSummary(provider, a));
  return {
    id: provider.id,
    name: provider.name,
    type: provider.type,
    cli: cliSummary(provider),
    baseUrl: provider.baseUrl,
    strategy: provider.strategy,
    cooldownHours: clampHours(provider.cooldownHours ?? config.cooldownHours),
    timeoutMs: provider.timeoutMs,
    maxAttempts: provider.maxAttempts || 0,
    hasCustomHeaders: !!provider.headers,
    hasModelStyles: !!provider.modelStyles,
    rotateEveryMinutes: Number(provider.rotateEveryMinutes) || 0,
    managed: isManagedProvider(provider),
    // Identities run in parallel lanes, each on its own exit IP: a spent lane is
    // rebuilt while the others keep answering.
    identityPool: managedPoolSize(provider),
    // When every lane was last given a new identity at once (0 = never). The floor
    // between those sweeps is `allLanesRenewGapMinutes` in the snapshot.
    allLanesSweptAt: Number(providerState(provider.id).allLanesSweptAt) || 0,
    // How many requests are inside this provider's free-tier ladder right now. More
    // than one and identity rotation mints per request instead of parking lanes
    // (see "Identity burst" above); the dashboard shows it as "N in the ladder".
    demand: freeDemandOf(provider.id),
    // Advertised context window; clients read it and stop trimming at 8k.
    contextWindow: contextWindowOf(provider),
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
  const locked = exclusiveProvider();
  const hinted = req.headers['x-router-provider'];
  if (hinted) {
    const byId = findProvider(String(hinted));
    if (byId) return { provider: byId, model: body?.model };
    const byType = config.providers.find((p) => p.type === hinted);
    if (byType) return { provider: byType, model: body?.model };
    // Exclusive mode: a header naming a provider that is not configured is an
    // error, never a hint to pick somebody else.
    if (locked) return { provider: null, model: body?.model, unknown: String(hinted) };
  }
  const model = typeof body?.model === 'string' ? body.model : '';
  if (model.includes('/')) {
    const [prefix, ...rest] = model.split('/');
    const byId = findProvider(prefix);
    if (byId) return { provider: byId, model: rest.join('/') };
    // `vendor/model` with no such vendor: in exclusive mode this is the exact
    // shape that used to fall through to "first healthy provider" and get the
    // answer from a different vendor. Refuse it instead.
    if (locked) return { provider: null, model, unknown: prefix };
  }
  if (locked) return { provider: locked, model };
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
    // A lane that signed in is a different *principal* with its own daily allowance,
    // so it carries its own credential. The anonymous path keeps the literal "public"
    // - one shared bucket - and the credential goes where the CLI puts it: the body's
    // `apiKey` (measured in opencode-linux-x64@1.19.4's bundle) as well as the header.
    const token = usableKey(account.apiKey);
    if (token) payload.apiKey = token;
    return {
      style, responseStyle: 'openai',
      url: `${String(baseUrl || '').replace(/\/+$/, '')}${ctx.suffix || '/chat/completions'}`,
      headers: {
        'content-type': 'application/json',
        accept: 'text/event-stream',
        authorization: `Bearer ${token || 'public'}`,
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

async function callUpstream(provider, account, ctx, limits = {}) {
  // A signed-in lane's access token is short lived; renew it before it is used
  // rather than discovering the 401 halfway through a ladder.
  if (account && account.refreshToken && Number(account.tokenExpiresAt) > 0
    && Number(account.tokenExpiresAt) - Date.now() < SIGNIN_REFRESH_MARGIN_MS) {
    await refreshSignedInLane(provider, account).catch(() => {});
  }
  const spec = buildUpstream(provider, account, ctx);
  const controller = new AbortController();
  const timeoutMs = Number(provider.timeoutMs) > 0 ? Number(provider.timeoutMs) : 30_000;
  // The free tier hands in its own, shorter limits: the first byte has to arrive
  // inside the ladder's remaining budget, not inside the provider's whole-answer
  // timeout, or a single stalled exit eats the minute on its own.
  const headMs = Math.max(1_000, Math.min(timeoutMs, Number(limits.headMs) || timeoutMs));
  const timer = setTimeout(() => controller.abort(new Error('upstream timeout')), headMs);
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
  // The host's own IP can be a clean one (measured: a fresh server IP answered
  // models that were 429 from an older IP for hours), so a free-tier request
  // starts direct and only moves into the exit pool after a quota answer - that
  // is what ctx.forceDirect tracks and what the quota branch clears. Every other
  // provider picks from the pool exactly as configured.
  const picked = proxies && !ctx.forceDirect
    ? (ctx.lane !== undefined ? proxies.pickLane(provider, ctx.lane) : proxies.pick(provider))
    : null;
  let upstream;
  try {
    if (picked) {
      try {
        upstream = await proxiedRequest(url, {
          method: ctx.method === 'GET' ? 'GET' : 'POST',
          headers,
          body: payload,
          proxy: picked.proxy,
          // A dead exit accepts the local TCP connect and then stalls the
          // CONNECT/TLS/head path, so the pool path gets a first-byte deadline of
          // its own instead of the provider's whole-response timeout. A healthy
          // exit answers its headers in ~1-3s (measured). `idleMs` covers the
          // body for buffered (non-streaming) callers and is zero for streams,
          // which the client is already draining.
          timeoutMs: headMs,
          idleMs: ctx.aggregate ? Math.max(5_000, Math.min(EXIT_BODY_STALL_MS, Number(limits.bodyIdleMs) || EXIT_BODY_STALL_MS)) : 0,
          insecure: !!proxies.state.settings.insecure,
          signal: controller.signal,
        });
      } catch (err) {
        err.exit = picked.exit;
        err.exitStall = err.stall === true || /timeout|stalled/i.test(err.message || '');
        throw err;
      }
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
    // Pass the upstream's own clock through, not just its sentence: a client that
    // never sees retry-after will re-ask within milliseconds and burn the ladder.
    // When the free-day ceiling is the longer story, the ceiling wins - it is the
    // same instant every lane reports, and the only clock worth sleeping on.
    const upstreamWait = Math.ceil(retryAfterMs(upstream.headers) / 1000);
    const ceilingWait = freeDry.until > now() ? Math.round((freeDry.until - now()) / 1000) : 0;
    const waitSecs = Math.max(upstreamWait, ceilingWait);
    if (waitSecs > 0) headers['retry-after'] = String(waitSecs);
    res.writeHead(upstream.status, headers);
    res.end(JSON.stringify({ error: { message: `${message}${hint}`, type, upstreamStatus: upstream.status, provider: provider.id } }));
    return;
  }

  // ---- upstream streams, client asked for one JSON object ------------------
  // Only when the upstream really speaks SSE. Pollinations and friends answer a
  // non-streaming request with plain JSON; running the SSE aggregator over that
  // produced a well-formed but *empty* assistant message.
  const upstreamType = String(upstream.headers.get('content-type') || '').toLowerCase();
  if (ctx.aggregate && upstream.body && upstream.status < 300 && upstreamType.includes('event-stream')) {
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
  headers['x-router-model'] = String(ctx.model || '');
  headers['x-router-attempts'] = String(attempts);
  headers['x-router-exit'] = ctx.exitId || 'direct';
  headers['x-router-rotations'] = String(ctx.rotations || 0);
  const len = upstream.headers.get('content-length');
  if (!ctx.stream && len) headers['content-length'] = len;
  if (upstream.status >= 400) {
    const upstreamWait = Math.ceil(retryAfterMs(upstream.headers) / 1000);
    if (upstreamWait > 0) headers['retry-after'] = String(upstreamWait);
  }
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
  // The parked free tier has one unlock instant for the whole provider - the daily
  // reset the upstream named - and it applies even to a lane that never answered a
  // 429 itself. Reporting it is what lets a client back off to the right moment
  // instead of retrying on a 12h default (or being told "try again" with no figure).
  const parkedUntil = (PROVIDER_TYPES[provider.type] || PROVIDER_TYPES.custom).style === 'opencode-free' && freeTierParked()
    ? freeDry.until
    : 0;
  const nextUnlock = parkedUntil || accounts
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
      unlocksInSeconds: parkedUntil
        // The free tier's park is one instant for the whole provider. Reporting a
        // lane's own 1-second park next to a multi-hour `retry-after` is what made
        // the exhausted-provider answer read as "cooling down" on a spent day.
        ? Math.max(0, Math.round((parkedUntil - now()) / 1000))
        : (as.limitedUntil > now() ? Math.round((as.limitedUntil - now()) / 1000) : 0),
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
            ? (parkedUntil
              // The account is not "cooling down". The router measured the upstream
              // window at >= FREE_DAY_HORIZON_MS and positively identified the
              // *day*: `parkedUntil` is the reset instant the upstream named. Say
              // that, and name the instant, instead of reporting a 1-second lane
              // park next to a 9-hour `retry-after` and sending the caller looking
              // for a rotation that cannot exist.
              ? `Provider "${provider.id}" has spent its upstream free allowance - the ceiling is the day, not this box. Every egress IP, identity, client build and model this router can mint was already measured against it, and the allowance is not keyed on any of them, so rotation cannot clear it. It returns at ${new Date(parkedUntil).toISOString()}.`
              : `All accounts for provider "${provider.id}" are rate-limited and cooling down.`)
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
// A limit is answered by minting a new identity on *every* lane at once (see
// queueAllLaneRefreshes), not only on the lane that reported it. Without a floor a
// burst of cold requests would mint a fresh set for every 429 the ladder collects,
// so one sweep per episode is allowed by default.
// `allLanesRenewGapMinutes: 0` in config.json means "sweep on every limit".
const ALL_LANES_RENEW_GAP_MS = 60_000;
const MANAGED_BACKOFF_MS = 60_000;
// Free-tier identity lanes.
//
// One identity means one IP means one allowance, and an allowance that runs out
// stops the operator until it refreshes. So the free tier runs several identities
// in parallel, each pinned to its own exit IP and its own client build: while one
// lane answers 429 the request simply continues on the next, and the spent lane
// is rebuilt in the background (new IP, new identity, new build). Nothing here
// touches Marzban - lanes are plain xray exits from the pool above.
const MANAGED_POOL_DEFAULT = 4;
const MANAGED_POOL_MAX = 6;
// A lane that ran out is parked for this long while it is rebuilt, so it is not
// picked again in the middle of the rebuild.
const LANE_REFRESH_COOLDOWN_MS = 30_000;
// How long a request may wait for a replacement lane when every lane is spent.
const LANE_WAIT_MS = 12_000;

// Identity burst.
//
// The all-lane sweep is written for the *next* request: one limit, four fresh
// identities waiting. It is the wrong answer for the requests that are already
// here. A sweep parks every lane for `LANE_REFRESH_COOLDOWN_MS` and its floor
// throttles the next one to `allLanesRenewGapMinutes`, so a client that sends
// several requests at once (an agent fanning out, two panes, a retry loop) has
// them serialising behind one parked lane, waiting out an interval that exists to
// protect the pool, and collecting the same 429 the ladder was built to clear.
//
// So identity rotation is demand driven as well. While more than one request is
// in the free tier's ladder:
//   - a spent lane is rebuilt in place and stays in service instead of being
//     parked for half a minute (its identity is local state: burn the exit, drop
//     the pin, mint a session, pin another exit - no network, no waiting);
//   - the all-lane sweep stands down, because those lanes are in other requests'
//     hands and taking them away is what starved them;
//   - a request that finds every lane parked mints an identity on the lane that
//     has waited longest, instead of sitting out `LANE_WAIT_MS` and then failing.
// Each request mints at most `LANE_BURST_MINT_MAX` identities per lane, so a
// burst of N requests costs O(N) identities - never a mint storm - and a single
// request behaves exactly as it did before this existed.
const LANE_BURST_MINT_MAX = 2;

/** How many requests are inside the free tier's ladder right now, per provider. */
const freeDemand = new Map();

function freeDemandOf(providerId) {
  return freeDemand.get(providerId) || 0;
}

function noteFreeDemand(providerId, delta) {
  const next = Math.max(0, freeDemandOf(providerId) + delta);
  if (next) freeDemand.set(providerId, next);
  else freeDemand.delete(providerId);
  return next;
}

/** The kill switch: `identityBurst: false` restores the one-request-at-a-time ladder. */
function identityBurstEnabled() {
  return config.identityBurst !== false;
}

/** More than one request in the ladder: rotation has to serve all of them. */
function freeDemandConcurrent(providerId) {
  return identityBurstEnabled() && freeDemandOf(providerId) > 1;
}
// How long a pooled exit gets to produce response headers before it counts as
// dead and the request moves on. Measured: healthy exits answer in 1-3s; a
// burned node accepts the connect and then never answers.
const EXIT_FIRST_BYTE_MS = 25_000;
// ...and the same for the body: an exit that hands over a head and then stops
// sending must be dropped instead of holding an aggregated answer forever.
const EXIT_BODY_STALL_MS = 20_000;

// The whole free-tier escalation (new IP -> new identity -> next lane -> model
// walk) has to finish inside a minute; past that the caller gets the fallback
// provider. The provider ladder stops earlier than the response deadline so the
// fallback hop still has time to answer inside the minute the caller was promised.
const FREE_LIMIT_BUDGET_MS = 60_000;
// `freeLadderBudgetSeconds` is the operator's dial on the cold-request ladder: 25s
// is right while the free tier still answers, but when the upstream allowance is
// spent the same 25s is spent proving it on every cold request. Deployments sitting
// on a dry ceiling set it to 6-8s and the fallback provider answers instead.
//
// Read per request, never captured in a module-level constant: at this point in the
// file `config` is still the compiled-in default (loadConfigSync() runs at the
// bottom), so a frozen value would silently ignore the operator's dial and keep
// spending the old 25s.
function freeProviderBudgetMs() {
  return clampBudgetMs(config.freeLadderBudgetSeconds, 25_000);
}
// Same ladder, shorter leash, for a provider whose lanes are *already* all spent
// or rebuilding when the request arrives: the rungs left are a formality (the
// ceiling is upstream, not local), and the caller should reach the fallback in
// about the time a cold answer takes instead of riding out the full budget.
function freeProviderDryBudgetMs() {
  return clampBudgetMs(config.freeLadderBudgetSeconds, 20_000, 0.8);
}
// How long the pool is left alone once two lanes have produced the same free-tier
// quota answer: the ceiling is on the upstream account, so rotating exits and
// minting identities only burns addresses that the next real request needs.
//
// The window grows on every consecutive dry probe (5m -> 15m -> 45m -> 2h): a spent
// allowance does not come back in five minutes, and each 5-minute expiry used to
// cost a real request a full ladder walk plus several exits. Any 2xx from the
// provider resets it. It is persisted, so a restart does not send the first request
// straight back into the ladder.
const FREE_DRY_BASE_MS = 5 * 60_000;
const FREE_DRY_MAX_MS = 2 * 60 * 60_000;
const freeDry = { hits: 0, until: 0, backoffMs: FREE_DRY_BASE_MS, ...(state.freeDry || {}) };

// The genuine CLI's allowance is its own bucket: it is the one client the gate
// serves when everyone else is refused, so a spent *anonymous API* window says
// nothing about it (and vice versa). Kept apart on purpose - sharing `freeDry` made
// the CLI transport inherit a park earned by forged requests and answer 429 without
// ever running the binary.
const cliDry = { until: 0, hits: 0, ...(state.cliDry || {}) };

function parkCliTier(retryMs, why) {
  const until = now() + Math.max(60_000, Number(retryMs) || 0);
  if (cliDry.until >= until) return false;
  cliDry.hits += 1;
  cliDry.until = until;
  state.cliDry = { until: cliDry.until, hits: cliDry.hits };
  persistState();
  console.warn(`[cli] opencode CLI reports the free allowance is spent (${why ? `"${String(why).slice(0, 90)}"` : 'no detail'}) - parked until ${new Date(until).toISOString()}`);
  pushEvent({ kind: 'cli-day-ceiling', until });
  return true;
}

function clearCliDry(why) {
  if (!cliDry.until && !cliDry.hits) return;
  cliDry.until = 0;
  cliDry.hits = 0;
  delete state.cliDry;
  persistState();
  console.warn(`[cli] the opencode CLI answered again - dry window cleared${why ? ` (${why})` : ''}`);
}

/**
 * How long the pool rests after a ceiling the ladder could not clear.
 * `null` means resting is switched off (`freePoolRestMinutes: 0`): the operator's
 * choice is to keep rotating on every request rather than wait, so the ladder is
 * always live. A positive `freePoolRestMinutes` pins that window; otherwise the
 * escalating backoff is used.
 */
function poolRestMs() {
  const raw = config.freePoolRestMinutes;
  if (raw === undefined || raw === null || raw === '') return freeDry.backoffMs;
  const minutes = Math.max(0, Number(raw) || 0);
  return minutes > 0 ? minutes * 60_000 : null;
}

/** Rest the pool for the current backoff window, then double the next one. */
function restFreePool(providerId, why) {
  const wait = poolRestMs();
  if (wait === null) {
    console.warn(`[proxy] ${providerId}: ${why} - resting is switched off (freePoolRestMinutes: 0), so the next request runs the ladder again`);
    return;
  }
  freeDry.until = now() + wait;
  const rested = Math.round(wait / 60_000);
  if (Number(config.freePoolRestMinutes) > 0) freeDry.backoffMs = wait;
  else freeDry.backoffMs = Math.min(FREE_DRY_MAX_MS, Math.round(freeDry.backoffMs * 3));
  state.freeDry = { hits: freeDry.hits, until: freeDry.until, backoffMs: freeDry.backoffMs };
  persistState();
  console.warn(`[proxy] ${providerId}: ${why} - resting the pool for ${rested}m; the fallback answers until then`);
}

// The horizon that separates "this exit, or this identity, is spent" from "the day
// is spent". The free tier answers both with the same body; only the second carries
// a retry-after measured in hours, and it is the *same absolute instant* for every
// address and every model.
//
// Measured against the live service on 2026-10-24: eleven exits that had never
// carried a request (nine countries), six freshly minted sessions, and all nine
// keyless models - every one answered 429 with the same countdown to 00:00 UTC,
// while the host IP was already dry. Nothing a client controls moves it, so a
// day-scale answer is parked on the first sighting instead of being probed with
// exits, builds, sessions, projects and model handles the way a short-lived limit
// is (which is the ladder below, and it is right for a limit that moving clears).
const FREE_DAY_HORIZON_MS = 30 * 60_000;
const FREE_DAY_HORIZON_MAX_MS = 36 * 60 * 60_000;

/**
 * The horizon actually in force. `freeDayHorizonMinutes` overrides the measured
 * default; **0 means never park** - every limit is treated as exit/identity-scale
 * and runs the ladder (rotate the exit, mint an identity, walk the pool). That is
 * the operator's setting when they would rather keep moving than wait: the ladder
 * is still the thing that clears a limit while the day has allowance, and a spent
 * day simply keeps answering 429 with the upstream's own clock in `retry-after`.
 */
function freeDayHorizonMs() {
  const raw = config.freeDayHorizonMinutes;
  if (raw === undefined || raw === null || raw === '') return FREE_DAY_HORIZON_MS;
  return Math.max(0, Number(raw) || 0) * 60_000;
}

/**
 * Is the free tier parked at this instant?
 *
 * One question, asked everywhere the ladder decides whether to move, so
 * `freeDayHorizonMinutes: 0` means *never* park rather than "stop creating new
 * parks": a window recorded by an earlier policy (it lives in `state.json`) must
 * not keep the ladder switched off after the operator has asked for it back. With
 * parking off, the answer's own clock still reaches the client - the upstream's
 * `retry-after` is passed through - so "keep rotating" costs exits and identities
 * but never leaves the caller without a time to come back at.
 */
function freeTierParked() {
  return freeDayHorizonMs() > 0 && freeDry.until > now();
}

/**
 * Park the free tier until the reset instant the upstream itself named.
 *
 * Returns false when it is already parked at least that long, so a burst of
 * requests cannot keep re-announcing (and re-persisting) the same verdict.
 */
function parkFreeTier(providerId, retryMs, why) {
  const horizon = Math.min(retryMs, FREE_DAY_HORIZON_MAX_MS);
  const until = now() + horizon;
  if (freeDry.until >= until) return false;
  freeDry.hits += 1;
  freeDry.until = until;
  // The escalating window keeps its meaning across a restart: it is never shorter
  // than the day the upstream just named.
  freeDry.backoffMs = Math.max(freeDry.backoffMs, Math.min(FREE_DRY_MAX_MS, horizon));
  state.freeDry = { hits: freeDry.hits, until: freeDry.until, backoffMs: freeDry.backoffMs };
  persistState();
  console.warn(`[proxy] ${providerId}: free tier is spent for the day (${Math.round(horizon / 60_000)}m to the reset${why ? `; "${String(why).slice(0, 90)}"` : ''}) - parking it${config.exclusiveProvider ? ' and retrying it on the next request' : ', the fallback answers until then'}`);
  pushEvent({ kind: 'free-day-ceiling', pid: providerId, until: freeDry.until, retryAfterSeconds: Math.round(retryMs / 1000) });
  return true;
}

/** The free tier answered again: forget the backoff so the pool is used normally. */
function clearFreeDry(providerId) {
  if (!freeDry.until && freeDry.hits === 0 && freeDry.backoffMs === FREE_DRY_BASE_MS) return;
  freeDry.hits = 0;
  freeDry.until = 0;
  freeDry.backoffMs = FREE_DRY_BASE_MS;
  delete state.freeDry;
  persistState();
  console.warn(`[proxy] ${providerId}: free allowance answered again - dry backoff reset`);
}
// A retry that follows a 429 is usually answered quickly (or not at all): a
// stalled exit is abandoned in eight seconds instead of twenty-five, because the
// next lane is cheap and the caller's budget is not. The first attempt keeps the
// full deadline - big contexts are legitimately slow before the first byte.
const EXIT_RETRY_FIRST_BYTE_MS = 8_000;

// The free tier was measured to hold roughly 900KB of context, so the window the
// router advertises is a full million tokens. Clients that read the model list
// take their limit from here; without it they guess a tiny window and trim the
// conversation long before the upstream does.
const DEFAULT_CONTEXT_WINDOW = 1_000_000;
const DEFAULT_MAX_OUTPUT_TOKENS = 32_000;
// Prefill, not generation: a large body is legitimately slow before the first
// byte, so the deadline grows with the payload instead of killing a big prompt
// on a deadline sized for a chat one-liner. ~25ms per extra KB, capped.
const PROMPT_BUDGET_PER_KB_MS = 25;
const PROMPT_BUDGET_FREE_KB = 8;
const PROMPT_BUDGET_MAX_MS = 75_000;
// How much of that allowance the *ladder* may spend. A quota answer is fast, so the
// ladder gets a small slice and the rest stays with the attempt that actually streams.
const PROMPT_BUDGET_LADDER_MAX_MS = 15_000;

/**
 * The free-tier ladder budget, in milliseconds.
 *
 * `freeLadderBudgetSeconds` (config) is the operator's dial; `defaultMs` is the
 * already-final value used when nothing is configured (25s cold, 20s dry - the 20s is
 * 80% of the 25s, so the pair keeps the ratio the ladder was tuned for). `fraction`
 * therefore applies only to a *configured* value, and the 3s floor keeps an
 * aggressively short dial from switching the ladder off; the dry leash never drops
 * below that floor either.
 */
function clampBudgetMs(seconds, defaultMs, fraction = 1) {
  const raw = Number(seconds);
  if (!Number.isFinite(raw) || raw <= 0) return Math.round(defaultMs);
  return Math.max(3_000, Math.min(120_000, Math.round(raw * 1000 * fraction)));
}

/**
 * `freeLadderBudgetSeconds` as it is stored: a positive number of seconds clamped to
 * 3..120 (one decimal), or `undefined` when unset/invalid, which means "use the
 * built-in 25s cold / 20s dry pair". Used by both the config loader and the admin
 * settings route so a hand-edited config and a dashboard POST cannot disagree.
 */
function clampLadderSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return Math.round(Math.max(3, Math.min(120, n)) * 10) / 10;
}

function contextWindowOf(provider) {
  const n = Number(provider && provider.contextWindow);
  return n > 0 ? Math.round(n) : DEFAULT_CONTEXT_WINDOW;
}

/** Extra first-byte time a body of this size deserves, in milliseconds. */
function promptSizeBudget(bytes) {
  if (!bytes) return 0;
  const kb = bytes / 1024 - PROMPT_BUDGET_FREE_KB;
  if (kb <= 0) return 0;
  return Math.min(PROMPT_BUDGET_MAX_MS, Math.round(kb * PROMPT_BUDGET_PER_KB_MS));
}

/** The fields a client reads a model's limits from, OpenAI and Anthropic style. */
function limitFields(provider) {
  const context = contextWindowOf(provider);
  const output = DEFAULT_MAX_OUTPUT_TOKENS;
  return { limit: { context, output, input: context }, context_length: context, max_input_tokens: context, max_output_tokens: output };
}

function isManagedProvider(provider) {
  return !!(PROVIDER_TYPES[provider && provider.type] || {}).manageAccounts;
}

/** How many identity lanes a managed provider runs (`identityPool`, default 4). */
function managedPoolSize(provider) {
  const raw = (provider && provider.identityPool) ?? config.freeIdentityPool ?? process.env.ROUTER_FREE_IDENTITIES;
  const n = Math.round(Number(raw));
  if (!Number.isFinite(n) || n <= 0) return MANAGED_POOL_DEFAULT;
  return Math.min(MANAGED_POOL_MAX, Math.max(1, n));
}

/** The lane an account belongs to; 0 when the provider is not lane-based. */
function laneKeyOf(account) {
  const n = Number(account && account.lane);
  return String(Number.isFinite(n) && n >= 0 ? n : 0);
}

function newManagedAccount(provider, lane = 0) {
  const acc = normalizeAccount({ name: `identity-${lane + 1}`, apiKey: '' });
  acc.keyless = true;
  acc.lane = lane;
  acc.gen = 1;
  return acc;
}

/**
 * Guarantee the lane pool exists. Rows are positional (index === lane), so a
 * rebuilt identity keeps its lane and its slot instead of churning the array.
 */
function ensureManagedAccount(provider) {
  if (!isManagedProvider(provider)) return null;
  provider.accounts = provider.accounts || [];
  const size = managedPoolSize(provider);
  if (provider.accounts.length > size) {
    const ps = providerState(provider.id);
    for (const a of provider.accounts.slice(size)) if (ps.accounts) delete ps.accounts[a.id];
    provider.accounts = provider.accounts.slice(0, size);
    if (proxies) for (const lane of Object.keys(proxies.state.lanes[provider.id] || {})) {
      if (Number(lane) >= size) proxies.releaseLane(provider.id, lane);
    }
    saveConfig().catch(() => {});
  }
  let created = false;
  let lanesStamped = false;
  for (let lane = 0; lane < size; lane += 1) {
    let acc = provider.accounts[lane];
    if (!acc) {
      acc = provider.accounts[lane] = newManagedAccount(provider, lane);
      const as = accountState(provider.id, acc.id);
      // Pinned to the newest build, shared by every lane: the build is not part of
      // the rotating identity (see OPENCODE_FREE_UAS).
      as.uaIndex = OPENCODE_FREE_UA_INDEX;
      as.sessionId = '';
      as.refreshing = false;
      openCodeSession(provider, acc);
      created = true;
      pushEvent({ kind: 'identity-created', pid: provider.id, aid: acc.id, lane });
    }
    if (acc.lane === undefined) { acc.lane = lane; lanesStamped = true; }
  }
  // Pin an exit per lane straight away, so every identity has its own address
  // before its first request and the dashboard can show the one it will use.
  if (provider.proxy === 'rotate' && proxies && proxies.state.settings.enabled) {
    for (let lane = 0; lane < size; lane += 1) {
      if (provider.accounts[lane]) proxies.pickLane(provider, String(lane));
    }
  }
  if (created || lanesStamped) {
    // `lanesStamped` matters on a config that already carried the rows (an older
    // file, a hand-edited one): without this write the lane indices lived only in
    // memory until some unrelated save, and a config read by an operator or a tool
    // showed rows with no lane at all.
    saveConfig().catch(() => {});
    if (created) console.warn(`[proxy] ${provider.id}: identity pool up with ${size} lane(s), each on its own exit IP`);
  }
  const list = provider.accounts.filter((a) => a.enabled !== false);
  return list.find((a) => !isLimited(accountState(provider.id, a.id))) || list[0] || null;
}

/**
 * Reinstall the client: drop a spent identity and mint a replacement that
 * presents a *completely* different client, not just a new token.
 *
 * What changes: account id and name, session id and `x-opencode-project` handle,
 * plus the previous cooldown and (when the
 * provider opted into the pool) the exit IP. A quota that is accounted per
 * session/instance is cleared by this; a quota accounted upstream against the
 * whole free pool is not - measured against the live service, a freshly minted
 * session with a fresh project handle, three client versions, three models and
 * four egress IPs all answer `429 FreeUsageLimitError` with
 * `retry-after: 57142` (~15.9h). That is why the caller also honours the
 * upstream window instead of reinstalling in a loop.
 */
function reinstallManagedClient(provider, oldAccount, reason, { rotateExit = false } = {}) {
  const as = accountState(provider.id, oldAccount.id);
  // The build stays on the newest one. A limit is answered with a new session, a
  // new project handle and (when the provider opted into the pool) a new exit IP -
  // never by stepping the client version down.
  as.uaIndex = OPENCODE_FREE_UA_INDEX;
  as.lastRenewAt = Date.now();
  as.reinstalledAt = Date.now();
  as.reinstallReason = String(reason || '').slice(0, 120);
  as.limitedUntil = 0;
  as.gen = (Number(as.gen) || 1) + 1;
  oldAccount.gen = as.gen;
  state.totals.renewals = (state.totals.renewals || 0) + 1;
  state.totals.reinstalls = (state.totals.reinstalls || 0) + 1;
  // Mint the session now instead of lazily on the next request, so the rebuilt
  // client is complete and observable the moment this returns (and the admin
  // API can hand back the session it will actually use).
  as.sessionId = '';
  openCodeSession(provider, oldAccount);
  if (rotateExit && provider.proxy === 'rotate' && proxies && proxies.activeExits().length > 1) {
    proxies.rotateLane(provider, laneKeyOf(oldAccount), `reinstall after ${reason}`);
  }
  pushEvent({ kind: 'reinstall', pid: provider.id, aid: oldAccount.id, lane: oldAccount.lane, reason });
  // A lane rebuild is also "this lane is back in service", and every rebuild goes
  // through here: the in-place one a burst lends, the background queue and the
  // ladder's own identity step. Reporting it once, centrally, is what makes the
  // burst path observable (and with `identityBurst: false` the burst events stay
  // inert by definition, because this switch is off).
  if (identityBurstEnabled()) {
    pushEvent({ kind: 'lane-rebuilt', pid: provider.id, aid: oldAccount.id, lane: oldAccount.lane, reason });
  }
  console.warn(`[proxy] ${provider.id}#${laneKeyOf(oldAccount)}: client reinstalled (${reason}); new session, new project, client ${OPENCODE_FREE_UAS[as.uaIndex]}`);
  saveConfig().catch(() => {});
  persistState();
  return oldAccount;
}

/**
 * Rebuild one spent lane in the background: new exit IP first, then a new client
 * identity on it. The request that hit the limit does not wait for this - it
 * continues on the next lane - which is the whole point of running several.
 */
const lanesRefreshing = new Set();

function queueLaneRefresh(provider, account, reason) {
  const key = `${provider.id}#${laneKeyOf(account)}`;
  if (lanesRefreshing.has(key)) return false;
  lanesRefreshing.add(key);
  const as = accountState(provider.id, account.id);
  as.refreshing = true;
  as.limitedUntil = now() + LANE_REFRESH_COOLDOWN_MS;
  persistState();
  setImmediate(async () => {
    try {
      if (provider.proxy === 'rotate' && proxies) {
        const burned = proxies.state.lanes[provider.id] ? proxies.state.lanes[provider.id][laneKeyOf(account)] : '';
        if (burned) proxies.noteResult(burned, false, { burn: true });
        proxies.releaseLane(provider.id, laneKeyOf(account));
      }
      reinstallManagedClient(provider, account, reason);
      if (provider.proxy === 'rotate' && proxies) proxies.pickLane(provider, laneKeyOf(account));
      as.refreshing = false;
      as.limitedUntil = 0;
      console.warn(`[proxy] ${provider.id}#${laneKeyOf(account)}: lane rebuilt in the background (${reason})`);
    } catch (err) {
      as.refreshing = false;
      as.limitedUntil = now() + LANE_REFRESH_COOLDOWN_MS;
      console.warn(`[proxy] ${provider.id}#${laneKeyOf(account)}: lane rebuild failed: ${err.message}`);
    } finally {
      lanesRefreshing.delete(key);
      persistState();
    }
  });
  return true;
}

function laneReady(provider, account) {
  const as = accountState(provider.id, account.id);
  return account.enabled !== false && !as.refreshing && !isLimited(as);
}

/**
 * Spacing between "renew every lane" sweeps, in milliseconds.
 * `allLanesRenewGapMinutes` (config) overrides it; 0 means every limit.
 */
function allLanesRenewGapMs() {
  const raw = config.allLanesRenewGapMinutes;
  if (raw === undefined || raw === null || raw === '') return ALL_LANES_RENEW_GAP_MS;
  return Math.max(0, Number(raw) || 0) * 60_000;
}

/**
 * Hit the limit on one lane -> mint a new identity on *all* of them.
 *
 * The operator's rule: a limit is the whole provider's problem, not one lane's. If
 * only the lane that reported it is rebuilt, the next request spends itself
 * rediscovering that the other three are spent too; sweeping all four means the next
 * request starts on a complete set of fresh identities (new session, new project
 * handle, new client build and a new exit pin each - everything
 * reinstallManagedClient changes).
 *
 * Runs through queueLaneRefresh per lane, so each lane keeps its slot and gets the
 * usual background treatment: the reporting request does not wait for the exits to
 * be re-pinned. Throttled to one sweep per `allLanesRenewGapMinutes` (default one
 * minute) because a cold burst would otherwise mint a fresh set for every 429 the
 * ladder collects. Returns how many lanes were actually queued - lanes already
 * rebuilding are left to finish, they are getting a new identity anyway.
 */
function queueAllLaneRefreshes(provider, reason) {
  if (!isManagedProvider(provider)) return { swept: false, queued: 0 };
  // Concurrent requests own the lanes. Sweeping would park four identities out
  // from under them and hand this request the very wait it is trying to avoid, so
  // the sweep stands down and every request mints its own identity as it needs one
  // (lendIdentity / rebuildLaneNow). One request alone still sweeps as before.
  if (freeDemandConcurrent(provider.id)) {
    console.warn(`[proxy] ${provider.id}: all-lane sweep stood down - ${freeDemandOf(provider.id)} requests are in the ladder, each minting its own identity (${reason})`);
    return { swept: false, queued: 0 };
  }
  // Per provider, not global: one provider sitting on a dry ceiling must not
  // suppress another provider's sweep. Kept in the provider's runtime state so it
  // survives a restart instead of granting a free sweep on every boot.
  const ps = providerState(provider.id);
  const lastSweep = Number(ps.allLanesSweptAt) || 0;
  const gapMs = allLanesRenewGapMs();
  if (gapMs > 0 && now() - lastSweep < gapMs) {
    console.warn(`[proxy] ${provider.id}: all-lane identity sweep skipped (${Math.round((gapMs - (now() - lastSweep)) / 1000)}s of the ${Math.round(gapMs / 1000)}s floor left); rebuilding the reporting lane only`);
    return { swept: false, queued: 0 };
  }
  ensureManagedAccount(provider);
  // Stamped before the loop: one request must not sweep twice when a second lane
  // reports the same limit a moment later.
  ps.allLanesSweptAt = now();
  const lanes = (provider.accounts || []).filter((a) => a.enabled !== false);
  let queued = 0;
  for (const lane of lanes) {
    try {
      if (queueLaneRefresh(provider, lane, reason)) queued += 1;
    } catch (err) {
      console.warn(`[proxy] ${provider.id}#${laneKeyOf(lane)}: sweep could not queue the rebuild: ${err.message}`);
    }
  }
  if (queued) {
    pushEvent({ kind: 'reinstall-all', pid: provider.id, n: queued, reason });
    console.warn(`[proxy] ${provider.id}: limit hit - minting a new identity on ${queued}/${lanes.length} lane(s), each on a fresh exit (${reason})`);
  }
  persistState();
  return { swept: true, queued };
}

/**
 * Start a pool rebuild in the background, at most once every 20s. A request must
 * not sit on a subscription fetch and a test run while the caller is waiting, but
 * an empty pool has to refill itself or every lane loses its only move.
 */
let poolKickAt = 0;
function kickPool(reason) {
  if (!proxies || Date.now() - poolKickAt < 20_000) return;
  poolKickAt = Date.now();
  Promise.resolve(proxies.replenish(reason)).catch(() => {});
}

async function waitForLane(provider, ms) {
  const deadline = Date.now() + Math.max(0, ms);
  while (Date.now() < deadline) {
    if ((provider.accounts || []).some((a) => laneReady(provider, a))) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  return (provider.accounts || []).some((a) => laneReady(provider, a));
}

/**
 * Rebuild a lane on the caller's own thread, keeping it in service.
 *
 * The same work as `queueLaneRefresh` - burn the exit that answered the limit,
 * drop its pin, mint a new identity, pin another exit - minus the park. The
 * background version is right when nobody else wants the lane, and wrong when
 * several requests do: the 30s park is exactly what makes the second request wait
 * 12s and then collect the same 429. Every step here is local state, so this
 * returns in microseconds and the lane is usable the moment it does.
 */
function rebuildLaneNow(provider, account, reason) {
  const laneKey = laneKeyOf(account);
  const as = accountState(provider.id, account.id);
  try {
    if (provider.proxy === 'rotate' && proxies) {
      const burned = proxies.state.lanes[provider.id] ? proxies.state.lanes[provider.id][laneKey] : '';
      if (burned) proxies.noteResult(burned, false, { burn: true });
      proxies.releaseLane(provider.id, laneKey);
    }
    reinstallManagedClient(provider, account, reason);
    if (provider.proxy === 'rotate' && proxies) proxies.pickLane(provider, laneKey);
    as.refreshing = false;
    as.limitedUntil = 0;
    console.warn(`[proxy] ${provider.id}#${laneKey}: lane rebuilt in place (${reason}); it stays in service for the other requests`);
    return true;
  } catch (err) {
    as.refreshing = false;
    as.limitedUntil = now() + LANE_REFRESH_COOLDOWN_MS;
    console.warn(`[proxy] ${provider.id}#${laneKey}: in-place rebuild failed: ${err.message}`);
    return false;
  }
}

/**
 * Mint an identity for a request that found every lane spent or parked.
 *
 * Picks the lane that would otherwise wait longest (the one parked furthest into
 * the future, then the one idle longest), rebuilds it in place and hands it back.
 * A lane already under a background rebuild is skipped - it is getting a new
 * identity anyway - and `tried` keeps one request from lending itself the same
 * lane twice.
 */
function lendIdentity(provider, tried) {
  const lanes = (provider.accounts || []).filter((a) => a.enabled !== false);
  const eligible = lanes.filter((a) => {
    if (tried && tried.has(a.id)) return false;
    return !accountState(provider.id, a.id).refreshing;
  });
  if (!eligible.length) return null;
  eligible.sort((x, y) => {
    const ax = accountState(provider.id, x.id);
    const ay = accountState(provider.id, y.id);
    return (Number(ay.limitedUntil) || 0) - (Number(ax.limitedUntil) || 0)
      || (Number(ax.lastUsed) || 0) - (Number(ay.lastUsed) || 0);
  });
  const lane = eligible[0];
  const demand = freeDemandOf(provider.id);
  pushEvent({ kind: 'identity-lent', pid: provider.id, aid: lane.id, lane: lane.lane, demand });
  console.warn(`[proxy] ${provider.id}#${laneKeyOf(lane)}: every lane is spent while ${demand} requests are in the ladder - minting this lane's identity now instead of waiting for a rebuild`);
  rebuildLaneNow(provider, lane, `identity on demand (${demand} requests in the ladder)`);
  return lane;
}

// The historical name: the request path, the admin API and the tests use it.
const renewManagedIdentity = reinstallManagedClient;

/**
 * The operator's own recipe, in one call: drop the client, move the IP, install a
 * new client with a new identity.
 *
 * That sequence *is* the ladder's shape and it clears what a ladder can clear - a
 * limit accounted per lane, per exit address or per session, which is what a live
 * day's allowance produces (measured: the same request answered 200 from a fresh
 * exit in the minute the old one answered 429). It cannot clear the anonymous
 * *day* bucket: tested today through 15 v2ray exits with a fresh session, project
 * and build each, and once more with no client state and no credential at all -
 * every one answered 429 counting to the same `2026-10-25T00:00:00Z`. Recycling is
 * still the right move at the instant that bucket resets, which is why the free
 * tier does it by itself when the parked window expires.
 */
function recycleLane(provider, account, reason) {
  const lane = laneKeyOf(account);
  let burned = '';
  let next = null;
  if (provider.proxy === 'rotate' && proxies && proxies.state.settings.enabled) {
    // 1) mark the address this lane is on as spent...
    burned = (proxies.state.lanes[provider.id] || {})[lane] || '';
    if (burned) proxies.noteResult(burned, false, { burn: true });
    // 2) ...and move the lane to a different one (`rotateLane` refuses to hand the
    //    current pin back, so the new address is genuinely a different exit).
    next = proxies.rotateLane(provider, lane, reason);
  }
  // 3) drop the client and install a new identity on that fresh address: new
  //    session, new project handle, next build, cooldown cleared.
  const fresh = reinstallManagedClient(provider, account, reason);
  return { account: fresh, lane: Number(lane), burnedExit: burned, exit: next ? (next.exitIp || next.id || '') : '' };
}

// ---------------------------------------------------------------------------
// The genuine CLI as the transport (see opencode-cli-driver.js for the why)
// ---------------------------------------------------------------------------

/** The conversation as one prompt: the CLI is an agent and takes a message. */
function promptFromMessages(body) {
  const msgs = Array.isArray(body && body.messages) ? body.messages : [];
  const out = [];
  for (const m of msgs) {
    const role = String((m && m.role) || 'user');
    let content = '';
    if (typeof m.content === 'string') content = m.content;
    else if (Array.isArray(m.content)) {
      content = m.content.map((p) => (p && (p.text || p.content)) || '').filter(Boolean).join('\n');
    }
    if (!content.trim()) continue;
    if (role === 'system') out.push(content.trim());
    else out.push(`${role === 'assistant' ? 'Assistant' : 'User'}: ${content.trim()}`);
  }
  // The last user turn is the message to answer; earlier turns become context.
  if (out.length > 1 && /^User: /.test(out[out.length - 1])) {
    const last = out.pop();
    return `${out.join('\n\n')}\n\n${last.replace(/^User: /, '')}`;
  }
  return out.join('\n\n');
}

/** Milliseconds to the next 00:00 UTC - the instant the free allowance returns. */
function msToNextUtcMidnight() {
  const d = new Date();
  const next = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, 0, 0, 0, 0);
  return Math.max(60_000, next - Date.now());
}

/**
 * How long the CLI transport cools down after the genuine client reports its limit.
 *
 * Not the day: measured 2026-10-25, an identity that reported `Free usage limit
 * reached` after five requests answered again about ten minutes later, while every
 * forged shape in the same window counted down to 00:00 UTC. The genuine client's
 * allowance therefore behaves like a short window that refills, so parking it until
 * midnight would throw away the rest of the day for nothing.
 */
function cliLimitCooldownMs() {
  const minutes = Number(config.cliLimitCooldownMinutes);
  return Math.max(60_000, (Number.isFinite(minutes) && minutes >= 0 ? minutes : 12) * 60_000);
}

/** The default location of the router's own copy of the CLI. */
function defaultCliPath() {
  return String(process.env.ROUTER_OPENCODE_CLI || config.cliPath || '/opt/llm-router/bin/opencode');
}

/**
 * The long-lived `opencode serve` this router may talk to instead of spawning the
 * binary per request. Measured: ~1.5s per answer through the server against ~4-13s
 * for a fresh spawn, and no ~400M memory spike per request.
 */
function defaultCliServeUrl() {
  return String(process.env.ROUTER_OPENCODE_SERVE || config.cliServeUrl || 'http://127.0.0.1:47999');
}

/**
 * The HOME of the `opencode serve` process, whose log names the truth about the
 * allowance. The shipped unit runs it with `/var/lib/oc-serve`.
 */
function defaultCliServeHome() {
  return String(process.env.ROUTER_OPENCODE_SERVE_HOME || config.cliServeHome || '/var/lib/oc-serve');
}

/** One driver, reused unless the provider points somewhere else. */
function cliDriverFor(provider) {
  const binPath = String(provider.cliPath || defaultCliPath());
  const homeDir = String(provider.cliHome || path.join(DATA_DIR, 'oc-cli-home'));
  const args = Array.isArray(provider.cliArgs) ? provider.cliArgs.map(String) : [];
  const mode = provider.cliMode === 'serve' || (provider.cliMode !== 'spawn' && provider.cliServeUrl) ? 'serve' : 'spawn';
  const serveUrl = String(provider.cliServeUrl || defaultCliServeUrl());
  const serveHome = String(provider.cliServeHome || defaultCliServeHome());
  if (opencodeCli && opencodeCli.binPath === binPath && opencodeCli.homeDir === homeDir
    && opencodeCli.mode === mode && opencodeCli.serveUrl === serveUrl && opencodeCli.serveHome === serveHome
    && JSON.stringify(opencodeCli.args) === JSON.stringify(args)) return opencodeCli;
  return new OpenCodeCliDriver({ binPath, homeDir, serveHome, args, mode, serveUrl, log: (m) => console.warn(m) });
}

/**
 * The HOME of one client identity.
 *
 * A fresh HOME is a fresh installation of the genuine client - the only "new
 * identity" this transport has, because the anonymous tier carries no token to
 * rotate (the client sends the literal `public`). Measured 2026-10-25: a fresh
 * identity answered while the previous one reported the limit, so identities are
 * minted into a small pool and paired with a different exit when one runs dry.
 */
function cliIdentityHome(provider, identity) {
  const base = String(provider.cliHome || path.join(DATA_DIR, 'oc-cli-home'));
  if (!identity) return base;
  return identity === 'signed' ? `${base}-signed` : `${base}-id${identity}`;
}

/** Is this identity already a bootstrapped installation? */
function cliIdentityIsWarm(provider, identity) {
  try {
    return fs.existsSync(path.join(cliIdentityHome(provider, identity), '.local', 'share', 'opencode', 'opencode.db'));
  } catch { return false; }
}

/** A spawn-mode driver bound to one identity's HOME. */
function cliSpawnDriver(provider, identity) {
  return new OpenCodeCliDriver({
    binPath: String(provider.cliPath || defaultCliPath()),
    homeDir: cliIdentityHome(provider, identity),
    args: Array.isArray(provider.cliArgs) ? provider.cliArgs.map(String) : [],
    mode: 'spawn',
    log: (m) => console.warn(m),
  });
}

/** How many client identities this provider may mint (one HOME each). */
function cliIdentityPool(provider) {
  const raw = provider.cliIdentityPool !== undefined ? provider.cliIdentityPool : config.cliIdentityPool;
  const n = Number(raw);
  return Math.max(1, Math.min(8, Number.isFinite(n) && n > 0 ? Math.floor(n) : 3));
}

/**
 * Warm the identity pool in the background.
 *
 * A fresh HOME is a *cold* installation: its first run pays for the client's own
 * bootstrap (measured about a minute), which no request budget can afford - a cold
 * identity therefore always timed out before it could answer. Warming happens once,
 * at boot, one identity at a time and without an exit (the bootstrap is not slowed by
 * a proxy); after that a rotated identity answers in seconds, which is what makes
 * "new identity, new address, retry" actually deliver.
 */
async function warmCliIdentities(provider) {
  const pool = cliIdentityPool(provider);
  const model = String(provider.warmModel || config.cliWarmModel || 'deepseek-v4.1-flash-free');
  const signedIn = (provider.accounts || []).find((a) => a.signedIn && a.apiKey && a.enabled !== false);
  const authContent = cliAuthContent(signedIn);
  const wantExit = provider.proxy === 'rotate' && !!proxies && proxies.state.settings.enabled;
  for (let i = 1; i <= pool; i += 1) {
    const home = cliIdentityHome(provider, i);
    if (fs.existsSync(path.join(home, '.local', 'share', 'opencode', 'opencode.db'))) continue;
    const driver = cliSpawnDriver(provider, i);
    if (!driver.available()) return;
    // The warm-up talks to the same edge, so it leaves through an exit too - a
    // bootstrap from the server's own address is exactly the packet that must not
    // exist. No exit means no warm-up; the identity stays cold instead of leaking.
    const picked = cliProxyEnv(provider, { rotate: true });
    if (wantExit && !picked.choice) {
      console.warn('[cli] no live exit - skipping the identity warm-up rather than using the server address');
      return;
    }
    const t0 = now();
    // eslint-disable-next-line no-await-in-loop
    const r = await driver.run({ model, prompt: 'warm-up: reply with just ok', timeoutMs: 240_000, env: picked.env || {}, authContent });
    console.warn(`[cli] warmed identity ${i} in ${now() - t0}ms (${r.ok ? 'answered' : r.limit ? 'limited' : r.error || 'failed'})`);
  }
}

/**
 * One attempt's ceiling.
 *
 * Not the provider's 300s: a limited client retries with backoff, so a long ceiling
 * turns a refusal into a client-side timeout. The transport detects the limit itself
 * (the client's log in serve mode, its output in spawn mode) and usually finishes an
 * attempt in seconds; this is only the backstop.
 */
function cliAttemptTimeoutMs(provider) {
  const raw = Number(config.cliAttemptTimeoutMs);
  const cap = Number.isFinite(raw) && raw > 0 ? raw : 60_000;
  const own = Number(provider.timeoutMs) > 0 ? Number(provider.timeoutMs) : cap;
  return Math.max(15_000, Math.min(own, cap));
}
/**
 * One exit as an HTTPS_PROXY URL.
 *
 * `pick()` hands back a wrapper (`{ mode, exit, proxy }`) while `rotate()` hands back
 * the exit itself, so both shapes are accepted here rather than at every call site.
 */
function cliExitEnv(picked) {
  if (!picked) return null;
  const proxy = picked.proxy
    || (proxies && picked.id !== undefined ? { host: '127.0.0.1', port: proxies.portForExit(picked) } : null);
  if (!proxy || !proxy.host || !proxy.port) return null;
  return `http://${proxy.host}:${proxy.port}`;
}

/**
 * The next exit for the CLI transport, taken only from the exits whose inbound
 * actually exists.
 *
 * `activeExits()` can carry far more entries than the manager started: measured here,
 * 400 ids against 15 started inbounds. `portForExit()` maps an exit to
 * `basePort + its index`, so any entry past the started range points at a port nobody
 * listens on - and a CLI handed that proxy does not fail, it *waits*, which looks
 * exactly like the hang this transport has already been chased through twice.
 */
function cliExitChoice(provider, { rotate = false } = {}) {
  if (!proxies || !proxies.state.settings.enabled || provider.proxy !== 'rotate') return null;
  const all = proxies.activeExits();
  const max = Number(proxies.state.settings.maxActive) || 0;
  const live = max > 0 ? all.slice(0, max) : all;
  if (!live.length) return null;
  const key = provider.id || 'default';
  const cursors = state.cliExitIndex || {};
  const at = rotate ? ((Number(cursors[key]) || 0) + 1) % live.length : (Number(cursors[key]) || 0) % live.length;
  const exit = live[at];
  const port = proxies.portForExit(exit);
  if (!port) return null;
  if (rotate) {
    state.cliExitIndex = { ...cursors, [key]: at };
    persistState();
  }
  return { url: `http://127.0.0.1:${port}`, exit: exit.id, ip: exit.exitIp || 'unknown' };
}

/**
 * The credential the genuine client should speak with, if a principal exists.
 *
 * `OPENCODE_AUTH_CONTENT` is the client's own channel for this. The anonymous tier
 * addresses a literal `public` credential, so a signed-in principal is a *different*
 * bucket - the only client-side thing that resets an exhausted day.
 */
function cliAuthContent(account) {
  if (!account || !account.signedIn || !account.apiKey) return '';
  const rec = {
    opencode: {
      type: 'oauth',
      refresh: String(account.refreshToken || ''),
      access: String(account.apiKey),
      expires: Number(account.tokenExpiresAt) || Date.now() + 3600_000,
    },
  };
  if (account.orgId) rec.opencode.accountId = String(account.orgId);
  return JSON.stringify(rec);
}

/**
 * The proxy environment for one CLI attempt.
 *
 * The exit is not optional: the whole point of this transport is that the edge sees a
 * v2ray address, never the server's own. When no live exit can be handed over, the
 * request is refused here rather than sent from the server's address.
 */
function cliProxyEnv(provider, { rotate = false } = {}) {
  const choice = cliExitChoice(provider, { rotate });
  if (!choice) return { env: null, choice: null };
  return {
    env: {
      HTTPS_PROXY: choice.url,
      HTTP_PROXY: choice.url,
      ALL_PROXY: choice.url,
      // The local calls (and the router itself) must not be pushed through the exit.
      NO_PROXY: '127.0.0.1,localhost',
    },
    choice,
  };
}

function cliStreamChunk(model, text, finish = null) {
  return `data: ${JSON.stringify({
    id: 'chatcmpl-cli',
    object: 'chat.completion.chunk',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, delta: finish ? {} : { content: text }, finish_reason: finish }],
  })}\n\n`;
}

/**
 * Serve a request by running the genuine CLI.
 *
 * The CLI carries its own free allowance: when it says the limit is reached, that
 * is a *day-scale* answer with the same 00:00 UTC reset the API reports, so the
 * tier is parked (no exit burned, no identity minted) and the client is told the
 * instant, instead of being handed a generic 503.
 */
async function handleOpenCodeCli(provider, body, model, req, res) {
  const t0 = now();
  const baseModel = stripThinkingSuffix(model || '');
  state.totals.requests += 1;
  const driver = cliDriverFor(provider);

  const parked = cliDry.until > now() ? cliDry.until : 0;
  if (parked) {
    state.totals.failed += 1;
    const wait = Math.max(1, Math.round((parked - now()) / 1000));
    return sendJson(res, 429, {
      error: {
        message: `the free window is cooling down: every client identity the router can mint came back limited or silent, so the next automatic retry is at ${new Date(parked).toISOString()} (${Math.max(1, Math.round(wait / 60))} minute(s) away). Nothing is broken and nothing needs doing - the router resumes by itself`,
        type: 'router_free_window',
        retryAfterSeconds: wait,
        resetsAt: new Date(parked).toISOString(),
      },
    }, { 'retry-after': String(wait) });
  }
  if (!driver.available()) {
    state.totals.failed += 1;
    return sendJson(res, 503, {
      error: {
        message: driver.mode === 'serve'
          ? `the opencode serve transport has no URL configured (cliServeUrl) - set it, or use cliMode "spawn"`
          : `the OpenCode CLI is not installed at ${driver.binPath} - install it (install.sh --with-opencode-cli) or set cliPath on this provider`,
        type: 'router_cli_missing',
      },
    });
  }

  const prompt = promptFromMessages(body) || 'hello';
  const wantsStream = !!(body && body.stream);
  const headers = baseResponseHeaders(200, wantsStream ? 'text/event-stream' : 'application/json; charset=utf-8', provider);
  headers['x-router-account'] = 'cli';
  headers['x-router-model'] = baseModel;
  headers['x-router-transport'] = driver.mode === 'serve' ? 'opencode-serve' : 'opencode-cli';
  const streamHeaders = { ...headers, 'cache-control': 'no-cache, no-transform', connection: 'keep-alive', 'x-accel-buffering': 'no' };

  const controller = new AbortController();
  res.on('close', () => { if (!res.writableEnded) controller.abort(); });
  const timeoutMs = cliAttemptTimeoutMs(provider);

  /**
   * The ladder for this transport.
   *
   * Attempt 0 is whatever the provider prefers (the long-lived server when it is
   * configured). If the genuine client reports its limit, the next attempts mint a
   * *new identity* (a fresh HOME - a fresh installation, since the anonymous tier
   * has no token to rotate) and hand it a *different exit* (the CLI honours
   * HTTPS_PROXY), which is the closest thing to "a new client from a new address"
   * that this tier can be given. Measured 2026-10-25: a fresh identity answered
   * while the previous one reported the limit.
   */
  const pool = cliIdentityPool(provider);
  const startIdentity = Number(state.cliIdentity && state.cliIdentity.next) || 0;
  const budgetMs = (() => {
    const raw = Number(config.cliLadderBudgetMs);
    return Number.isFinite(raw) && raw > 0 ? raw : 45_000;
  })();
  const ladderStart = Date.now();
  const signedIn = (provider.accounts || []).find((a) => a.signedIn && a.apiKey && a.enabled !== false);
  const authContent = cliAuthContent(signedIn);
  const wantExit = provider.proxy === 'rotate' && !!proxies && proxies.state.settings.enabled;
  const attempts = [];
  // A signed-in principal goes first: it is a different bucket (free models stay free
  // for it), which is the only client-side thing that resets an exhausted day.
  if (authContent) attempts.push({ mode: 'spawn', identity: 'signed', label: 'signed-in', rotateExit: false, auth: authContent });
  if (driver.mode === 'serve') attempts.push({ mode: 'serve', identity: null, rotateExit: false });
  for (let i = 0; i < pool; i += 1) {
    const identity = (startIdentity + i) % pool + 1;
    attempts.push({ mode: 'spawn', identity, rotateExit: i > 0 || driver.mode !== 'serve' });
  }

  let result = null;
  let used = attempts[0];
  let rotated = 0;
  let exhaustedRetryable = false;
  let ranAnything = false;
  let noExit = false;
  for (const attempt of attempts) {
    // A fresh identity is a *cold* installation: its first run pays for the client's
    // own bootstrap (measured ~1 minute), so the walk is time-boxed - the client gets
    // an answer (or the cooldown) inside the budget, and the identity it touched is
    // warm for the next request.
    const elapsed = Date.now() - ladderStart;
    if (attempt !== attempts[0] && elapsed >= budgetMs) {
      console.warn(`[cli] ladder budget of ${budgetMs}ms is spent - not minting another identity for this request`);
      break;
    }
    // A cold identity still has to bootstrap the client before it can answer
    // (measured about a minute - see warmCliIdentities), so the ladder's own budget
    // would kill it before its first token and the kill would look like a limit.
    // Cold attempts get the full attempt ceiling instead.
    const cold = attempt.mode === 'spawn' && attempt.identity && !cliIdentityIsWarm(provider, attempt.identity);
    const attemptMs = attempt === attempts[0] || cold
      ? timeoutMs
      : Math.max(8_000, Math.min(timeoutMs, budgetMs - elapsed));
    if (cold) console.warn(`[cli] identity ${attempt.identity} is cold - giving it the full ${timeoutMs}ms ceiling instead of the ladder budget`);
    // Each attempt gets its own exit: the edge must see a v2ray address, never the
    // server's own. When no live exit can be handed over, the attempt is skipped -
    // and if nothing could run at all, the request is refused rather than leaked.
    let env = {};
    let choice = null;
    if (attempt.mode === 'spawn') {
      const picked = cliProxyEnv(provider, { rotate: attempt.rotateExit });
      choice = picked.choice;
      if (choice) {
        env = picked.env;
        attempt.exit = choice.exit;
        attempt.exitIp = choice.ip;
      } else if (wantExit) {
        noExit = true;
        console.warn('[cli] no live exit is available - refusing to send this request from the server address');
        continue;
      }
    }
    const attemptDriver = attempt.mode === 'serve' ? driver : cliSpawnDriver(provider, attempt.identity);
    if (!attemptDriver.available()) continue;
    ranAnything = true;
    if (attempt !== attempts[0]) rotated += 1;
    result = await attemptDriver.run({
      model: baseModel,
      prompt,
      timeoutMs: attemptMs,
      env,
      signal: controller.signal,
      onDelta: null,
      authContent: attempt.auth || '',
    });
    used = attempt;
    if (result.ok || result.auth) break;
    // A limit is not the only signature of a spent window: a client that is being
    // refused retries with backoff, so a timeout or an aborted call means the same
    // thing. Both rotate; a plain failure (spawn error) does not burn identities.
    const retryable = result.limit || /timeout|aborted|unreachable|killed|ECONN/i.test(String(result.error || ''));
    if (retryable) exhaustedRetryable = true;
    if (!retryable) break;
    console.warn(`[cli] ${attempt.mode}${attempt.identity ? ` identity ${attempt.identity}` : ''}${attempt.exit ? ` via exit ${attempt.exit} (${attempt.exitIp})` : ''} came back ${result.limit ? 'limited' : result.error} after ${result.ms}ms - rotating identity and exit`);
  }
  if (rotated) {
    state.cliIdentity = { next: ((startIdentity + rotated) % pool) };
    persistState();
  }
  if (!ranAnything && noExit) {
    state.totals.failed += 1;
    return sendJson(res, 503, {
      error: {
        message: 'no live v2ray exit is available and this transport never puts the server address on the wire - refresh the pool in the dashboard Network tab',
        type: 'router_no_exit',
      },
    });
  }
  // Say which client actually answered: the transport, and the identity it used.
  const transportName = used && used.mode === 'serve' ? 'opencode-serve' : 'opencode-cli';
  headers['x-router-transport'] = transportName;
  streamHeaders['x-router-transport'] = transportName;
  if (used && used.identity) {
    headers['x-router-identity'] = String(used.identity);
    streamHeaders['x-router-identity'] = String(used.identity);
  }
  if (used && used.exit) {
    headers['x-router-exit'] = String(used.exit);
    streamHeaders['x-router-exit'] = String(used.exit);
  }

  // A kill this box inflicted on itself (the ladder's deadline) is not the upstream
  // allowance. Parking on it turns a slow model into a phantom daily ceiling and
  // hides the real cause behind a "free window" message, so it answers as its own
  // failure and parks nothing.
  const deadlineKilled = !result.ok && !result.limit && !result.auth
    && (result.killedBy === 'router-deadline' || /deadline/i.test(String(result.error || '')));
  if (deadlineKilled) {
    state.totals.failed += 1;
    console.warn(`[cli] ${provider.id}/${baseModel}: killed by ${used && used.mode === 'serve' ? 'the serve' : 'the ladder'} deadline after ${result.ms}ms (identity ${used && used.identity ? used.identity : '-'}) - not the allowance, nothing parked`);
    const secs = 30;
    if (!res.headersSent) {
      return sendJson(res, 504, {
        error: {
          message: `the opencode CLI was still working when this attempt's deadline ran out and the router killed it (${result.error}). That is a local deadline, not the free allowance - nothing is parked and the tier is still available. Retry, or raise cliAttemptTimeoutMs / cliLadderBudgetMs`,
          type: 'router_cli_deadline',
          retryAfterSeconds: secs,
          transport: 'opencode-cli',
        },
      }, { 'retry-after': String(secs) });
    }
    if (!res.writableEnded) res.destroy();
    return;
  }

  if (result.limit || result.auth || (!result.ok && exhaustedRetryable)) {
    const wait = result.auth ? msToNextUtcMidnight() : cliLimitCooldownMs();
    parkCliTier(wait, result.error);
    state.totals.failed += 1;
    if (!res.headersSent) {
      const secs = Math.max(1, Math.round(wait / 1000));
      return sendJson(res, 429, {
        error: {
          message: result.auth
            ? `the opencode CLI reports no usable credential; it needs a sign-in or a key, and the transport stays parked until ${new Date(now() + wait).toISOString()}`
            : `every client identity the router can mint came back limited or silent (last: ${result.error || 'killed'}); this window refills, so the transport cools down for ${Math.round(wait / 60_000)} minute(s) and resumes by itself`,
          type: result.auth ? 'router_cli_auth' : 'router_free_window',
          retryAfterSeconds: secs,
        },
      }, { 'retry-after': String(secs) });
    }
    if (!res.writableEnded) res.destroy();
    return;
  }

  if (!result.ok) {
    state.totals.failed += 1;
    console.warn(`[cli] ${provider.id}/${baseModel} failed: code=${result.code} signal=${result.signal || '-'} ms=${result.ms} ${result.stderr || ''}`);
    if (!res.headersSent) {
      return sendJson(res, 502, {
        error: {
          message: `the opencode CLI could not answer: ${result.error || `exit ${result.code}`}${result.stderr ? ` - ${result.stderr}` : ''}`,
          type: 'router_cli_failed',
          transport: 'opencode-cli',
        },
      });
    }
    if (!res.writableEnded) res.destroy();
    return;
  }

  state.totals.success += 1;
  if (wantsStream) {
    state.totals.streamed += 1;
    if (!res.headersSent) res.writeHead(200, streamHeaders);
    res.write(cliStreamChunk(baseModel, result.text || ''));
    res.write(cliStreamChunk(baseModel, '', 'stop'));
    res.write('data: [DONE]\n\n');
    res.end();
  } else {
    const payload = JSON.stringify({
      id: 'chatcmpl-cli',
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: baseModel,
      choices: [{ index: 0, message: { role: 'assistant', content: result.text || '' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    });
    headers['content-length'] = Buffer.byteLength(payload);
    res.writeHead(200, headers);
    res.end(payload);
  }
  state.totals.bytesOut += Buffer.byteLength(result.text || '');
  if (typeof reqLog === 'function') reqLog(`[cli] ${provider.id}/${baseModel} ok ${now() - t0}ms`);
}

/**
 * Recycle every lane the operator asked for, then ask the edge once.
 * Shared by `/admin/api/provider/<id>/recycle` and the pool-prefixed alias
 * `/admin/api/proxies/provider/<id>/recycle`, so either guess works.
 */async function handleRecycle(req, res, provider) {
  if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
  const payload = await readJsonBody(req).catch(() => ({}));
  const enabled = (provider.accounts || []).filter((a) => a.enabled !== false);
  const targets = payload.allLanes ? enabled : enabled.slice(0, 1);
  if (!targets.length) return sendJson(res, 409, { error: { message: `provider "${provider.id}" has no enabled identity to recycle`, type: 'no_accounts' } });
  const done = targets.map((acc) => {
    const r = recycleLane(provider, acc, `manual recycle (${payload.reason || 'dashboard'})`);
    return { lane: r.lane, account: r.account.name, burnedExit: r.burnedExit, exit: r.exit };
  });
  await saveConfig();
  const probe = payload.probe === false ? null : await probeProviderOnce(provider, targets[0]);
  if (probe && probe.status === 200) clearFreeDry(provider.id);
  console.warn(`[proxy] ${provider.id}: recycled ${done.length} lane(s) - new IP + new identity; edge said ${probe ? `${probe.status}${probe.retryAfterSeconds ? ` retry-after ${probe.retryAfterSeconds}s` : ''}` : '(not probed)'}`);
  return sendJson(res, 200, { ok: true, recycled: done, probe });
}

/** One signed request to the provider, so a recycle can report what the edge said. */async function probeProviderOnce(provider, account, model = 'deepseek-v4.1-flash-free') {
  let built;
  try {
    built = buildUpstream(provider, account, {
      body: { model, messages: [{ role: 'user', content: 'say ok' }] },
      model, method: 'POST', stream: false, suffix: '/chat/completions',
      style: (PROVIDER_TYPES[provider.type] || {}).style, responseStyle: 'openai', translate: false, aggregate: true,
    });
  } catch (err) {
    return { status: 0, error: err.message };
  }
  try {
    const res = await fetch(built.url, {
      method: 'POST', headers: built.headers, body: JSON.stringify(built.payload), signal: AbortSignal.timeout(30_000),
    });
    const retryAfterSeconds = Math.round(retryAfterMs(res.headers) / 1000) || 0;
    let message = '';
    if (res.status >= 400) {
      const text = await res.text();
      try { message = JSON.parse(text).error.message; } catch { message = text.slice(0, 140); }
    } else {
      try { if (res.body && res.body.cancel) await res.body.cancel(); } catch { /* noop */ }
    }
    return {
      status: res.status,
      retryAfterSeconds,
      resetsAt: retryAfterSeconds ? new Date(Date.now() + retryAfterSeconds * 1000).toISOString() : '',
      message: String(message).slice(0, 180),
    };
  } catch (err) {
    return { status: 0, error: err.message };
  }
}

// ---------------------------------------------------------------------------
// Sign-in: add a principal, not another fake identity
// ---------------------------------------------------------------------------

async function opencodeHttp(pathname, { method = 'GET', token = '', body = null, timeoutMs = 20_000 } = {}) {
  const headers = { accept: 'application/json', 'user-agent': openCodeUA({ id: 'signin' }, { id: 'signin' }) };
  if (body) headers['content-type'] = 'application/json';
  if (token) headers.authorization = `Bearer ${token}`;
  const url = `${OPENCODE_CONSOLE}${pathname}`;
  const payload = body ? JSON.stringify(body) : null;
  // Console traffic goes through an exit as well. The whole point of this build is
  // that the operator's own address is never the one the edge - or the console - sees,
  // so there is no direct fallback here: without an exit this call is refused.
  const picked = cliProxyEnv({ id: 'opencode-console', proxy: 'rotate' }, { rotate: true });
  if (!picked.choice) throw new Error('no live v2ray exit: refusing to reach the OpenCode console from the server address');
  const res = await proxiedRequest(url, {
    method,
    headers,
    body: payload,
    proxy: { host: '127.0.0.1', port: Number(String(picked.choice.url).split(':').pop()) },
    timeoutMs,
    insecure: !!(proxies && proxies.state.settings.insecure),
  });
  const text = await new Response(res.body).text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* html error page */ }
  try { res.cancel(); } catch { /* noop */ }
  return { ok: !!res.ok, status: res.status, json, text, exit: picked.choice.exit, exitIp: picked.choice.ip };
}

/** Store a signed-in principal as a new lane, with its own daily allowance. */
function addSignedInLane(provider, tokens, user) {
  const accounts = provider.accounts || (provider.accounts = []);
  const lane = accounts.length;
  const acc = newManagedAccount(provider, lane);
  acc.name = `signed-in-${lane + 1}${user && user.email ? ` (${user.email})` : ''}`;
  acc.apiKey = tokens.access_token;
  acc.signedIn = true;
  acc.refreshToken = tokens.refresh_token || '';
  acc.tokenExpiresAt = Date.now() + (Number(tokens.expires_in) || 3600) * 1000;
  if (user && user.email) acc.email = String(user.email);
  if (user && user.id) acc.orgId = String(user.id);
  accounts.push(acc);
  // The lane pool slices to `identityPool`, so the pool has to grow to hold a
  // signed-in principal - it is a different allowance, not spare capacity.
  provider.identityPool = Math.min(MANAGED_POOL_MAX, Math.max(managedPoolSize(provider), accounts.length));
  const as = accountState(provider.id, acc.id);
  as.uaIndex = OPENCODE_FREE_UA_INDEX;
  as.sessionId = '';
  openCodeSession(provider, acc);
  saveConfig().catch(() => {});
  persistState();
  pushEvent({ kind: 'signed-in', pid: provider.id, aid: acc.id, lane, email: acc.email || '' });
  console.warn(`[proxy] ${provider.id}: signed in as ${acc.email || 'a console account'} - lane ${lane} now serves as its own principal (its own daily allowance)`);
  return acc;
}

/** Refresh a signed-in lane's access token before it expires. */
async function refreshSignedInLane(provider, account) {
  if (!account || !account.refreshToken) return false;
  const r = await opencodeHttp('/auth/device/token', {
    method: 'POST',
    body: { grant_type: 'refresh_token', refresh_token: account.refreshToken, client_id: OPENCODE_CLIENT_ID },
  });
  if (!r.json || !r.json.access_token) {
    console.warn(`[proxy] ${provider.id}/${account.name}: token refresh failed (HTTP ${r.status}); it will be re-tried on the next request`);
    return false;
  }
  account.apiKey = r.json.access_token;
  if (r.json.refresh_token) account.refreshToken = r.json.refresh_token;
  account.tokenExpiresAt = Date.now() + (Number(r.json.expires_in) || 3600) * 1000;
  saveConfig().catch(() => {});
  console.warn(`[proxy] ${provider.id}/${account.name}: access token refreshed`);
  return true;
}

/**
 * Run the device flow: ask for a code, then poll until the operator approves it in
 * the browser. Each approval adds a principal, so N sign-ins are N daily buckets.
 */
async function startOpenCodeSignin(provider) {
  const existing = opencodeSignins.get(provider.id);
  if (existing && existing.status === 'pending' && existing.expiresAt > now()) return existing;
  const r = await opencodeHttp('/auth/device/code', { method: 'POST', body: { client_id: OPENCODE_CLIENT_ID } });
  if (!r.json || !r.json.device_code) {
    const detail = (r.json && (r.json.error || r.json.message)) || r.text.slice(0, 160) || `HTTP ${r.status}`;
    throw new Error(`device code request failed: ${detail}`);
  }
  const st = {
    status: 'pending',
    userCode: String(r.json.user_code || ''),
    // The edge answers with a path (`/console/device?...`); resolve it the way the
    // CLI does (`new URL(v, vt + '/')`) so the dashboard and curl show a URL that
    // can actually be opened.
    verificationUri: (() => {
      const raw = String(r.json.verification_uri_complete || r.json.verification_uri || '');
      try { return new URL(raw, `${OPENCODE_CONSOLE}/`).href; } catch { return raw; }
    })(),
    deviceCode: r.json.device_code,
    interval: Math.max(2, Number(r.json.interval) || 5),
    expiresAt: now() + (Number(r.json.expires_in) || 900) * 1000,
    startedAt: now(),
    error: '',
    email: '',
  };
  opencodeSignins.set(provider.id, st);
  pushEvent({ kind: 'signin-started', pid: provider.id });
  console.warn(`[proxy] ${provider.id}: sign-in code ${st.userCode} - approve it at ${st.verificationUri}`);
  void pollOpenCodeSignin(provider, st);
  return st;
}

async function pollOpenCodeSignin(provider, st) {
  while (st.status === 'pending' && now() < st.expiresAt) {
    await new Promise((r) => setTimeout(r, st.interval * 1000));
    if (st.status !== 'pending') return;
    let r;
    try {
      r = await opencodeHttp('/auth/device/token', {
        method: 'POST',
        body: { grant_type: OPENCODE_DEVICE_GRANT, device_code: st.deviceCode, client_id: OPENCODE_CLIENT_ID },
      });
    } catch (err) {
      st.error = err.message;
      continue;
    }
    const j = r.json || {};
    if (j.access_token) {
      let user = null;
      try {
        const u = await opencodeHttp('/api/user', { token: j.access_token });
        if (u.json) user = u.json;
      } catch { /* the token is what matters; the label is a nicety */ }
      const acc = addSignedInLane(provider, j, user);
      st.status = 'done';
      st.email = acc.email || '';
      st.error = '';
      return;
    }
    if (j.error === 'authorization_pending') continue;
    if (j.error === 'slow_down') { st.interval += 5; continue; }
    st.status = 'failed';
    st.error = String(j.error || j.message || `HTTP ${r.status}`);
    console.warn(`[proxy] ${provider.id}: sign-in failed: ${st.error}`);
    return;
  }
  if (st.status === 'pending') {
    st.status = 'expired';
    st.error = 'the code expired before it was approved';
    console.warn(`[proxy] ${provider.id}: sign-in code expired`);
  }
}

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
  // Exclusive mode: never answer from another vendor, whatever the config says.
  // The loader already strips the list; this is the second door, for a config
  // that is edited while the router runs.
  if (config.exclusiveProvider) return [];
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
  if (!fb) return { ok: false, reason: 'missing-provider', status: 0, message: `provider "${hop.provider}" is not configured` };
  const account = (fb.accounts || []).find((a) => a.enabled !== false);
  if (!account) return { ok: false, reason: 'no-account', status: 0, message: `provider "${hop.provider}" has no enabled account` };
  if ((PROVIDER_TYPES[fb.type] || PROVIDER_TYPES.custom).style !== 'openai') {
    return { ok: false, reason: 'style', status: 0, message: `provider "${hop.provider}" does not speak the OpenAI wire format` };
  }
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
      // Read the reason before cancelling: a hop that says "one request at a time"
      // is worth another round, one that says "policy" never is.
      const status = call.upstream.status;
      const retryMs = retryAfterMs(call.upstream.headers);
      let message = '';
      try { message = await peekError(call.upstream, 1024); } catch { /* noop */ }
      try { if (call.controller && call.controller.abort) call.controller.abort(); } catch { /* noop */ }
      return { ok: false, reason: classifyFailure(status, message), status, message, retryMs };
    }
    ctx2.exitId = call.exit ? call.exit.id : 'direct';
    state.totals.providerFallbacks = (state.totals.providerFallbacks || 0) + 1;
    pushEvent({ kind: 'provider-fallback', pid: fb.id, to: hop.model });
    console.warn(`[proxy] ${fb.id}: answering with ${hop.model} (configured fallback; the first-choice provider had nothing left)`);
    await relay(fb, account, call.upstream, call.controller, ctx2, res, 1);
    return { ok: true };
  } catch (err) {
    console.warn(`[proxy] ${fb.id}: fallback hop failed: ${err.message}`);
    return { ok: false, reason: 'network', status: 0, message: err.message };
  }
}

// How long the last resort keeps trying before the caller is told it could not be
// served. A hop that answers "one request at a time" (pollinations Classic) clears
// in seconds, and a shared free allowance comes back on its own, so the caller gets
// an answer instead of that hop's limit. `fallbackRetrySeconds` overrides it.
const FALLBACK_RETRY_MS = 30_000;
const FALLBACK_BACKOFF_MIN_MS = 1_200;
const FALLBACK_BACKOFF_MAX_MS = 6_000;
const FALLBACK_MAX_ROUNDS = 6;

function fallbackRetryDeadlineMs() {
  const raw = Number(config.fallbackRetrySeconds);
  if (!Number.isFinite(raw) || raw <= 0) return FALLBACK_RETRY_MS;
  return Math.min(300_000, Math.max(1_000, Math.round(raw * 1000)));
}

/**
 * The last resort, with retries: walk the configured hops, and when they all refuse,
 * wait out the shortest window any of them quoted and walk them again - until
 * `fallbackRetrySeconds` (default 30s) is spent, the round cap is reached, the
 * client hangs up, or a hop finally answers.
 *
 * The point is that a limit on one hop must never become a limit for the caller:
 * a `429` saying "your previous request is still running" is a queue, not a
 * verdict, and the next round usually gets through. Hops that are *deterministically*
 * dead (no such provider, no enabled account, wrong wire format, or a policy gate
 * that no retry can pass) are dropped from the walk instead of being re-tried, so a
 * misconfigured hop cannot hold the chain open for the whole deadline.
 *
 * Returns `{ ok, failures }`: `ok` also means "the client is gone, stop". `failures`
 * is what every hop said, in order, so the final error can report the last resort's
 * real reason instead of the first-choice provider's limit.
 */
async function relayFallbackChain(provider, body, res, hops) {
  if (!hops.length) return { ok: false, failures: [] };
  const clientGone = () => res.writableEnded || res.destroyed || !!(res.socket && res.socket.destroyed);
  const failures = [];
  const dead = new Set();
  const deadline = now() + fallbackRetryDeadlineMs();
  let round = 0;
  while (round < FALLBACK_MAX_ROUNDS && now() < deadline) {
    round += 1;
    let waitMs = 0;
    let live = 0;
    for (let i = 0; i < hops.length; i += 1) {
      if (res.headersSent || clientGone()) return { ok: true, failures };
      if (dead.has(i)) continue;
      const hop = hops[i];
      const out = await relayViaFallback(hop, body, res);
      if (out.ok) {
        if (round > 1) pushEvent({ kind: 'fallback-retry', pid: provider.id, round, n: failures.length });
        return { ok: true, failures };
      }
      if (out.reason === 'missing-provider' || out.reason === 'no-account' || out.reason === 'style') {
        // Nothing a retry can change: this hop is not a candidate at all.
        dead.add(i);
        console.warn(`[proxy] ${hop.provider}/${hop.model}: dropped from the fallback walk (${out.message})`);
        continue;
      }
      live += 1;
      failures.push({ provider: hop.provider, model: hop.model, status: out.status || 0, kind: out.reason, message: String(out.message || '').slice(0, 180), retryMs: Number(out.retryMs) || 0 });
      // The shortest window any hop offered is the one worth waiting: a hop that
      // quoted nothing gets the floor, so one silent hop cannot stretch the round.
      const quoted = Number(out.retryMs) > 0 ? Number(out.retryMs) : 0;
      const hopWait = Math.min(FALLBACK_BACKOFF_MAX_MS, Math.max(FALLBACK_BACKOFF_MIN_MS, quoted));
      waitMs = waitMs ? Math.min(waitMs, hopWait) : hopWait;
    }
    if (!live) break;
    const left = deadline - now();
    if (left <= 0) break;
    if (waitMs > left) waitMs = left;
    const last = failures[failures.length - 1] || {};
    console.warn(`[proxy] ${provider.id}: every fallback hop refused (${last.provider}/${last.model} ${last.status || 'n/a'}${last.message ? ` "${String(last.message).slice(0, 60)}"` : ''}); retrying in ${(waitMs / 1000).toFixed(1)}s (round ${round}/${FALLBACK_MAX_ROUNDS})`);
    await new Promise((r) => { const t = setTimeout(r, waitMs); if (t.unref) t.unref(); });
  }
  return { ok: clientGone(), failures: failures.slice(-8) };
}

async function handleProxy(req, res, url) {
  const t0 = now();
  // Wall-clock ceiling for the free tier's limit ladder (rotate -> reinstall ->
  // model walk -> rotate -> session). Past it the request falls through to the
  // fallback provider instead of leaving the caller waiting.
  let freeLimitDeadline = t0 + freeProviderBudgetMs();
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
  // Size of what we have to hand over: a million-token context needs a much
  // longer first-byte ceiling than the one a short chat does, and its budget is
  // extended by the same amount so the ladder is not cut off mid-prefill.
  const promptBytes = Buffer.byteLength(JSON.stringify(body || ''));
  const contextMs = promptSizeBudget(promptBytes);
  // `contextMs` is a *first-byte* allowance, not ladder time: a quota refusal comes
  // back in ~300ms whatever the prompt weighs, so adding it to the ladder deadline
  // made a 3MB agent turn sit for 95s before the fallback was even tried.
  freeLimitDeadline += Math.min(contextMs, PROMPT_BUDGET_LADDER_MAX_MS);

  const { provider, model, unknown } = resolveProvider(body, req);
  if (!provider) {
    state.totals.failed += 1;
    if (unknown) {
      return sendJson(res, 404, {
        error: {
          message: `this router serves only "${config.exclusiveProvider}"; "${unknown}" is not configured, and the request will not be answered by another provider`,
          type: 'router_unknown_provider',
        },
      });
    }
    return sendJson(res, 503, {
      error: {
        message: config.providers.length
          ? 'No provider has a healthy account. Add or reset accounts in the dashboard at /admin.'
          : 'No providers configured. Open /admin to add one.',
        type: 'router_no_provider',
      },
    });
  }
  // The CLI transport is a client, not an account: it needs no identity lane, and
  // its own free allowance is the only quota in play. Handled before the account
  // machinery below, which would otherwise ask it for an account it never has.
  if ((PROVIDER_TYPES[provider.type] || {}).style === 'opencode-cli') {
    return handleOpenCodeCli(provider, body, model, req, res);
  }

  if (!(provider.accounts || []).length) {
    // A managed provider created a second ago has no identity yet; mint it here
    // so "add provider, send a request" works instead of answering 503.
    if (isManagedProvider(provider)) ensureManagedAccount(provider);
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
  const requestedModel = String(model || '');
  const triedModels = new Set([requestedModel]);
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
  // Free tier: first attempt from the host's own IP, then the pool (see above).
  // The request is counted into the provider's ladder demand here, because
  // everything below reads it: two requests in the ladder need two identities, and
  // the all-lane sweep must stand down instead of parking the lane the other one
  // is using (see "Identity burst" above).
  let freeDemandHeld = false;
  let releaseFreeDemand = () => {};
  if (style === 'opencode-free') {
    ctx.forceDirect = true;
    noteFreeDemand(provider.id, 1);
    freeDemandHeld = true;
    releaseFreeDemand = () => {
      if (!freeDemandHeld) return;
      freeDemandHeld = false;
      noteFreeDemand(provider.id, -1);
    };
    // Backstop: a client that hangs up mid-ladder must not leave the count high.
    res.on('close', releaseFreeDemand);
    // Every lane is spent or rebuilding already: this request is here to learn
    // the same limit again, so cap the ladder short. `contextMs` is added back
    // because a big prompt still needs its own head start. A concurrent request is
    // the exception: it can mint a lane of its own, so its long ladder is real.
    if ((provider.accounts || []).every((a) => !laneReady(provider, a)) && !freeDemandConcurrent(provider.id)) {
      freeLimitDeadline = t0 + freeProviderDryBudgetMs() + contextMs;
    }
  }
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
  const maxAttempts = provider.maxAttempts > 0
    ? provider.maxAttempts
    : (style === 'opencode-free'
      // One attempt per lane, plus room for the model walk and the session trick
      // once every lane is spent, so a handover never eats the last attempt.
      ? Math.max((provider.accounts || []).length + 4, 8)
      : Math.max(1, (provider.accounts || []).length));
  let sawLimit = false;
  // The longest window any upstream quoted during this request, carried to the
  // final answer. A lane-scale 429 (`retry-after: 2`) used to end up as a bare
  // 429 with no header, and every SDK that honours `retry-after` - the openai
  // client, the opencode CLI, fetch-retry - then re-asks within milliseconds and
  // burns the ladder it was supposed to wait out.
  let lastUpstreamRetryMs = 0;
  let laneWaitDone = false;
  // Identities this request minted for lanes that were all parked (see "Identity
  // burst" above). Capped, so one request cannot mint a storm on its own.
  let burstMints = 0;
  // Set the moment a day-scale quota answer parks the tier. It ends the whole
  // provider attempt loop, not just this lane's: every remaining lane shares the
  // same daily bucket, so attempt 2 would only spend another exit-free 429 and,
  // worse, a local 2xx on one lane would clear the park that the answer earned.
  let dayCeiling = false;
  // One all-lane sweep per request, whatever `allLanesRenewGapMinutes` says. A single
  // ladder can end in "spent" on several lanes in a row, and four new identities per
  // lane-ending is a mint storm, not a fresh start: the point of the rule is that the
  // *next* request finds four fresh identities, which one sweep already achieves.
  // The configured floor then governs how often a new request may sweep again.
  let allLanesSweptHere = false;
  const sweepAllLanes = (why) => {
    if (allLanesSweptHere) return 0;
    const { swept, queued } = queueAllLaneRefreshes(provider, why);
    if (swept) allLanesSweptHere = true;
    return queued;
  };
  // Cross-provider last resort, resolved once: in exclusive mode this is always
  // empty, and that changes how a known-dry ceiling is treated just below.
  const fallbackHops = providerFallbackHops(provider);
  // The ceiling is upstream and account-wide, so while the pool is resting a fresh
  // ladder only re-learns the same 429 and costs the caller its first byte - which
  // is exactly what made a big prompt look like "the router is broken". Go straight
  // to the configured fallback hops and leave the free tier alone until it recovers.
  //
  // With nowhere to hand the request to (exclusive mode, or no fallback configured)
  // skipping the ladder is not an optimisation, it is a guaranteed failure: the
  // caller asked for this provider, so prove the ceiling again instead - that is
  // what makes the request start working the moment the allowance returns.
  const freeCeilingDry = style === 'opencode-free' && freeTierParked() && fallbackHops.length > 0;
  if (freeCeilingDry) {
    // A known-dry day is a limit, not a missing account: if the fallback hops cannot
    // answer either, the client is told when the tier is back (429 +
    // retryAfterSeconds) instead of being handed a 503 that reads like a broken
    // configuration. In exclusive mode that 429 is the answer.
    sawLimit = true;
    console.warn(`[proxy] ${provider.id}: free ceiling known dry (${Math.round((freeDry.until - now()) / 1000)}s left) - skipping the ladder, answering from the fallback`);
  } else if (style === 'opencode-free' && freeTierParked()) {
    console.warn(`[proxy] ${provider.id}: free ceiling known dry (${Math.round((freeDry.until - now()) / 1000)}s left) - trying it anyway, there is no fallback provider to hand the request to`);
  } else if (style === 'opencode-free' && freeDry.until && freeDry.until <= now() && freeDry.reclaimed !== freeDry.until) {
    // The parked day ended, so the upstream allowance is fresh again. Wake the pool
    // once: the rebuilds and exit re-pins that were deliberately held back during
    // the dry window would otherwise all land on the first request of the new day,
    // and that request would spend the fresh allowance on them.
    freeDry.reclaimed = freeDry.until;
    kickPool(`free allowance reset for ${provider.id}`);
    // The bucket just refilled: present a brand-new client on a brand-new address
    // for it, which is the operator's own recipe (drop the client, move the IP,
    // install a new identity) fired at the one instant it can actually help.
    // Answers still on the way from yesterday's spend cannot clear it earlier.
    let recycled = 0;
    for (const laneAcc of provider.accounts || []) {
      if (laneAcc.enabled === false) continue;
      try { recycleLane(provider, laneAcc, 'daily allowance reset'); recycled += 1; } catch { /* keep the lane */ }
    }
    console.warn(`[proxy] ${provider.id}: free allowance window has reset - recycled ${recycled} lane(s) onto fresh addresses with new identities for the new day`);
  }

  for (let attempt = 1; !freeCeilingDry && !dayCeiling && attempt <= maxAttempts; attempt += 1) {
    let account = selectAccount(provider, tried);
    if (!account && style === 'opencode-free' && !laneWaitDone && now() < freeLimitDeadline) {
      laneWaitDone = true;
      // Concurrent request: mint the identity it needs on the lane that has waited
      // longest, rather than queue behind a rebuild it is not the only customer of.
      if (freeDemandConcurrent(provider.id) && burstMints < LANE_BURST_MINT_MAX) {
        const lent = lendIdentity(provider, tried);
        if (lent) {
          burstMints += 1;
          account = lent;
        }
      }
      if (!account) {
        // Every lane is spent or rebuilding. Rather than failing the caller, wait a
        // moment for the fastest rebuild - that is still far shorter than the
        // upstream's own retry window, and the lanes are being rebuilt in parallel.
        const budget = Math.min(LANE_WAIT_MS, Math.max(0, freeLimitDeadline - now()));
        if (await waitForLane(provider, budget)) {
          tried.clear();
          account = selectAccount(provider, tried);
        }
      }
    }
    if (!account) break;
    tried.add(account.id);
    if (isManagedProvider(provider)) ctx.lane = laneKeyOf(account);
    let renewals = 0;

    const rotateWanted = provider.proxy === 'rotate' && !!proxies && proxies.state.settings.enabled;
    const rotateMax = rotateWanted ? Math.max(0, Number(proxies.state.settings.rotateAttempts) || 0) : 0;
    let rotations = 0;
    let lastExitId = '';
    let relayed = false;
    let lastFailure = null;
    let lastFailureKey = '';
    let netRetries = 0;
    let sessionRetries = 0;
    let modelsThisIp = 0;
    const freeLadder = style === 'opencode-free';

    // Inner loop: same account, new exit IP. A rate limit here is usually
    // IP-bound, so rotating the exit beats burning the account for 12 hours.
    for (;;) {
      // The budget is checked *before* the attempt, not only after a refusal: an
      // exit that never answers would otherwise run past the minute on its own.
      const remaining = freeLadder ? freeLimitDeadline - now() : Infinity;
      if (freeLadder && remaining <= 0) {
        console.warn(`[proxy] ${provider.id}/${account.name}: free-tier budget spent before the attempt; giving up on this provider`);
        break;
      }
      // A burn promotes a spare and xray is re-pointed at it; dialling while that
      // restart is in flight would hit a closed port and burn the spare too.
      if (proxies && proxies.restarting) await proxies.settle(4000);
      const as = accountState(provider.id, account.id);
      as.requests = (as.requests || 0) + 1;
      as.lastUsed = now();
      openWindow(provider, account);

      let upstream;
      let controller;
      let call;
      try {
        call = await callUpstream(provider, account, ctx, {
          // Never more than the first-byte deadline, never less than a fair 6s
          // (the budget check above already guarantees a positive remainder).
          // Retries get the short deadline: a lane handover costs less than
          // waiting out a stalled exit - but a large body still gets its prefill
          // allowance, halved on a retry because the exit, not the prompt, is on
          // trial there.
          //
          // The 6s floor is capped by the operator's own ladder budget: it exists so
          // an attempt is not handed a token deadline, not to overrule
          // `freeLadderBudgetSeconds`. Without the cap a 3s dial still spent 6s on
          // every request, which is the exact cost the dial was added to remove.
          headMs: Math.min(
            (attempt === 1 ? EXIT_FIRST_BYTE_MS : EXIT_RETRY_FIRST_BYTE_MS) + (attempt === 1 ? contextMs : Math.round(contextMs / 2)),
            Math.max(Math.min(6_000, freeProviderBudgetMs()), remaining),
          ),
          // Buffered answers also get an idle deadline on the body.
          bodyIdleMs: remaining,
        });
        ({ upstream, controller } = call);
      } catch (err) {
        as.failures = (as.failures || 0) + 1;
        as.lastStatus = 0;
        as.lastError = err.message.slice(0, 120);
        pushEvent({ kind: 'error', pid: provider.id, aid: account.id, st: 0 });
        persistState();
        console.warn(`[proxy] ${provider.id}/${account.name} network error: ${err.message}`);
        // Any exit-level failure leaves the pool: a stalled or resetting exit is
        // spent the same way a limited one is, and the pool must rebuild from it.
        if (err.exit && proxies) proxies.noteResult(err.exit.id, false, { burn: true });
        // The exit is at fault, not the identity, so change the exit first - a
        // stall and a reset get the same answer. Only when the hops are spent is
        // the identity rebuilt, and only on the free tier (elsewhere the account
        // itself is what failed).
        if (rotateWanted && rotations < Math.max(1, rotateMax) && proxies.activeExits().length > 1) {
          const next = proxies.rotate(`${err.exitStall ? 'stalled' : 'failed'} exit on ${provider.id}`, provider.id);
          if (next) {
            rotations += 1;
            ctx.forceDirect = false;
            console.warn(`[proxy] exit ${err.exit ? err.exit.id : '?'} ${err.exitStall ? `produced no response in ${Math.round(Math.min(EXIT_FIRST_BYTE_MS, remaining) / 1000)}s` : 'failed'}; rotated to ${next.id} (${rotations}/${Math.max(1, rotateMax)})`);
            continue;
          }
          void proxies.replenish('every active exit failed');
        }
        if (freeLadder && rotations > 0 && renewals < 1 && now() < freeLimitDeadline) {
          const fresh = reinstallManagedClient(provider, account, `network error through exit ${err.exit ? err.exit.id : '?'}`);
          renewals += 1;
          account = fresh;
          console.warn(`[proxy] ${provider.id}: no exit answered; reinstalled the client and retrying`);
          continue;
        }
        // Transient socket drops are worth one retry on the same identity before
        // the provider is called unhealthy.
        if (netRetries < 1 && now() < freeLimitDeadline) {
          netRetries += 1;
          await new Promise((r) => setTimeout(r, 400));
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
        if (retryMs > lastUpstreamRetryMs) lastUpstreamRetryMs = retryMs;
        const snippet = await peekError(upstream);
        const kind = classifyFailure(upstream.status, snippet);
        lastFailure = { status: upstream.status, kind, message: snippet };
        // The same answer from a second exit means the condition is not the
        // exit (policy gate, bad model handle): stop burning the pool.
        const repeated = rotations > 0 && lastFailureKey === `${upstream.status}|${snippet}`;
        lastFailureKey = `${upstream.status}|${snippet}`;
        const freeTier = (PROVIDER_TYPES[provider.type] || {}).style === 'opencode-free';
        // Remember an account-wide ceiling only once a lane has spent its whole
        // ladder against it (see the deadline break below): the body itself is not
        // proof, a ladder that could not clear it is.
        const accountWide = /subscribe|sign ?up|upgrade|\/go\b/i.test(String(snippet || ''));
        if (isManagedProvider(provider) && renewals < 1 && !freeTier) {
          // Non-free managed providers have no exit pool: reinstall is the only
          // lever, so it runs first for them. The free tier rotates first (see
          // the ladder below) and handles its own reinstall there.
          const rt = accountState(provider.id, account.id);
          // 0 means "reinstall at once"; unset keeps the conservative spacing
          // that stops an identity being minted on every single 429.
          const rawGap = config.reinstallGapMinutes;
          const gapMs = (rawGap === undefined || rawGap === null || rawGap === '')
            ? MANAGED_RENEW_GAP_MS
            : Math.max(0, Number(rawGap) || 0) * 60_000;
          const since = Date.now() - (rt.lastRenewAt || 0);
          if (since >= gapMs) {
            const fresh = reinstallManagedClient(provider, account, `HTTP ${upstream.status}`);
            renewals += 1;
            account = fresh;
            continue;
          }
        }
        if (freeTier && kind === 'quota') {
          // The free allowance is accounted per IP *and* per model, and both
          // reset by moving. Measured on this box: the same request answered 200
          // from a fresh exit in the minute the old one answered 429. The
          // operator's order is therefore IP first, identity second:
          //   1. the next exit IP, retrying the model the client asked for,
          //   2. a reinstalled client (new identity, session, project, UA) on
          //      that fresh IP when it limits as well,
          //   3. then the rest of this lane's model pool on its own IP, and a
          //      rebuilt session,
          //   4. only now hand the request to the next identity lane, whose IP
          //      and identity are untouched, while this lane is rebuilt in the
          //      background (new IP, new identity, new build),
          //   5. if every lane is spent, wait briefly for the first rebuild,
          //   6. the caller's configured fallback provider.
          // Handing over is deliberately *after* the local rungs: a lane's exit
          // is the shared resource, and hopping lanes on the first 429 burns four
          // IPs on a dry model while the model pool beside it is untried. The
          // order only leaves step 5 on the caller's clock, and the rebuild
          // happens on the lanes that are parked - never on the serving one. Every
          // exit that answers a limit is burned, so the pool rebuilds itself
          // instead of cycling back onto spent IPs. Nothing cools an account:
          // the free tier carries no timer.
          //
          // One answer outranks all of it: a *day-scale* retry-after. The body is
          // identical either way ("Free usage limit reached. Subscribe to OpenCode
          // Go"), so it cannot tell a spent address from a spent day - the horizon
          // can, and only the spent day is immune to every rung below. Parking on
          // the first sighting turns the first request after the daily ceiling into
          // one cheap 429 (no exit burned, no identity minted, no rebuild queued),
          // which is what an exclusive deployment (no hop to hand over to) wants
          // too: it is retried on the next request instead of being probed.
          // `freeDayHorizonMinutes: 0` switches the park off entirely: the answer is
          // then treated as exit/identity-scale like any other limit and the ladder
          // below runs, which is what an operator asks for when they would rather
          // keep rotating than wait out the upstream's clock.
          const dayHorizonMs = freeDayHorizonMs();
          if (dayHorizonMs > 0 && retryMs >= dayHorizonMs) {
            parkFreeTier(provider.id, retryMs, snippet);
            // Every lane of the provider shares the one bucket, so every lane is
            // spent - and marking them is also what puts the reset instant on the
            // *first* answer a client gets, instead of making each lane learn it
            // with an attempt of its own.
            for (const lane of provider.accounts || []) {
              markLimited(provider, lane, upstream.status, { ms: retryMs, message: snippet });
            }
            dayCeiling = true;
            break;
          }
          if (now() > freeLimitDeadline) {
            console.warn(`[proxy] ${provider.id}/${account.name}: free-tier limit ladder spent its ${Math.round((freeLimitDeadline - t0 - contextMs) / 1000)}s budget after ${rotations} rotation(s); giving up on this provider`);
            // A ladder that burned its whole budget without clearing the limit
            // means the ceiling is upstream of this box: rotating exits and
            // minting identities only spend addresses the next real request needs.
            // Rest the pool for a short window and answer from the fallback.
            if (accountWide) {
              freeDry.hits += 1;
              // Two ladders in a row could not clear it - that is the account, not
              // the exit. One is kept as a fluke so a single slow burst does not
              // switch the pool off.
              if (freeDry.hits >= 2) restFreePool(provider.id, 'ceiling is upstream, not the exit');
            }
            // The budget running out on a quota is still "I hit the limit": every lane
            // gets a new identity, on the same floor as the other sweep, so the next
            // request starts on a complete set rather than re-proving the ceiling lane
            // by lane. This is the path a dry deployment takes most often, because a
            // 6-8s `freeLadderBudgetSeconds` ends the ladder here rather than at the
            // per-lane handover below.
            sweepAllLanes(`HTTP ${upstream.status} - ladder budget spent`);
            break;
          }
          if (freeTierParked()) {
            // Known-dry ceiling: leave the exit in the pool instead of burning it
            // for an answer this box cannot change, and hand the request on. The
            // pool still has to refill itself for the moment the ceiling lifts, so
            // the background rebuild is kicked here too - resting is not sleeping.
            kickPool(`dry free ceiling for ${provider.id}`);
            console.warn(`[proxy] ${provider.id}/${account.name}: free ceiling is dry upstream - not burning exit ${call.exit ? call.exit.id : 'direct'}; ${fallbackHops.length ? 'answering from the fallback' : 'reporting it to the client'}`);
            break;
          }
          const laneKey = laneKeyOf(account);
          // No exit in the pool means there is nothing to hop to: treat the IP
          // step as impossible rather than attempted, so the identity step (the
          // next best move from the same IP) still happens - and start refilling
          // the pool in the background, because the next request will need it.
          const poolUsable = proxies.activeExits().length > 0;
          if (!poolUsable) kickPool(`empty exit pool for ${provider.id}`);
          const rotationPossible = rotateWanted && rotateMax > 0 && poolUsable;
          // One IP hop per lane and request: with several lanes alive, burning
          // three exits on one identity before trying a healthy lane next door is
          // waste - the hops are cheap, the pool is not.
          //
          // `accountWide` disables the hop entirely. When the body says the
          // allowance itself is spent ("Free usage limit reached. Subscribe to
          // OpenCode Go"), no address and no identity can clear it: the hop would
          // burn an exit, and the refill under it would hold the caller for the
          // length of a subscription fetch. Hand over instead.
          const canRotate = !accountWide
            && rotationPossible
            && rotations < (freeLadder ? Math.min(rotateMax, 1) : rotateMax);
          // The exit that answered the limit is spent - unless the body says the
          // ceiling is the account ("Free usage limit reached. Subscribe to OpenCode
          // Go"), in which case no address can clear it and burning one only spends
          // the pool the next real request needs. That case is handled by resting
          // the pool below, not by a rotation.
          if (call.exit) proxies.noteResult(call.exit.id, false, { burn: !accountWide });
          const rt = accountState(provider.id, account.id);
          const rawGap = config.reinstallGapMinutes;
          const gapMs = (rawGap === undefined || rawGap === null || rawGap === '')
            ? MANAGED_RENEW_GAP_MS
            : Math.max(0, Number(rawGap) || 0) * 60_000;
          // One IP change first; the identity goes only when that did not help.
          // A provider with no pool has no IP to move to, so it reinstalls on the
          // first limit instead. One reinstall per lane and request at most; the
          // free tier ignores the spacing, because a rebuilt identity is exactly
          // what a spent lane needs.
          const reinstallDue = renewals < 1
            && (rotations > 0 || !rotationPossible)
            && (freeLadder || Date.now() - (rt.lastRenewAt || 0) >= gapMs);
          if (canRotate && !reinstallDue) {
            ctx.forceDirect = false; // never go back to the host IP, it is spent
            const pool = proxies.activeExits();
            let next = pool.length ? proxies.rotateLane(provider, laneKey, `HTTP ${upstream.status} on ${provider.id}`) : null;
            if (!next) {
              // Pool drained (or never filled): rebuild it now, and give the
              // rebuild's time back to the budget - waiting on the pool is not
              // the caller's fault.
              const rebuildStart = now();
              console.warn(`[proxy] ${provider.id}: no usable exit left, rebuilding the pool with new subscriptions`);
              // Bounded: a subscription fetch is the pool's job, not the caller's.
              // The rebuild keeps running in the background either way; past five
              // seconds the request moves on with whatever the pool has.
              const rebuilt = await Promise.race([
                proxies.replenish(`no usable exit for ${provider.id}`, { maxCandidates: 15 }),
                new Promise((resolve) => { const t = setTimeout(() => resolve(null), 5_000); if (t.unref) t.unref(); }),
              ]);
              freeLimitDeadline += now() - rebuildStart;
              next = rebuilt && rebuilt.ok ? proxies.rotateLane(provider, laneKey, `rebuilt pool for ${provider.id}`) : null;
            }
            if (next) {
              rotations += 1;
              // A fresh IP gets the requested model again, not the tail of a walk.
              triedModels.clear();
              triedModels.add(requestedModel);
              ctx.model = requestedModel;
              modelsThisIp = 0;
              console.warn(`[proxy] ${provider.id}#${laneKey}/${account.name} -> ${upstream.status}; retrying ${requestedModel} on exit ${next.id} (${rotations}/${rotateMax})`);
              // Running low: refill in the background while this request keeps
              // hopping, so the next request never starts on an empty pool.
              if (proxies.activeExits().length <= 3) void proxies.replenish('pool running low');
              continue;
            }
          }
          if (reinstallDue) {
            reinstallManagedClient(provider, account, `HTTP ${upstream.status} on a fresh exit`);
            renewals += 1;
            console.warn(`[proxy] ${provider.id}#${laneKey}/${account.name} -> ${upstream.status}${rotations ? ' on the new exit too' : ''}; reinstalled this lane and retrying`);
            continue;
          }
          // Hand over only once this lane's own IP step is done, or when no exit
          // was left to do it with. Anything earlier and four lanes are burned on
          // a dry model while the model pool beside them is untried; the walk and
          // the session trick cost one request each and often answer at once.
          const others = (provider.accounts || []).filter((a) => a.id !== account.id && laneReady(provider, a)).length;
          const canHandOver = others > 0 && rotations > 0;
          if (!canHandOver) {
            const nextModel = freeFallbacks.find((m) => !triedModels.has(m));
            // On the host's own IP walk the whole pool (it is the IP we know is
            // clean); once we are on a shared exit, two models is enough before
            // moving on.
            const modelsPerIp = ctx.forceDirect ? freeFallbacks.length : 2;
            if (modelsThisIp < modelsPerIp && nextModel && modelFallbacks < freeFallbacks.length) {
              const dryModel = ctx.model;
              triedModels.add(nextModel);
              modelFallbacks += 1;
              modelsThisIp += 1;
              ctx.model = nextModel;
              state.totals.modelFallbacks = (state.totals.modelFallbacks || 0) + 1;
              pushEvent({ kind: 'model-fallback', pid: provider.id, aid: account.id, from: dryModel, to: nextModel });
              console.warn(`[proxy] ${provider.id}/${account.name}: ${dryModel} is out of allowance; answering with ${nextModel} instead (${modelFallbacks}/${freeFallbacks.length})`);
              continue;
            }
            if (sessionRetries < 1 && now() < freeLimitDeadline) {
              // A new session is a new allowance for per-session accounting.
              sessionRetries += 1;
              ctx.freshSession = true;
              console.warn(`[proxy] ${provider.id}/${account.name} -> ${upstream.status} on the free tier; minting a new session and retrying`);
              continue;
            }
          }
          // Concurrent requests: this lane is spent for *this* one, but parking it
          // (and sweeping the rest) would take the lanes the other requests are
          // using and hand them the same 12s wait. Rebuild it in place instead -
          // new identity, new exit - and leave it in service; the outer loop hands
          // this request to the next lane meanwhile.
          // A lane that is the last one any request could use must never be parked.
          // With no spare, parking it turns a rate limit into `router_no_healthy_account`
          // for the whole park window - the router refusing on its own account, not
          // the upstream's, which is exactly the "rate-limited and cooling down"
          // the operator saw with `identityPool: 1`. Same reasoning as the
          // concurrent case: the lanes in flight belong to other requests and a
          // sweep would take them away. Either way, rebuild in place, stay in
          // service, and hand this request over.
          const readyLanes = (provider.accounts || []).filter((a) => laneReady(provider, a));
          const soleLane = readyLanes.length <= 1;
          if (freeDemandConcurrent(provider.id) || soleLane) {
            const beside = freeDemandOf(provider.id) - 1;
            if (soleLane || renewals < LANE_BURST_MINT_MAX) {
              renewals += 1;
              rebuildLaneNow(provider, account, soleLane
                ? `HTTP ${upstream.status} - last ready lane, rebuilt in place so the provider stays answerable`
                : `HTTP ${upstream.status} - spent beside ${beside} concurrent request(s)`);
            } else {
              // Two fresh identities did not clear it either: rest the lane for a
              // second rather than offer it to the next request as if it were new.
              const tired = accountState(provider.id, account.id);
              tired.limitedUntil = now() + 1_000;
              persistState();
            }
            console.warn(`[proxy] ${provider.id}#${laneKey}/${account.name} -> ${upstream.status} [quota] ${soleLane ? 'last ready lane' : `spent beside ${beside} concurrent request(s)`}; rebuilt in place, no park and no sweep; handing over`);
            ctx.model = requestedModel;
            triedModels.clear();
            triedModels.add(requestedModel);
            modelsThisIp = 0;
            break;
          }
          // This lane is spent: park it, rebuild it off the request path, and let
          // the outer loop hand the request to the next lane instead of waiting.
          markLimited(provider, account, upstream.status, { ms: LANE_REFRESH_COOLDOWN_MS, message: snippet });
          // The pool is what pays for this ladder, so notice when it has just been
          // spent dry on an account-wide ceiling: with no exit left there is
          // nothing for the next lane (or the next request) to move to, and every
          // further attempt only logs "no working exit to switch to". Rest the pool
          // for a backoff window and let the rebuild catch up in the background.
          if (accountWide && proxies.activeExits().length === 0 && freeDry.until < now()) {
            restFreePool(provider.id, 'pool spent dry on an upstream ceiling');
          }
          // The operator's rule: a limit is answered with a new identity on *every*
          // lane, not only on the one that reported it - so the next request starts
          // on four fresh identities instead of spending itself proving the other
          // three are spent as well. Throttled: a cold burst must not turn one dry
          // ceiling into a stream of new identities.
          const swept = sweepAllLanes(`HTTP ${upstream.status} - lane spent`);
          const queued = swept > 0
            ? true
            : queueLaneRefresh(provider, account, `HTTP ${upstream.status} - lane spent`);
          console.warn(`[proxy] ${provider.id}#${laneKey}/${account.name} -> ${upstream.status} [quota] lane spent${snippet ? ` (${snippet.slice(0, 80)})` : ''} after ${rotations} rotation(s), ${renewals} reinstall(s), ${modelFallbacks} model(s); ${swept > 0 ? `minting a new identity on all ${swept} lane(s)` : queued ? 'rebuilding it in the background' : 'already rebuilding'}, ${others} lane(s) ready; handing over`);
          // The next attempt starts on the requested model regardless.
          ctx.model = requestedModel;
          triedModels.clear();
          triedModels.add(requestedModel);
          modelsThisIp = 0;
          break;
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
        // The free allowance is answering again: drop the dry backoff so the next
        // request uses the tier instead of the fallback.
        if (style === 'opencode-free') clearFreeDry(provider.id);
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

    if (relayed) {
      // The ladder is over for this request, so it stops counting towards demand:
      // the response may stream for minutes, and a stream is not a request that
      // needs an identity.
      releaseFreeDemand();
      return;
    }
    if (rotations) pushEvent({ kind: 'rotated', pid: provider.id, aid: account.id, n: rotations });
    void lastExitId;
  }
  releaseFreeDemand();

  // Configured cross-provider fallback, tried only when nothing on this provider
  // could answer (every model out of allowance or every account cooling). Empty
  // in exclusive mode, so the caller gets this provider's own error instead.
  let fallbackFailures = [];
  if (!res.headersSent) {
    const chain = await relayFallbackChain(provider, body, res, fallbackHops);
    if (chain.ok) return;
    fallbackFailures = chain.failures;
  }

  state.totals.failed += 1;
  if (!res.headersSent) {
    const { error, status } = allExhausted(provider, tried, sawLimit);
    // Never report the first-choice provider's limit as if it were the whole story:
    // when the last resort also refused, say what it said and when to come back.
    if (fallbackFailures.length) {
      error.fallback = fallbackFailures;
      error.message = `${error.message} Every fallback hop refused as well (last: ${fallbackFailures[fallbackFailures.length - 1].provider}/${fallbackFailures[fallbackFailures.length - 1].model} ${fallbackFailures[fallbackFailures.length - 1].status || 'no status'}${fallbackFailures[fallbackFailures.length - 1].message ? ` - ${fallbackFailures[fallbackFailures.length - 1].message}` : ''}).`;
    }
    // The last resort's verdict is the honest status: a hop that answered 429 with a
    // retry window is a rate limit, not a router outage, and answering 503 for it only
    // invites a hot retry loop. Only when nothing quoted a limit does the ladder's own
    // story decide (429 after a limit, 503 otherwise).
    const lastHopStatus = fallbackFailures.length ? Number(fallbackFailures[fallbackFailures.length - 1].status) : 0;
    const lastHopLimited = lastHopStatus === 429 || lastHopStatus === 402 || lastHopStatus === 403;
    const answerStatus = lastHopLimited ? 429 : (status || (sawLimit ? 429 : 503));
    // A 429 with no Retry-After is an invitation to hot-loop. The body already
    // carries retryAfterSeconds, but every SDK that honours the header (the
    // openai client, the opencode CLI, fetch-retry) looks only at the header, so
    // the answer is also stamped with it. For the free-day ceiling this is the
    // ~16h clock to 00:00 UTC: the client sleeps once instead of retrying in a
    // storm. Never advertise 0 -- that reads as "retry now".
    const dryUntil = provider && (PROVIDER_TYPES[provider.type] || {}).style === 'opencode-free' && freeDry.until > now() ? freeDry.until : 0;
    const hopRetryMs = fallbackFailures.length ? Number(fallbackFailures[fallbackFailures.length - 1].retryMs) || 0 : 0;
    const waitSecs = Math.max(
      Number(error.retryAfterSeconds) || 0,
      dryUntil ? Math.round((dryUntil - now()) / 1000) : 0,
      lastUpstreamRetryMs ? Math.ceil(lastUpstreamRetryMs / 1000) : 0,
      hopRetryMs ? Math.ceil(hopRetryMs / 1000) : 0,
    );
    sendJson(res, answerStatus, { error }, waitSecs > 0 ? { 'retry-after': String(Math.max(1, waitSecs)) } : null);
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
    // Exclusive mode: the dashboard shows the lock so a "why can't I add a
    // provider" question answers itself.
    exclusiveProvider: config.exclusiveProvider || '',
    cooldownHours: clampHours(config.cooldownHours),
    cooldownStatuses: config.cooldownStatuses,
    // Free-tier ladder dial (seconds); 25 is what an unset config behaves as, and
    // the dry leash is always 80% of it - both shown so the dashboard can label the
    // input with the numbers the router is actually using.
    freeLadderBudgetSeconds: config.freeLadderBudgetSeconds ?? 25,
    freeLadderDryBudgetMs: freeProviderDryBudgetMs(),
    // Floor between all-lane identity sweeps (minutes); 0 means every limit. Each
    // provider reports its own last sweep as `allLanesSweptAt`.
    allLanesRenewGapMinutes: allLanesRenewGapMs() / 60_000,
    // Identity burst: while several requests are in the ladder, each mints its own
    // identity instead of queueing behind a parked lane (see LANE_BURST_MINT_MAX).
    identityBurst: identityBurstEnabled(),
    // What counts as a spent *day* rather than a spent exit/identity (minutes), and
    // how long the pool rests after a dry ladder (0 = never). Both are the numbers
    // the router is actually using, so the settings dialog labels its inputs with
    // them instead of a stale default.
    freeDayHorizonMinutes: freeDayHorizonMs() / 60_000,
    freePoolRestMinutes: poolRestMs() === null ? 0 : Math.round(poolRestMs() / 60_000),
    // How long the last resort retries before the caller is told it could not be
    // served. 30s unless `fallbackRetrySeconds` says otherwise.
    fallbackRetryMs: fallbackRetryDeadlineMs(),
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
// answers a quota error rather than a 403 "only from within OpenCode").
//
// The build is NOT part of the rotating identity. An old build is what the gate's
// own floor rejects (HTTP 426) and a newer one never changed a quota answer, so
// every identity presents the newest entry in this list. What rotates on a limit
// is the session, the project handle and the exit IP - never the build.
const OPENCODE_FREE_UAS = [
  'opencode/latest/2.0.26/cli',
  'opencode/latest/2.0.31/cli',
  'opencode/latest/2.1.0/cli',
];

/** The build every identity presents. Append a newer validated build above. */
const OPENCODE_FREE_UA_INDEX = OPENCODE_FREE_UAS.length - 1;
const OPENCODE_FINGERPRINT_TOOLS = ['bash', 'glob', 'grep', 'read'];
const BASE62 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

// ---------------------------------------------------------------------------
// OpenCode Console sign-in (the one lever that changes the principal)
// ---------------------------------------------------------------------------
// The anonymous free tier is a single shared daily bucket: address, identity,
// build, session, project, model and path were all measured against it and none of
// them moves it (see the free-day ceiling above). What does move it is *who is
// asking*, and the real CLI changes that with a device-flow sign-in. Taken from the
// shipped bundle (opencode-linux-x64@1.19.4):
//
//   POST {console}/auth/device/code   {client_id:"opencode-cli"}
//        -> {device_code, user_code, verification_uri_complete, expires_in, interval}
//   POST {console}/auth/device/token  {grant_type:"urn:ietf:params:oauth:grant-type:device_code",
//                                      device_code, client_id:"opencode-cli"}
//        -> {access_token, refresh_token, expires_in} | {error:"authorization_pending"|"slow_down"|...}
//   POST {console}/auth/device/token  {grant_type:"refresh_token", refresh_token, client_id}
//   GET  {console}/api/user           Authorization: Bearer <access_token> -> {id, email}
//
// and it attaches the credential as `apiKey` in the *body* ("public" only when it
// has none), which is where a signed-in lane puts it too.
const OPENCODE_CONSOLE = process.env.OPENCODE_CONSOLE || 'https://opencode.ai/console';
const OPENCODE_CLIENT_ID = 'opencode-cli';
const OPENCODE_DEVICE_GRANT = 'urn:ietf:params:oauth:grant-type:device_code';
const SIGNIN_REFRESH_MARGIN_MS = 5 * 60_000;
/** providerId -> { status, userCode, verificationUri, expiresAt, error, email } */
const opencodeSignins = new Map();

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

/** The client version this identity presents: the newest validated build, always.
 *  The build does not rotate - a limit is answered with a new session, a new
 *  project handle and a new exit IP, never by stepping the client backwards. */
function openCodeUA(provider, account) {
  return OPENCODE_FREE_UAS[OPENCODE_FREE_UA_INDEX];
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
  const style = (def || PROVIDER_TYPES[provider.type] || PROVIDER_TYPES.custom).style;
  // The CLI transport is account-less by design - its "credential" is the genuine
  // binary itself, which is exactly what makes it the one client the gate serves.
  if (style === 'opencode-cli') {
    const driver = cliDriverFor(provider);
    return driver.available();
  }
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
    } else if ((PROVIDER_TYPES[provider.type] || {}).style === 'opencode-cli') {
      // The free tier has no /models to GET: the client itself knows the list, so
      // ask it (`opencode models opencode`). Without this the router advertised an
      // empty catalogue and clients showed "no model available".
      ids = await cliDriverFor(provider).listModels({ env: (cliProxyEnv(provider, { rotate: true }).env || {}) });
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
    if (ids) return ids.map((m) => ({ id: `${p.id}/${m}`, object: 'model', owned_by: p.id, provider: p.id, type: p.type, ...limitFields(p) }));
    const def = PROVIDER_TYPES[p.type] || PROVIDER_TYPES.custom;
    if (!providerIsReachable(p, def)) return [];
    return [{ id: `${p.id}/*`, object: 'model', owned_by: p.id, provider: p.id, type: p.type, ...limitFields(p) }];
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
    // Exclusive mode: an import may restore accounts, never a second vendor.
    if (config.exclusiveProvider) {
      const all = Array.isArray(converted.config.providers) ? converted.config.providers : [];
      const kept = all.filter((p) => p.id === config.exclusiveProvider);
      const dropped = all.filter((p) => p.id !== config.exclusiveProvider).map((p) => p.id);
      if (dropped.length) converted.warnings.push(`exclusive mode: ignored provider(s) ${dropped.join(', ')}`);
      if (converted.replace && !kept.length) {
        return sendJson(res, 400, {
          error: { message: `this router is locked to "${config.exclusiveProvider}"; the backup has no such provider and cannot replace the config`, type: 'provider_locked' },
        });
      }
      converted.config.providers = kept;
      converted.summary.providers = kept.length;
    }
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
      for (const key of ['accounts', 'headers', 'modelStyles', 'maxAttempts', 'cooldownHours', 'timeoutMs', 'strategy', 'baseUrl', 'name', 'type', 'proxy', 'rotateEveryMinutes', 'identityPool', 'contextWindow', 'freeFallback']) {
        if (body[key] === undefined) seeded[key] = existing[key];
      }
    }
    const provider = normalizeProvider({ ...seeded, ...body });
    // Exclusive mode: the router is locked to one provider. Adding a second one
    // would put another vendor back in the answer path, which is the bug this
    // mode exists to prevent.
    if (config.exclusiveProvider && provider.id !== config.exclusiveProvider) {
      return sendJson(res, 409, {
        error: {
          message: `this router is locked to "${config.exclusiveProvider}" (exclusiveProvider in config.json); no other provider can be added`,
          type: 'provider_locked',
        },
      });
    }
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

  // Alias for the same action on the unprefixed path (the pool-prefixed one lives
  // with the other pool routes), so either URL the operator guesses works.
  if (method === 'POST' && a === 'provider' && b && c === 'recycle') {
    return handleRecycle(req, res, findProvider(b));
  }

  // ---- the CLI transport's own controls ------------------------------------
  // "Retry now" from the dashboard: clear the cooldown so the next request walks
  // the ladder again instead of waiting it out. Optionally re-warm the identities.
  if (method === 'POST' && a === 'provider' && b && c === 'cli' && d === 'reset') {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: `no provider "${b}"` } });
    const body = await readJsonBody(req).catch(() => ({}));
    clearCliDry('dashboard retry');
    cliDry.hits = 0;
    state.cliDry = { until: 0, hits: 0 };
    persistState();
    if (body && body.warm) {
      warmCliIdentities(provider).catch((err) => console.warn(`[cli] warm-up failed: ${err.message}`));
    }
    return sendJson(res, 200, { ok: true, provider: providerSummary(provider) });
  }

  // ---- sign-in: add a principal (its own daily allowance) ------------------
  // The anonymous free tier is one shared bucket that no address, identity, build,
  // session, project, model or path moves. A signed-in principal is a different
  // bucket, which is why this is the only thing that lifts the ceiling; N sign-ins
  // are N allowances, and the ladder rotates across them like any other lane.
  if (method === 'POST' && a === 'provider' && b && c === 'signin' && !d) {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
    try {
      const st = await startOpenCodeSignin(provider);
      return sendJson(res, 200, {
        ok: true, status: st.status, userCode: st.userCode, verificationUri: st.verificationUri,
        expiresAt: st.expiresAt, intervalSeconds: st.interval,
      });
    } catch (err) {
      return sendJson(res, 502, { error: { message: err.message, type: 'signin_failed' } });
    }
  }
  if (method === 'POST' && a === 'provider' && b && c === 'signin' && d === 'cancel') {
    opencodeSignins.delete(b);
    return sendJson(res, 200, { ok: true, cancelled: true });
  }
  if (method === 'GET' && a === 'provider' && b && c === 'signin') {
    const provider = findProvider(b);
    if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
    const st = opencodeSignins.get(provider.id) || null;
    const signedIn = (provider.accounts || []).filter((x) => x.signedIn)
      .map((x) => ({ id: x.id, name: x.name, email: x.email || '', lane: x.lane, expiresInSeconds: Math.max(0, Math.round((Number(x.tokenExpiresAt) - now()) / 1000)) }));
    return sendJson(res, 200, {
      ok: true,
      pending: st ? { status: st.status, userCode: st.userCode, verificationUri: st.verificationUri, expiresAt: st.expiresAt, error: st.error } : null,
      signedIn,
      console: OPENCODE_CONSOLE,
    });
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
    // Throw the burn pile away, test new candidates and refill the pool. Runs by
    // itself when the pool drains; this is the manual lever for it.
    if (method === 'POST' && b === 'replenish') {
      const body = await readJsonBody(req);
      const out = await proxies.replenish(body.reason || 'manual (dashboard)', { maxCandidates: Number(body.maxCandidates) || 0 });
      return sendJson(res, 200, { ok: true, ...out, snapshot: proxies.snapshot() });
    }
    // Reinstall the client: new identity, session, project handle and client
    // version, with any cooldown cleared. `renew` stays as the historical path.
    // Every lane is rebuilt, which is what "reinstall the client" means now that
    // one provider runs several; `?lane=N` limits it to one.
    if (method === 'POST' && b === 'provider' && c && (d === 'reinstall' || d === 'renew')) {
      const provider = findProvider(c);
      if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
      if (!isManagedProvider(provider)) return sendJson(res, 409, { error: { message: `provider "${provider.id}" does not manage its identities`, type: 'not_managed' } });
      ensureManagedAccount(provider);
      const only = url.searchParams.get('lane');
      const lanes = (provider.accounts || []).filter((a) => only === null || laneKeyOf(a) === String(only));
      const rebuilt = [];
      for (const lane of lanes) rebuilt.push(reinstallManagedClient(provider, lane, 'manual', { rotateExit: true }));
      await saveConfig();
      const first = rebuilt[0] || null;
      const as = first ? accountState(provider.id, first.id) : {};
      return sendJson(res, 200, {
        ok: rebuilt.length > 0,
        identities: rebuilt.map((a) => accountSummary(provider, a)),
        account: first ? accountSummary(provider, first) : null,
        client: first ? {
          name: first.name,
          session: maskKey(as.sessionId || ''),
          project: as.projectId || 'global',
          userAgent: openCodeUA(provider, first),
          cooldownCleared: !(as.limitedUntil > now()),
        } : null,
        reinstalls: state.totals.reinstalls || 0,
      });
    }

    // Their own recipe as one call: drop the client, move the IP, install a new
    // identity - and say what the edge answered, so the result is never a guess.
    if (method === 'POST' && b === 'provider' && c && d === 'recycle') {
      return handleRecycle(req, res, findProvider(c));
    }

    if (method === 'POST' && b === 'provider' && c && d === 'rotate-exit') {
      const provider = findProvider(c);
      if (!provider) return sendJson(res, 404, { error: { message: 'provider not found', type: 'not_found' } });
      if (provider.proxy !== 'rotate') return sendJson(res, 409, { error: { message: `provider "${provider.id}" is not using the rotating pool (proxy=${provider.proxy})`, type: 'not_rotating' } });
      // Next IP: every identity lane moves to a fresh exit, so the whole pool of
      // identities is on new addresses rather than one of them.
      const lanes = (provider.accounts || []).map((a) => laneKeyOf(a));
      if (lanes.length > 1) {
        const moved = lanes.map((lane) => {
          const exit = proxies.rotateLane(provider, lane, 'next ip (dashboard)');
          return exit ? { lane, exit: { id: exit.id, host: exit.host, exitIp: exit.exitIp } } : null;
        }).filter(Boolean);
        return sendJson(res, 200, { ok: moved.length > 0, exits: moved, exit: moved[0] ? moved[0].exit : null });
      }
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
      if (config.exclusiveProvider === provider.id) {
        return sendJson(res, 409, {
          error: {
            message: `"${provider.id}" is the locked provider (exclusiveProvider); it cannot be deleted. Clear exclusiveProvider in config.json first.`,
            type: 'provider_locked',
          },
        });
      }
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
    if (body.defaultProvider != null && !config.exclusiveProvider) config.defaultProvider = String(body.defaultProvider);
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
    // Free-tier ladder dial. An empty box or 0 means "back to the default", which is
    // why this deletes the key instead of storing a zero the loader would drop anyway.
    if (body.freeLadderBudgetSeconds != null) {
      const ladderSeconds = clampLadderSeconds(body.freeLadderBudgetSeconds);
      if (ladderSeconds) config.freeLadderBudgetSeconds = ladderSeconds;
      else delete config.freeLadderBudgetSeconds;
    }
    // All-lane identity sweep floor, in minutes. 0 is a real setting here ("renew
    // every lane on every limit"), so it is stored rather than treated as "unset".
    if (body.allLanesRenewGapMinutes != null && body.allLanesRenewGapMinutes !== '') {
      const sweepMinutes = Number(body.allLanesRenewGapMinutes);
      if (Number.isFinite(sweepMinutes) && sweepMinutes >= 0) config.allLanesRenewGapMinutes = Math.min(1440, sweepMinutes);
    }
    if (body.identityBurst != null) config.identityBurst = !!body.identityBurst;
    // What counts as a spent *day* rather than a spent exit/identity. 0 is a real
    // setting ("never park - always run the ladder"), so it is stored, not dropped.
    if (body.freeDayHorizonMinutes != null && body.freeDayHorizonMinutes !== '') {
      const horizonMinutes = Number(body.freeDayHorizonMinutes);
      if (Number.isFinite(horizonMinutes) && horizonMinutes >= 0) config.freeDayHorizonMinutes = Math.min(1440, horizonMinutes);
    }
    // Pool rest after a ceiling the ladder could not clear. 0 is a real setting
    // ("never rest - the next request tries again"), positive pins a fixed window.
    if (body.freePoolRestMinutes != null && body.freePoolRestMinutes !== '') {
      const restMinutes = Number(body.freePoolRestMinutes);
      if (Number.isFinite(restMinutes) && restMinutes >= 0) config.freePoolRestMinutes = Math.min(1440, restMinutes);
    }
    await saveConfig();
    return sendJson(res, 200, { ok: true, settings: { cooldownHours: config.cooldownHours, defaultProvider: config.defaultProvider, cors: config.cors, cooldownStatuses: config.cooldownStatuses, freeLadderBudgetSeconds: config.freeLadderBudgetSeconds ?? 25, allLanesRenewGapMinutes: allLanesRenewGapMs() / 60_000, identityBurst: identityBurstEnabled(), freeDayHorizonMinutes: freeDayHorizonMs() / 60_000, freePoolRestMinutes: poolRestMs() === null ? 0 : Math.round(poolRestMs() / 60_000), hasProxyKeys: config.proxyKeys.length > 0, loggingRequests: !!(config.logging && config.logging.requests) } });
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
  '.txt': 'text/plain; charset=utf-8',
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

  if (url.pathname === '/admin' || url.pathname === '/admin/' || url.pathname === '/' || url.pathname.startsWith('/assets/') || url.pathname.startsWith('/sub/')) {
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
        id: `${p.id}/*`, object: 'model', owned_by: p.id, provider: p.id, type: p.type, ...limitFields(p),
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
  // `state` is replaced by the file on disk, so the dry backoff has to be picked up
  // here rather than in the module-level literal (which runs before the load).
  if (state.freeDry && typeof state.freeDry === 'object') Object.assign(freeDry, state.freeDry);
  const { file, created } = loadConfigSync();
  if (created) {
    saveConfig().catch((err) => console.error(`[config] could not write config.json: ${err.message}`));
    console.warn('[config] no config.json found - wrote defaults. Open /admin and add accounts.');
  }
  sampleMemory();
  proxies = new ProxyManager({ dataDir: DATA_DIR, log: (m) => console.warn(m) });
  proxies.init();
  // Mint the identity lanes now rather than on the first request: a cold burst has
  // to find its lanes (and their pinned exits) already there, which is the whole
  // point of `identityPool`. Idempotent - an existing pool is only re-pinned - and
  // it is what makes the dashboard show the lanes before any traffic arrives.
  for (const p of config.providers) {
    if (!isManagedProvider(p)) continue;
    try { ensureManagedAccount(p); } catch (err) { console.warn(`[proxy] ${p.id}: could not mint the identity pool: ${err.message}`); }
  }
  freebuff = new FreebuffDriver({ stateFile: path.join(DATA_DIR, 'freebuff-state.json'), log: (m) => console.warn(m) });
  freebuff.init();
  // The CLI transport keeps a HOME of its own next to the state files, so it never
  // shares credentials or sessions with anything else on the box.
  opencodeCli = new OpenCodeCliDriver({
    binPath: defaultCliPath(),
    homeDir: path.join(DATA_DIR, 'oc-cli-home'),
    args: Array.isArray(config.cliArgs) ? config.cliArgs.map(String) : [],
    mode: config.cliMode === 'serve' ? 'serve' : 'spawn',
    serveUrl: defaultCliServeUrl(),
    log: (m) => console.warn(m),
  });
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
  // Scheduled upkeep: re-fetch the subscriptions, test what is new, fold the
  // working exits into the pool. Default every 12h; see the Network tab.
  proxies.startScheduler();
  // A rotated identity only helps if it is warm by the time it is needed, and its
  // first run is far too slow to fit in a request. Warm the CLI identity pool in the
  // background, after the exits are up so the choice of exit is real.
  for (const provider of config.providers) {
    if ((PROVIDER_TYPES[provider.type] || {}).style !== 'opencode-cli') continue;
    if (config.cliWarmIdentities === false) continue;
    setTimeout(() => {
      warmCliIdentities(provider).catch((err) => console.warn(`[cli] identity warm-up failed: ${err.message}`));
    }, 5000).unref?.();
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
