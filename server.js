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
  'opencode-free': { label: 'OpenCode Free (no key)', baseUrl: 'https://opencode.ai/zen/v1', style: 'opencode-free' },
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
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
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
  return out;
}

function normalizeProvider(p) {
  const type = PROVIDER_TYPES[p.type] ? p.type : 'custom';
  const def = PROVIDER_TYPES[type];
  const accounts = Array.isArray(p.accounts) ? p.accounts.map((a, i) => normalizeAccount(a, `account-${i + 1}`)) : [];
  return {
    id: String(p.id || uid('prov')),
    name: String(p.name || def.label),
    type,
    baseUrl: String(p.baseUrl || def.baseUrl || ''),
    strategy: p.strategy === 'sequential' ? 'sequential' : 'round-robin',
    cooldownHours: clampHours(p.cooldownHours),
    timeoutMs: Number(p.timeoutMs) > 0 ? Number(p.timeoutMs) : 30_000,
    maxAttempts: Number(p.maxAttempts) > 0 ? Math.min(Number(p.maxAttempts), 32) : 0,
    headers: p.headers && typeof p.headers === 'object' ? { ...p.headers } : undefined,
    modelStyles: p.modelStyles && typeof p.modelStyles === 'object' ? { ...p.modelStyles } : undefined,
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
  out.providers = Array.isArray(out.providers) ? out.providers.map(normalizeProvider) : [];
  if (process.env.ADMIN_KEY) out.adminKey = process.env.ADMIN_KEY;
  return out;
}

let config = defaultConfig();

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
  totals: { requests: 0, success: 0, failed: 0, streamed: 0, failovers: 0, limitEvents: 0, unlocks: 0, bytesOut: 0 },
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

function markLimited(provider, account, status) {
  const as = accountState(provider.id, account.id);
  as.limitedUntil = now() + cooldownMs(provider);
  as.lastStatus = status;
  as.limits = (as.limits || 0) + 1;
  state.totals.limitEvents += 1;
  state.totals.failovers += 1;
  pushEvent({ kind: 'limited', pid: provider.id, aid: account.id, st: status });
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

  if (provider.strategy === 'sequential') {
    for (let i = 0; i < n; i += 1) if (eligible(i)) return list[i];
    return null;
  }

  for (let k = 0; k < n; k += 1) {
    const i = (ps.rrIndex + k) % n;
    if (eligible(i)) {
      ps.rrIndex = (i + 1) % n;
      return list[i];
    }
  }
  return null;
}

function accountSummary(provider, account) {
  const as = accountState(provider.id, account.id);
  return {
    id: account.id,
    name: account.name,
    maskedKey: maskKey(account.apiKey),
    enabled: account.enabled !== false,
    requests: as.requests || 0,
    failures: as.failures || 0,
    limits: as.limits || 0,
    limitedUntil: isLimited(as) ? as.limitedUntil : 0,
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

  if (style === 'opencode' || style === 'opencode-free') {
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
  let upstream;
  try {
    upstream = await fetch(spec.url, {
      method: ctx.method === 'GET' ? 'GET' : 'POST',
      headers: spec.headers,
      body: ctx.method === 'GET' ? undefined : JSON.stringify(spec.payload),
      signal: controller.signal,
      redirect: 'follow',
    });
  } finally {
    clearTimeout(timer);
  }
  return { upstream, controller, style: spec.style };
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
    headers['access-control-expose-headers'] = 'x-router-provider,x-router-account,x-router-attempts';
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
    headers['x-router-attempts'] = String(attempts);
    headers['content-length'] = Buffer.byteLength(payload);
    res.writeHead(status, headers);
    res.end(payload);
    return;
  }

  // ---- streaming / passthrough: zero-copy, never buffered -------------------
  const headers = baseResponseHeaders(upstream.status, contentType, provider);
  headers['x-router-account'] = String(account?.id || '');
  headers['x-router-attempts'] = String(attempts);
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
    source.pipe(res);
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
  return {
    error: {
      message: dueToLimit
        ? `All accounts for provider "${provider.id}" are rate-limited (429/402/403) and cooling down.`
        : `No healthy account available for provider "${provider.id}".`,
      type: 'router_no_healthy_account',
      provider: provider.id,
      tried: [...tried],
      healthyRemaining: healthyLeft.length,
      retryAfterSeconds: nextUnlock ? Math.max(1, Math.round((nextUnlock - now()) / 1000)) : 0,
      nextUnlockAt: nextUnlock || null,
    },
  };
}

async function handleProxy(req, res, url) {
  const t0 = now();
  state.totals.requests += 1;

  if (config.proxyKeys.length) {
    const auth = String(req.headers.authorization || '');
    const key = auth.replace(/^Bearer\s+/i, '').trim();
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
    translate: isChat && style !== 'openai' && style !== 'opencode-free',
  };
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
  } else if (style === 'anthropic' || style === 'gemini') {
    ctx.responseStyle = style;
  } else {
    ctx.responseStyle = 'openai';
  }

  const tried = new Set();
  const maxAttempts = provider.maxAttempts > 0 ? provider.maxAttempts : Math.max(1, (provider.accounts || []).length);
  let sawLimit = false;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const account = selectAccount(provider, tried);
    if (!account) break;
    tried.add(account.id);

    const as = accountState(provider.id, account.id);
    as.requests = (as.requests || 0) + 1;
    as.lastUsed = now();

    let upstream;
    let controller;
    try {
      ({ upstream, controller } = await callUpstream(provider, account, ctx));
    } catch (err) {
      as.failures = (as.failures || 0) + 1;
      as.lastStatus = 0;
      as.lastError = 'network';
      pushEvent({ kind: 'error', pid: provider.id, aid: account.id, st: 0 });
      persistState();
      console.error(`[proxy] ${provider.id}/${account.name} network error: ${err.message}`);
      continue;
    }

    as.lastStatus = upstream.status;

    const cooldownStatuses = config.cooldownStatuses;
    if (cooldownStatuses.includes(upstream.status)) {
      sawLimit = true;
      markLimited(provider, account, upstream.status);
      try { await upstream.body?.cancel(); } catch { /* noop */ }
      console.warn(`[proxy] ${provider.id}/${account.name} -> ${upstream.status}, cooling down ${clampHours(provider.cooldownHours ?? config.cooldownHours)}h, failing over`);
      continue;
    }

    if (config.retryStatuses.includes(upstream.status) && attempt < maxAttempts) {
      as.failures = (as.failures || 0) + 1;
      pushEvent({ kind: 'retry', pid: provider.id, aid: account.id, st: upstream.status });
      persistState();
      try { await upstream.body?.cancel(); } catch { /* noop */ }
      continue;
    }

    const ok = upstream.status >= 200 && upstream.status < 300;
    if (ok) state.totals.success += 1; else state.totals.failed += 1;
    if (ok && ctx.stream && ctx.translate) state.totals.streamed += 1;
    else if (ok && ctx.stream) state.totals.streamed += 1;
    pushEvent({ kind: ok ? 'ok' : 'http', pid: provider.id, aid: account.id, st: upstream.status, ms: now() - t0, s: ctx.stream ? 1 : 0 });
    persistState();

    let bytes = 0;
    if (!ctx.stream && !ctx.translate) {
      const len = Number(upstream.headers.get('content-length'));
      if (Number.isFinite(len)) bytes = len;
    }
    if (bytes) state.totals.bytesOut += bytes;

    res.on('finish', () => {
      console.log(`[proxy] ${req.method} ${url.pathname} -> ${provider.id}/${account.name} ${upstream.status} ${now() - t0}ms attempts=${attempt}${ctx.stream ? ' stream' : ''}`);
    });

    try {
      await relay(provider, account, upstream, controller, ctx, res, attempt);
    } catch (err) {
      console.error(`[proxy] relay error after ${now() - t0}ms: ${err.message}`);
      if (!res.headersSent) sendJson(res, 502, { error: { message: `upstream relay failed: ${err.message}`, type: 'router_relay_error' } });
      else res.destroy();
    }
    return;
  }

  state.totals.failed += 1;
  if (!res.headersSent) {
    const { error } = allExhausted(provider, tried, sawLimit);
    sendJson(res, sawLimit ? 429 : 503, { error });
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
      for (const key of ['accounts', 'headers', 'modelStyles', 'maxAttempts', 'cooldownHours', 'timeoutMs', 'strategy', 'baseUrl', 'name', 'type']) {
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
    const body = await readJsonBody(req);
    const incoming = Array.isArray(body.accounts) ? body.accounts : [body];
    // Keyless providers (OpenCode Free, local servers) legitimately store no key.
    const keyless = (PROVIDER_TYPES[provider.type] || PROVIDER_TYPES.custom).style === 'opencode-free'
      || (PROVIDER_TYPES[provider.type] || {}).keyless === true;
    const added = [];
    for (const raw of incoming) {
      const acc = typeof raw === 'string' ? parseAccountLine(raw) : normalizeAccount(raw, `account-${provider.accounts.length + 1}`);
      if (!acc.apiKey && !keyless) continue;
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
    const spec = modelListRequest(provider, account);
    try {
      const upstream = await fetch(spec.url, { headers: spec.headers, signal: AbortSignal.timeout(15_000) });
      const text = await upstream.text();
      let json = null;
      try { json = JSON.parse(text); } catch { /* non-JSON: report the raw head */ }
      const models = extractModelIds(json);
      return sendJson(res, 200, {
        ok: upstream.ok,
        status: upstream.status,
        source: spec.url,
        provider: provider.id,
        count: models.length,
        models: models.length ? models : undefined,
        raw: models.length ? undefined : text.slice(0, 2000),
      });
    } catch (err) {
      return sendJson(res, 502, { error: { message: `model list fetch failed: ${err.message}`, type: 'upstream_error' } });
    }
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
    if (Array.isArray(body.proxyKeys)) config.proxyKeys = body.proxyKeys.map(String).filter(Boolean);
    if (Array.isArray(body.cooldownStatuses) && body.cooldownStatuses.length) config.cooldownStatuses = body.cooldownStatuses.map(Number);
    await saveConfig();
    return sendJson(res, 200, { ok: true, settings: { cooldownHours: config.cooldownHours, defaultProvider: config.defaultProvider, cors: config.cors, cooldownStatuses: config.cooldownStatuses, hasProxyKeys: config.proxyKeys.length > 0 } });
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

const server = http.createServer((req, res) => {
  let url;
  try { url = new URL(req.url, 'http://localhost'); }
  catch { return sendJson(res, 400, { error: { message: 'bad request url', type: 'bad_request' } }); }

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,PATCH,DELETE,OPTIONS',
      'access-control-allow-headers': 'authorization,content-type,x-admin-key,x-router-provider',
      'access-control-max-age': '86400',
    });
    return res.end();
  }

  if (url.pathname === '/health') {
    const mem = process.memoryUsage();
    return sendJson(res, 200, {
      ok: true,
      version: PKG_VERSION,
      node: process.version,
      uptimeSeconds: Math.round((now() - state.startedAt) / 1000),
      rss: mem.rss,
      requests: state.totals.requests,
      providers: config.providers.length,
      accounts: config.providers.reduce((s, p) => s + (p.accounts || []).length, 0),
    });
  }

  if (url.pathname.startsWith('/admin/api/')) {
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
    const data = config.providers.flatMap((p) => (p.accounts || []).filter((a) => a.enabled !== false).map((a) => ({
      id: `${p.id}/*`, object: 'model', owned_by: p.id, provider: p.id, type: p.type,
    })));
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
