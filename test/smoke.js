'use strict';
/**
 * Smoke test for the LLM API Router. No network access and no API keys needed:
 * it starts a mock upstream and the real server.js as a child process, then
 * verifies routing, failover, the 12-hour cooldown, streaming passthrough,
 * anthropic/gemini translation, the admin API and memory behaviour.
 *
 *   node test/smoke.js        (or: npm run smoke)
 *
 * Everything runs in a temp directory; config.json/state.json in the repo are untouched.
 */const http = require('node:http');
const net = require('node:net');
const crypto = require('node:crypto');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-smoke-'));
const REPO = path.join(__dirname, '..');
let MOCK_PORT = 0; // ephemeral
const ROUTER_PORT = 20000 + Math.floor(Math.random() * 20000);
const CONFIG = path.join(TMP, 'config.json');
const STATE = path.join(TMP, 'state.json');

let pass = 0, fail = 0;
let ipLimitedLeft = 1;
const proxyHits = [];
const PROXY_BASE_PORT = 21000 + Math.floor(Math.random() * 5000);
const { TELEMETRY_SCHEMA } = require(path.join(REPO, 'proxy-manager.js'));
const results = [];
function check(name, cond, detail = '') {
  if (cond) { pass++; results.push(`  PASS  ${name}`); console.log(`  PASS  ${name}`); }
  else { fail++; results.push(`  FAIL  ${name}${detail ? ' :: ' + detail : ''}`); console.log(`  FAIL  ${name}${detail ? ' :: ' + detail : ''}`); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const step = (s) => console.log(`--- STEP ${s}`);

// ---------------------------------------------------------------------------
// mock upstream
// ---------------------------------------------------------------------------
function bodyOf(req) {
  return new Promise((resolve) => {
    const c = [];
    req.on('data', (d) => c.push(d));
    req.on('end', () => resolve(Buffer.concat(c).toString('utf8')));
  });
}

const mock = http.createServer(async (req, res) => {
  const body = await bodyOf(req);
  const url = new URL(req.url, 'http://x');
  let json = {};
  try { json = JSON.parse(body || '{}'); } catch {}

  // freebuff protocol mock (see handleFreebuff): the router only reaches it
  // because FB_API_HOST is pointed at this process.
  if (url.pathname === '/api/v1/freebuff/models' || url.pathname.startsWith('/api/v1/agent-runs')
      || url.pathname === '/api/v1/chat/completions' || url.pathname.startsWith('/api/v1/freebuff/session')
      || url.pathname === '/api/v1/freebuff/device-keys') {
    return handleFreebuff(req, res, url, Buffer.from(body || '', 'utf8'), json);
  }

  // Simulated IP-bound limit: the first call 429s whatever the key, the next works.
  if ((req.headers.authorization || '').replace(/^Bearer\s+/i, '') === 'key-ip-limit' && ipLimitedLeft > 0) {
    ipLimitedLeft -= 1;
    res.writeHead(429, { 'content-type': 'application/json' });
    return res.end('{"error":{"message":"rate limited by IP"}}');
  }

  const bearer = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const anthropicKey = req.headers['x-api-key'] || '';
  const geminiKey = req.headers['x-goog-api-key'] || '';
  const key = bearer || anthropicKey || geminiKey;
  const nChunks = Number(json.n ?? req.headers['x-test-chunks'] ?? 4);

  if (key === 'key-limited') { res.writeHead(429, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"rate limited"}}'); }
  if (key === 'key-retry-after') { res.writeHead(429, { 'content-type': 'application/json', 'retry-after': '2' }); return res.end('{"error":{"message":"slow down, retry shortly"}}'); }
  if (key === 'key-freetier') { res.writeHead(403, { 'content-type': 'application/json' }); return res.end(JSON.stringify({ type: 'error', error: { type: 'FreeTierError', message: "OpenCode's free tier can only be used from within OpenCode. Download it at https://opencode.ai" } })); }
  if (key === 'key-mode-403') { res.writeHead(403, { 'content-type': 'application/json' }); return res.end('{"error":"free_mode_cli_required"}'); }
  if (key === 'key-credits-402') { res.writeHead(402, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"Out of credits."}}'); }
  if (key === 'key-payment') { res.writeHead(402, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"payment required"}}'); }
  if (key === 'key-forbidden') { res.writeHead(403, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"forbidden"}}'); }
  if (key === 'key-broken') { res.writeHead(500, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"boom"}}'); }
  // Keyless routes (OpenCode Free / zen passthrough) accept no credentials.
  if (!key && !url.pathname.startsWith('/zen/')) { res.writeHead(401, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"no key"}}'); }

  // anthropic style
  if (url.pathname.endsWith('/messages')) {
    if (json.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(`event: message_start\ndata: ${JSON.stringify({ type: 'message_start', message: { id: 'msg_1', model: json.model, usage: { input_tokens: 11, output_tokens: 0 } } })}\n\n`);
      res.write(`event: content_block_start\ndata: ${JSON.stringify({ type: 'content_block_start', index: 0 })}\n\n`);
      for (let i = 0; i < nChunks; i++) {
        res.write(`event: content_block_delta\ndata: ${JSON.stringify({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: `A${i} ` } })}\n\n`);
      }
      res.write(`event: message_delta\ndata: ${JSON.stringify({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: nChunks } })}\n\n`);
      res.write(`event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`);
      return res.end();
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({
      id: 'msg_1', model: json.model, stop_reason: 'end_turn',
      content: [{ type: 'text', text: 'anthropic says hi' }],
      usage: { input_tokens: 11, output_tokens: 3 },
    }));
  }

  // --- OpenCode: four wire formats behind one provider -----------------------
  // /inference/v1/models  (model discovery, no key needed)
  if (url.pathname === '/inference/v1/models') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ object: 'list', data: [
      { id: 'big-pickle' }, { id: 'claude-sonnet-4-5' }, { id: 'gemini-3.1-pro' }, { id: 'gpt-5.5' }, { id: 'glm-5.2' },
    ].map((m) => ({ ...m, object: 'model', owned_by: 'opencode' })) }));
  }
  // OpenAI Responses API (GPT / Grok / Muse family)
  if (url.pathname === '/inference/openai/v1/responses') {
    if (json.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      for (let i = 0; i < nChunks; i++) res.write(`data: ${JSON.stringify({ type: 'response.output_text.delta', delta: `R${i} ` })}\n\n`);
      res.write(`data: ${JSON.stringify({ type: 'response.completed', response: { status: 'completed', usage: { input_tokens: 9, output_tokens: nChunks, total_tokens: 9 + nChunks } } })}\n\n`);
      return res.end();
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({
      id: 'resp_1', model: json.model, status: 'completed',
      output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'responses says hi' }] }],
      usage: { input_tokens: 9, output_tokens: 4, total_tokens: 13 },
    }));
  }
  // OpenCode OpenAI-compatible route
  if (url.pathname === '/inference/v1/chat/completions') {
    const ua = String(req.headers['user-agent'] || '');
    const session = String(req.headers['x-opencode-session'] || '');
    const aliases = [req.headers['x-opencode-session-id'], req.headers['x-session-affinity'], req.headers['x-session-id']];
    const toolNames = (Array.isArray(json.tools) ? json.tools : []).map((x) => String(x?.function?.name || x?.name || ''));
    const missing = ['bash', 'glob', 'grep', 'read'].filter((n) => !toolNames.includes(n));
    const problems = [];
    if (req.headers.authorization !== 'Bearer public') problems.push('authorization');
    if (!/^opencode\/latest\/\d+\.\d+\.\d+\/cli$/.test(ua)) problems.push('user-agent');
    if (req.headers['x-opencode-client'] !== 'cli') problems.push('client');
    if (!/^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/.test(session)) problems.push('session');
    if (aliases.some((a) => a !== session)) problems.push('session-aliases');
    if (json.stream !== true) problems.push('stream');
    if (missing.length) problems.push('tools:' + missing.join('|'));
    if (problems.length) {
      fb.freeTierRejects += 1;
      res.writeHead(403, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ type: 'error', error: { type: 'FreeTierError', message: "OpenCode's free tier can only be used from within OpenCode. Download it at https://opencode.ai", problems } }));
    }
    fb.freeTierAccepted += 1;
    // Per-model allowance, like the live service: an id listed here is dry while
    // every other model on the same identity keeps answering.
    if (Array.isArray(fb.modelQuotaExhausted) && fb.modelQuotaExhausted.includes(String(json.model))) {
      fb.modelQuota429 += 1;
      // The live edge always quotes a retry-after on a quota 429. Off by default
      // here so tests that do not care see the bare 429 they were written for.
      res.writeHead(429, { 'content-type': 'application/json', ...(fb.quotaRetryAfter ? { 'retry-after': String(fb.quotaRetryAfter) } : {}) });
      return res.end(JSON.stringify({ type: 'error', error: { type: 'FreeUsageLimitError', message: 'Free usage limit reached. Subscribe to OpenCode Go to keep going.' } }));
    }
    if (fb.stallNext > 0) {
      // Accept the request and never answer: the exit (or the host IP) went
      // quiet after the tunnel was up. The router must give up on it by itself.
      fb.stallNext -= 1;
      fb.stalled += 1;
      return;
    }
    if (fb.burn429 > 0) {
      fb.burn429 -= 1;
      res.writeHead(429, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ type: 'error', error: { type: 'FreeUsageLimitError', message: 'free usage limit reached for this session' } }));
    }
    if (json.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      for (let i = 0; i < 4; i++) {
        res.write(`data: ${JSON.stringify({ id: 'oc1', object: 'chat.completion.chunk', model: json.model, choices: [{ index: 0, delta: { content: `OC${i} ` }, finish_reason: null }] })}\n\n`);
      }
      res.write(`data: ${JSON.stringify({ id: 'oc1', object: 'chat.completion.chunk', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
      res.write('data: [DONE]\n\n');
      return res.end();
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ id: 'oc1', object: 'chat.completion', model: json.model, choices: [{ index: 0, message: { role: 'assistant', content: 'opencode says hi' }, finish_reason: 'stop' }] }));
  }

  if (url.pathname === '/inference/openai/v1/chat/completions' || url.pathname === '/zen/v1/chat/completions') {
    if (json.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      for (let i = 0; i < nChunks; i++) {
        res.write(`data: ${JSON.stringify({ id: 'oc1', object: 'chat.completion.chunk', model: json.model, choices: [{ index: 0, delta: { content: `OC${i} ` }, finish_reason: null }] })}\n\n`);
      }
      res.write('data: [DONE]\n\n');
      return res.end();
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({
      id: 'oc1', object: 'chat.completion', model: json.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'opencode says hi' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 3, completion_tokens: 4, total_tokens: 7 },
    }));
  }

  // gemini style
  if (url.pathname.includes(':streamGenerateContent')) {    res.writeHead(200, { 'content-type': 'text/event-stream' });
    for (let i = 0; i < nChunks; i++) {
      res.write(`data: ${JSON.stringify({ candidates: [{ content: { role: 'model', parts: [{ text: `G${i} ` }] } }] })}\n\n`);
    }
    res.write(`data: ${JSON.stringify({ candidates: [{ content: { parts: [{ text: '' }] }, finishReason: 'STOP' }], usageMetadata: { promptTokenCount: 7, candidatesTokenCount: nChunks, totalTokenCount: 7 + nChunks } })}\n\n`);
    return res.end();
  }
  if (url.pathname.includes(':generateContent')) {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({
      candidates: [{ content: { role: 'model', parts: [{ text: 'gemini says hi' }] }, finishReason: 'STOP' }],
      usageMetadata: { promptTokenCount: 7, candidatesTokenCount: 4, totalTokenCount: 11 },
    }));
  }

  // openai style
  if (json.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
    if (nChunks > 0) {
      for (let i = 0; i < nChunks; i++) {
        res.write(`data: ${JSON.stringify({ id: 'c1', object: 'chat.completion.chunk', model: json.model, choices: [{ index: 0, delta: { content: `tok${i} ` }, finish_reason: null }] })}\n\n`);
        if (i % 50 === 49) await sleep(1);
      }
    }
    res.write(`data: ${JSON.stringify({ id: 'c1', object: 'chat.completion.chunk', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
    res.write('data: [DONE]\n\n');
    return res.end();
  }
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({
    id: 'chatcmpl-1', object: 'chat.completion', model: json.model,
    choices: [{ index: 0, message: { role: 'assistant', content: 'pong' }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 5, completion_tokens: 1, total_tokens: 6 },
  }));
});

// ---------------------------------------------------------------------------
// config for the router under test
// ---------------------------------------------------------------------------
function writeConfig() {
  const base = { adminKey: 'test-admin-key', host: '127.0.0.1', port: ROUTER_PORT, cooldownHours: 12 };
  const cfg = {
    ...base,
    cooldownStatuses: [429, 402, 403],
    retryStatuses: [500, 502, 503, 504],
    defaultProvider: 'seq-provider',
    cors: true,
    proxyKeys: [],
    // The free tier must reinstall on the spot, not wait out the 10 minute gap.
    reinstallGapMinutes: 0,
    providers: [
      {
        id: 'seq-provider', name: 'Sequential Test', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'sequential', cooldownHours: 12, timeoutMs: 10000, maxAttempts: 0,
        accounts: [
          { id: 'a-limited', name: 'first-limited', apiKey: 'key-limited', enabled: true },
          { id: 'a-ok', name: 'second-ok', apiKey: 'key-ok', enabled: true },
          { id: 'a-ok2', name: 'third-ok', apiKey: 'key-ok-2', enabled: true },
        ],
      },
      {
        id: 'rr-provider', name: 'Round Robin Test', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'round-robin', cooldownHours: 12, timeoutMs: 10000, maxAttempts: 0,
        headers: { 'X-Test-Header': 'keep-me' },
        accounts: [
          { id: 'rr-1', name: 'rr-one', apiKey: 'key-rr-1', enabled: true },
          { id: 'rr-2', name: 'rr-two', apiKey: 'key-rr-2', enabled: true },
          { id: 'rr-3', name: 'rr-three', apiKey: 'key-rr-3', enabled: true },
        ],
      },
      {
        id: 'payment-provider', name: 'Payment Test', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'sequential', cooldownHours: 12, timeoutMs: 10000, maxAttempts: 0,
        accounts: [
          { id: 'p-402', name: 'payment-402', apiKey: 'key-payment', enabled: true },
          { id: 'p-403', name: 'forbidden-403', apiKey: 'key-forbidden', enabled: true },
          { id: 'p-429', name: 'limited-429', apiKey: 'key-limited', enabled: true },
        ],
      },
      {
        id: 'cooldown-provider', name: 'Cooldown Semantics', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'sequential', cooldownHours: 12, timeoutMs: 10000, maxAttempts: 0,
        accounts: [
          { id: 'c-retry', name: 'retry-after-key', apiKey: 'key-retry-after', enabled: true },
        ],
      },
      {
        id: 'freetier-provider', name: 'Policy Gate', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'round-robin', timeoutMs: 10000, proxy: 'rotate', cooldownHours: 12, maxAttempts: 0,
        accounts: [{ id: 'ft-1', name: 'keyless', apiKey: 'key-freetier', enabled: true }],
      },
      {
        id: 'disabled-provider', name: 'Disabled Account', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'sequential', timeoutMs: 10000, maxAttempts: 0,
        accounts: [{ id: 'd-1', name: 'switched-off', apiKey: 'key-ok', enabled: false }],
      },
      {
        id: 'error-provider', name: 'Non Quota Error', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'sequential', cooldownHours: 12, timeoutMs: 10000, maxAttempts: 0,
        accounts: [
          { id: 'e-403', name: 'mode-403-key', apiKey: 'key-mode-403', enabled: true },
          { id: 'e-402', name: 'credits-402-key', apiKey: 'key-credits-402', enabled: true },
        ],
      },
      {
        id: 'broken-provider', name: 'Broken Upstream', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'sequential', cooldownHours: 12, timeoutMs: 10000, maxAttempts: 1,
        accounts: [{ id: 'b-500', name: 'broken-500', apiKey: 'key-broken', enabled: true }],
      },
      {
        id: 'anthropic-test', name: 'Anthropic Test', type: 'anthropic', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'round-robin', timeoutMs: 10000, accounts: [{ id: 'an-1', name: 'anthropic-1', apiKey: 'key-ant-ok', enabled: true }],
      },
      {
        id: 'opencode-test', name: 'OpenCode Test', type: 'opencode', baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference`,
        strategy: 'round-robin', timeoutMs: 10000, accounts: [{ id: 'oc-1', name: 'oc-key', apiKey: 'key-oc-ok', enabled: true }],
      },
      {
        id: 'opencode-free-test', name: 'OpenCode Free Test', type: 'opencode-free', baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference/v1`,
        strategy: 'round-robin', timeoutMs: 10000, accounts: [{ id: 'ocf-1', name: 'keyless', apiKey: '', enabled: true }],
      },
      {
        id: 'empty-managed', name: 'Empty Managed', type: 'opencode-free', baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference/v1`,
        strategy: 'sequential', timeoutMs: 10000, accounts: [], identityPool: 1,
      },
      {
        id: 'proxy-test', name: 'Proxy Rotation Test', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'round-robin', timeoutMs: 10000, proxy: 'rotate',
        accounts: [{ id: 'pt-1', name: 'ip-limited-key', apiKey: 'key-ip-limit', enabled: true }],
      },
      {
        id: 'freebuff-test', name: 'freebuff Test', type: 'freebuff', baseUrl: 'http://127.0.0.1:1',
        strategy: 'round-robin', timeoutMs: 10000, proxy: 'direct',
        accounts: [{ id: 'fb-1', name: 'freebuff-account', apiKey: '11111111-2222-3333-4444-555555555555', enabled: true }],
      },
      {
        id: 'managed-test', name: 'Managed Identity Test', type: 'opencode-free',
        baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference/v1`, strategy: 'round-robin', timeoutMs: 10000, proxy: 'direct',
        accounts: [{ id: 'mid-1', name: 'auto-1', apiKey: '', enabled: true }], identityPool: 1,
        fallbackProviders: [{ provider: 'fallback-provider', model: 'fallback-model' }],
      },
      {
        id: 'free-rotate-test', name: 'Free Tier Over the Pool', type: 'opencode-free',
        baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference/v1`, strategy: 'round-robin', timeoutMs: 10000,
        proxy: 'rotate', rotateAttempts: 3, identityPool: 2,
        accounts: [{ id: 'frt-1', name: 'keyless', apiKey: '', enabled: true }],
        fallbackProviders: [{ provider: 'fallback-provider', model: 'fallback-model' }],
      },
      {
        id: 'lane-default-test', name: 'Default Lane Pool', type: 'opencode-free',
        baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference/v1`, strategy: 'round-robin', timeoutMs: 10000,
        proxy: 'direct', accounts: [],
      },
      {
        id: 'fallback-provider', name: 'Fallback Target', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
        strategy: 'sequential', timeoutMs: 10000, maxAttempts: 0,
        accounts: [{ id: 'fbk-1', name: 'fallback-ok', apiKey: 'key-ok-fallback', enabled: true }],
      },
      {
        id: 'gemini-test', name: 'Gemini Test', type: 'gemini', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1beta`,
        strategy: 'round-robin', timeoutMs: 10000, accounts: [{ id: 'gm-1', name: 'gemini-1', apiKey: 'key-gem-ok', enabled: true }],
      },
    ],
  };
  fs.writeFileSync(CONFIG, JSON.stringify(cfg, null, 2));
}

// ---------------------------------------------------------------------------
// mock freebuff backend: enforces the real protocol (Ed25519 signatures over
// the documented canonical string, catalog fetchId, admission, run ledger,
// Buffy preamble). Any mismatch fails the test loudly.
// ---------------------------------------------------------------------------
const fb = {
  publicKey: null,
  keyId: 'key_test_1',
  fetchId: 'fbf1.testfetchid',
  instanceId: '',
  runId: '',
  verifyOk: 0,
  verifyFail: 0,
  signedPaths: [],
  preambleOk: 0,
  handle: 'fbm1.testhandle',
  modelKey: 'm-test1',
  freeTierAccepted: 0,
  freeTierRejects: 0,
  modelQuotaExhausted: [],
  modelQuota429: 0,
  quotaRetryAfter: 0,
  burn429: 0,
  stallNext: 0,
  stalled: 0,
};

function fbCanonical({ method, path: p, ts, bodyHash, fetchId }) {
  return ['freebuff-device-v1', method, p, String(ts), bodyHash, fetchId || ''].join('\n');
}

function fbVerify(req, rawBody, fetchId) {
  try {
    const pk = fb.publicKey;
    if (!pk) throw new Error('no device key registered');
    const canonical = fbCanonical({
      method: req.method,
      path: new URL(req.url, 'http://x').pathname,
      ts: req.headers['x-freebuff-device-ts'],
      bodyHash: crypto.createHash('sha256').update(rawBody || Buffer.alloc(0)).digest('hex'),
      fetchId,
    });
    const key = crypto.createPublicKey({ key: Buffer.from(pk, 'base64url'), format: 'der', type: 'spki' });
    const ok = crypto.verify(null, Buffer.from(canonical), key, Buffer.from(String(req.headers['x-freebuff-device-sig']), 'base64url'));
    if (!ok) throw new Error('signature mismatch');
    fb.verifyOk += 1;
    fb.signedPaths.push(new URL(req.url, 'http://x').pathname);
    return null;
  } catch (err) {
    fb.verifyFail += 1;
    return err.message;
  }
}

async function handleFreebuff(req, res, url, rawBody, jsonBody) {
  const send = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };

  if (url.pathname === '/api/v1/freebuff/models') {
    if (req.headers['x-freebuff-catalog-protocol'] !== '1' || req.headers['x-freebuff-client'] !== 'cli') {
      return send(400, { error: 'catalog protocol/client header missing' });
    }
    const bad = fbVerify(req, null, '');
    if (bad) return send(401, { error: `signature rejected: ${bad}` });
    return send(200, { fetchId: fb.fetchId, models: [{ key: fb.modelKey, handle: fb.handle, name: 'Test Model', family: 'test' }] });
  }

  if (url.pathname === '/api/v1/freebuff/device-keys') {
    // First contact: there is no stored key yet, so the server verifies against
    // the public key carried in the request itself.
    if (jsonBody && jsonBody.publicKey) fb.publicKey = jsonBody.publicKey;
    const bad = fbVerify(req, rawBody, '');
    if (bad) return send(401, { error: `signature rejected: ${bad}` });
    if (!jsonBody || !jsonBody.publicKey) return send(400, { error: 'publicKey required' });
    return send(200, { keyId: fb.keyId });
  }

  if (url.pathname === '/api/v1/freebuff/session/admission') {
    const bad = fbVerify(req, rawBody, fb.fetchId);
    if (bad) return send(401, { error: `signature rejected: ${bad}` });
    if (req.headers['x-freebuff-model'] !== fb.handle) return send(400, { error: 'x-freebuff-model must be the catalog handle' });
    if (!req.headers['x-freebuff-instance-id']) return send(400, { error: 'x-freebuff-instance-id required' });
    if (!req.headers['x-freebuff-wallet-spend-limit']) return send(400, { error: 'spend limit required' });
    fb.instanceId = req.headers['x-freebuff-instance-id'];
    return send(200, { status: 'active', accessTier: 'limited', instanceId: fb.instanceId, model: fb.modelKey, remainingMs: 3600000, freebucks: { balance: 15 } });
  }

  if (url.pathname === '/api/v1/agent-runs') {
    const bad = fbVerify(req, rawBody, fb.fetchId);
    if (bad) return send(401, { error: `signature rejected: ${bad}` });
    if (jsonBody && jsonBody.action === 'START') {
      if (jsonBody.agentId !== 'freebuff-desktop-thread-local-v3') return send(400, { error: 'agentId mismatch' });
      fb.runId = 'run_test_1';
      return send(200, { runId: fb.runId });
    }
    if (jsonBody && jsonBody.action === 'FINISH') {
      if (jsonBody.runId !== fb.runId) return send(400, { error: `runId Not Found: ${jsonBody.runId}` });
      return send(200, { status: 'ok' });
    }
    return send(400, { error: 'bad action' });
  }

  if (url.pathname === '/api/v1/freebuff/session' && req.method === 'DELETE') {
    const bad = fbVerify(req, null, fb.fetchId);
    if (bad) return send(401, { error: `signature rejected: ${bad}` });
    return send(200, { status: 'ended' });
  }

  if (url.pathname === '/api/v1/chat/completions') {
    const bad = fbVerify(req, rawBody, fb.fetchId);
    if (bad) return send(401, { error: `signature rejected: ${bad}` });
    const meta = (jsonBody && jsonBody.codebuff_metadata) || {};
    if (meta.run_id !== fb.runId || !fb.runId) return send(400, { message: 'No runId found in request body' });
    if (meta.cost_mode !== 'free') return send(403, { error: 'cost_mode must be free' });
    if (meta.freebuff_instance_id !== fb.instanceId) return send(400, { error: 'instance mismatch' });
    if (jsonBody.model !== fb.handle) return send(400, { error: 'model must be the handle' });
    if (req.headers['x-freebuff-sse-gzip-recovery'] !== '1') return send(410, { error: 'freebuff_update_required' });
    if (!String(req.headers['user-agent'] || '').includes('runtime/browser')) return send(403, { error: 'free_mode_cli_required' });
    const first = (jsonBody.messages || [])[0] || {};
    if (first.role !== 'system' || !String(first.content).startsWith('You are Buffy, the coding agent behind Codebuff.')) {
      return send(403, { error: 'free_mode_cli_required' });
    }
    fb.preambleOk += 1;
    if (jsonBody.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(`data: ${JSON.stringify({ id: 'fb1', object: 'chat.completion.chunk', choices: [{ index: 0, delta: { content: 'freebuff says hi' }, finish_reason: null }] })}\n\n`);
      res.write(`data: ${JSON.stringify({ id: 'fb1', object: 'chat.completion.chunk', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
      res.write('data: [DONE]\n\n');
      return res.end();
    }
    return send(200, {
      id: 'fb1', object: 'chat.completion', model: fb.handle,
      choices: [{ index: 0, message: { role: 'assistant', content: 'freebuff says hi' }, finish_reason: 'stop' }],
      usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 },
    });
  }

  return send(404, { error: 'no such freebuff route' });
}

/** Minimal HTTP CONNECT proxy: the router's proxiedRequest talks to this. */
function startConnectProxy(onConnect) {
  const srv = http.createServer((req, res) => { res.writeHead(405); res.end(); });
  srv.innerRequests = [];
  srv.on('connect', (req, clientSocket, head) => {
    const [host, port] = String(req.url).split(':');
    if (onConnect) onConnect(req.url);
    const upstream = net.connect(Number(port), host, () => {
      clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      if (head && head.length) upstream.write(head);
      upstream.pipe(clientSocket);
      // Count the requests that actually travel through the tunnel: a client
      // that opens its own connection instead egresses from this host and
      // every exit measurement becomes fiction.
      clientSocket.on('data', (chunk) => {
        const first = chunk.toString('latin1').split('\r\n')[0];
        if (/^(GET|POST|PUT|PATCH|DELETE) /.test(first)) srv.innerRequests.push(first);
      });
      clientSocket.pipe(upstream);
    });
    upstream.on('error', () => clientSocket.destroy());
    clientSocket.on('error', () => upstream.destroy());
  });
  return srv;
}

// ---------------------------------------------------------------------------
// helpers against the router
// ---------------------------------------------------------------------------
const ADMIN = { 'x-admin-key': 'test-admin-key', 'content-type': 'application/json' };
const routerUrl = (p) => `http://127.0.0.1:${ROUTER_PORT}${p}`;

async function chat(opts = {}) {
  const res = await fetch(routerUrl('/v1/chat/completions'), {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(opts.headers || {}) },
    body: JSON.stringify({ model: opts.model || 'gpt-test', stream: !!opts.stream, messages: opts.messages || [{ role: 'user', content: 'ping' }], ...(opts.n ? { n: opts.n } : {}) }),
  });
  let text = '';
  if (opts.stream && res.ok) {
    for await (const chunk of res.body) text += Buffer.from(chunk).toString('utf8');
  } else {
    text = await res.text();
  }
  return { status: res.status, headers: res.headers, text };
}

async function rss() {
  const r = await fetch(routerUrl('/health'));
  return (await r.json()).rss;
}

async function memOf() {
  const r = await fetch(routerUrl('/admin/api/state'), { headers: ADMIN });
  return (await r.json()).memory;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
(async () => {
  await new Promise((r) => mock.listen(0, '127.0.0.1', r));
  MOCK_PORT = mock.address().port;
  console.log(`mock upstream on 127.0.0.1:${MOCK_PORT}`);

  // Two CONNECT proxies stand in for two Xray exits. The router dials
  // inboundBasePort + <index of exit in the active pool>, so pointing
  // inboundBasePort at these two ports makes the pool real without xray.
  const proxyA = startConnectProxy((target) => proxyHits.push(`a:${target}`));
  const proxyB = startConnectProxy((target) => proxyHits.push(`b:${target}`));
  await new Promise((r) => proxyA.listen(PROXY_BASE_PORT, '127.0.0.1', r));
  await new Promise((r) => proxyB.listen(PROXY_BASE_PORT + 1, '127.0.0.1', r));
  fs.writeFileSync(path.join(TMP, 'proxies.json'), JSON.stringify({
    version: 1,
    settings: {
      enabled: true, xrayPath: 'xray-not-installed', inboundBasePort: PROXY_BASE_PORT, maxActive: 4,
      rotateAttempts: 3, rotateOnStatuses: [429, 402, 403], markDeadAfter: 3, testBatchSize: 2, testBasePort: PROXY_BASE_PORT + 50,
    },
    sources: [{ id: 's1', url: 'http://127.0.0.1:1/sub', enabled: false, lastFetched: 0, count: 0, error: '' }],
    exits: [
      { id: 'x1', name: 'exit one', protocol: 'vless', host: '10.0.0.1', port: 443, uri: 'vless://x1', status: 'ok', latencyMs: 120, exitIp: '10.0.0.1', failures: 0, testedBy: TELEMETRY_SCHEMA, outbound: { protocol: 'freedom' } },
      { id: 'x2', name: 'exit two', protocol: 'vless', host: '10.0.0.2', port: 443, uri: 'vless://x2', status: 'ok', latencyMs: 240, exitIp: '10.0.0.2', failures: 0, testedBy: TELEMETRY_SCHEMA, outbound: { protocol: 'freedom' } },
      { id: 'x3', name: 'exit three', protocol: 'vless', host: '10.0.0.3', port: 443, uri: 'vless://x3', status: 'untested', latencyMs: 0, exitIp: '', failures: 0, testedBy: TELEMETRY_SCHEMA, outbound: { protocol: 'freedom' } },
    ],
    active: ['x1', 'x2'],
  }));

  fs.rmSync(STATE, { force: true });
  writeConfig();

  const child = spawn(process.execPath, [path.join(REPO, 'server.js')], {
    cwd: REPO,
    env: { ...process.env, ROUTER_CONFIG: CONFIG, ROUTER_STATE: STATE, PORT: String(ROUTER_PORT), HOST: '127.0.0.1', FB_API_HOST: `http://127.0.0.1:${MOCK_PORT}` },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let routerLog = '';
  child.stdout.on('data', (d) => { routerLog += d.toString(); globalThis.__routerLog = routerLog; });
  child.stderr.on('data', (d) => { routerLog += d.toString(); globalThis.__routerLog = routerLog; });
  child.on('exit', (code, sig) => { console.log(`DIAG router process exited code=${code} signal=${sig}`); });

  // wait for /health
  let up = false;
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(routerUrl('/health')); if (r.ok) { up = true; break; } } catch {}
    await sleep(200);
  }
  check('router boots and answers /health', up, routerLog.slice(-400));
  if (!up) { mock.close(); child.kill(); console.log(results.join('\n')); process.exit(1); }

  const health = await (await fetch(routerUrl('/health'))).json();
  const idleRss = health.rss;
  results.push(`  INFO  idle RSS = ${(idleRss / 1048576).toFixed(2)} MB (node ${health.node})`);

  // 1. sequential + 429 failover, transparent retry
  const r1 = await chat({ headers: { 'x-router-provider': 'seq-provider' } });
  check('sequential: first account 429 -> request still succeeds via next account',
    r1.status === 200 && r1.headers.get('x-router-account') === 'a-ok',
    `status=${r1.status} account=${r1.headers.get('x-router-account')}`);
  check('failover counted in header x-router-attempts=2', r1.headers.get('x-router-attempts') === '2', r1.headers.get('x-router-attempts'));

  // 2. cooldown persisted to state.json with ~12h deadline
  await sleep(700);
  if (r1.status !== 200 || r1.headers.get('x-router-account') !== 'a-ok') {
    console.log(`DIAG r1 status=${r1.status} account=${r1.headers.get('x-router-account')} body=${r1.text.slice(0, 300)}`);
  }
  let st = {};
  try { st = JSON.parse(fs.readFileSync(STATE, 'utf8')); } catch (e) { console.log('DIAG state.json unreadable: ' + e.message); }
  if (!st.providers || !st.providers['seq-provider']) {
    console.log('DIAG state.json providers = ' + JSON.stringify(st.providers));
    console.log('DIAG router log:\n' + routerLog);
  }
  st.providers = st.providers || {};
  st.providers['seq-provider'] = st.providers['seq-provider'] || { accounts: {} };
  st.providers['seq-provider'].accounts = st.providers['seq-provider'].accounts || {};
  const lim = st.providers['seq-provider'].accounts['a-limited'] || { limitedUntil: 0 };
  const hoursLeft = (lim.limitedUntil - Date.now()) / 3600000;
  check('429 account marked limited ~12h and saved to state.json', hoursLeft > 11.9 && hoursLeft <= 12.01, `hoursLeft=${hoursLeft.toFixed(3)}`);
  check('state.json stores counters only (no api keys, no bodies)',
    !fs.readFileSync(STATE, 'utf8').includes('key-limited') && !fs.readFileSync(STATE, 'utf8').includes('ping'),
    'secret or prompt leaked into state.json');

  // 3. sequential sticks to the first healthy account while it is healthy
  const s2 = await chat({ headers: { 'x-router-provider': 'seq-provider' } });
  const s3 = await chat({ headers: { 'x-router-provider': 'seq-provider' } });
  check('sequential: keeps using the first healthy account',
    s2.headers.get('x-router-account') === 'a-ok' && s3.headers.get('x-router-account') === 'a-ok',
    `${s2.headers.get('x-router-account')}, ${s3.headers.get('x-router-account')}`);

  // 4. round-robin rotates
  const seen = [];
  for (let i = 0; i < 6; i++) seen.push((await chat({ headers: { 'x-router-provider': 'rr-provider' } })).headers.get('x-router-account'));
  check('round-robin rotates across all 3 accounts', new Set(seen).size === 3, seen.join(','));
  check('round-robin is evenly distributed', seen.filter((x) => x === 'rr-1').length === 2 && seen[0] !== seen[1], seen.join(','));

  // 5a. all accounts on cooldown statuses -> 429 + retryAfterSeconds
  {
    const rr = await chat({ headers: { 'x-router-provider': 'payment-provider' } });
    check('payment-provider: 402/403/429 accounts all cool down -> 429 for the client',
      rr.status === 429, `status=${rr.status} body=${rr.text.slice(0, 160)}`);
    const parsed = JSON.parse(rr.text);
    check('exhausted response carries retryAfterSeconds + retryable provider info',
      Number(parsed.error.retryAfterSeconds) > 0 && parsed.error.healthyRemaining === 0 && parsed.error.tried.length === 3,
      rr.text.slice(0, 240));
    // Body-only retryAfterSeconds is invisible to every SDK that reads the header
    // (openai client, opencode CLI, fetch-retry): they re-ask in milliseconds.
    const ra = Number(rr.headers.get('retry-after'));
    check('exhausted response also carries a Retry-After header matching the body',
      Number.isFinite(ra) && ra > 0 && Math.abs(ra - Number(parsed.error.retryAfterSeconds)) <= 2,
      `header=${rr.headers.get('retry-after')} body=${parsed.error.retryAfterSeconds}`);
  }

  // 5b. a hard upstream 5xx is relayed (never dressed up as success)
  {
    const rr = await chat({ headers: { 'x-router-provider': 'broken-provider' } });
    check('broken upstream 500 is relayed as 500, not swallowed', rr.status === 500, `status=${rr.status}`);
  }

  await sleep(700); // let the router's debounced state flush land on disk
  const allState = JSON.parse(fs.readFileSync(STATE, 'utf8')).providers;
  const pstate = allState['payment-provider'].accounts;
  check('402 marks limited', pstate['p-402'].limitedUntil > Date.now(), JSON.stringify(pstate['p-402']));
  check('403 marks limited', pstate['p-403'].limitedUntil > Date.now(), JSON.stringify(pstate['p-403']));
  check('429 marks limited', pstate['p-429'].limitedUntil > Date.now(), JSON.stringify(pstate['p-429']));
  check('500 does NOT trigger the 12h cooldown', !allState['broken-provider'].accounts['b-500'].limitedUntil, JSON.stringify(allState['broken-provider'].accounts['b-500']));

  // 6. streaming passthrough is byte-faithful and unbuffered
  const sr = await chat({ stream: true, n: 250, headers: { 'x-router-provider': 'rr-provider' } });
  const chunkCount = (sr.text.match(/data: /g) || []).length;
  check('streaming: content-type is text/event-stream', (sr.headers.get('content-type') || '').includes('text/event-stream'), sr.headers.get('content-type'));
  check('streaming: all 250 chunks + final + [DONE] relayed untouched',
    chunkCount === 252 && sr.text.trimEnd().endsWith('data: [DONE]'), `chunks=${chunkCount}`);

  // 7. anthropic translation (stream + non-stream)
  const a1 = await chat({ n: 5, headers: { 'x-router-provider': 'anthropic-test' } });
  const a1j = JSON.parse(a1.text);
  check('anthropic -> OpenAI non-stream translation',
    a1.status === 200 && a1j.choices[0].message.content === 'anthropic says hi' && a1j.usage.prompt_tokens === 11, a1.text.slice(0, 200));
  const a2 = await chat({ stream: true, n: 5, headers: { 'x-router-provider': 'anthropic-test' } });
  const a2chunks = (a2.text.match(/"object":"chat\.completion\.chunk"/g) || []).length;
  const a2content = a2.text.split('\n').filter((l) => l.startsWith('data: ') && !l.includes('[DONE]'))
    .map((l) => { try { return JSON.parse(l.slice(6)).choices[0].delta.content || ''; } catch { return ''; } }).join('');
  check('anthropic -> OpenAI stream translation emits OpenAI chunks',
    a2chunks >= 5 && a2content === 'A0 A1 A2 A3 ' && a2.text.trimEnd().endsWith('data: [DONE]'),
    `chunks=${a2chunks} content="${a2content}"`);

  // 8. gemini translation
  const g1 = await chat({ headers: { 'x-router-provider': 'gemini-test' } });
  const g1j = JSON.parse(g1.text);
  check('gemini -> OpenAI non-stream translation',
    g1.status === 200 && g1j.choices[0].message.content === 'gemini says hi' && g1j.choices[0].finish_reason === 'stop', g1.text.slice(0, 200));
  const g2 = await chat({ stream: true, n: 4, headers: { 'x-router-provider': 'gemini-test' } });
  check('gemini -> OpenAI stream translation ends with [DONE] and emits chunks',
    (g2.text.match(/"object":"chat\.completion\.chunk"/g) || []).length >= 5 && g2.text.trimEnd().endsWith('data: [DONE]'), g2.text.slice(-160));

  // 9. admin auth + masking + dashboard
  const bad = await fetch(routerUrl('/admin/api/state'), { headers: { 'x-admin-key': 'wrong' } });
  check('admin API rejects a wrong key with 401', bad.status === 401, String(bad.status));
  const snap = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
  check('admin state exposes masked keys only',
    snap.providers.every((p) => p.accounts.every((a) => !a.maskedKey || a.maskedKey.includes('•'))), JSON.stringify(snap.providers[0].accounts));
  check('admin state reports limited accounts with a countdown timestamp',
    snap.providers.find((p) => p.id === 'seq-provider').accounts.find((a) => a.id === 'a-limited').limitedUntil > Date.now());
  const dash = await fetch(routerUrl('/admin'));
  const dashHtml = await dash.text();
  check('dashboard served at /admin with Tailwind + Alpine + Lucide',
    dash.status === 200 && dashHtml.includes('cdn.tailwindcss.com') && dashHtml.includes('alpinejs') && dashHtml.includes('lucide') && dashHtml.includes('x-admin-key'));
  const healthCounters = await (await fetch(routerUrl('/health'))).json();
  check('health exposes routed request counters', healthCounters.requests >= 15, JSON.stringify(healthCounters));

  // 10. manual reset clears the 12h cooldown
  const reset = await fetch(routerUrl('/admin/api/provider/seq-provider/account/a-limited/reset'), { method: 'POST', headers: ADMIN });
  const resetBody = await reset.json();
  check('reset now clears limitedUntil', reset.status === 200 && resetBody.account.limitedUntil === 0, JSON.stringify(resetBody));
  const afterReset = await chat({ headers: { 'x-router-provider': 'seq-provider' } });
  check('after reset the first account is tried first again (dark key 429s -> failover to a-ok)',
    afterReset.headers.get('x-router-account') === 'a-ok' && afterReset.headers.get('x-router-attempts') === '2',
    `account=${afterReset.headers.get('x-router-account')} attempts=${afterReset.headers.get('x-router-attempts')}`);

  // 11. strategy toggle via admin API
  const strat = await fetch(routerUrl('/admin/api/provider/rr-provider/strategy'), { method: 'POST', headers: ADMIN, body: JSON.stringify({ strategy: 'sequential' }) });
  check('strategy switch persisted', (await strat.json()).provider.strategy === 'sequential');
  check('strategy written to config.json', JSON.parse(fs.readFileSync(CONFIG, 'utf8')).providers.find((p) => p.id === 'rr-provider').strategy === 'sequential');

  // 11b. editing a provider from the dashboard must not silently wipe its custom headers
  await fetch(routerUrl('/admin/api/providers'), {
    method: 'POST', headers: ADMIN, force: true,
    body: JSON.stringify({
      id: 'rr-provider', name: 'RR renamed', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
      strategy: 'sequential', cooldownHours: 12, timeoutMs: 10000, maxAttempts: 0, force: true,
    }),
  });
  const rrAfter = JSON.parse(fs.readFileSync(CONFIG, 'utf8')).providers.find((p) => p.id === 'rr-provider');
  check('provider edit applies the change', rrAfter.name === 'RR renamed', rrAfter.name);
  check('provider edit preserves custom headers when the field is left blank',
    rrAfter.headers && rrAfter.headers['X-Test-Header'] === 'keep-me', JSON.stringify(rrAfter.headers));
  check('accounts survive a provider edit', rrAfter.accounts.length === 3, JSON.stringify(rrAfter.accounts.map((a) => a.id)));

  // 12. OmniRoute-style backup import
  const omni = {
    providers: [
      { name: 'Omni OpenRouter', provider: 'openrouter', accounts: [{ label: 'omni-1', key: 'sk-or-omni-1234567890' }] },
      { name: 'Omni OpenAI', type: 'openai', keys: ['sk-omni-0987654321'] },
      { name: 'Omni Local', type: 'generic-openai', baseUrl: 'http://127.0.0.1:1234/v1', accounts: [{ name: 'l1', token: 'local-token-123' }] },
    ],
  };
  const imp = await fetch(routerUrl('/admin/api/import'), { method: 'POST', headers: ADMIN, body: JSON.stringify(omni) });
  const impBody = await imp.json();
  check('OmniRoute-style backup import succeeds', imp.status === 200 && impBody.imported.accounts === 3, JSON.stringify(impBody).slice(0, 300));
  const cfgAfter = JSON.parse(fs.readFileSync(CONFIG, 'utf8'));
  check('imported openrouter provider detected by name hint',
    cfgAfter.providers.some((p) => p.type === 'openrouter' && p.accounts.some((a) => a.apiKey.startsWith('sk-or-omni'))),
    JSON.stringify(cfgAfter.providers.map((p) => p.id + ':' + p.type)));
  check('imported custom baseUrl preserved', cfgAfter.providers.some((p) => p.baseUrl === 'http://127.0.0.1:1234/v1'));

  // 13. export round-trip
  const exp = await fetch(routerUrl('/admin/api/export'), { headers: ADMIN });
  const expBody = await exp.json();
  check('export produces a full backup envelope', exp.status === 200 && expBody.kind === 'llm-router-backup' && expBody.config.providers.length >= 5);

  // 14. memory: relay 4x the bytes and require the RSS delta NOT to scale with it
  const CONC = 24;
  const round = async (chunks) => {
    const jobs = [];
    for (let i = 0; i < CONC; i++) jobs.push(chat({ stream: true, n: chunks, headers: { 'x-router-provider': 'rr-provider' } }));
    return Promise.all(jobs);
  };
  await round(300); // warm-up: let the allocator and socket pools reach steady state
  await sleep(1500);
  const base = await rss();
  const small = await round(1500);
  await sleep(1500);
  const afterSmall = await rss();
  const mem = await memOf();
  const big = await round(6000);
  await sleep(1500);
  const afterBig = await rss();
  const smallBytes = small.reduce((s, r) => s + r.text.length, 0);
  const bigBytes = big.reduce((s, r) => s + r.text.length, 0);
  const dSmall = afterSmall - base;
  const dBig = afterBig - afterSmall;
  results.push(`  INFO  load: ${CONC} concurrent streams x 1500 chunks = ${(smallBytes / 1048576).toFixed(2)} MB relayed -> RSS +${(dSmall / 1048576).toFixed(2)} MB`);
  results.push(`  INFO  load: ${CONC} concurrent streams x 6000 chunks = ${(bigBytes / 1048576).toFixed(2)} MB relayed (${(bigBytes / smallBytes).toFixed(1)}x) -> RSS +${(dBig / 1048576).toFixed(2)} MB`);
  results.push(`  INFO  after small round: heapUsed=${(mem.heapUsed / 1048576).toFixed(2)} MB, arrayBuffers=${(mem.arrayBuffers / 1048576).toFixed(2)} MB, external=${(mem.external / 1048576).toFixed(2)} MB`);
  check('all concurrent streams completed at both payload sizes',
    small.concat(big).every((d) => d.status === 200 && d.text.includes('[DONE]')));
  // On a shared VM raw RSS is noise-dominated (measured 3.9-18MB swings for the
  // same build), so the default is a generous ratio test and the tight numbers
  // are opt-in with SMOKE_STRICT_MEM=1.
  const strict = process.env.SMOKE_STRICT_MEM === '1';
  const rssOk = strict ? dBig < Math.max(dSmall * 1.6, 4 * 1048576) : dBig < Math.max(dSmall * 3, 24 * 1048576);
  if (strict) {
    check('RSS delta does not scale with relayed bytes (strict)', rssOk,
      `small=${(dSmall / 1048576).toFixed(2)}MB big=${(dBig / 1048576).toFixed(2)}MB`);
  } else {
    results.push(`  INFO  RSS delta small=${(dSmall / 1048576).toFixed(2)}MB big=${(dBig / 1048576).toFixed(2)}MB (${(bigBytes / smallBytes).toFixed(1)}x bytes) - ratio ${(dBig / Math.max(1, dSmall)).toFixed(2)}`);
    check('RSS delta stays sub-linear in relayed bytes (tolerant unless SMOKE_STRICT_MEM=1)', rssOk,
      `small=${(dSmall / 1048576).toFixed(2)}MB big=${(dBig / 1048576).toFixed(2)}MB`);
  }
  const heapCap = process.env.SMOKE_STRICT_MEM === '1' ? 40 : 96;
  const abCap = process.env.SMOKE_STRICT_MEM === '1' ? 4 : 16;
  check('JS heap stays far below the relayed byte volume (streams are not buffered)',
    mem.heapUsed < heapCap * 1048576 && mem.arrayBuffers < abCap * 1048576,
    `heapUsed=${(mem.heapUsed / 1048576).toFixed(2)}MB (<${heapCap}) arrayBuffers=${(mem.arrayBuffers / 1048576).toFixed(2)}MB (<${abCap})`);
  check('RSS stays inside the 128 MB container budget', afterBig < 128 * 1048576, `${(afterBig / 1048576).toFixed(2)}MB`);
  check('no 401/429 leakage in the load window', small.concat(big).filter((d) => d.status !== 200).length === 0);

  // 16. OpenCode: one account, four wire formats chosen per model id
  {
    const oc = await chat({ model: 'glm-5.2', headers: { 'x-router-provider': 'opencode-test' } });
    check('opencode: glm-* hits /openai/v1/chat/completions (passthrough)',
      oc.status === 200 && JSON.parse(oc.text).choices[0].message.content === 'opencode says hi', oc.text.slice(0, 160));

    const cl = await chat({ model: 'claude-sonnet-4-5', headers: { 'x-router-provider': 'opencode-test' } });
    check('opencode: claude-* routed to /anthropic/v1/messages and translated',
      cl.status === 200 && JSON.parse(cl.text).choices[0].message.content === 'anthropic says hi', cl.text.slice(0, 160));

    const clS = await chat({ model: 'claude-sonnet-4-5', stream: true, n: 3, headers: { 'x-router-provider': 'opencode-test' } });
    check('opencode: claude-* streams as OpenAI chunks',
      clS.text.includes('"object":"chat.completion.chunk"') && clS.text.trimEnd().endsWith('data: [DONE]'), clS.text.slice(0, 140));

    const gm = await chat({ model: 'gemini-3.1-pro', headers: { 'x-router-provider': 'opencode-test' } });
    check('opencode: gemini-* routed to /google/v1beta generateContent',
      gm.status === 200 && JSON.parse(gm.text).choices[0].message.content === 'gemini says hi', gm.text.slice(0, 160));

    const rs = await chat({ model: 'gpt-5.5', headers: { 'x-router-provider': 'opencode-test' } });
    const rsJson = rs.status === 200 ? JSON.parse(rs.text) : {};
    check('opencode: gpt-* routed to the Responses API and translated',
      rs.status === 200 && rsJson.choices[0].message.content === 'responses says hi' && rsJson.usage.prompt_tokens === 9, rs.text.slice(0, 220));

    const rsS = await chat({ model: 'gpt-5.5', stream: true, n: 3, headers: { 'x-router-provider': 'opencode-test' } });
    const rsText = rsS.text.split('\n').filter((l) => l.startsWith('data: ') && !l.includes('[DONE]'))
      .map((l) => { try { return JSON.parse(l.slice(6)).choices[0].delta.content || ''; } catch { return ''; } }).join('');
    // the bridge rebuilds the payload (unknown fields like "n" are dropped), so the
    // mock answers with its own default of 4 deltas.
    check('opencode: Responses SSE translated to chat.completion.chunk with [DONE]',
      rsText === 'R0 R1 R2 R3 ' && rsS.text.trimEnd().endsWith('data: [DONE]'), `"${rsText}"`);

    const free = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'opencode-free-test' } });
    const freeJson = free.status === 200 ? JSON.parse(free.text) : {};
    check('opencode-free: the attested identity is accepted and a non-stream client gets one JSON object',
      free.status === 200 && freeJson.object === 'chat.completion' && freeJson.choices[0].message.content === 'OC0 OC1 OC2 OC3 ',
      `${free.status} ${free.text.slice(0, 200)}`);
    check('opencode-free: the upstream really saw Bearer public, the v2 UA, a ses_ session and the tool quartet',
      fb.freeTierAccepted === 1 && fb.freeTierRejects === 0,
      `accepted=${fb.freeTierAccepted} rejected=${fb.freeTierRejects}`);
    const freeStream = await chat({ model: 'big-pickle', stream: true, headers: { 'x-router-provider': 'opencode-free-test' } });
    check('opencode-free: a streaming client gets the SSE pass-through',
      freeStream.status === 200 && (freeStream.headers.get('content-type') || '').includes('text/event-stream')
        && freeStream.text.includes('OC0 ') && freeStream.text.trimEnd().endsWith('data: [DONE]'),
      freeStream.text.slice(0, 160));

    const models = await (await fetch(routerUrl('/admin/api/provider/opencode-test/models'), { headers: ADMIN })).json();
    check('admin model discovery returns the upstream model list',
      models.count === 5 && models.models.includes('claude-sonnet-4-5') && models.source.endsWith('/inference/v1/models'),
      JSON.stringify(models).slice(0, 240));

    const keylessAdd = await fetch(routerUrl('/admin/api/provider/opencode-free-test/accounts'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ accounts: [{ name: 'second-keyless', apiKey: '' }] }),
    });
    check('opencode-free refuses a hand-added identity (the router manages them)', keylessAdd.status === 409, String(keylessAdd.status));
    const plainAdd = await fetch(routerUrl('/admin/api/provider/rr-provider/accounts'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ accounts: [{ name: 'second-manual', apiKey: '' }] }),
    });
    check('a normal provider still accepts an empty-key account', (await plainAdd.json()).added === 1, 'empty-key add rejected');
  }

  // 17. V2Ray exit pool: a 429 rotates the exit instead of burning the account
  {
    const snap = await (await fetch(routerUrl('/admin/api/proxies'), { headers: ADMIN })).json();
    check('proxies: pool loaded from proxies.json with 2 working exits',
      snap.counts.ok === 2 && snap.active === 2, JSON.stringify(snap.counts));

    proxyHits.length = 0;
    const r = await chat({ headers: { 'x-router-provider': 'proxy-test' } });
    check('proxy rotate: IP-bound 429 is retried through the next exit and succeeds',
      r.status === 200 && r.headers.get('x-router-rotations') === '1' && r.headers.get('x-router-exit') === 'x2',
      `status=${r.status} exit=${r.headers.get('x-router-exit')} rotations=${r.headers.get('x-router-rotations')}`);
    check('proxy rotate: both exits were CONNECTed',
      proxyHits.length === 2 && proxyHits[0].startsWith('a:127.0.0.1:') && proxyHits[1].startsWith('b:127.0.0.1:'),
      JSON.stringify(proxyHits));
    // The blind spot that shipped a proxy path which silently egressed
    // directly: the CONNECT succeeded, but was the request inside it?
    check('proxy rotate: the HTTP requests really travelled inside those tunnels',
      proxyA.innerRequests.length === 1 && proxyB.innerRequests.length === 1,
      `a=${JSON.stringify(proxyA.innerRequests)} b=${JSON.stringify(proxyB.innerRequests)}`);

    await sleep(700);
    const st = JSON.parse(fs.readFileSync(STATE, 'utf8'));
    const pt = st.providers['proxy-test'].accounts['pt-1'];
    check('proxy rotate: the account was NOT put on a 12h cooldown', !pt.limitedUntil, JSON.stringify(pt));

    const beforeDirect = proxyA.innerRequests.length + proxyB.innerRequests.length;
    const direct = await chat({ headers: { 'x-router-provider': 'rr-provider' } });
    check('providers without a proxy still go direct', direct.headers.get('x-router-exit') === 'direct', direct.headers.get('x-router-exit'));
    check('a direct provider never opens a tunnel',
      proxyA.innerRequests.length + proxyB.innerRequests.length === beforeDirect,
      'a tunnel was opened for a direct provider');

    const before = (await (await fetch(routerUrl('/admin/api/proxies'), { headers: ADMIN })).json()).index;
    const rot = await (await fetch(routerUrl('/admin/api/proxies/rotate'), { method: 'POST', headers: ADMIN })).json();
    check('manual rotate advances the pool index', rot.ok === true && rot.exit && rot.index !== before, JSON.stringify(rot).slice(0, 140));

    const bad = await (await fetch(routerUrl('/admin/api/proxies/version'), { method: 'POST', headers: ADMIN })).json();
    check('missing xray binary is reported, not fatal', bad.ok === false && /xray/i.test(bad.path), JSON.stringify(bad).slice(0, 160));
  }

  // 18. freebuff: full signed handshake against a protocol-enforcing mock
  {
    const r = await chat({ model: fb.modelKey, headers: { 'x-router-provider': 'freebuff-test' } });
    const body = r.status === 200 ? JSON.parse(r.text) : {};
    check('freebuff: signed catalog + admission + run + completion succeed',
      r.status === 200 && body.choices[0].message.content === 'freebuff says hi', `${r.status} ${r.text.slice(0, 200)}`);
    check('freebuff: every request carried a valid Ed25519 signature',
      fb.verifyOk >= 5 && fb.verifyFail === 0, `ok=${fb.verifyOk} fail=${fb.verifyFail} paths=${fb.signedPaths.join(',')}`);
    check('freebuff: the Buffy preamble was injected', fb.preambleOk === 1, String(fb.preambleOk));

    const s = await chat({ model: fb.modelKey, stream: true, headers: { 'x-router-provider': 'freebuff-test' } });
    check('freebuff: streaming passthrough with [DONE]',
      s.status === 200 && s.text.includes('freebuff says hi') && s.text.trimEnd().endsWith('data: [DONE]'), s.text.slice(0, 160));

    const models = await (await fetch(routerUrl('/admin/api/provider/freebuff-test/models'), { headers: ADMIN })).json();
    check('freebuff: dashboard model list comes from the signed catalog',
      models.count === 1 && models.models[0] === fb.modelKey, JSON.stringify(models).slice(0, 200));

    const fbs = await (await fetch(routerUrl('/admin/api/freebuff'), { headers: ADMIN })).json();
    check('freebuff: session state is exposed (device key, catalog, instance)',
      fbs.accounts.length === 1 && fbs.accounts[0].deviceKeyId === fb.keyId && fbs.accounts[0].catalogSize === 1,
      JSON.stringify(fbs).slice(0, 240));
  }

  // 19. connect flow: a provider can be added without any credential, and the
  //     key can be attached later from the same page
  {
    const conn = await fetch(routerUrl('/admin/api/connect'), {
      method: 'POST', headers: ADMIN,
      body: JSON.stringify({
        provider: { id: 'connect-test', name: 'Connect Test', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, strategy: 'round-robin', proxy: 'direct' },
        account: { name: '', apiKey: '' },
      }),
    });
    const connBody = await conn.json();
    check('connect: a provider is created with a keyless account in one call',
      conn.status === 201 && connBody.provider.id === 'connect-test' && connBody.account && connBody.account.keyless === true,
      JSON.stringify(connBody).slice(0, 220));

    const later = await fetch(routerUrl('/admin/api/provider/connect-test/account/' + connBody.account.id), {
      method: 'PATCH', headers: ADMIN, body: JSON.stringify({ apiKey: 'sk-added-later' }),
    });
    const laterBody = await later.json();
    check('connect: the credential can be attached later', later.status === 200 && laterBody.account.maskedKey.includes('•'), JSON.stringify(laterBody).slice(0, 160));

    const emptyAdd = await fetch(routerUrl('/admin/api/provider/rr-provider/accounts'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ accounts: [{ name: 'paste-key-later' }] }),
    });
    check('accounts: a named account with no key is accepted (key can come later)',
      (await emptyAdd.json()).added === 1, 'empty-key add rejected');

    const probe = await fetch(routerUrl('/admin/api/probe-models'), {
      method: 'POST', headers: ADMIN,
      body: JSON.stringify({ provider: { id: 'probe', type: 'opencode', baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference` }, account: { apiKey: 'probe-key' } }),
    });
    const probeBody = await probe.json();
    check('connect: "check models" probes an unsaved provider', probe.status === 200 && probeBody.ok === true && probeBody.count === 5, JSON.stringify(probeBody).slice(0, 200));
  }

  // 20. a test run with no usable xray binary must fail fast and say why
  {
    const start = await fetch(routerUrl('/admin/api/proxies/test'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ retest: true }),
    });
    const body = await start.json();
    check('proxy test: a missing xray binary is refused immediately with a clear reason',
      start.status === 409 && /xray/i.test(body.error.message), `${start.status} ${JSON.stringify(body).slice(0, 160)}`);
    await sleep(400);
    const status = await (await fetch(routerUrl('/admin/api/proxies'), { headers: ADMIN })).json();
    check('proxy test: the refusal leaves no half-started run behind',
      status.test.running === false && status.test.total === 0 && /xray/i.test(status.test.error),
      JSON.stringify(status.test).slice(0, 200));
    check('proxy test: exits stay untouched (no false "dead" verdicts)',
      status.counts.ok === 2 && status.counts.dead === 0, JSON.stringify(status.counts));
  }

  // 21. time-boxed account rotation: swap the token every N minutes
  {
    const prov = {
      id: 'window-provider', name: 'Window Test', type: 'custom', baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`,
      strategy: 'sequential', timeoutMs: 10000, rotateEveryMinutes: 0.02, // 1.2s
      accounts: [
        { id: 'w-1', name: 'token-1', apiKey: 'key-window-1', enabled: true },
        { id: 'w-2', name: 'token-2', apiKey: 'key-window-2', enabled: true },
      ],
    };
    const made = await fetch(routerUrl('/admin/api/providers'), { method: 'POST', headers: ADMIN, body: JSON.stringify({ ...prov, force: true }) });
    check('window: provider with rotateEveryMinutes is created', made.status === 201 || made.status === 200, String(made.status));

    const seq = [];
    for (let i = 0; i < 3; i += 1) {
      const r = await chat({ headers: { 'x-router-provider': 'window-provider' } });
      seq.push(r.headers.get('x-router-account'));
      await sleep(1400); // let the 1.2s window close
    }
    check('window: the account is swapped once its window closes (token-1 -> token-2 -> token-1)',
      seq[0] === 'w-1' && seq[1] === 'w-2' && seq[2] === 'w-1', seq.join(' -> '));

    const snap = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const wp = snap.providers.find((p) => p.id === 'window-provider');
    const wid = wp.accounts.find((a) => a.windowStartedAt);
    check('window: state exposes the window so the dashboard can count it down',
      wp.rotateEveryMinutes === 0.02 && !!wid && wid.windowExpiresAt === wid.windowStartedAt + 1200,
      JSON.stringify({ rotateEvery: wp.rotateEveryMinutes, started: wid && wid.windowStartedAt, expires: wid && wid.windowExpiresAt }));

    const off = await fetch(routerUrl('/admin/api/provider/window-provider'), { method: 'PATCH', headers: ADMIN, body: JSON.stringify({ rotateEveryMinutes: 0 }) });
    check('window: turning it off keeps the accounts', (await off.json()).provider.accounts.length === 2);
  }

  // 22. cooldown semantics: honour retry-after, and do not treat a protocol
  //     error as a 12-hour rate limit
  {
    const r = await chat({ headers: { 'x-router-provider': 'cooldown-provider' } });
    check('cooldown: all accounts limited still answers 429', r.status === 429, String(r.status));
    const body = JSON.parse(r.text);
    check('cooldown: the upstream message and unlock time are reported back',
      Array.isArray(body.error.accounts) && /retry shortly/i.test(body.error.accounts[0].message) && body.error.accounts[0].unlocksInSeconds > 0,
      JSON.stringify(body.error).slice(0, 240));
    await sleep(700);
    const st = JSON.parse(fs.readFileSync(STATE, 'utf8')).providers['cooldown-provider'].accounts['c-retry'];
    const waitS = Math.round((st.limitedUntil - Date.now()) / 1000);
    check('cooldown: retry-after 2s beats the 12h default', waitS <= 3 && waitS >= 0, `wait=${waitS}s`);

    const err = await chat({ headers: { 'x-router-provider': 'error-provider' } });
    check('cooldown: a non-quota 403 falls through to the next account, then 429 with reasons',
      err.status === 429 && JSON.parse(err.text).error.reasons.some((x) => /free_mode_cli_required/i.test(x)), err.text.slice(0, 240));
    await sleep(700);
    const est = JSON.parse(fs.readFileSync(STATE, 'utf8')).providers['error-provider'].accounts;
    const wait403 = Math.round((est['e-403'].limitedUntil - Date.now()) / 60000);
    const wait402 = Math.round((est['e-402'].limitedUntil - Date.now()) / 60000);
    check('cooldown: the protocol 403 gets minutes, not 12 hours', wait403 <= 6 && est['e-403'].lastLimitedKind === 'error', `403=${wait403}m kind=${est['e-403'].lastLimitedKind}`);
    check('cooldown: a credit 402 keeps the long quota cooldown', wait402 >= 700 && est['e-402'].lastLimitedKind === 'quota', `402=${wait402}m kind=${est['e-402'].lastLimitedKind}`);
  }

  // 23. a policy gate reproduces on every exit: no rotation storm, no 12h ban,
  //     and the client sees the upstream error instead of a generic 503
  {
    proxyHits.length = 0;
    const r = await chat({ headers: { 'x-router-provider': 'freetier-provider' } });
    const body = JSON.parse(r.text);
    check('policy: the upstream 403 reaches the client with its own message',
      r.status === 403 && body.error.type === 'upstream_error' && /free tier|FreeTier/i.test(body.error.message),
      `${r.status} ${JSON.stringify(body).slice(0, 200)}`);
    check('policy: the exits were NOT rotated for a policy gate', proxyHits.length <= 1, `connects=${proxyHits.length}`);
    await sleep(700);
    const ft = JSON.parse(fs.readFileSync(STATE, 'utf8')).providers['freetier-provider'].accounts['ft-1'];
    const mins = Math.round((ft.limitedUntil - Date.now()) / 60000);
    check('policy: the account gets minutes, not 12 hours', mins <= 6 && ft.lastLimitedKind === 'policy', `${mins}m kind=${ft.lastLimitedKind}`);

    const off = await chat({ headers: { 'x-router-provider': 'disabled-provider' } });
    const offBody = JSON.parse(off.text);
    check('disabled accounts: distinct error type, no retryAfterSeconds: 0',
      off.status === 503 && offBody.error.type === 'router_no_enabled_accounts' && !('retryAfterSeconds' in offBody.error),
      `${off.status} ${JSON.stringify(offBody.error).slice(0, 200)}`);
  }

  // 24. leak filter: an exit measured as egressing on our own IP is never used
  {
    const { ProxyManager, TELEMETRY_SCHEMA } = require(path.join(REPO, 'proxy-manager.js'));
    const pm = new ProxyManager({ dataDir: TMP });
    pm.localIp = '203.0.113.9';
    pm.state.exits = [
      { id: 'good', status: 'ok', testedBy: TELEMETRY_SCHEMA, exitIp: '198.51.100.7', latencyMs: 300 },
      { id: 'leak', status: 'leak', testedBy: TELEMETRY_SCHEMA, exitIp: '', latencyMs: 10 },
      { id: 'stale', status: 'ok', testedBy: 1, exitIp: '198.51.100.8', latencyMs: 20 },
      { id: 'selfsame', status: 'ok', testedBy: TELEMETRY_SCHEMA, exitIp: '203.0.113.9', latencyMs: 15 },
    ];
    pm.state.active = ['good', 'leak', 'stale', 'selfsame'];
    const working = pm.workingExits().map((e) => e.id);
    check('leak: leaks, stale telemetry and self-egress exits are excluded from the pool',
      working.length === 1 && working[0] === 'good', JSON.stringify(working));
    const active = pm.activeExits().map((e) => e.id);
    check('leak: the active set is filtered the same way', active.length === 1 && active[0] === 'good', JSON.stringify(active));
  }

  // 25. inbound auth accepts what real clients send
  {
    const key = 'router-key-for-smoke';
    await fetch(routerUrl('/admin/api/settings'), { method: 'POST', headers: ADMIN, body: JSON.stringify({ proxyKeys: [key] }) });
    const bad = await fetch(routerUrl('/v1/messages'), {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': 'nope' },
      body: JSON.stringify({ model: 'gpt-test', max_tokens: 8, messages: [{ role: 'user', content: 'hi' }] }),
    });
    check('inbound auth: a wrong x-api-key is rejected', bad.status === 401, String(bad.status));
    const good = await fetch(routerUrl('/v1/messages'), {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': key },
      body: JSON.stringify({ model: 'gpt-test', max_tokens: 8, messages: [{ role: 'user', content: 'hi' }] }),
    });
    check('inbound auth: the Anthropic x-api-key header is accepted on /v1/messages', good.status === 200, `${good.status} ${(await good.text()).slice(0, 120)}`);
    const q = await fetch(routerUrl('/v1/models?key=' + key));
    check('inbound auth: /v1/models follows the router key too', q.status === 200, String(q.status));
    const noKey = await fetch(routerUrl('/v1/models'));
    check('inbound auth: /v1/models without a key is refused once keys are set', noKey.status === 401, String(noKey.status));
    const cleared = await fetch(routerUrl('/admin/api/settings'), { method: 'POST', headers: ADMIN, body: JSON.stringify({ proxyKeys: [], clearProxyKeys: true }) });
    const clearedBody = await cleared.json();
    check('inbound auth: clearProxyKeys really removes the keys', clearedBody.settings.hasProxyKeys === false, JSON.stringify(clearedBody.settings));
  }

  // 26. opencode-free manages its own identities: no manual add, auto-renewal
  //     on a limit, and a provider with no identity gets one.
  {
    const manual = await fetch(routerUrl('/admin/api/provider/managed-test/accounts'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ accounts: [{ name: 'mine', apiKey: '' }] }),
    });
    const manualBody = await manual.json();
    check('managed: adding an identity by hand is refused', manual.status === 409 && manualBody.error.type === 'accounts_managed', `${manual.status} ${JSON.stringify(manualBody).slice(0, 140)}`);

    // A managed provider created a second ago has no identity yet: the first
    // request has to mint one instead of answering router_no_account.
    const emptyReq = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'empty-managed' } });
    check('managed: a provider with no identity mints one on the first request',
      emptyReq.status === 200, `${emptyReq.status} ${emptyReq.text.slice(0, 160)}`);
    const seeded = (await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json()).providers.find((p) => p.id === 'empty-managed');
    check('managed: exactly one identity was minted', !!seeded && seeded.accountCount === 1, JSON.stringify(seeded && seeded.accountCount));

    const before = (await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json());
    const beforeAccount = before.providers.find((p) => p.id === 'managed-test').accounts[0];
    const beforeGen = beforeAccount.gen;
    fb.burn429 = 1;
    const r = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'managed-test' } });
    check('managed: an exhausted identity is replaced and the request still succeeds',
      r.status === 200 && JSON.parse(r.text).choices[0].message.content.length > 0, `${r.status} ${r.text.slice(0, 160)}`);
    await sleep(700);
    const after = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const mp = after.providers.find((p) => p.id === 'managed-test');
    const afterAccount = mp.accounts[0];
    check('managed: exactly one identity is kept and it is a rebuild of the same slot',
      mp.accountCount === 1 && afterAccount.id === beforeAccount.id, `${beforeAccount.id} -> ${afterAccount.id} count=${mp.accountCount}`);
    check('managed: the identity generation moved on',
      Number(afterAccount.gen) > Number(beforeGen), `gen ${beforeGen} -> ${afterAccount.gen}`);
    check('managed: the renewal is counted and nothing was cooled down',
      (after.totals.renewals || 0) >= 1 && mp.limitedCount === 0, `renewals=${after.totals.renewals} limited=${mp.limitedCount}`);

    const renew = await fetch(routerUrl('/admin/api/proxies/provider/managed-test/renew'), { method: 'POST', headers: ADMIN, body: '{}' });
    const renewBody = await renew.json();
    check('managed: the dashboard can force a new identity', renew.status === 200 && renewBody.account && renewBody.account.keyless === true, JSON.stringify(renewBody).slice(0, 160));

    // reinstalling means a different client in every field the router controls,
    // not just a new token: project handle, client version and the session.
    const first = await (await fetch(routerUrl('/admin/api/proxies/provider/managed-test/reinstall'), { method: 'POST', headers: ADMIN, body: '{}' })).json();
    const second = await (await fetch(routerUrl('/admin/api/proxies/provider/managed-test/reinstall'), { method: 'POST', headers: ADMIN, body: '{}' })).json();
    check('reinstall: returns the rebuilt client fingerprint',
      /^p-[0-9a-f]{16}$/.test(String(first.client && first.client.project))
      && /^opencode\/latest\//.test(String(first.client && first.client.userAgent))
      && String(first.client && first.client.session).length > 0,
      JSON.stringify(first).slice(0, 200));
    // The build is pinned, so a rebuild changes the project handle and the session
    // and leaves the client version alone.
    check('reinstall: two rebuilds present a different project handle, on the newest build',
      first.client.project !== second.client.project
      && first.client.userAgent === second.client.userAgent
      && first.client.userAgent === 'opencode/latest/2.1.0/cli',
      `${first.client.project}/${first.client.userAgent} vs ${second.client.project}/${second.client.userAgent}`);
    check('reinstall: the cooldown is cleared and the rebuild is counted',
      first.client.cooldownCleared === true && second.reinstalls > first.reinstalls, `cleared=${first.client.cooldownCleared} ${first.reinstalls}->${second.reinstalls}`);

    // The free allowance is accounted per model, so a dry model must not cost
    // the client an error: the router answers with the next free model that has
    // allowance and keeps the identity (nothing is cooled down).
    fb.modelQuotaExhausted = ['big-pickle'];
    const dry = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'managed-test' } });
    fb.modelQuotaExhausted = [];
    let dryBody = {};
    try { dryBody = JSON.parse(dry.text); } catch { /* reported below */ }
    check('free fallback: a dry model is answered by another free model',
      dry.status === 200
      && dry.headers.get('x-router-model') === 'nemotron-3.5-lightning-free'
      && String(dryBody?.choices?.[0]?.message?.content || '').length > 0,
      `${dry.status} x-router-model=${dry.headers.get('x-router-model')} body=${dry.text.slice(0, 120)}`);
    const afterFallback = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const mpFallback = afterFallback.providers.find((p) => p.id === 'managed-test');
    check('free fallback: a dry model does not cool the account down',
      mpFallback.limitedCount === 0 && (afterFallback.totals.modelFallbacks || 0) >= 1,
      `limited=${mpFallback.limitedCount} fallbacks=${afterFallback.totals.modelFallbacks}`);

    // last resort: every free model dry -> the configured provider answers instead
    fb.modelQuotaExhausted = ['big-pickle', 'nemotron-3.5-lightning-free', 'beam-free', 'flint-40b-free', 'mimo-v2.6-flash-free', 'nemotron-3.5-super-vl-free'];
    const hop = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'managed-test' } });
    fb.modelQuotaExhausted = [];
    check('provider fallback: an exhausted free tier is answered by the configured provider',
      hop.status === 200
      && hop.headers.get('x-router-provider') === 'fallback-provider'
      && hop.headers.get('x-router-model') === 'fallback-model',
      `${hop.status} provider=${hop.headers.get('x-router-provider')} model=${hop.headers.get('x-router-model')} body=${hop.text.slice(0, 120)}`);
    const afterHop = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    check('provider fallback: the hop is counted', (afterHop.totals.providerFallbacks || 0) >= 1, `providerFallbacks=${afterHop.totals.providerFallbacks}`);

    // No timer on the free tier: even the worst case must leave the account
    // retryable immediately (the upstream's ~16h retry-after is not adopted).
    const hopAccount = (afterHop.providers.find((p) => p.id === 'managed-test').accounts || [])[0] || {};
    const cooldownLeftMs = Number(hopAccount.limitedUntil || 0) - Date.now();
    check('free tier: a quota answer leaves no cooldown standing',
      cooldownLeftMs < 5000, `limitedUntil in ${Math.round(cooldownLeftMs / 1000)}s`);

    // a managed provider that lost its identity gets one on the next request
    const st = JSON.parse(fs.readFileSync(CONFIG, 'utf8'));
    const target = st.providers.find((p) => p.id === 'managed-test');
    target.accounts = [];
    fs.writeFileSync(CONFIG, JSON.stringify(st, null, 2));
    await fetch(routerUrl('/admin/api/provider/managed-test/accounts'), { method: 'POST', headers: ADMIN, body: '{}' }).catch(() => {});
    check('managed: the managed provider still advertises itself as managed', mp.managed === true, String(mp.managed));
  }

  // 28. opencode-free again: a limit moves the exit IP first and the identity
  //     second, the whole ladder stays inside a minute, and a spent exit is
  //     burned so the pool rebuilds itself instead of cycling onto it.
  {
    const direct = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'free-rotate-test' } });
    check('free rotate: the first attempt is made from the host IP', direct.status === 200 && direct.headers.get('x-router-exit') === 'direct',
      `${direct.status} exit=${direct.headers.get('x-router-exit')}`);

    // Lanes are minted on the first request, then stay: the slot survives a
    // rebuild and no lane is ever replaced by another row.
    const state1 = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const laneProvider = state1.providers.find((p) => p.id === 'free-rotate-test');
    const beforeIds = laneProvider.accounts.map((a) => a.id);
    check('lanes: the pool size is honoured and every lane is a separate identity',
      laneProvider.accountCount === 2 && new Set(beforeIds).size === 2,
      `accounts=${laneProvider.accountCount} ids=${beforeIds.join(',')}`);
    const pins = laneProvider.accounts.map((a) => a.exit);
    check('lanes: every lane holds its own exit',
      pins.every(Boolean) && new Set(pins).size === pins.length, JSON.stringify(pins));

    fb.burn429 = 1;
    const hop = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'free-rotate-test' } });
    const hopExit = hop.headers.get('x-router-exit');
    check('free rotate: one limit is answered from the next exit, not a new identity',
      hop.status === 200 && !!hopExit && hopExit !== 'direct' && hop.headers.get('x-router-rotations') === '1',
      `${hop.status} exit=${hopExit} rotations=${hop.headers.get('x-router-rotations')} body=${hop.text.slice(0, 120)}`);
    const afterHop = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const hopIds = afterHop.providers.find((p) => p.id === 'free-rotate-test').accounts.map((a) => a.id);
    check('free rotate: the identity survived the IP hop', JSON.stringify(hopIds) === JSON.stringify(beforeIds), `${JSON.stringify(beforeIds)} -> ${JSON.stringify(hopIds)}`);
    const snap1 = await (await fetch(routerUrl('/admin/api/proxies'), { headers: ADMIN })).json();

    // The fresh IP is limited as well: now the identity goes, and the request
    // still answers - this is the "reinstall if the exit did not help" half.
    fb.burn429 = 2;
    const esc = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'free-rotate-test' } });
    const afterEsc = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const escLanes = afterEsc.providers.find((p) => p.id === 'free-rotate-test');
    check('free rotate: a second limit on the fresh IP reinstalls that lane and succeeds',
      esc.status === 200 && escLanes.accountCount === 2
      && escLanes.accounts.some((a) => Number(a.gen) > 1),
      `${esc.status} gens=${escLanes.accounts.map((a) => a.gen).join(',')} rotations=${esc.headers.get('x-router-rotations')} burnsLeft=${fb.burn429}`);
    check('free rotate: the rebuilt lane still answers through the pool', esc.headers.get('x-router-exit') !== 'direct',
      String(esc.headers.get('x-router-exit')));
    check('lanes: the sweep floor is exposed and defaults to a minute',
      afterEsc.allLanesRenewGapMinutes === 1, String(afterEsc.allLanesRenewGapMinutes));
    // The two dials an operator uses to choose "keep rotating" over "wait it out":
    // 0 is a real setting on both, so it must survive the settings route and be
    // reported by the snapshot (the dialog labels its inputs with these numbers).
    check('dials: the day horizon and the pool rest are reported',
      afterEsc.freeDayHorizonMinutes === 30 && afterEsc.freePoolRestMinutes >= 0,
      `horizon=${afterEsc.freeDayHorizonMinutes} rest=${afterEsc.freePoolRestMinutes}`);
    await fetch(routerUrl('/admin/api/settings'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ freeDayHorizonMinutes: 0, freePoolRestMinutes: 0 }),
    });
    const zeroed = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    check('dials: 0 on both means never park and never rest',
      zeroed.freeDayHorizonMinutes === 0 && zeroed.freePoolRestMinutes === 0,
      `horizon=${zeroed.freeDayHorizonMinutes} rest=${zeroed.freePoolRestMinutes}`);
    await fetch(routerUrl('/admin/api/settings'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ freeDayHorizonMinutes: 30, freePoolRestMinutes: 5 }),
    });
    const restored = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    check('dials: a positive value pins the window again',
      restored.freeDayHorizonMinutes === 30 && restored.freePoolRestMinutes === 5,
      `horizon=${restored.freeDayHorizonMinutes} rest=${restored.freePoolRestMinutes}`);
    const snap2 = await (await fetch(routerUrl('/admin/api/proxies'), { headers: ADMIN })).json();
    const burnedIds = (snap2.events || []).filter((e) => e.kind === 'exit-burned').map((e) => e.exit);
    const activeIds = snap1.exits.filter((e) => e.active).map((e) => e.id);
    check('free rotate: the exit that answered the limit is burned out of the pool',
      burnedIds.some((id) => activeIds.includes(id)) && snap2.counts.ok <= activeIds.length,
      `burnedEvents=${burnedIds.join(',')} activeBefore=${activeIds.join(',')} activeNow=${snap2.active}`);

    // Worst case: everything 429s, so the ladder runs to its end. Every lane is
    // handed over, nothing waits for a rebuild that cannot help, and the caller
    // is given the configured fallback provider well inside the minute.
    // Snapshot what each lane presents *before* the ladder so the sweep can be
    // compared per lane: the rebuilds are background and land fast, so a baseline
    // taken after the request would already contain their new identities.
    const beforeSweep = new Map(escLanes.accounts.map((a) => [a.id, `${a.gen}:${a.build}`]));
    fb.burn429 = 99;
    const t0 = Date.now();
    const dead = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'free-rotate-test' } });
    const spent = Date.now() - t0;
    fb.burn429 = 0;
    check('free rotate: an exhausted ladder falls through to the fallback provider inside 60s',
      dead.status === 200 && dead.headers.get('x-router-provider') === 'fallback-provider' && spent < 60000,
      `${dead.status} provider=${dead.headers.get('x-router-provider')} ${spent}ms`);
    check('free rotate: the ladder is bounded, not an unbounded retry storm',
      Number(dead.headers.get('x-router-rotations') || 0) <= 3, String(dead.headers.get('x-router-rotations')));

    // Both lanes were handed over and parked, with a rebuild queued for each
    // rather than run on the caller's clock.
    const afterDead = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const deadLanes = afterDead.providers.find((p) => p.id === 'free-rotate-test').accounts;
    check('lanes: both spent lanes are rebuilt after an exhausted ladder',
      deadLanes.length === 2 && deadLanes.every((a) => Number(a.gen) > 1 || a.refreshing),
      deadLanes.map((a) => `${a.name}gen${a.gen}${a.refreshing ? ' refreshing' : ''}`).join(','));

    // The limit belongs to the provider, not to the lane that reported it: one
    // ladder ending in "spent" sweeps *every* lane, so the next request starts on a
    // complete set of fresh identities instead of re-learning that the others are
    // dry as well. One sweep, covering all lanes, and not one per lane.
    const sweeps = (afterDead.events || []).filter((e) => e.kind === 'reinstall-all' && e.pid === 'free-rotate-test');
    check('lanes: one limit mints a new identity on every lane, not only on the one that reported it',
      sweeps.length === 1 && sweeps[0].n === 2, JSON.stringify(sweeps));

    // ...and every one of them really is a new identity: the rebuild runs off the
    // request path, so wait for it and compare each lane against itself - new
    // generation and new session, presenting the same newest build.
    let swept = deadLanes;
    for (let i = 0; i < 50 && swept.some((a) => a.refreshing); i += 1) {
      await sleep(100);
      const st = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
      swept = st.providers.find((p) => p.id === 'free-rotate-test').accounts;
    }
    check('lanes: the sweep finishes - every lane steps to a new generation, build unchanged',
      swept.length === 2 && swept.every((a) => !a.refreshing && beforeSweep.get(a.id) !== `${a.gen}:${a.build}`)
      && new Set(swept.map((a) => a.build)).size === 1,
      swept.map((a) => `${a.name}gen${a.gen}/${a.build}${a.refreshing ? ' refreshing' : ''}`).join(','));

    // The burn pile is deleted and a fresh batch is taken on for the next hops.
    const replenished = await (await fetch(routerUrl('/admin/api/proxies/replenish'), { method: 'POST', headers: ADMIN, body: '{}' })).json();
    check('replenish: the burned exits are deleted from the pool',
      !replenished.snapshot.exits.some((e) => e.id === hopExit || e.status === 'burned'),
      replenished.snapshot.exits.map((e) => `${e.id}:${e.status}`).join(','));
  }

  // 28b. the default lane pool, and lanes as isolated as the design promises.
  {
    const st = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    const def = st.providers.find((p) => p.id === 'lane-default-test');
    check('lanes: an untouched free provider defaults to four identities', def.identityPool === 4, String(def.identityPool));
    const req = await chat({ model: 'big-pickle', headers: { 'x-router-provider': 'lane-default-test' } });
    check('lanes: the default pool is minted on the first request', req.status === 200, `${req.status} ${req.text.slice(0, 120)}`);
    const seeded = (await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json()).providers.find((p) => p.id === 'lane-default-test');
    check('lanes: four separate identities exist, each with its own session on the newest build',
      seeded.accountCount === 4
      && new Set(seeded.accounts.map((a) => a.lane)).size === 4
      && new Set(seeded.accounts.map((a) => a.build)).size === 1,
      `count=${seeded.accountCount} lanes=${seeded.accounts.map((a) => a.lane).join(',')} builds=${seeded.accounts.map((a) => a.build).join(',')}`);
  }

  // 28c. the context window the router advertises: clients trim on this number,
  //      and an unset one made them cut the conversation off at a few thousand
  //      tokens while the free tier still holds ~900KB.
  {
    const listed = await (await fetch(routerUrl('/v1/models'), { headers: ADMIN })).json();
    const rows = listed.data || [];
    check('context: every advertised model carries the million-token window',
      rows.length > 0 && rows.every((m) => m.limit && m.limit.context === 1000000 && m.max_input_tokens === 1000000),
      `rows=${rows.length} contexts=${rows.map((m) => m.limit && m.limit.context).slice(0, 6).join(',')}`);
    const st = await (await fetch(routerUrl('/admin/api/state'), { headers: ADMIN })).json();
    check('context: the provider summary exposes the window for the dashboard',
      (st.providers || []).every((p) => p.contextWindow === 1000000),
      `windows=${(st.providers || []).map((p) => p.contextWindow).join(',')}`);
    // A large body earns a longer first-byte deadline than a chat one-liner, and
    // the ladder's budget grows with it instead of being cut off mid-prefill.
    const big = 'x'.repeat(400 * 1024);
    const bigRes = await chat({ model: 'big-pickle', messages: [{ role: 'user', content: big }], headers: { 'x-router-provider': 'lane-default-test' } });
    check('context: a 400KB prompt still answers', bigRes.status === 200, `${bigRes.status} ${bigRes.text.slice(0, 120)}`);
  }

  // 29. burn bookkeeping without a network: one hit is enough to leave the pool,
  //     rotation skips what was burned, and a refill never re-applies it.
  {
    const { ProxyManager } = require('../proxy-manager.js');
    const pm = new ProxyManager({ dataDir: fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-burn-')), log: () => {} });
    // Its own port range: the default is production's, and an xray left behind on
    // those ports keeps receiving traffic (reuseport) long after the test ends.
    Object.assign(pm.state.settings, { inboundBasePort: 23800, testBasePort: 23840 });
    pm.state.exits = ['b1', 'b2', 'b3'].map((id) => ({
      id, status: 'ok', testedBy: TELEMETRY_SCHEMA, exitIp: `10.0.0.${id.slice(1)}`, latencyMs: 100, failures: 0, uri: `vless://${id}`,
    }));
    pm.state.active = ['b1', 'b2', 'b3'];
    pm.reindex();
    pm.noteResult('b1', false, { burn: true });
    check('burn: one limit takes the exit out of the pool at once',
      pm.byId.get('b1').status === 'burned' && !pm.state.active.includes('b1'),
      `${pm.byId.get('b1').status} active=${pm.state.active.join(',')}`);
    check('burn: no markDeadAfter failures are needed for it', pm.byId.get('b1').failures === 1, String(pm.byId.get('b1').failures));
    const picked = pm.rotate('test', 'provider-x');
    check('burn: rotation never lands on a burned exit', !!picked && picked.id !== 'b1', picked && picked.id);
    pm.state.exits.push({ id: 'u1', status: 'untested', uri: 'vless://u1', failures: 0, latencyMs: 0, exitIp: '' });
    pm.reindex();
    await pm.replenish('test');
    check('replenish: the burned exit is deleted and cannot be re-applied',
      !pm.byId.has('b1') && !pm.state.exits.some((e) => e.status === 'burned') && !pm.state.active.includes('b1'),
      pm.state.exits.map((e) => `${e.id}:${e.status}`).join(','));
    // Never leave a listener (or its child) behind: the run is over.
    await pm.stopMain().catch(() => {});
  }

  // 30. transport deadlines: both a silent CONNECT (stalled TLS handshake) and a
  //     head-then-silence body must fail at the deadline. Measured on the live
  //     pool before this fix: a stalled handshake hung for 82s because the
  //     tunnel cleared its socket timeout and nothing re-armed it.
  {
    const net = require('node:net');
    const { proxiedRequest } = require('../proxy-client.js');

    const silentTunnel = net.createServer((sock) => {
      sock.once('data', () => sock.write('HTTP/1.1 200 Connection Established\r\n\r\n'));
      sock.on('error', () => {});
    });
    await new Promise((r) => silentTunnel.listen(0, '127.0.0.1', r));
    let handshakeErr = null;
    const t0 = Date.now();
    try {
      await proxiedRequest('https://example.test/v1/chat/completions', {
        method: 'POST', body: '{}', proxy: { host: '127.0.0.1', port: silentTunnel.address().port }, timeoutMs: 1200,
      });
    } catch (err) { handshakeErr = err; }
    const handshakeMs = Date.now() - t0;
    check('transport: a tunnel that goes silent before the TLS handshake fails at the deadline',
      handshakeMs < 2500 && !!handshakeErr && /no response head/.test(handshakeErr.message) && handshakeErr.stall === true,
      `${handshakeMs}ms "${handshakeErr && handshakeErr.message}"`);

    const stallingBody = net.createServer((sock) => {
      sock.once('data', () => {
        sock.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        sock.once('data', () => sock.write('HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n'));
      });
      sock.on('error', () => {});
    });
    await new Promise((r) => stallingBody.listen(0, '127.0.0.1', r));
    let bodyMsg = '';
    let bodyErr = null;
    const t1 = Date.now();
    try {
      const res = await proxiedRequest('http://example.test/v1/chat/completions', {
        method: 'POST', body: '{}', proxy: { host: '127.0.0.1', port: stallingBody.address().port }, timeoutMs: 1500, idleMs: 900,
      });
      const reader = res.body.getReader();
      for (;;) { const { done } = await reader.read(); if (done) break; }
    } catch (err) { bodyErr = err; bodyMsg = err.message; }
    const bodyMs = Date.now() - t1;
    check('transport: a body that stops after its head fails on the idle deadline',
      bodyMs < 3500 && /stalled/.test(bodyMsg) && bodyErr.stall === true, `${bodyMs}ms "${bodyMsg}"`);

    silentTunnel.close();
    stallingBody.close();
  }

  // 15. cooldown expiry: a past deadline must be treated as healthy by the sweeper path
  const pastDeadline = await fetch(routerUrl('/admin/api/provider/seq-provider/account/a-limited/reset'), { method: 'POST', headers: ADMIN });
  check('unlock path exercised without restart', pastDeadline.status === 200);

  // 27. scheduled upkeep: the due-math and the disable switch, without a network.
  //     (The full cycle is exercised on a live host; here we pin the clock logic.)
  {
    const { ProxyManager } = require('../proxy-manager.js');
    const pm = new ProxyManager({ dataDir: fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-sched-')), log: () => {} });
    pm.state.settings.autoRefresh = true;
    pm.state.settings.refreshIntervalHours = 12;
    pm.state.auto = { lastRun: Date.now() - 11 * 3600_000 };
    check('scheduler: not due before the interval elapses', pm.isAutoRunDue() === false);
    pm.state.auto = { lastRun: Date.now() - 13 * 3600_000 };
    check('scheduler: due once the interval has passed', pm.isAutoRunDue() === true);
    check('scheduler: next run is lastRun + interval',
      Math.abs(pm.nextAutoRunAt() - (pm.state.auto.lastRun + 12 * 3600_000)) < 1000,
      `next=${pm.nextAutoRunAt()}`);
    pm.state.settings.autoRefresh = false;
    check('scheduler: the switch disables it', pm.nextAutoRunAt() === 0 && pm.isAutoRunDue() === false);
    pm.state.settings.autoRefresh = true;
    pm.state.settings.refreshIntervalHours = 0;
    check('scheduler: 0 hours means never due', pm.nextAutoRunAt() === 0 && pm.isAutoRunDue() === false);
    pm.state.settings.refreshIntervalHours = 12;
    // The clock is anchored to real events: the last fetch beats an older
    // lastRun, so a deploy does not reset the interval, and a stale fetch makes
    // the cycle due right away. A bare save (updatedAt) must not move it.
    pm.state.auto = { lastRun: 0 };
    pm.state.exits = [];
    pm.state.updatedAt = Date.now();
    pm.state.sources = [{ id: 's1', url: 'https://example.invalid/sub', enabled: true, lastFetched: Date.now() - 10 * 3600_000, count: 0, error: '' }];
    check('scheduler: anchors to the newest subscription fetch, not the last save', pm.isAutoRunDue() === false
      && Math.abs(pm.nextAutoRunAt() - (Date.now() + 2 * 3600_000)) < 5000, `next=${pm.nextAutoRunAt()}`);
    pm.state.sources[0].lastFetched = Date.now() - 13 * 3600_000;
    check('scheduler: a stale fetch makes it due', pm.isAutoRunDue() === true);
    // A test that ran later than the fetch anchors it as well.
    pm.state.exits = [{ id: 'x', status: 'ok', lastTested: Date.now() - 3600_000 }];
    check('scheduler: the newest exit test wins over an older fetch', pm.isAutoRunDue() === false);
    pm.state.exits = [{ id: 'x', status: 'ok', lastTested: Date.now() - 14 * 3600_000 }];
    check('scheduler: a stale test is due too', pm.isAutoRunDue() === true);
    check('scheduler: a disabled source still anchors the clock', pm.nextAutoRunAt() > 0);
    // The guards run before any fetch, so this stays offline.
    pm.test.running = true;
    const refused = await pm.runAutoRefresh('smoke');
    pm.test.running = false;
    check('scheduler: refuses to run while a test is in flight', refused.skipped === 'a test run is already in progress', JSON.stringify(refused));
    pm.state.sources = [];
    const noSource = await pm.runAutoRefresh('smoke');
    check('scheduler: skips cleanly with no enabled source', noSource.skipped === 'no enabled subscription source', JSON.stringify(noSource));
    check('scheduler: a skipped cycle still stamps lastRun', (pm.state.auto.lastRun || 0) > 0);
  }

  // 31. the free-tier ladder's wall clock is an operator dial: top-level
  //     `freeLadderBudgetSeconds` (3..120, default 25s) has to be read per
  //     request. A regression that snapshots it once at module load - before
  //     loadConfigSync() has run - silently keeps the compiled-in 25s, so every
  //     cold request spends 25s proving a ceiling the operator already dialled
  //     down. This router is given 3s against an upstream that accepts the
  //     request and never answers: only the ladder's own deadline can end it
  //     inside 9s, and the configured fallback must be the one that answers.
  {
    const ladderTmp = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-ladder-'));
    // An upstream that reads the body and never sends a first byte: the ladder
    // has to give up on it by itself (a stall, not an HTTP error).
    const hang = { hits: 0, sockets: new Set() };
    const hangMock = http.createServer((req, res) => {
      hang.hits += 1;
      hang.sockets.add(res.socket);
      req.on('data', () => {});
      req.on('end', () => { /* accepted, deliberately never answered */ });
    });
    const portProbe = net.createServer();
    let ladderChild = null;
    try {
      await new Promise((r) => hangMock.listen(0, '127.0.0.1', r));
      const HANG_PORT = hangMock.address().port;
      await new Promise((r) => portProbe.listen(0, '127.0.0.1', r));
      const LADDER_PORT = portProbe.address().port;
      await new Promise((r) => portProbe.close(() => r()));

      const ladderCfg = {
        adminKey: 'test-admin-key', host: '127.0.0.1', port: LADDER_PORT, cooldownHours: 12,
        cooldownStatuses: [429, 402, 403], retryStatuses: [500, 502, 503, 504],
        defaultProvider: 'ladder-free', cors: true, proxyKeys: [],
        // 3s, not the 25s default: the dial is the only thing that may cut this
        // ladder short. Two lanes, so a second rung exists that a dial-ignoring
        // build would still be entitled to climb (and blow the client guard on).
        freeLadderBudgetSeconds: 3,
        providers: [
          {
            id: 'ladder-free', name: 'Hanging Free Tier', type: 'opencode-free',
            baseUrl: `http://127.0.0.1:${HANG_PORT}/inference/v1`, strategy: 'round-robin',
            timeoutMs: 60000, proxy: 'direct', identityPool: 2,
            accounts: [{ id: 'ladder-1', name: 'hanging-lane', apiKey: '', enabled: true }],
            fallbackProviders: [{ provider: 'ladder-fallback', model: 'fallback-model' }],
          },
          {
            id: 'ladder-fallback', name: 'Ladder Fallback', type: 'custom',
            baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, strategy: 'sequential', timeoutMs: 10000,
            accounts: [{ id: 'ladder-fb-1', name: 'fallback-ok', apiKey: 'key-ok', enabled: true }],
          },
        ],
      };
      const ladderConfig = path.join(ladderTmp, 'config.json');
      fs.writeFileSync(ladderConfig, JSON.stringify(ladderCfg, null, 2));
      ladderChild = spawn(process.execPath, [path.join(REPO, 'server.js')], {
        cwd: REPO,
        env: { ...process.env, ROUTER_CONFIG: ladderConfig, ROUTER_STATE: path.join(ladderTmp, 'state.json'), PORT: String(LADDER_PORT), HOST: '127.0.0.1', FB_API_HOST: `http://127.0.0.1:${MOCK_PORT}` },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let ladderLog = '';
      ladderChild.stdout.on('data', (d) => { ladderLog += d.toString(); });
      ladderChild.stderr.on('data', (d) => { ladderLog += d.toString(); });
      const ladderUrl = (p) => `http://127.0.0.1:${LADDER_PORT}${p}`;

      let ladderUp = false;
      for (let i = 0; i < 60; i++) {
        try { const r = await fetch(ladderUrl('/health')); if (r.ok) { ladderUp = true; break; } } catch {}
        await sleep(200);
      }
      check('free ladder budget: a second router boots with freeLadderBudgetSeconds=3', ladderUp, ladderLog.slice(-400));

      if (ladderUp) {
        // Its own guard: a snapshot regression makes this request wait out the
        // 25s default, so fail in 12s instead of hanging the suite on it.
        const guard = new AbortController();
        const guardTimer = setTimeout(() => guard.abort(), 12000);
        const t0 = Date.now();
        let status = 0;
        let providerHeader = '';
        let text = '';
        try {
          const r = await fetch(ladderUrl('/v1/chat/completions'), {
            method: 'POST', signal: guard.signal,
            headers: { 'content-type': 'application/json', 'x-router-provider': 'ladder-free' },
            body: JSON.stringify({ model: 'big-pickle', stream: false, messages: [{ role: 'user', content: 'ping' }] }),
          });
          status = r.status;
          providerHeader = r.headers.get('x-router-provider') || '';
          text = await r.text();
        } catch (err) {
          text = `no answer: ${err.message}`;
        } finally {
          clearTimeout(guardTimer);
        }
        const elapsed = Date.now() - t0;
        let content = '';
        try { content = JSON.parse(text).choices[0].message.content; } catch { /* reported below */ }
        if (status !== 200 || elapsed >= 12000) console.log(`DIAG ladder: status=${status} elapsed=${elapsed}ms hits=${hang.hits} log=${ladderLog.slice(-600)}`);
        // 3s of ladder + the fallback hop, far below the 25s default. The lower
        // bound proves the request really spent the budget instead of failing fast.
        check('free ladder budget: the dial is honoured per request - the 3s ladder answers well inside the 25s default',
          elapsed < 9000 && elapsed >= 2500, `elapsed=${elapsed}ms status=${status} body=${text.slice(0, 160)}`);
        check('free ladder budget: the shortened ladder hands the request to the configured fallback provider',
          status === 200 && providerHeader === 'ladder-fallback' && content === 'pong',
          `status=${status} provider=${providerHeader} elapsed=${elapsed}ms body=${text.slice(0, 160)}`);
        check('free ladder budget: the hanging upstream was really attempted (the bound is a ladder deadline, not a fast local error)',
          hang.hits >= 1, `hits=${hang.hits} elapsed=${elapsed}ms`);
      }
    } finally {
      if (ladderChild) ladderChild.kill('SIGTERM');
      for (const sock of hang.sockets) { try { sock.destroy(); } catch { /* already gone */ } }
      await new Promise((r) => { hangMock.close(() => r()); setTimeout(r, 1000); });
      fs.rmSync(ladderTmp, { recursive: true, force: true });
    }
  }

  // 32. exclusive mode: one provider, and nothing else in the router. A client
  //     that pins `opencode-free/<model>` must never be answered by another
  //     vendor - not through a leftover `fallbackProviders` entry, not through a
  //     model prefix that names a provider which is not configured, and not
  //     through the "first healthy provider" fallthrough.
  {
    const exTmp = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-exclusive-'));
    let exChild = null;
    const portProbe = net.createServer();
    try {
      await new Promise((r) => portProbe.listen(0, '127.0.0.1', r));
      const EX_PORT = portProbe.address().port;
      await new Promise((r) => portProbe.close(() => r()));
      const exCfg = {
        adminKey: 'test-admin-key', host: '127.0.0.1', port: EX_PORT, cooldownHours: 12,
        cooldownStatuses: [429, 402, 403], retryStatuses: [500, 502, 503, 504],
        defaultProvider: '', cors: true, proxyKeys: [],
        // 3s of ladder: enough to prove the tier is white-listed again, short
        // enough that a regression cannot hold the suite for the 25s default.
        freeLadderBudgetSeconds: 3,
        exclusiveProvider: 'opencode-free',
        providers: [
          {
            id: 'opencode-free', name: 'Locked Free Tier', type: 'opencode-free',
            baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference/v1`, strategy: 'round-robin',
            timeoutMs: 10000, proxy: 'direct', identityPool: 1, freeFallback: false,
            accounts: [{ id: 'ex-1', name: 'keyless', apiKey: '', enabled: true }],
            // Deliberate leftover: it must be dropped at load, not used.
            fallbackProviders: [{ provider: 'pollinations', model: 'openai-fast' }],
          },
          {
            id: 'pollinations', name: 'Other Vendor', type: 'custom',
            baseUrl: `http://127.0.0.1:${MOCK_PORT}/v1`, strategy: 'sequential', timeoutMs: 10000,
            accounts: [{ id: 'pl-1', name: 'other-vendor', apiKey: 'key-ok', enabled: true }],
          },
        ],
      };
      const exConfig = path.join(exTmp, 'config.json');
      fs.writeFileSync(exConfig, JSON.stringify(exCfg, null, 2));
      exChild = spawn(process.execPath, [path.join(REPO, 'server.js')], {
        cwd: REPO,
        env: { ...process.env, ROUTER_CONFIG: exConfig, ROUTER_STATE: path.join(exTmp, 'state.json'), PORT: String(EX_PORT), HOST: '127.0.0.1', FB_API_HOST: `http://127.0.0.1:${MOCK_PORT}` },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let exLog = '';
      exChild.stdout.on('data', (d) => { exLog += d.toString(); });
      exChild.stderr.on('data', (d) => { exLog += d.toString(); });
      const exUrl = (p) => `http://127.0.0.1:${EX_PORT}${p}`;
      const exChat = async (body, headers = {}) => {
        const r = await fetch(exUrl('/v1/chat/completions'), {
          method: 'POST',
          headers: { 'content-type': 'application/json', ...headers },
          body: JSON.stringify(body),
        });
        return { status: r.status, headers: r.headers, text: await r.text() };
      };

      let exUp = false;
      for (let i = 0; i < 60; i++) {
        try { const r = await fetch(exUrl('/health')); if (r.ok) { exUp = true; break; } } catch {}
        await sleep(200);
      }
      check('exclusive: a router locked to one provider boots', exUp, exLog.slice(-400));

      if (exUp) {
        const snap = await (await fetch(exUrl('/admin/api/state'), { headers: ADMIN })).json();
        check('exclusive: the lock is exposed and every other provider is dropped at load',
          snap.exclusiveProvider === 'opencode-free'
          && snap.providers.length === 1
          && snap.providers[0].id === 'opencode-free',
          `locked=${snap.exclusiveProvider} providers=${snap.providers.map((p) => p.id).join(',')}`);

        const ok = await exChat({ model: 'opencode-free/big-pickle', stream: false, messages: [{ role: 'user', content: 'ping' }] });
        let okBody = {};
        try { okBody = JSON.parse(ok.text); } catch { /* reported below */ }
        check('exclusive: the pinned model is served by the locked provider',
          ok.status === 200 && ok.headers.get('x-router-provider') === 'opencode-free'
          && okBody.choices?.[0]?.message?.content === 'OC0 OC1 OC2 OC3 ',
          `${ok.status} provider=${ok.headers.get('x-router-provider')} body=${ok.text.slice(0, 140)}`);

        // The exact live failure: a model naming a provider that is not in the
        // router, and a header naming one. Both used to be answered by whoever
        // was healthy; both must be refused now.
        const byModel = await exChat({ model: 'pollinations/openai-fast', stream: false, messages: [{ role: 'user', content: 'ping' }] });
        let byModelBody = {};
        try { byModelBody = JSON.parse(byModel.text); } catch { /* reported below */ }
        check('exclusive: a model prefix naming an unconfigured provider is refused, not routed',
          byModel.status === 404 && byModelBody.error?.type === 'router_unknown_provider',
          `${byModel.status} ${byModel.text.slice(0, 160)}`);

        const byHeader = await exChat({ model: 'big-pickle', stream: false, messages: [{ role: 'user', content: 'ping' }] }, { 'x-router-provider': 'pollinations' });
        check('exclusive: a header naming another provider is refused too',
          byHeader.status === 404, `${byHeader.status} ${byHeader.text.slice(0, 160)}`);

        const add = await fetch(exUrl('/admin/api/providers'), {
          method: 'POST', headers: ADMIN,
          body: JSON.stringify({ id: 'pollinations', name: 'Other Vendor', type: 'custom', baseUrl: 'https://example.invalid/v1' }),
        });
        const addBody = await add.json();
        check('exclusive: the admin API refuses to put a second provider back',
          add.status === 409 && addBody.error?.type === 'provider_locked', `${add.status} ${JSON.stringify(addBody).slice(0, 160)}`);

        // The free tier is dry: with `freeFallback: false` there is no model walk
        // and no cross-provider hop, so the client gets this provider's own 429 -
        // and the tier is tried again on the next request instead of being skipped
        // because a previous ladder marked the ceiling dry.
        const before = fb.modelQuota429;
        fb.modelQuotaExhausted = ['big-pickle'];
        fb.quotaRetryAfter = 2; // the live edge always quotes one; short = a lane limit
        const dry1 = await exChat({ model: 'opencode-free/big-pickle', stream: false, messages: [{ role: 'user', content: 'ping' }] });
        const dry2 = await exChat({ model: 'opencode-free/big-pickle', stream: false, messages: [{ role: 'user', content: 'ping' }] });
        fb.modelQuotaExhausted = [];
        fb.quotaRetryAfter = 0;
        check('exclusive: a dry free model fails with this provider\'s own 429, never a substitution',
          dry1.status === 429 && dry1.headers.get('x-router-provider') !== 'pollinations'
          && !dry1.text.includes('openai-fast') && !dry2.text.includes('openai-fast'),
          `status=${dry1.status} provider=${dry1.headers.get('x-router-provider')} body=${dry1.text.slice(0, 200)}`);
        check('exclusive: a known-dry ceiling is tried again instead of being skipped (no fallback to hand over to)',
          fb.modelQuota429 - before >= 2, `tier attempts=${fb.modelQuota429 - before}`);
        const dryRa = Number(dry1.headers.get('retry-after'));
        check('exclusive: a 429 carries Retry-After (never absent, never 0), so a client sleeps instead of hot-looping',
          Number.isFinite(dryRa) && dryRa >= 2, `retry-after=${dry1.headers.get('retry-after')}`);
      }
    } finally {
      if (exChild) exChild.kill('SIGTERM');
      fs.rmSync(exTmp, { recursive: true, force: true });
    }
  }

  // 28. The genuine-CLI transport (`opencode-cli`): the router runs the real
  //     binary because the free tier's gate is on the client, not the request.
  //     Offline here - a fixture stands in for the binary.
  {
    const cliTmp = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-cli-'));
    let cliChild = null;
    const probe = net.createServer();
    try {
      await new Promise((r) => probe.listen(0, '127.0.0.1', r));
      const CLI_PORT = probe.address().port;
      await new Promise((r) => probe.close(() => r()));
      const fake = path.join(REPO, 'test', 'fixtures', 'fake-opencode-cli.js');
      const cliCfg = {
        adminKey: 'test-admin-key', host: '127.0.0.1', port: CLI_PORT, cooldownHours: 12,
        cooldownStatuses: [429, 402, 403], retryStatuses: [500, 502, 503, 504],
        defaultProvider: '', cors: true, proxyKeys: [],
        exclusiveProvider: 'opencode-cli',
        providers: [{
          id: 'opencode-cli', name: 'CLI transport', type: 'opencode-cli',
          strategy: 'round-robin', timeoutMs: 30_000, proxy: 'direct',
          // The fixture is a node script: run it with the same node that runs the suite.
          cliPath: process.execPath, cliArgs: [fake],
          accounts: [],
        }],
      };
      const cliConfig = path.join(cliTmp, 'config.json');
      fs.writeFileSync(cliConfig, JSON.stringify(cliCfg, null, 2));
      cliChild = spawn(process.execPath, [path.join(REPO, 'server.js')], {
        cwd: REPO,
        env: { ...process.env, ROUTER_CONFIG: cliConfig, ROUTER_STATE: path.join(cliTmp, 'state.json'), PORT: String(CLI_PORT), HOST: '127.0.0.1' },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let cliLog = '';
      cliChild.stdout.on('data', (d) => { cliLog += d.toString(); });
      cliChild.stderr.on('data', (d) => { cliLog += d.toString(); });
      const cliUrl = (p) => `http://127.0.0.1:${CLI_PORT}${p}`;
      const cliChat = async (body, headers = {}) => {
        const r = await fetch(cliUrl('/v1/chat/completions'), {
          method: 'POST',
          headers: { 'content-type': 'application/json', ...headers },
          body: JSON.stringify(body),
        });
        return { status: r.status, headers: r.headers, text: await r.text() };
      };

      let up = false;
      for (let i = 0; i < 40 && !up; i += 1) {
        try { up = (await fetch(cliUrl('/health'))).ok; } catch { await sleep(150); }
      }
      check('cli transport: the router boots with an account-less provider', up === true, cliLog.slice(-300) || cliChild.exitCode);

      const ok = await cliChat({ model: 'opencode-cli/big-pickle', stream: false, messages: [{ role: 'system', content: 'be brief' }, { role: 'user', content: 'hello there' }] });
      let okBody = {};
      try { okBody = JSON.parse(ok.text); } catch { /* reported below */ }
      check('cli transport: a chat request is answered from the binary\'s stdout',
        ok.status === 200
        && String(okBody.choices?.[0]?.message?.content || '').includes('FAKE OK')
        && ok.headers.get('x-router-transport') === 'opencode-cli',
        `${ok.status} transport=${ok.headers.get('x-router-transport')} body=${ok.text.slice(0, 160)}`);
      check('cli transport: the answer is a chat.completion with the requested model',
        okBody.object === 'chat.completion' && okBody.model === 'big-pickle',
        JSON.stringify({ object: okBody.object, model: okBody.model }));

      const streamed = await cliChat({ model: 'opencode-cli/big-pickle', stream: true, messages: [{ role: 'user', content: 'hi' }] });
      check('cli transport: streaming answers with SSE chunks and [DONE]',
        streamed.status === 200 && streamed.text.includes('chat.completion.chunk') && streamed.text.trim().endsWith('data: [DONE]'),
        `${streamed.status} ${streamed.text.slice(-80)}`);

      const rotated = await cliChat({ model: 'opencode-cli/big-pickle', stream: false, messages: [{ role: 'user', content: '__ROTATE__ keep going' }] });
      check('cli transport: a limit on one identity is answered by minting another (new HOME) and retrying',
        rotated.status === 200 && rotated.text.includes('ROTATED OK')
        && String(rotated.headers.get('x-router-identity') || '').length > 0,
        `${rotated.status} identity=${rotated.headers.get('x-router-identity')} body=${rotated.text.slice(0, 140)}`);

      const limited = await cliChat({ model: 'opencode-cli/big-pickle', stream: false, messages: [{ role: 'user', content: '__LIMIT__ please' }] });
      const limitBody = JSON.parse(limited.text);
      const limitRa = Number(limited.headers.get('retry-after'));
      check('cli transport: the CLI\'s own limit becomes a 429 with a short refill window, not the day',
        limited.status === 429 && limitBody.error?.type === 'router_free_window'
        && Number.isFinite(limitRa) && limitRa > 60 && limitRa <= 24 * 3600,
        `${limited.status} ra=${limited.headers.get('retry-after')} body=${limited.text.slice(0, 160)}`);

      const afterLimit = await cliChat({ model: 'opencode-cli/big-pickle', stream: false, messages: [{ role: 'user', content: 'hello again' }] });
      check('cli transport: a spent allowance parks the tier instead of spawning the binary again',
        afterLimit.status === 429 && Number(afterLimit.headers.get('retry-after')) > 60,
        `${afterLimit.status} ra=${afterLimit.headers.get('retry-after')}`);

      const snap = await (await fetch(cliUrl('/admin/api/state'), { headers: ADMIN })).json();
      check('cli transport: the provider is advertised and needs no identity pool',
        snap.providers.some((p) => p.id === 'opencode-cli' && p.accountCount === 0)
        && snap.exclusiveProvider === 'opencode-cli',
        JSON.stringify(snap.providers.map((p) => `${p.id}:${p.accountCount}`)));
    } finally {
      if (cliChild) cliChild.kill('SIGTERM');
      fs.rmSync(cliTmp, { recursive: true, force: true });
    }
  }

  // 29. The `opencode serve` transport: the same genuine client, reached through a
  //     long-lived server instead of a fresh spawn per request. Unit-tested against
  //     a fake server here (fast, offline); the router path is the same handler the
  //     spawn tests above exercise.
  {
    const { OpenCodeCliDriver } = require('../opencode-cli-driver.js');
    const fake = http.createServer((req, res) => {
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        if (req.method === 'POST' && req.url === '/api/session') {
          res.writeHead(200, { 'content-type': 'application/json' });
          return res.end(JSON.stringify({ data: { id: 'ses_fake_serve' } }));
        }
        if (req.method === 'POST' && req.url === '/session/ses_fake_serve/message') {
          const parsed = JSON.parse(body || '{}');
          const text = (parsed.parts || []).map((p) => p.text || '').join(' ');
          res.writeHead(200, { 'content-type': 'application/json' });
          if (/__LIMIT__/.test(text)) {
            return res.end(JSON.stringify({ info: { error: { message: 'Free usage limit reached' } }, parts: [] }));
          }
          return res.end(JSON.stringify({ info: { modelID: 'big-pickle' }, parts: [{ type: 'text', text: `SERVE OK: ${text.slice(0, 40)}` }] }));
        }
        // The trap: the prefixed spelling answers the web UI with a 200.
        res.writeHead(200, { 'content-type': 'text/html' });
        res.end('<!doctype html><html></html>');
      });
    });
    await new Promise((r) => fake.listen(0, '127.0.0.1', r));
    const serveUrl = `http://127.0.0.1:${fake.address().port}`;
    try {
      const driver = new OpenCodeCliDriver({ mode: 'serve', serveUrl, log: () => {} });
      check('serve transport: available() follows the URL, not a binary', driver.available() === true);

      const ok = await driver.run({ model: 'big-pickle', prompt: 'hello serve' });
      check('serve transport: an answer comes from the session message route',
        ok.ok === true && ok.text.includes('SERVE OK') && ok.code === 200,
        JSON.stringify({ ok: ok.ok, text: ok.text, code: ok.code, error: ok.error }));

      const limited = await driver.run({ model: 'big-pickle', prompt: '__LIMIT__ now' });
      check('serve transport: the limit is detected from the body, not from a status code',
        limited.limit === true && limited.ok === false && limited.code === 200,
        JSON.stringify({ limit: limited.limit, ok: limited.ok, code: limited.code }));

      const dead = new OpenCodeCliDriver({ mode: 'serve', serveUrl: 'http://127.0.0.1:1', log: () => {} });
      const unreachable = await dead.run({ model: 'big-pickle', prompt: 'hi' });
      check('serve transport: an unreachable server is reported, never a silent hang',
        unreachable.ok === false && /unreachable/.test(unreachable.error), unreachable.error);
    } finally {
      fake.close();
    }
  }

  console.log('\n==================== RESULTS ====================');
  console.log(results.join('\n'));
  console.log(`\n  ${pass} passed, ${fail} failed`);
  console.log('=================================================');
  console.log('\n--- router log tail ---');
  console.log(routerLog.split('\n').slice(-12).join('\n'));
  if (process.env.SMOKE_LOG_FILE) fs.writeFileSync(process.env.SMOKE_LOG_FILE, routerLog);

  child.kill('SIGTERM');
  await sleep(300);
  mock.close();
  process.exit(fail ? 1 : 0);
})().catch((err) => { console.error('HARNESS ERROR', err); console.error('--- router log ---\n' + (globalThis.__routerLog || '(unavailable)')); process.exit(2); });
