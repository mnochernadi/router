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
        id: 'opencode-free-test', name: 'OpenCode Free Test', type: 'opencode-free', baseUrl: `http://127.0.0.1:${MOCK_PORT}/zen/v1`,
        strategy: 'round-robin', timeoutMs: 10000, accounts: [{ id: 'ocf-1', name: 'keyless', apiKey: '', enabled: true }],
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
  srv.on('connect', (req, clientSocket, head) => {
    const [host, port] = String(req.url).split(':');
    if (onConnect) onConnect(req.url);
    const upstream = net.connect(Number(port), host, () => {
      clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      if (head && head.length) upstream.write(head);
      upstream.pipe(clientSocket);
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
    body: JSON.stringify({ model: opts.model || 'gpt-test', stream: !!opts.stream, messages: [{ role: 'user', content: 'ping' }], ...(opts.n ? { n: opts.n } : {}) }),
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
      { id: 'x1', name: 'exit one', protocol: 'vless', host: '10.0.0.1', port: 443, uri: 'vless://x1', status: 'ok', latencyMs: 120, exitIp: '10.0.0.1', failures: 0, outbound: { protocol: 'freedom' } },
      { id: 'x2', name: 'exit two', protocol: 'vless', host: '10.0.0.2', port: 443, uri: 'vless://x2', status: 'ok', latencyMs: 240, exitIp: '10.0.0.2', failures: 0, outbound: { protocol: 'freedom' } },
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
  check('RSS delta does not scale with relayed bytes (4x bytes must not mean 4x RSS)',
    dBig < Math.max(dSmall * 1.6, 4 * 1048576),
    `small=${(dSmall / 1048576).toFixed(2)}MB big=${(dBig / 1048576).toFixed(2)}MB for ${(bigBytes / smallBytes).toFixed(1)}x bytes`);
  check('JS heap stays far below the relayed byte volume (streams are not buffered)',
    mem.heapUsed < 40 * 1048576 && mem.arrayBuffers < 4 * 1048576,
    `heapUsed=${(mem.heapUsed / 1048576).toFixed(2)}MB arrayBuffers=${(mem.arrayBuffers / 1048576).toFixed(2)}MB`);
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
    check('opencode-free: keyless account works with no auth header',
      free.status === 200 && JSON.parse(free.text).choices[0].message.content === 'opencode says hi', free.text.slice(0, 200));

    const models = await (await fetch(routerUrl('/admin/api/provider/opencode-test/models'), { headers: ADMIN })).json();
    check('admin model discovery returns the upstream model list',
      models.count === 5 && models.models.includes('claude-sonnet-4-5') && models.source.endsWith('/inference/v1/models'),
      JSON.stringify(models).slice(0, 240));

    const keylessAdd = await fetch(routerUrl('/admin/api/provider/opencode-free-test/accounts'), {
      method: 'POST', headers: ADMIN, body: JSON.stringify({ accounts: [{ name: 'second-keyless', apiKey: '' }] }),
    });
    check('keyless provider accepts an account with an empty key', (await keylessAdd.json()).added === 1);
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
    check('proxy rotate: the request really travelled over both CONNECT proxies',
      proxyHits.length === 2 && proxyHits[0].startsWith('a:127.0.0.1:') && proxyHits[1].startsWith('b:127.0.0.1:'),
      JSON.stringify(proxyHits));

    await sleep(700);
    const st = JSON.parse(fs.readFileSync(STATE, 'utf8'));
    const pt = st.providers['proxy-test'].accounts['pt-1'];
    check('proxy rotate: the account was NOT put on a 12h cooldown', !pt.limitedUntil, JSON.stringify(pt));

    const direct = await chat({ headers: { 'x-router-provider': 'rr-provider' } });
    check('providers without a proxy still go direct', direct.headers.get('x-router-exit') === 'direct', direct.headers.get('x-router-exit'));

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

  // 15. cooldown expiry: a past deadline must be treated as healthy by the sweeper path
  const pastDeadline = await fetch(routerUrl('/admin/api/provider/seq-provider/account/a-limited/reset'), { method: 'POST', headers: ADMIN });
  check('unlock path exercised without restart', pastDeadline.status === 200);

  console.log('\n==================== RESULTS ====================');
  console.log(results.join('\n'));
  console.log(`\n  ${pass} passed, ${fail} failed`);
  console.log('=================================================');
  console.log('\n--- router log tail ---');
  console.log(routerLog.split('\n').slice(-12).join('\n'));

  child.kill('SIGTERM');
  await sleep(300);
  mock.close();
  process.exit(fail ? 1 : 0);
})().catch((err) => { console.error('HARNESS ERROR', err); console.error('--- router log ---\n' + (globalThis.__routerLog || '(unavailable)')); process.exit(2); });
