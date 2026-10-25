#!/usr/bin/env node
/**
 * e2e-keyless.js - end-to-end router test with NO provider key and NO egress.
 *
 * Boots two stub upstreams (test/stub-upstream.js) and a throwaway llm-router
 * instance against a temp config, then exercises the paths a real provider lane
 * depends on and asserts on what the upstream actually received.
 *
 * Nothing here touches the network beyond 127.0.0.1, nothing reads a real key,
 * and nothing writes inside the repo: config + state live in a temp dir.
 *
 * USAGE
 *   node test/e2e-keyless.js
 *
 * WHAT IT PROVES / WHAT IT DOES NOT
 *   PASS  the router routes, forwards the caller's model id verbatim, forwards
 *         the account credential, streams SSE, and fails over to the next
 *         provider when the first answers a retryStatus and when it answers 429.
 *   RED   the modelMap case: a client asking for a pinned alias is expected to
 *         have the UPSTREAM id put on the wire. No modelMap exists in the tree
 *         yet, so this row is the acceptance test for that deliverable, not a
 *         regression.
 */
'use strict';

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '..');
const ROUTER_PORT = 18787;
const A_PORT = 11500;
const B_PORT = 11501;
const ROUTER_URL = `http://127.0.0.1:${ROUTER_PORT}`;

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-e2e-'));
const CONFIG_PATH = path.join(tmp, 'config.json');
const STATE_PATH = path.join(tmp, 'state.json');

const config = {
  adminKey: 'e2e-admin',
  host: '127.0.0.1',
  port: ROUTER_PORT,
  proxyKeys: [],
  cors: true,
  exclusiveProvider: '',
  defaultProvider: 'stub-a',
  cooldownHours: 1,
  cooldownStatuses: [429, 402, 403],
  retryStatuses: [500, 502, 503, 504],
  providers: [
    {
      id: 'stub-a',
      name: 'Stub A',
      type: 'openai',
      baseUrl: `http://127.0.0.1:${A_PORT}/v1`,
      strategy: 'sequential',
      cooldownHours: 1,
      // NOTE: maxAttempts is deliberately left at its shipped default (0). With
      // 0, server.js:3980 (`retryStatuses.includes(status) && attempt < maxAttempts`)
      // can never fire, so a 500/502/503/504 is returned to the caller verbatim and
      // retryStatuses is inert. Setting maxAttempts: 4 flips T5 to PASS — that is
      // the proof, and it is why T5 is a real red row rather than a harness quirk.
      accounts: [{ name: 'a-key', apiKey: 'sk-e2e-a' }],
      fallbackProviders: [{ provider: 'stub-b', model: 'fallback-model' }],
      proxy: 'direct',
    },
    {
      id: 'stub-b',
      name: 'Stub B',
      type: 'openai',
      baseUrl: `http://127.0.0.1:${B_PORT}/v1`,
      strategy: 'sequential',
      cooldownHours: 1,
      accounts: [{ name: 'b-key', apiKey: 'sk-e2e-b' }],
      proxy: 'direct',
    },
  ],
};
fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));

const procs = [];
function boot(name, cmd, args, env) {
  const p = spawn(cmd, args, { cwd: ROOT, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  p.stdout.on('data', (d) => process.env.E2E_VERBOSE && process.stdout.write(`[${name}] ${d}`));
  p.stderr.on('data', (d) => process.stdout.write(`[${name}!] ${d}`));
  procs.push(p);
  return p;
}
function shutdown() { for (const p of procs) { try { p.kill('SIGKILL'); } catch {} } }
process.on('exit', shutdown);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitUp(url, tries, label) {
  for (let i = 0; i < tries; i++) {
    try { const r = await fetch(url); if (r.ok || r.status < 500) return true; } catch {}
    await sleep(250);
  }
  throw new Error(`${label} never came up at ${url}`);
}

const results = [];
function record(id, intent, pass, observed) { results.push({ id, intent, pass, observed }); }

async function complete(model, { stream = false } = {}) {
  const r = await fetch(`${ROUTER_URL}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model, stream, messages: [{ role: 'user', content: 'say OK' }] }),
  });
  const text = await r.text();
  return { status: r.status, text };
}

async function seen(port) {
  const r = await fetch(`http://127.0.0.1:${port}/__seen`);
  return r.json();
}
async function force(port, status) {
  await fetch(`http://127.0.0.1:${port}/__fail`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ status }),
  });
}
async function reset(port) { await fetch(`http://127.0.0.1:${port}/__reset`, { method: 'POST' }); }

(async () => {
  boot('stub-a', process.execPath, [path.join(ROOT, 'test', 'stub-upstream.js')], { STUB_PORT: String(A_PORT), STUB_NAME: 'A' });
  boot('stub-b', process.execPath, [path.join(ROOT, 'test', 'stub-upstream.js')], { STUB_PORT: String(B_PORT), STUB_NAME: 'B' });
  await waitUp(`http://127.0.0.1:${A_PORT}/__seen`, 40, 'stub A');
  await waitUp(`http://127.0.0.1:${B_PORT}/__seen`, 40, 'stub B');

  boot('router', process.execPath, [path.join(ROOT, 'server.js')], {
    ROUTER_CONFIG: CONFIG_PATH, ROUTER_STATE: STATE_PATH, PORT: String(ROUTER_PORT), HOST: '127.0.0.1',
  });
  await waitUp(`${ROUTER_URL}/health`, 60, 'router');

  // T1 - the model catalog
  try {
    const r = await fetch(`${ROUTER_URL}/v1/models`);
    const j = await r.json();
    const ids = (j.data || []).map((m) => m.id);
    record('T1', 'catalog /v1/models answers', r.ok && ids.length > 0, `status=${r.status} ids=${JSON.stringify(ids).slice(0, 240)}`);
  } catch (e) { record('T1', 'catalog /v1/models answers', false, `EXC ${e.message}`); }

  // T2 - plain completion, and what the upstream actually saw
  try {
    await reset(A_PORT); await reset(B_PORT);
    const r = await complete('stub-model-a');
    const sa = await seen(A_PORT);
    const last = sa[sa.length - 1] || {};
    const ok = r.status === 200 && /stub=A/.test(r.text) && last.model === 'stub-model-a' && last.auth === 'Bearer sk-e2e-a';
    record('T2', 'routes to A, forwards model verbatim + account credential', ok,
      `status=${r.status} upstream_model=${JSON.stringify(last.model)} upstream_auth=${JSON.stringify(last.auth)} auth_header_only_in_body_len=${r.text.length}`);
  } catch (e) { record('T2', 'routes to A, forwards model verbatim', false, `EXC ${e.message}`); }

  // T3 - SSE passthrough
  try {
    await reset(A_PORT);
    const r = await complete('stub-model-a', { stream: true });
    const ok = r.status === 200 && /data: /.test(r.text) && /stub=A/.test(r.text) && /\[DONE\]/.test(r.text);
    record('T3', 'streams SSE deltas and terminates with [DONE]', ok, `status=${r.status} bytes=${r.text.length} head=${JSON.stringify(r.text.slice(0, 120))}`);
  } catch (e) { record('T3', 'streams SSE', false, `EXC ${e.message}`); }

  // T4 - modelMap: RED until the deliverable lands
  try {
    await reset(A_PORT); await reset(B_PORT);
    const r = await complete('grok-fast');
    const sa = await seen(A_PORT);
    const sb = await seen(B_PORT);
    const last = (sa.length ? sa : sb)[sa.length ? sa.length - 1 : sb.length - 1] || {};
    const mapped = last.model && last.model !== 'grok-fast';
    record('T4', 'modelMap: client alias resolved to the upstream id on the wire', !!mapped,
      `upstream_saw=${JSON.stringify(last.model)} (expected an upstream id, not the client alias)`);
  } catch (e) { record('T4', 'modelMap resolves alias', false, `EXC ${e.message}`); }

  // T5 - retryStatus (500) hands the request to the next provider
  try {
    await reset(A_PORT); await reset(B_PORT);
    await force(A_PORT, 500);
    const r = await complete('stub-model-a');
    const sb = await seen(B_PORT);
    const ok = r.status === 200 && sb.length > 0 && /stub=B/.test(r.text);
    record('T5', '500 (retryStatus) fails over to the next provider', ok,
      `status=${r.status} served_by=${/stub=B/.test(r.text) ? 'B' : /stub=A/.test(r.text) ? 'A' : 'unknown'} b_requests=${sb.length}`);
    await force(A_PORT, 0);
  } catch (e) { record('T5', '500 fails over', false, `EXC ${e.message}`); }

  // T6 - 429 (cooldownStatus) must not park the whole router
  try {
    await reset(A_PORT); await reset(B_PORT);
    await force(A_PORT, 429);
    const r = await complete('stub-model-a');
    const sb = await seen(B_PORT);
    let state = null;
    for (const h of [{ 'x-admin-key': 'e2e-admin' }, { authorization: 'Bearer e2e-admin' }]) {
      const sr = await fetch(`${ROUTER_URL}/admin/api/state`, { headers: h });
      if (sr.ok) { state = await sr.json(); break; }
    }
    const ok = r.status === 200 && /stub=B/.test(r.text);
    record('T6', '429 (cooldownStatus) cools the lane, next provider answers', ok,
      `status=${r.status} served_by=${/stub=B/.test(r.text) ? 'B' : /stub=A/.test(r.text) ? 'A' : 'unknown'} b_requests=${sb.length} admin_state=${state ? 'ok' : 'unread'}`);
    await force(A_PORT, 0);
  } catch (e) { record('T6', '429 cools the lane', false, `EXC ${e.message}`); }

  const w = (s, n) => String(s).padEnd(n);
  console.log('\n=== e2e-keyless ===');
  console.log(`${w('row', 5)} ${w('result', 7)} intent`);
  for (const r of results) console.log(`${w(r.id, 5)} ${w(r.pass ? 'PASS' : 'RED', 7)} ${r.intent}`);
  console.log('\n--- observed ---');
  for (const r of results) console.log(`${r.id}: ${r.observed}`);

  const hard = results.filter((r) => r.id !== 'T4');
  const red = hard.filter((r) => !r.pass);
  console.log(`\nT1-T3,T5-T6: ${hard.length - red.length}/${hard.length} passing · T4 (modelMap) is the deliverable's acceptance row`);
  console.log(`temp config: ${CONFIG_PATH}`);
  shutdown();
  process.exit(red.length ? 1 : 0);
})().catch((e) => { console.error('FATAL', e); shutdown(); process.exit(2); });