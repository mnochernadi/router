'use strict';
/**
 * identity-burst: end-to-end test for the free tier's *identity burst* path.
 *
 *   node test/identity-burst.js        (exit 0 = all checks passed)
 *
 * Standalone and self-contained: it starts a mock upstream, boots the real
 * server.js as a child process against a temp config (config.json/state.json in
 * the repo are untouched), drives it over HTTP and prints TAP-ish lines
 * (`ok - <name>` / `not ok - <name>`). Nothing is imported from test/smoke.js and
 * no npm package is used.
 *
 * The contract under test (frozen):
 *   1. `identityBurst` is a boolean config key, default true, kill switch false.
 *      GET /admin/api/state (x-admin-key) exposes `identityBurst` at the top
 *      level and `demand` (requests currently inside the provider's free-tier
 *      ladder) on every provider summary.
 *   2. While MORE THAN ONE request is in that provider's ladder:
 *        - a lane that hits a quota 429 is rebuilt IN PLACE and stays in service
 *          (no `limitedUntil` park, no `refreshing`), event `lane-rebuilt`;
 *        - the all-lane sweep stands down (no new `reinstall-all` event);
 *        - a request that finds every lane parked/busy is handed a freshly minted
 *          identity on the lane that has waited longest (event `identity-lent`)
 *          instead of waiting LANE_WAIT_MS (12000ms) and failing.
 *   3. Sequential behaviour is unchanged: a lone request whose ladder ends spent
 *      still gets the all-lane sweep (`reinstall-all`).
 *
 * The mock upstream accounts the allowance *per identity*: each distinct
 * `x-opencode-session` serves `freeRequestsPerSession` requests and then answers
 * the free-tier quota 429 for good. A freshly minted identity therefore gets
 * through while a spent one keeps failing - deterministically, whatever order the
 * concurrent requests reach the mock in.
 */

const http = require('node:http');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');

const REPO = path.join(__dirname, '..');
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-router-burst-'));
const ADMIN_KEY = 'burst-admin-key';
const ADMIN = { 'x-admin-key': ADMIN_KEY, 'content-type': 'application/json' };
const PID = 'burst-test';
const MODEL = 'big-pickle';
// server.js: how long a request waits for a replacement lane when every lane is
// spent. The burst path must beat this by a wide margin.
const LANE_WAIT_MS = 12_000;
const BURST_LATENCY_BUDGET_MS = 6_000;
const ROUTER_PORT_A = 20000 + Math.floor(Math.random() * 15000);
const ROUTER_PORT_B = ROUTER_PORT_A + 1;
const CONFIG_A = path.join(TMP, 'config-burst.json');
const CONFIG_B = path.join(TMP, 'config-off.json');
const STATE_A = path.join(TMP, 'state-burst.json');
const STATE_B = path.join(TMP, 'state-off.json');

// ---------------------------------------------------------------------------
// results
// ---------------------------------------------------------------------------
let pass = 0;
let fail = 0;
function check(name, cond, detail = '') {
  const line = cond ? `ok - ${name}` : `not ok - ${name}${detail ? ` # ${detail}` : ''}`;
  if (cond) pass += 1; else fail += 1;
  console.log(line);
}
const info = (msg) => console.log(`# ${msg}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// mock upstream: the minimal opencode-free surface, with a per-identity allowance
// ---------------------------------------------------------------------------
const mock = {
  // 'budget': each distinct session serves `freeRequestsPerSession` requests,
  //           then answers 429 forever (a spent identity keeps failing).
  // 'deny':   every request answers 429.
  mode: 'budget',
  freeRequestsPerSession: 1,
  retryAfterSeconds: 2,
  delayMs: 0,
  hits: new Map(),      // x-opencode-session -> requests the mock has answered
  log: [],              // { at, session, status }
  accepted: 0,
  denied: 0,
  rejects: 0,
};

function bodyOf(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on('data', (d) => chunks.push(d));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
  });
}

/** Sessions the mock has answered since `t0`, with how many requests each served. */
function sessionsSince(t0) {
  const out = new Map();
  for (const row of mock.log) {
    if (row.at < t0) continue;
    out.set(row.session, (out.get(row.session) || 0) + 1);
  }
  return out;
}

const mockServer = http.createServer(async (req, res) => {
  const body = await bodyOf(req);
  const url = new URL(req.url, 'http://x');
  let json = {};
  try { json = JSON.parse(body || '{}'); } catch { /* reported as a 403 below */ }
  if (url.pathname !== '/inference/v1/chat/completions' || req.method !== 'POST') {
    res.writeHead(404, { 'content-type': 'application/json' });
    return res.end('{"error":{"message":"no such route"}}');
  }

  // The router must present the free-tier attestation or nothing here is testing
  // the free tier at all.
  const problems = [];
  if (req.headers.authorization !== 'Bearer public') problems.push('authorization');
  if (!/^opencode\/latest\/\d+\.\d+\.\d+\/cli$/.test(String(req.headers['user-agent'] || ''))) problems.push('user-agent');
  if (req.headers['x-opencode-client'] !== 'cli') problems.push('client');
  const session = String(req.headers['x-opencode-session'] || '');
  if (!/^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/.test(session)) problems.push('session');
  const toolNames = (Array.isArray(json.tools) ? json.tools : []).map((t) => String((t && t.function && t.function.name) || (t && t.name) || ''));
  const missing = ['bash', 'glob', 'grep', 'read'].filter((n) => !toolNames.includes(n));
  if (missing.length) problems.push(`tools:${missing.join('|')}`);
  if (json.stream !== true) problems.push('stream');
  if (problems.length) {
    mock.rejects += 1;
    res.writeHead(403, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({
      type: 'error',
      error: { type: 'FreeTierError', message: "OpenCode's free tier can only be used from within OpenCode. Download it at https://opencode.ai", problems },
    }));
  }

  const served = mock.hits.get(session) || 0;
  mock.hits.set(session, served + 1);
  const spent = served >= mock.freeRequestsPerSession;
  if (mock.delayMs > 0) await sleep(mock.delayMs);

  if (mock.mode === 'deny' || spent) {
    mock.denied += 1;
    mock.log.push({ at: Date.now(), session, status: 429 });
    res.writeHead(429, { 'content-type': 'application/json', 'retry-after': String(mock.retryAfterSeconds) });
    return res.end(JSON.stringify({
      type: 'error',
      error: { type: 'FreeUsageLimitError', message: 'Free usage limit reached. Subscribe to OpenCode Go to keep going.' },
    }));
  }

  mock.accepted += 1;
  mock.log.push({ at: Date.now(), session, status: 200 });
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
  for (let i = 0; i < 3; i += 1) {
    res.write(`data: ${JSON.stringify({ id: 'burst1', object: 'chat.completion.chunk', model: json.model, choices: [{ index: 0, delta: { content: `B${i} ` }, finish_reason: null }] })}\n\n`);
  }
  res.write(`data: ${JSON.stringify({ id: 'burst1', object: 'chat.completion.chunk', model: json.model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
  res.write('data: [DONE]\n\n');
  return res.end();
});

// ---------------------------------------------------------------------------
// router harness
// ---------------------------------------------------------------------------
/**
 * Config for the router under test: exactly one free-tier provider with two
 * identity lanes and no exit pool (`proxy: 'direct'`), so every lane move is
 * local and observable.
 *
 * `allLanesRenewGapMinutes: 0` removes the sweep floor, which leaves the burst
 * logic as the only thing that can hold a sweep back - that is what makes "no
 * reinstall-all during a burst" mean something. `freeTierCooldownMinutes: 5`
 * keeps a parked lane parked long enough to observe (the free tier otherwise
 * gets a 1s timer from markLimited).
 */
function writeConfig(file, port, { identityBurst } = {}) {
  const cfg = {
    adminKey: ADMIN_KEY,
    host: '127.0.0.1',
    port,
    cooldownHours: 12,
    cooldownStatuses: [429, 402, 403],
    retryStatuses: [500, 502, 503, 504],
    defaultProvider: PID,
    cors: true,
    proxyKeys: [],
    reinstallGapMinutes: 0,
    allLanesRenewGapMinutes: 0,
    freeTierCooldownMinutes: 5,
    providers: [{
      id: PID,
      name: 'Identity Burst Test',
      type: 'opencode-free',
      baseUrl: `http://127.0.0.1:${MOCK_PORT}/inference/v1`,
      strategy: 'round-robin',
      timeoutMs: 10_000,
      proxy: 'direct',
      identityPool: 2,
      accounts: [],
    }],
  };
  // Left unset on the first boot on purpose: the default must be true.
  if (identityBurst !== undefined) cfg.identityBurst = identityBurst;
  fs.writeFileSync(file, `${JSON.stringify(cfg, null, 2)}\n`);
}

async function bootRouter({ label, port, configFile, stateFile }) {
  fs.rmSync(stateFile, { force: true });
  const child = spawn(process.execPath, [path.join(REPO, 'server.js')], {
    cwd: REPO,
    env: { ...process.env, ROUTER_CONFIG: configFile, ROUTER_STATE: stateFile, PORT: String(port), HOST: '127.0.0.1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const boot = { label, child, port, base: `http://127.0.0.1:${port}`, log: '', exit: '' };
  child.stdout.on('data', (d) => { boot.log += d.toString(); });
  child.stderr.on('data', (d) => { boot.log += d.toString(); });
  child.on('exit', (code, signal) => { boot.exit = `code=${code} signal=${signal}`; });
  for (let i = 0; i < 75; i += 1) {
    try { const r = await fetch(`${boot.base}/health`); if (r.ok) { boot.up = true; break; } } catch { /* not up yet */ }
    await sleep(200);
  }
  return boot;
}

async function stopRouter(boot) {
  if (!boot || !boot.child || boot.child.exitCode !== null) return;
  boot.child.kill('SIGTERM');
  for (let i = 0; i < 25 && boot.child.exitCode === null; i += 1) await sleep(80);
  if (boot.child.exitCode === null) boot.child.kill('SIGKILL');
  await sleep(50);
}

/** POST /v1/chat/completions through the router; never throws. */
async function chat(boot, opts = {}) {
  const t0 = Date.now();
  try {
    const res = await fetch(`${boot.base}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-router-provider': PID, ...(opts.headers || {}) },
      body: JSON.stringify({ model: opts.model || MODEL, stream: false, messages: [{ role: 'user', content: 'ping' }] }),
    });
    return { status: res.status, headers: res.headers, text: await res.text(), ms: Date.now() - t0, error: '' };
  } catch (err) {
    return { status: 0, headers: new Headers(), text: '', ms: Date.now() - t0, error: err.message };
  }
}

async function adminState(boot) {
  try {
    const res = await fetch(`${boot.base}/admin/api/state`, { headers: ADMIN });
    if (!res.ok) return { __status: res.status, __text: (await res.text()).slice(0, 200) };
    return await res.json();
  } catch (err) {
    return { __error: err.message };
  }
}

const providerOf = (snap) => (snap && Array.isArray(snap.providers) ? snap.providers.find((p) => p.id === PID) : null);
const accountsOf = (snap) => (providerOf(snap) || {}).accounts || [];
const eventsOf = (snap) => (snap && Array.isArray(snap.events) ? snap.events : []);
const eventsSince = (snap, t0, kind) => eventsOf(snap).filter((e) => Number(e.t) >= t0 && (!kind || e.kind === kind) && e.pid === PID);
const nowAhead = (ms) => Date.now() + ms;

/** Highest `demand` seen for the provider while a burst is in flight. */
async function maxDemandDuring(boot, ms) {
  let max = -1;
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const snap = await adminState(boot);
    const d = Number((providerOf(snap) || {}).demand);
    if (Number.isFinite(d) && d > max) max = d;
    await sleep(60);
  }
  return max;
}

/** Poll until no lane is rebuilding; returns the last snapshot. */
async function waitForIdleLanes(boot, ms = 4000) {
  let snap = await adminState(boot);
  const deadline = Date.now() + ms;
  while (Date.now() < deadline && accountsOf(snap).some((a) => a.refreshing)) {
    await sleep(80);
    snap = await adminState(boot);
  }
  return snap;
}

/** Poll until `demand` is back to 0; returns the last observed value. */
async function waitForDemandZero(boot, ms = 4000) {
  let demand = NaN;
  const deadline = Date.now() + ms;
  for (;;) {
    const snap = await adminState(boot);
    demand = Number((providerOf(snap) || {}).demand);
    if (demand === 0 || Date.now() >= deadline) return { demand, snap };
    await sleep(60);
  }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
let MOCK_PORT = 0;
let childA = null;
let childB = null;

(async () => {
  await new Promise((r) => mockServer.listen(0, '127.0.0.1', r));
  MOCK_PORT = mockServer.address().port;
  info(`mock upstream on 127.0.0.1:${MOCK_PORT}`);

  // No exit pool: `proxy: 'direct'` plus a disabled pool keeps every lane move
  // local (no xray, no subscription fetch).
  fs.writeFileSync(path.join(TMP, 'proxies.json'), `${JSON.stringify({
    version: 1,
    settings: { enabled: false, xrayPath: 'xray-not-installed', inboundBasePort: 20900, maxActive: 2, rotateAttempts: 1, markDeadAfter: 3 },
    sources: [],
    exits: [],
    active: [],
  }, null, 2)}\n`);

  // -------------------------------------------------------------------------
  // A. boot with identityBurst left at its default
  // -------------------------------------------------------------------------
  writeConfig(CONFIG_A, ROUTER_PORT_A);
  childA = await bootRouter({ label: 'burst', port: ROUTER_PORT_A, configFile: CONFIG_A, stateFile: STATE_A });
  check('router boots and answers /health', childA.up === true, childA.log.slice(-400) || childA.exit);
  if (!childA.up) throw new Error(`router did not boot: ${childA.exit || 'no /health'}`);

  {
    const snap = await adminState(childA);
    const p = providerOf(snap);
    check('identityBurst defaults to true', snap.identityBurst === true, `identityBurst=${JSON.stringify(snap.identityBurst)}`);
    check('every provider summary exposes an integer demand',
      !!p && Number.isInteger(p.demand) && p.demand >= 0,
      `demand=${JSON.stringify(p && p.demand)} (provider ${p ? 'present' : 'missing'})`);
    check('demand starts at 0 with nothing in the ladder', Number(p && p.demand) === 0, `demand=${p && p.demand}`);
    check('the free provider runs identityPool lanes, minted before the first request is served',
      !!p && p.identityPool === 2 && accountsOf(snap).length === 2,
      `identityPool=${p && p.identityPool} accounts=${accountsOf(snap).length}`);
    // The lane index is what gives an identity its own slot and its own exit pin, so
    // it has to reach disk: dropping it collapsed every lane onto lane 0 on the next
    // restart, which is a pool that rotates nothing.
    const onDisk = JSON.parse(fs.readFileSync(CONFIG_A, 'utf8')).providers[0].accounts || [];
    check('the lane index is persisted, so a restart cannot collapse the pool',
      onDisk.length === 2 && onDisk.every((a, i) => Number(a.lane) === i),
      `lanes=${JSON.stringify(onDisk.map((a) => a.lane))}`);
  }

  // -------------------------------------------------------------------------
  // B. warm the pool: one request per lane, which also spends both identities
  //    (the mock's allowance is per session: 1 request, then 429 forever)
  // -------------------------------------------------------------------------
  const warmT0 = Date.now();
  const warm1 = await chat(childA);
  const warm2 = await chat(childA);
  const laneA = warm1.headers.get('x-router-account');
  const laneB = warm2.headers.get('x-router-account');
  check('the free tier serves a lone warm-up request', warm1.status === 200, `status=${warm1.status} ${warm1.error} ${warm1.text.slice(0, 160)}`);
  check('the warm-up answer is relayed as a chat.completion (harness sanity)',
    (() => { try { return String(JSON.parse(warm1.text).choices[0].message.content).includes('B0'); } catch { return false; } })(),
    warm1.text.slice(0, 200));
  check('two sequential requests are served by two different lanes',
    warm1.status === 200 && warm2.status === 200 && !!laneA && !!laneB && laneA !== laneB,
    `lane1=${laneA} lane2=${laneB} status=${warm1.status}/${warm2.status}`);
  {
    const spent = sessionsSince(warmT0);
    check('both lane identities are spent upstream before the concurrent phase',
      spent.size === 2 && [...spent.values()].every((n) => n >= mock.freeRequestsPerSession),
      `sessions=${spent.size} hits=${[...spent.values()].join(',')} rejections=${mock.rejects}`);
  }

  // -------------------------------------------------------------------------
  // C. concurrent burst with both lanes spent: both requests must still answer,
  //    on their own lane, without waiting out LANE_WAIT_MS and without a sweep
  // -------------------------------------------------------------------------
  const beforeBurst = await adminState(childA);
  const idsBefore = accountsOf(beforeBurst).map((a) => a.id);
  const gensBefore = accountsOf(beforeBurst).map((a) => Number(a.gen) || 1);
  mock.delayMs = 300; // keep the requests in the ladder long enough to sample demand

  const burstT0 = Date.now();
  const burstJobs = [chat(childA), chat(childA)];
  const peakDemand = await maxDemandDuring(childA, 700);
  const burst = await Promise.all(burstJobs);
  const burstT1 = Date.now();
  mock.delayMs = 0;
  const statuses = burst.map((r) => r.status);
  const lanes = burst.map((r) => r.headers.get('x-router-account'));
  const slowest = Math.max(...burst.map((r) => r.ms));

  check('both concurrent requests answer 200 while both lanes are spent',
    statuses.every((s) => s === 200),
    `status=${statuses.join(',')} bodies=${burst.map((r) => r.text.slice(0, 90)).join(' | ')}`);
  check('the two concurrent requests used two different lanes',
    !!lanes[0] && !!lanes[1] && lanes[0] !== lanes[1],
    `lanes=${lanes.join(',')}`);
  check(`neither concurrent request waited out LANE_WAIT_MS (${LANE_WAIT_MS}ms)`,
    slowest < BURST_LATENCY_BUDGET_MS,
    `slowest=${slowest}ms (${burst.map((r) => r.ms).join(',')})`);
  check('demand reflects the requests in flight (observed > 0 during the burst)',
    peakDemand >= 1, `peak demand=${peakDemand}`);

  const duringBurst = await adminState(childA);
  const burstEvents = eventsSince(duringBurst, burstT0);
  const burstKinds = burstEvents.map((e) => e.kind);
  const rebuilt = burstEvents.filter((e) => e.kind === 'lane-rebuilt');
  const lent = burstEvents.filter((e) => e.kind === 'identity-lent');
  info(`burst events: ${burstKinds.join(',') || '(none)'}`);
  check('a burst lane was rebuilt or a fresh identity was lent (identity-lent | lane-rebuilt)',
    rebuilt.length + lent.length >= 1,
    `kinds=${burstKinds.join(',') || '(none)'}`);
  check('the burst did not run the all-lane sweep (no new reinstall-all while >1 request is in the ladder)',
    eventsSince(duringBurst, burstT0, 'reinstall-all').length === 0,
    JSON.stringify(eventsSince(duringBurst, burstT0, 'reinstall-all')));

  // -------------------------------------------------------------------------
  // D. after the burst: lanes in service, nothing parked, demand back to 0
  // -------------------------------------------------------------------------
  const idle = await waitForIdleLanes(childA);
  const idleAccounts = accountsOf(idle);
  check('no lane is left rebuilding after the burst',
    idleAccounts.length === 2 && idleAccounts.every((a) => !a.refreshing),
    idleAccounts.map((a) => `${a.id}:refreshing=${!!a.refreshing}`).join(','));
  check('no lane is parked after the burst (limitedUntil is not in the future)',
    idleAccounts.length === 2 && idleAccounts.every((a) => Number(a.limitedUntil || 0) <= nowAhead(2000)),
    idleAccounts.map((a) => `${a.id}:+${Math.round((Number(a.limitedUntil || 0) - Date.now()) / 1000)}s`).join(','));
  check('the rebuilt lanes kept their slots (rebuilt in place, same lane rows)',
    JSON.stringify(idleAccounts.map((a) => a.id)) === JSON.stringify(idsBefore),
    `${JSON.stringify(idsBefore)} -> ${JSON.stringify(idleAccounts.map((a) => a.id))}`);
  check('at least one lane stepped to a new identity generation during the burst',
    idleAccounts.some((a, i) => (Number(a.gen) || 1) > (gensBefore[i] || 1)),
    `gens ${JSON.stringify(gensBefore)} -> ${JSON.stringify(idleAccounts.map((a) => Number(a.gen) || 1))}`);

  const settled = await waitForDemandZero(childA);
  check('demand returns to 0 once the burst settles', settled.demand === 0, `demand=${settled.demand}`);
  check('identityBurst is still true after the burst', settled.snap.identityBurst === true, `identityBurst=${JSON.stringify(settled.snap.identityBurst)}`);

  // -------------------------------------------------------------------------
  // E. sequential behaviour is unchanged: one request alone, everything 429ing,
  //    still ends in the existing all-lane sweep
  // -------------------------------------------------------------------------
  mock.mode = 'deny';
  mock.retryAfterSeconds = 2;
  const loneT0 = Date.now();
  const lone = await chat(childA);
  const loneT1 = Date.now();
  mock.mode = 'budget';
  const afterLone = await adminState(childA);
  const sweeps = eventsSince(afterLone, loneT0, 'reinstall-all');
  check('a lone request against an all-429 upstream is answered with a 429, not a wait',
    lone.status === 429, `status=${lone.status} ms=${lone.ms} body=${lone.text.slice(0, 140)}`);
  check('the lone request ends in the all-lane sweep (reinstall-all)',
    sweeps.length >= 1, `reinstall-all events=${sweeps.length} kinds=${eventsSince(afterLone, loneT0).map((e) => e.kind).join(',') || '(none)'}`);
  check('the lone request never waited out LANE_WAIT_MS either',
    lone.ms < LANE_WAIT_MS, `ms=${lone.ms} (${loneT1 - loneT0}ms wall)`);

  const afterSweep = await waitForIdleLanes(childA, 6000);
  check('the sweep from the lone request finishes (all lanes back in service)',
    accountsOf(afterSweep).length === 2 && accountsOf(afterSweep).every((a) => !a.refreshing),
    accountsOf(afterSweep).map((a) => `${a.id}:refreshing=${!!a.refreshing}`).join(','));
  const afterSweepDemand = await waitForDemandZero(childA);
  check('demand returns to 0 after the sequential sweep', afterSweepDemand.demand === 0, `demand=${afterSweepDemand.demand}`);

  // -------------------------------------------------------------------------
  // F. every lane parked (a day-scale 429 parks the tier and all its lanes):
  //    a concurrent request must be lent a fresh identity (identity-lent)
  //    instead of waiting LANE_WAIT_MS and failing
  // -------------------------------------------------------------------------
  mock.mode = 'deny';
  mock.retryAfterSeconds = 2400; // >= FREE_DAY_HORIZON_MS (30min): parks the tier
  const park = await chat(childA);
  mock.mode = 'budget';
  mock.retryAfterSeconds = 2;
  const parkedSnap = await adminState(childA);
  const parkedOk = accountsOf(parkedSnap).length === 2
    && accountsOf(parkedSnap).every((a) => Number(a.limitedUntil || 0) > nowAhead(60_000));
  check('precondition: a day-scale 429 parks every lane',
    park.status === 429 && parkedOk,
    `status=${park.status} lanes=${accountsOf(parkedSnap).map((a) => `+${Math.round((Number(a.limitedUntil || 0) - Date.now()) / 1000)}s`).join(',')}`);

  mock.delayMs = 200;
  const lendT0 = Date.now();
  const lendJobs = [chat(childA), chat(childA), chat(childA), chat(childA)];
  const lendPeak = await maxDemandDuring(childA, 600);
  const lent4 = await Promise.all(lendJobs);
  const lendT1 = Date.now();
  mock.delayMs = 0;
  const lendSlowest = Math.max(...lent4.map((r) => r.ms));
  const lendSnap = await adminState(childA);
  const lentEvents = eventsSince(lendSnap, lendT0, 'identity-lent');
  const lendKinds = eventsSince(lendSnap, lendT0).map((e) => e.kind);
  info(`lend events: ${lendKinds.join(',') || '(none)'} (peak demand ${lendPeak})`);

  check('every concurrent request is served although every lane was parked',
    lent4.every((r) => r.status === 200),
    `status=${lent4.map((r) => r.status).join(',')} bodies=${lent4.map((r) => r.text.slice(0, 70)).join(' | ')}`);
  check('no parked-lane request waited out LANE_WAIT_MS',
    lendSlowest < LANE_WAIT_MS - 2000,
    `slowest=${lendSlowest}ms (${lent4.map((r) => r.ms).join(',')})`);
  check('a request that found every lane parked was lent a fresh identity (identity-lent)',
    lentEvents.length >= 1,
    `identity-lent=${lentEvents.length} kinds=${lendKinds.join(',') || '(none)'}`);
  check('the identity-lent event carries the lane and the demand it was lent under',
    lentEvents.some((e) => e.lane !== undefined && e.aid !== undefined && Number.isFinite(Number(e.demand))),
    JSON.stringify(lentEvents.slice(0, 3)));

  const lendSettled = await waitForDemandZero(childA);
  check('demand returns to 0 after the lent identities settle', lendSettled.demand === 0, `demand=${lendSettled.demand}`);

  // -------------------------------------------------------------------------
  // G. the kill switch: a second boot with identityBurst:false
  // -------------------------------------------------------------------------
  await stopRouter(childA);
  childA = null;
  writeConfig(CONFIG_B, ROUTER_PORT_B, { identityBurst: false });
  childB = await bootRouter({ label: 'off', port: ROUTER_PORT_B, configFile: CONFIG_B, stateFile: STATE_B });
  check('second router boots with identityBurst:false in its config', childB.up === true, childB.log.slice(-400) || childB.exit);
  if (childB.up) {
    const snap = await adminState(childB);
    const p = providerOf(snap);
    check('identityBurst echoes false when the config sets it',
      snap.identityBurst === false, `identityBurst=${JSON.stringify(snap.identityBurst)}`);
    check('the provider summary still reports an integer demand with the switch off',
      !!p && Number.isInteger(p.demand), `demand=${JSON.stringify(p && p.demand)}`);

    // The burst path must be inert: same spent-lane concurrency, still served by
    // the sequential ladder, and no identity-lent / lane-rebuilt events.
    const offWarm = [await chat(childB), await chat(childB)];
    mock.delayMs = 200;
    const offT0 = Date.now();
    const offBurst = await Promise.all([chat(childB), chat(childB)]);
    mock.delayMs = 0;
    const offSnap = await adminState(childB);
    const offKinds = eventsSince(offSnap, offT0).map((e) => e.kind);
    check('with the switch off a spent-lane burst is still served (the ladder is unchanged)',
      offWarm.every((r) => r.status === 200) && offBurst.every((r) => r.status === 200),
      `warm=${offWarm.map((r) => r.status).join(',')} burst=${offBurst.map((r) => r.status).join(',')}`);
    check('with the switch off no burst event is recorded (identity-lent / lane-rebuilt are inert)',
      !offKinds.includes('identity-lent') && !offKinds.includes('lane-rebuilt'),
      `kinds=${offKinds.join(',') || '(none)'}`);
  }
})().catch((err) => {
  fail += 1;
  console.log(`not ok - harness completed # ${err.message}`);
  console.error(`# harness error: ${err && err.stack ? err.stack : err}`);
}).finally(async () => {
  await stopRouter(childA);
  await stopRouter(childB);
  try { if (mockServer.closeAllConnections) mockServer.closeAllConnections(); } catch { /* noop */ }
  await new Promise((r) => mockServer.close(r));
  if (fail) {
    const tail = [childA, childB].filter(Boolean).map((b) => `# --- router log (${b.label}) tail ---\n${b.log.split('\n').slice(-12).map((l) => `# ${l}`).join('\n')}`).join('\n');
    console.log(tail);
  }
  const total = pass + fail;
  console.log(`1..${total}`);
  console.log(`# ${pass} passed, ${fail} failed (mock: ${mock.accepted} allowed, ${mock.denied} quota 429, ${mock.rejects} attestation rejects)`);
  try { fs.rmSync(TMP, { recursive: true, force: true }); } catch { /* noop */ }
  process.exit(fail ? 1 : 0);
});
