'use strict';

/**
 * ============================================================================
 *  freebuff-driver.js -- freebuff / Codebuff as an upstream provider
 * ============================================================================
 *
 *  freebuff (codebuff.com) is OpenAI-shaped:
 *
 *      POST https://www.codebuff.com/api/v1/chat/completions   { model, messages, stream }
 *
 *  but it is not a bare API key endpoint. A call is only accepted when all of
 *  the following hold:
 *
 *    1. Authorization: Bearer <36-char account token>
 *    2. an Ed25519 device identity is registered for this account, and every
 *       request carries a signature over
 *           freebuff-device-v1\n<METHOD>\n<path>\n<ts>\n<sha256hex(body)>\n<fetchId>
 *       in x-freebuff-device-{key,ts,sig} (sig = base64url, no prehash)
 *    3. `model` is the catalog *handle* (fbm1.…) for the moment, taken from a
 *       signed catalog fetch (/api/v1/freebuff/models) which also yields fetchId
 *    4. an instance is admitted (/api/v1/freebuff/session/admission) and an
 *       agent run exists (/api/v1/agent-runs START) whose id is echoed in the
 *       completion body as codebuff_metadata.run_id
 *    5. the first message is the Buffy system preamble, and
 *       codebuff_metadata.cost_mode is "free", surface "cli"
 *    6. the egress IP is not in a blocked country - the service admits the
 *       session and then resets every completion when it is. That is why a
 *       provider of this type almost always wants a proxy exit (see
 *       proxy-manager.js).
 *
 *  All of that bookkeeping lives here. Verification: test/smoke.js runs a mock
 *  freebuff server that enforces rules 1-5 with real Ed25519 verification.
 */

const crypto = require('node:crypto');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');

const API_HOST = process.env.FB_API_HOST || 'https://www.codebuff.com';
const CATALOG_CLIENT = { 'x-freebuff-client': 'cli', 'x-freebuff-client-version': '0.2.32' };
const SESSION_CLIENT_VERSION = '0.0.183';
const RUN_AGENT_ID = 'freebuff-desktop-thread-local-v3';
const BUFFY_PREAMBLE = 'You are Buffy, the coding agent behind Codebuff.';
const COMPLETION_UA = 'ai-sdk/openai-compatible/0.0.0-test/codebuff ai-sdk/provider-utils/3.0.25 runtime/browser';
const INSTANCE_TTL_MS = 50 * 60 * 1000;

const b64url = (buf) => Buffer.from(buf).toString('base64url');
const sha256hex = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

/** The exact canonical string freebuff signs. Kept in one place on purpose. */
function canonical({ method, path: p, ts, bodyHash, fetchId }) {
  return ['freebuff-device-v1', method, p, String(ts), bodyHash, fetchId || ''].join('\n');
}

function sign(privateKey, payload) {
  return crypto.sign(null, Buffer.from(payload), privateKey).toString('base64url');
}

class FreebuffDriver {
  constructor({ stateFile, log = () => {} } = {}) {
    this.stateFile = stateFile;
    this.log = log;
    this.state = { version: 1, accounts: {} };
    this.catalogs = new Map(); // token -> { at, fetchId, models: [{ key, handle, name }] }
  }

  init() {
    try {
      const raw = JSON.parse(fs.readFileSync(this.stateFile, 'utf8'));
      if (raw && raw.accounts) this.state = { version: 1, ...raw };
    } catch { /* first boot */ }
  }

  async save() {
    const tmp = `${this.stateFile}.${process.pid}.tmp`;
    try {
      await fsp.writeFile(tmp, JSON.stringify(this.state, null, 2), { mode: 0o600 });
      await fsp.rename(tmp, this.stateFile);
    } catch (err) {
      this.log(`[freebuff] state save failed: ${err.message}`);
    }
  }

  accountState(providerId, accountId) {
    const key = `${providerId}:${accountId}`;
    let st = this.state.accounts[key];
    if (!st) {
      st = this.state.accounts[key] = {
        installId: crypto.randomUUID(),
        device: null,
        catalog: null,
        instance: null,
        runId: '',
        needsReauth: false,
        lastError: '',
      };
    }
    return st;
  }

  ensureDevice(st) {
    if (st.device && st.device.privateKey) return st.device;
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    st.device = {
      publicKey: publicKey.export({ type: 'spki', format: 'der' }).toString('base64url'),
      privateKey: privateKey.export({ type: 'pkcs8', format: 'der' }).toString('base64url'),
      keyId: '',
      registered: false,
    };
    return st.device;
  }

  privateKeyObject(st) {
    return crypto.createPrivateKey({ key: Buffer.from(st.device.privateKey, 'base64url'), format: 'der', type: 'pkcs8' });
  }

  signedHeaders(st, { method, path: p, body, fetchId, extra }) {
    this.ensureDevice(st);
    const ts = Date.now();
    const bodyHash = sha256hex(body ? Buffer.from(body) : Buffer.alloc(0));
    const payload = canonical({ method, path: p, ts, bodyHash, fetchId });
    const headers = {
      ...(extra || {}),
      'x-freebuff-device-key': st.device.keyId || st.device.publicKey,
      'x-freebuff-device-ts': String(ts),
      'x-freebuff-device-sig': sign(this.privateKeyObject(st), payload),
    };
    return headers;
  }

  sessionHeaders(st, token) {
    return {
      authorization: `Bearer ${token}`,
      'x-freebuff-client': 'desktop',
      'x-freebuff-client-version': SESSION_CLIENT_VERSION,
      'x-freebuff-install-id': st.installId,
      'x-freebuff-multi-session': '1',
      'x-fb-timezone': Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC',
    };
  }

  async call(st, token, { method, path: p, body, headers, timeoutMs = 20_000, fetchId, connectionClose = true }) {
    const payload = body == null ? null : (typeof body === 'string' ? body : JSON.stringify(body));
    const signed = this.signedHeaders(st, { method, path: p, body: payload, fetchId, extra: { ...this.sessionHeaders(st, token), ...(headers || {}) } });
    if (connectionClose) signed.connection = 'close'; // pooled sockets reset at ~11s
    const res = await fetch(`${API_HOST}${p}`, {
      method,
      headers: payload == null ? signed : { ...signed, 'content-type': 'application/json' },
      body: payload == null ? undefined : payload,
      redirect: 'follow',
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let json = null;
    try { json = JSON.parse(text); } catch { /* keep text */ }
    return { status: res.status, ok: res.ok, json, text, headers: res.headers };
  }

  // ---- lifecycle ---------------------------------------------------------

  async ensureDeviceRegistered(st, token) {
    if (st.device && st.device.registered) return st.device;
    const device = this.ensureDevice(st);
    const body = {
      publicKey: device.publicKey,
      platform: process.platform,
      name: 'llm-router',
    };
    const res = await this.call(st, token, { method: 'POST', path: '/api/v1/freebuff/device-keys', body, fetchId: '' });
    if (res.status === 401) { st.needsReauth = true; throw new Error('freebuff token rejected (401) - re-login in the app and paste the new token'); }
    if (!res.ok) throw new Error(`device-keys registration failed: HTTP ${res.status} ${res.text.slice(0, 160)}`);
    const keyId = (res.json && (res.json.keyId || res.json.id || res.json.deviceKeyId)) || device.publicKey;
    st.device = { ...device, keyId, registered: true };
    await this.save();
    return st.device;
  }

  async ensureCatalog(st, token, { force = false } = {}) {
    const cached = st.catalog;
    if (!force && cached && cached.models && cached.models.length && Date.now() - cached.at < 10 * 60 * 1000) return cached;
    await this.ensureDeviceRegistered(st, token);
    const res = await this.call(st, token, {
      method: 'GET',
      path: '/api/v1/freebuff/models',
      headers: { ...CATALOG_CLIENT, 'x-freebuff-catalog-protocol': '1' },
      fetchId: '',
    });
    if (!res.ok) throw new Error(`catalog fetch failed: HTTP ${res.status} ${res.text.slice(0, 160)}`);
    const rows = Array.isArray(res.json) ? res.json
      : (res.json && (res.json.models || res.json.data)) || [];
    const models = rows.map((m) => (typeof m === 'string' ? { key: m, handle: m, name: m } : {
      key: m.key || m.id || m.model || '',
      handle: m.handle || m.catalogHandle || m.fbm || (typeof m.model === 'string' && m.model.startsWith('fbm1.') ? m.model : ''),
      name: m.name || m.displayName || m.label || m.key || m.id || '',
      family: m.family || m.provider || '',
    })).filter((m) => m.handle);
    const fetchId = (res.json && (res.json.fetchId || res.json.catalogFetchId || res.json.catalog_fetch_id)) || '';
    st.catalog = { at: Date.now(), fetchId, models, raw: models.length ? undefined : res.text.slice(0, 500) };
    await this.save();
    return st.catalog;
  }

  resolveModel(catalog, requested) {
    const want = String(requested || '').trim();
    if (!catalog || !catalog.models.length) return want;
    if (want.startsWith('fbm1.')) return want;
    const byKey = catalog.models.find((m) => m.key === want);
    if (byKey) return byKey.handle;
    const lower = want.toLowerCase();
    const byName = catalog.models.find((m) => String(m.name).toLowerCase() === lower)
      || catalog.models.find((m) => String(m.name).toLowerCase().includes(lower))
      || catalog.models.find((m) => String(m.family).toLowerCase() === lower);
    if (byName) return byName.handle;
    return want;
  }

  async ensureInstance(st, token, handle) {
    const now = Date.now();
    if (st.instance && st.instance.id && st.instance.model === handle && st.instance.expiresAt - now > 60_000) return st.instance;
    const instanceId = crypto.randomUUID();
    const res = await this.call(st, token, {
      method: 'POST',
      path: '/api/v1/freebuff/session/admission',
      body: { source: 'freebuff-desktop' },
      headers: {
        'x-freebuff-model': handle,
        'x-freebuff-wallet-spend-limit': 'session',
        'x-freebuff-instance-id': instanceId,
        'x-freebuff-purchase-continuity': '1',
      },
      fetchId: st.catalog ? st.catalog.fetchId : '',
    });
    if (res.status === 401) { st.needsReauth = true; throw new Error('freebuff token rejected (401) on admission'); }
    if (!res.ok) throw new Error(`session admission failed: HTTP ${res.status} ${res.text.slice(0, 200)}`);
    const json = res.json || {};
    if (json.countryBlockReason) {
      st.lastError = `admitted but ${json.countryBlockReason} (${json.countryCode || '?'}) - completions will be reset; assign a proxy exit`;
      st.instance = { id: json.instanceId || instanceId, model: handle, expiresAt: now + (json.remainingMs || 0), blocked: true };
      await this.save();
      throw new Error(st.lastError);
    }
    if (json.status && !['active', 'ok'].includes(json.status)) {
      throw new Error(`session not active: ${json.status}${json.currentInstanceId ? ` (instance ${json.currentInstanceId} holds the slot - takeover header needed)` : ''}`);
    }
    st.instance = { id: json.instanceId || instanceId, model: handle, expiresAt: now + (json.remainingMs || INSTANCE_TTL_MS), blocked: false };
    st.lastError = '';
    await this.save();
    return st.instance;
  }

  async ensureRun(st, token) {
    if (st.runId) return st.runId;
    const res = await this.call(st, token, {
      method: 'POST',
      path: '/api/v1/agent-runs',
      body: { action: 'START', agentId: RUN_AGENT_ID, ancestorRunIds: [] },
      fetchId: st.catalog ? st.catalog.fetchId : '',
    });
    if (!res.ok) throw new Error(`agent run start failed: HTTP ${res.status} ${res.text.slice(0, 160)}`);
    const runId = (res.json && (res.json.runId || res.json.id)) || '';
    if (!runId) throw new Error('agent run start returned no runId');
    st.runId = runId;
    await this.save();
    return runId;
  }

  async finishRun(st, token, { status = 'completed', ok = true } = {}) {
    const runId = st.runId;
    if (!runId) return;
    st.runId = '';
    try {
      await this.call(st, token, {
        method: 'POST',
        path: '/api/v1/agent-runs',
        body: {
          action: 'FINISH',
          runId,
          status: ok ? status : 'error',
          totalSteps: 1,
          directCredits: 0,
          totalCredits: 0,
          steps: [],
        },
        fetchId: st.catalog ? st.catalog.fetchId : '',
      });
    } catch (err) {
      this.log(`[freebuff] run finish failed: ${err.message}`);
    }
    await this.save();
  }

  async endSession(st, token) {
    try {
      await this.call(st, token, { method: 'DELETE', path: '/api/v1/freebuff/session', headers: { 'x-freebuff-instance-id': st.instance ? st.instance.id : '' }, fetchId: st.catalog ? st.catalog.fetchId : '' });
    } catch { /* best effort */ }
    st.instance = null;
    await this.save();
  }

  /** First message must be the Buffy preamble or the service answers 403. */
  withPreamble(messages) {
    const list = Array.isArray(messages) ? messages.slice() : [];
    const first = list[0];
    const firstText = typeof first?.content === 'string' ? first.content
      : Array.isArray(first?.content) ? first.content.map((p) => p?.text || '').join('') : '';
    if (first && first.role === 'system' && firstText.startsWith(BUFFY_PREAMBLE)) return list;
    return [{ role: 'system', content: BUFFY_PREAMBLE }, ...list];
  }

  /**
   * Build the signed completion request for one chat call.
   * Returns { url, headers, payload, meta } - the router then sends it as-is.
   */
  async prepare({ provider, account, body, model, stream }) {
    const token = String(account.apiKey || '').trim();
    if (!token) throw Object.assign(new Error('freebuff account has no token'), { statusCode: 400 });
    const st = this.accountState(provider.id, account.id);
    if (st.needsReauth) throw Object.assign(new Error(st.lastError || 'freebuff account needs re-authentication'), { statusCode: 401 });

    const catalog = await this.ensureCatalog(st, token);
    const handle = this.resolveModel(catalog, model || body?.model);
    if (!handle) throw Object.assign(new Error('freebuff needs a model: use a catalog handle (fbm1.…) or a model key from the dashboard Models list'), { statusCode: 400 });
    const instance = await this.ensureInstance(st, token, handle);
    const runId = await this.ensureRun(st, token);

    const payload = {
      ...body,
      model: handle,
      messages: this.withPreamble(body?.messages),
      stream: !!stream,
      provider: { ...(body?.provider || {}), data_collection: 'deny' },
      codebuff_metadata: {
        ...(body?.codebuff_metadata || {}),
        freebuff_instance_id: instance.id,
        freebuff_multi_session: '1',
        run_id: runId,
        llm_step_number: '1',
        cost_mode: 'free',
        surface: 'cli',
      },
    };
    // Our own bookkeeping must never leak into their schema.
    delete payload.codebuff_metadata.trace_session_id;
    delete payload.codebuff_metadata.repo_snapshot;

    const raw = JSON.stringify(payload);
    const headers = this.signedHeaders(st, {
      method: 'POST',
      path: '/api/v1/chat/completions',
      body: raw,
      fetchId: catalog.fetchId,
      extra: {
        ...this.sessionHeaders(st, token),
        'content-type': 'application/json',
        'user-agent': COMPLETION_UA,
        'x-freebuff-sse-gzip-recovery': '1',
        'x-freebuff-acting-user-id': '',
        'x-freebuff-catalog-fetch': catalog.fetchId,
        connection: 'close',
      },
    });

    return {
      url: `${API_HOST}/api/v1/chat/completions`,
      headers,
      raw,
      meta: { runId, instanceId: instance.id, handle, accountKey: `${provider.id}:${account.id}` },
    };
  }

  async onCallFinished({ provider, account, status, ok }) {
    const st = this.accountState(provider.id, account.id);
    if (status === 401) {
      st.needsReauth = true;
      st.lastError = 'token rejected (401) - re-login in the app and paste the new token';
    } else if (status === 410) {
      st.lastError = 'update required (410) - sse-gzip-recovery header missing or client version stale';
    } else if (status === 428) {
      st.lastError = 'waiting room required (428)';
    } else if (status === 429) {
      st.lastError = 'rate limited (429)';
    } else if (ok) {
      st.lastError = '';
    }
    await this.finishRun(st, account.apiKey, { ok, status: ok ? 'completed' : 'error' });
  }

  /** Model list for the dashboard (catalog keys + handles). */
  async listModels({ provider, account }) {
    const token = String(account.apiKey || '').trim();
    const st = this.accountState(provider.id, account.id);
    const catalog = await this.ensureCatalog(st, token, { force: true });
    return catalog.models.map((m) => m.key || m.handle);
  }

  snapshot() {
    const out = [];
    for (const [key, st] of Object.entries(this.state.accounts)) {
      out.push({
        key,
        installId: st.installId,
        deviceKeyId: st.device ? (st.device.keyId || '(unregistered)') : '',
        catalogAt: st.catalog ? st.catalog.at : 0,
        catalogSize: st.catalog ? st.catalog.models.length : 0,
        fetchId: st.catalog ? st.catalog.fetchId : '',
        instance: st.instance ? { id: st.instance.id, model: st.instance.model, expiresAt: st.instance.expiresAt, blocked: !!st.instance.blocked } : null,
        runId: st.runId,
        needsReauth: !!st.needsReauth,
        lastError: st.lastError || '',
      });
    }
    return out;
  }
}

module.exports = { FreebuffDriver, canonical, API_HOST, BUFFY_PREAMBLE };
