#!/usr/bin/env node
/**
 * Reinstall the client for a managed provider (opencode-free and friends).
 *
 * "Reinstalling the client" is a full identity swap, not just a new token. One
 * call replaces the identity and the next request presents a different client in
 * every field the router controls:
 *
 *   account id + name   identity-N -> identity-N+1
 *   session id          ses_<12 hex><14 base62>, freshly minted per identity
 *   x-opencode-project  p-<16 hex>
 *   user-agent          the next entry of the validated client list
 *   cooldown            cleared, so the new client is tried immediately
 *   exit IP             rotated when the provider uses the pool (proxy=rotate)
 *
 * This clears a quota that upstream accounts per session or per client instance.
 * It does NOT clear a quota accounted against the whole free pool: measured
 * against the live service, a brand-new session with a fresh project handle,
 * three client versions, three models and four different egress IPs all answer
 * `429 FreeUsageLimitError` with `retry-after: 57142` (~15.9h). The script
 * therefore probes after reinstalling and tells you the truth, including the
 * upstream deadline, instead of pretending the ceiling moved.
 *
 * Usage:
 *   node scripts/reinstall-client.js --url http://127.0.0.1:8787 --key <ADMIN_KEY>
 *   node scripts/reinstall-client.js --provider opencode-free --no-probe
 *   ADMIN_KEY=... ROUTER_URL=http://127.0.0.1:8787 npm run reinstall-client
 *
 * Flags:
 *   --url <base>       router base URL            (env ROUTER_URL, default http://127.0.0.1:8787)
 *   --key <admin key>  admin key                  (env ADMIN_KEY, falls back to config.json's adminKey)
 *   --provider <id>    repeatable / comma list    (default: every managed provider)
 *   --model <id>       probe model                (default: the first free model of the provider type)
 *   --no-probe         only reinstall, do not spend a request
 *   --json             machine-readable output
 *   --help
 *
 * Exit code: 0 when every reinstalled client got a usable answer (or --no-probe),
 * 1 when the upstream still refuses, so cron/CI can tell the difference.
 */

'use strict';

const fs = require('node:fs');
const path = require('node:path');

const ARGS = process.argv.slice(2);

function flag(name) {
  const i = ARGS.indexOf(`--${name}`);
  return i === -1 ? null : (ARGS[i + 1] && !ARGS[i + 1].startsWith('--') ? ARGS[i + 1] : true);
}
const has = (name) => ARGS.includes(`--${name}`);

if (has('help') || has('h')) {
  console.log(fs.readFileSync(__filename, 'utf8').split('*/')[0].replace(/^\/\*\*?/, '').replace(/^ ?\* ?/gm, ''));
  process.exit(0);
}

const BASE = String(flag('url') || process.env.ROUTER_URL || 'http://127.0.0.1:8787').replace(/\/+$/, '');
const WANT = String(flag('provider') || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

/** Fall back to the admin key in the router's own config.json, like the CLI of a service would. */
function adminKeyFromConfig() {
  const candidates = [
    process.env.ROUTER_CONFIG,
    path.join(process.env.ROUTER_DATA_DIR || '', 'config.json'),
    '/var/lib/llm-router/config.json',
    path.join(__dirname, '..', 'config.json'),
  ].filter(Boolean);
  for (const file of candidates) {
    try {
      const cfg = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (cfg && cfg.adminKey) return { key: String(cfg.adminKey), from: file };
    } catch { /* keep looking */ }
  }
  return { key: '', from: '' };
}

const KEY = String(flag('key') || process.env.ADMIN_KEY || '');
const resolved = KEY ? { key: KEY, from: 'flag/env' } : adminKeyFromConfig();
if (!resolved.key) {
  console.error('No admin key. Pass --key, set ADMIN_KEY, or run this on the router host so config.json can be read.');
  process.exit(2);
}

const headers = { 'x-admin-key': resolved.key, 'content-type': 'application/json' };

async function api(pathname, init = {}) {
  const res = await fetch(`${BASE}${pathname}`, { ...init, headers: { ...headers, ...(init.headers || {}) } });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* non-JSON error page */ }
  return { status: res.status, json, text };
}

/** free model ids worth probing per provider type, cheapest first */
function probeModelFor(provider) {
  const explicit = flag('model');
  if (explicit && explicit !== true) return String(explicit);
  const free = (provider.accounts || []).find((a) => a.keyless) || {};
  if (provider.type === 'opencode-free') return 'deepseek-v4.1-flash-free';
  void free;
  return provider.defaultModel || 'gpt-4o-mini';
}

function retryAfterOf(text, res) {
  const header = res.headers.get('retry-after');
  if (header && Number.isFinite(Number(header))) return Number(header);
  // the router reports its own cooldown as retryAfterSeconds; upstream bodies
  // (and some proxies) spell it retry_after / retry-after.
  const m = /"retry_?after(?:_?seconds)?"\s*:\s*(\d+)|retry-after[:=]\s*(\d+)/i.exec(text || '');
  return m ? Number(m[1] || m[2]) : 0;
}

function human(seconds) {
  if (!seconds) return '';
  const h = Math.floor(seconds / 3600);
  const m = Math.round((seconds % 3600) / 60);
  return h ? `${h}h${m ? ` ${m}m` : ''}` : `${m}m`;
}

async function main() {
  const snapshot = await api('/admin/api/state');
  if (snapshot.status !== 200 || !snapshot.json) {
    console.error(`Cannot read /admin/api/state from ${BASE} (HTTP ${snapshot.status}). Wrong URL or admin key?`);
    process.exit(2);
  }
  let providers = (snapshot.json.providers || []).filter((p) => p.managed);
  if (WANT.length) providers = providers.filter((p) => WANT.includes(p.id));
  if (!providers.length) {
    console.log(WANT.length
      ? `No managed provider matched: ${WANT.join(', ')}. Managed today: ${(snapshot.json.providers || []).filter((p) => p.managed).map((p) => p.id).join(', ') || '(none)'}`
      : 'No managed provider on this router: nothing to reinstall. (Managed providers are the keyless, session-quota ones, e.g. opencode-free.)');
    process.exit(0);
  }

  const report = [];
  for (const provider of providers) {
    const before = (provider.accounts || [])[0] || {};
    const r = await api(`/admin/api/proxies/provider/${encodeURIComponent(provider.id)}/reinstall`, { method: 'POST', body: '{}' });
    if (r.status !== 200) {
      report.push({ provider: provider.id, ok: false, error: r.json?.error?.message || r.text.slice(0, 120) });
      continue;
    }
    const entry = {
      provider: provider.id,
      ok: true,
      from: before.name || '(none)',
      to: r.json.client?.name || '?',
      project: r.json.client?.project || '?',
      client: r.json.client?.userAgent || '?',
      session: r.json.client?.session || '?',
      probe: null,
    };

    if (!has('no-probe')) {
      const model = probeModelFor(provider);
      const t0 = Date.now();
      const probe = await fetch(`${BASE}/v1/chat/completions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-router-provider': provider.id },
        body: JSON.stringify({ model, messages: [{ role: 'user', content: 'say ok' }] }),
      });
      const text = await probe.text();
      const ra = retryAfterOf(text, probe);
      let message = '';
      try { message = JSON.parse(text).error?.message || ''; } catch { message = text.slice(0, 160); }
      entry.probe = {
        model,
        status: probe.status,
        ms: Date.now() - t0,
        retryAfterSeconds: ra,
        message: String(message).slice(0, 200),
      };
    }
    report.push(entry);
  }

  if (has('json')) {
    console.log(JSON.stringify({ base: BASE, keySource: resolved.from, report }, null, 2));
  } else {
    console.log(`router: ${BASE}   (admin key from ${resolved.from})`);
    for (const e of report) {
      if (!e.ok) { console.log(`\n${e.provider}: FAILED - ${e.error}`); continue; }
      console.log(`\n${e.provider}`);
      console.log(`  client     ${e.from} -> ${e.to}`);
      console.log(`  session    ${e.session}`);
      console.log(`  project    ${e.project}`);
      console.log(`  user-agent ${e.client}`);
      if (e.probe) {
        const p = e.probe;
        console.log(`  probe      ${p.model}: HTTP ${p.status} in ${(p.ms / 1000).toFixed(1)}s`);
        if (p.message) console.log(`             ${p.message}`);
        if (p.status === 200) {
          console.log('             the rebuilt client is accepted: quota cleared.');
        } else if (p.retryAfterSeconds) {
          console.log(`             upstream still refusing; its own deadline is ${p.retryAfterSeconds}s (~${human(p.retryAfterSeconds)}).`);
          console.log('             This ceiling is accounted upstream, not per client: reinstalling cannot move it.');
        }
      }
    }
    const refused = report.filter((e) => e.probe && e.probe.status >= 400).length;
    if (refused) console.log(`\n${refused}/${report.length} rebuilt client(s) still refused by upstream.`);
  }

  const failed = report.filter((e) => !e.ok || (e.probe && e.probe.status >= 400)).length;
  process.exit(failed ? 1 : 0);
}

main().catch((err) => {
  console.error(`reinstall failed: ${err.message}`);
  process.exit(2);
});
