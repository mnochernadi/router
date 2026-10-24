#!/usr/bin/env node
/**
 * Publish the working exits of the pool as a subscription file.
 *
 * The router is both consumer and producer here: this script reads the pool
 * state, keeps the exits a client could actually use (`ok`, with an egress IP
 * that is not this host's own) and writes them as a plain URI list under
 * `public/sub/`, which the server already serves statically. A tunnel or any
 * other front end can put that file on a public hostname; pointing a source at
 * it turns the pool into a feed that survives a subscription going stale.
 *
 * Only exits that tested good are published. Dead and burned entries stay out
 * so the file never advertises something the pool already gave up on.
 *
 * Usage:
 *   node scripts/build-sub.js                       # writes public/sub/monols.txt
 *   node scripts/build-sub.js --out public/sub/x.txt --min 5
 *
 * Flags:
 *   --state <file>   pool state        (default /var/lib/llm-router/proxies.json)
 *   --out <file>     output file       (default <repo>/public/sub/monols.txt)
 *   --min <n>        keep the old file when fewer than n exits survive (default 3)
 *   --quiet          no summary line
 */
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  if (i === -1) return fallback;
  const v = process.argv[i + 1];
  return v && !v.startsWith('--') ? v : true;
}

const statePath = String(arg('state', process.env.ROUTER_STATE || '/var/lib/llm-router/proxies.json'));
const outPath = path.resolve(ROOT, String(arg('out', 'public/sub/monols.txt')));
const min = Number(arg('min', 3)) || 3;
const quiet = arg('quiet', false) === true;

let state;
try {
  state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
} catch (err) {
  console.error(`cannot read ${statePath}: ${err.message}`);
  process.exit(1);
}

const localIp = state.localIp || '';
const seen = new Set();
const lines = [];
for (const exit of state.exits || []) {
  if (exit.status !== 'ok') continue;
  if (!exit.uri || !exit.exitIp || exit.exitIp === localIp) continue;
  const key = `${exit.protocol}|${exit.host}|${exit.port}`;
  if (seen.has(key)) continue;
  seen.add(key);
  lines.push(exit.uri.trim());
}

const stamp = new Date().toISOString().replace(/\.\d+Z$/, 'Z');
if (lines.length < min && fs.existsSync(outPath)) {
  if (!quiet) console.log(`kept the old file: only ${lines.length} working exit(s), minimum is ${min} (${stamp})`);
  process.exit(0);
}

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${lines.join('\n')}\n`, { mode: 0o644 });
if (!quiet) {
  const active = (state.active || []).length;
  console.log(`wrote ${lines.length} working exit(s) to ${path.relative(ROOT, outPath)} (pool active ${active}, ${stamp})`);
}
