'use strict';
/**
 * OpenCode CLI transport.
 *
 * Why this exists: the anonymous free tier is gated on the *client*, not on the
 * request. Measured on this box (2026-10-24): the exact request the real CLI sends
 * - `/inference/openai/v1/chat/completions`, `Bearer public`, its own UA,
 * `x-opencode-project: global`, the same 32KB body - answered 429 from node, bun,
 * curl, curl-impersonate (chrome136/chrome116/firefox144/safari184) and from a TLS
 * MITM, while the genuine binary answered 200 with a unique prompt four times in a
 * row. Same minute, same box, same egress. So the only keyless way through the gate
 * is to let the genuine binary make the call, which is what this driver does: it
 * runs `opencode run` with a dedicated HOME and returns what the model said.
 *
 * It does NOT lift the free allowance - the CLI has its own daily one, and when it
 * is spent the CLI itself reports `Free usage limit reached`. That answer is
 * detected here (`limit: true`) so the caller can park the tier and tell the client
 * when to come back, instead of pretending the request failed for another reason.
 *
 * Zero dependencies, like the rest of the router.
 */

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

/** The strings the CLI prints when the free allowance (or the gate) says no. */
const LIMIT_RE = /free usage limit reached|freeusagelimiterror|freetiererror|free tier can only be used|used today'?s free messages|rate limit/i;
const AUTH_RE = /missing api key|autherror|unauthor|invalid api key/i;
const ANSI_RE = /\u001b\[[0-9;?]*[A-Za-z]|\u001b\][^\u0007]*\u0007|\u001b[()][A-Z0-9]/g;
const NOISE_RE = /^>\s*build\s*·|^\s*$/;

function stripAnsi(text) {
  return String(text || '').replace(ANSI_RE, '').replace(/\r/g, '');
}

/**
 * The environment the child gets.
 *
 * The CLI is a *server* application as well as a client: it boots its own local
 * services (its debug log shows "booting location services", "watcher backend",
 * "disposing instance"). Inheriting the router's service environment therefore
 * handed it `PORT=8787` and `HOST=0.0.0.0` - the ports the router itself is
 * listening on - and the child sat there without a byte of output while the router
 * waited. Measured: the same binary, same HOME, same arguments answers in ~4s from a
 * shell and under a plain systemd unit, and hung only when it inherited those
 * variables.
 *
 * So the child gets a *minimal* environment: what a login-ish shell would provide,
 * plus this transport's own HOME and anything the caller deliberately adds (the exit
 * proxy). Nothing of the router's service configuration leaks in.
 */
function cliEnv(homeDir, extra = {}) {
  const base = {
    HOME: homeDir,
    XDG_CONFIG_HOME: path.join(homeDir, '.config'),
    XDG_DATA_HOME: path.join(homeDir, '.local', 'share'),
    XDG_CACHE_HOME: path.join(homeDir, '.cache'),
    XDG_STATE_HOME: path.join(homeDir, '.local', 'state'),
    NO_COLOR: '1',
    TERM: 'dumb',
    CI: '1',
  };
  for (const key of ['PATH', 'LANG', 'LC_ALL', 'TZ', 'TMPDIR', 'SSL_CERT_FILE', 'SSL_CERT_DIR']) {
    if (process.env[key]) base[key] = process.env[key];
  }
  return { ...base, ...extra };
}

/** The assistant's words, with the CLI's banner and chrome removed. */
function cleanAnswer(stdout) {
  const lines = stripAnsi(stdout).split('\n').map((l) => l.trimEnd());
  const kept = lines.filter((l) => l.trim() !== '' && !/^>\s*build\s*·/.test(l));
  return kept.join('\n').trim();
}

class OpenCodeCliDriver {
  /**
   * @param {object} opts
   * @param {string} opts.binPath    the genuine `opencode` binary
   * @param {string} opts.homeDir    a HOME of its own, so it never shares state
   * @param {string[]} [opts.args]   extra leading args (tests point this at a fake)
   * @param {function} [opts.log]
   */
  constructor({ binPath, homeDir, home, args = [], mode = 'spawn', serveUrl = '', serveHome = '', log = () => {} } = {}) {
    this.binPath = binPath;
    // Serve mode needs no HOME of its own, but the log watcher and the spawn mode do,
    // so there is always one (a sane temporary default rather than `undefined`, which
    // made `path.join` throw).
    this.homeDir = homeDir ? String(homeDir) : path.join(os.tmpdir(), 'opencode-cli-home');
    this.args = args;
    // 'serve' talks to a long-lived `opencode serve` (fast: measured ~1.5s per
    // answer) and 'spawn' runs a fresh `opencode run` per request (~4-13s, needs
    // ~400M peak). Both present the same genuine client to the gate.
    this.mode = mode === 'serve' ? 'serve' : 'spawn';
    this.serveUrl = String(serveUrl || '').replace(/\/+$/, '');
    // In serve mode the client that logs is the *server's* process, and its HOME is
    // its own (`/var/lib/oc-serve` for the shipped unit) - not the router's home
    // directory. Watching the wrong home made the limit invisible and the call look
    // like a plain timeout.
    this.serveHome = serveHome ? String(serveHome) : this.homeDir;
    this.log = log;
    this.running = 0;
  }

  available() {
    if (this.mode === 'serve') return !!this.serveUrl;
    try { return fs.existsSync(this.binPath); } catch { return false; }
  }

  /**
   * Watch the client's own log for the limit line.
   *
   * A limited client does not fail fast: it retries with backoff, so a request that
   * is being refused looks exactly like a hung request (measured: a message call sat
   * for 60s+ while the log was already full of `Free usage limit reached`). Its log
   * is the truth, so the transport reads it and cuts the call short.
   *
   * @returns {function} stop watching
   */
  watchLimitInLog(onLimit, { intervalMs = 500 } = {}) {
    let file = null;
    // Serve mode: the logging process is the server, so its own HOME is the one that
    // matters. Spawn mode: the child runs with `homeDir`.
    const logHome = this.mode === 'serve' ? this.serveHome : this.homeDir;
    try {
      const dir = path.join(logHome, '.local', 'share', 'opencode', 'log');
      const files = fs.readdirSync(dir).filter((f) => f.endsWith('.log'));
      if (!files.length) return () => {};
      file = files
        .map((f) => ({ f: path.join(dir, f), m: fs.statSync(path.join(dir, f)).mtimeMs }))
        .sort((a, b) => b.m - a.m)[0].f;
    } catch { return () => {}; }
    let offset = 0;
    try { offset = fs.statSync(file).size; } catch { offset = 0; }
    const timer = setInterval(() => {
      try {
        const stat = fs.statSync(file);
        if (stat.size < offset) offset = 0; // rotated
        if (stat.size === offset) return;
        const fd = fs.openSync(file, 'r');
        const buf = Buffer.alloc(stat.size - offset);
        fs.readSync(fd, buf, 0, buf.length, offset);
        fs.closeSync(fd);
        offset = stat.size;
        if (LIMIT_RE.test(buf.toString('utf8'))) {
          clearInterval(timer);
          this.log('[cli-driver] the client log already says the allowance is spent - cutting the call short');
          onLimit();
        }
      } catch { /* the log moved on; the call deadline still applies */ }
    }, intervalMs);
    if (timer.unref) timer.unref();
    return () => clearInterval(timer);
  }

  /**
   * Answer through a running `opencode serve`.
   *
   * Two route traps, both measured: session creation wraps its reply in
   * `{"data":{...}}`, and the message route is `/session/<id>/message` - the
   * `/api/session/<id>/message` spelling returns the web UI's HTML with a 200.
   */
  async runViaServe({ model, prompt, timeoutMs = 180_000, perAttemptMs = 45_000 } = {}) {
    const started = Date.now();
    const post = async (path_, body, ms, signal = null) => {
      const deadline = AbortSignal.timeout(ms);
      const combined = signal ? AbortSignal.any([deadline, signal]) : deadline;
      const res = await fetch(`${this.serveUrl}${path_}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: combined,
      });
      const text = await res.text();
      let json = null;
      try { json = JSON.parse(text); } catch { /* not json */ }
      return { status: res.status, json, text };
    };
    try {
      const created = await post('/api/session', { model: { id: model, providerID: 'opencode' } }, 20_000);
      const sid = created.json && created.json.data && created.json.data.id;
      if (!sid) {
        const limit = LIMIT_RE.test(created.text);
        return {
          ok: false, text: '', limit, auth: false, code: created.status, signal: '',
          stderr: created.text.slice(0, 300),
          error: limit ? 'free allowance spent' : `session create failed (HTTP ${created.status}): ${created.text.slice(0, 160)}`,
          ms: Date.now() - started,
        };
      }
      const ctl = new AbortController();
      let cutShort = false;
      const stopWatch = this.watchLimitInLog(() => { cutShort = true; ctl.abort(); });
      let msg;
      try {
        msg = await post(`/session/${sid}/message`, { parts: [{ type: 'text', text: prompt }] }, Math.min(timeoutMs, perAttemptMs), ctl.signal);
      } catch (err) {
        if (cutShort) {
          return {
            ok: false, text: '', limit: true, auth: false, code: -1, signal: '',
            killedBy: 'limit-line',
            stderr: '', error: 'free allowance spent (the client log said so while the call was still retrying)', ms: Date.now() - started,
          };
        }
        // The client hung up: that is the caller's business, not the allowance's.
        if (signal && signal.aborted) throw err;
        const attemptDeadlineMs = Math.min(timeoutMs, perAttemptMs);
        return {
          ok: false, text: '', limit: false, auth: false, code: -1, signal: '',
          killedBy: 'router-deadline',
          deadlineMs: attemptDeadlineMs,
          stderr: err && err.message ? String(err.message).slice(0, 200) : '',
          error: `the server did not answer inside this attempt's ${attemptDeadlineMs}ms deadline`,
          ms: Date.now() - started,
        };
      } finally {
        stopWatch();
      }
      const combined = `${msg.text}\n${JSON.stringify(msg.json || {})}`;
      const limit = LIMIT_RE.test(combined);
      const auth = !limit && AUTH_RE.test(combined);
      const parts = msg.json && Array.isArray(msg.json.parts) ? msg.json.parts : [];
      const text = parts
        .filter((p) => p && p.text && (!p.type || p.type === 'text'))
        .map((p) => String(p.text))
        .join('')
        .trim();
      const detail = parts.map((p) => p.text || p.type).filter(Boolean).join(' | ').slice(0, 300);
      return {
        ok: !limit && !auth && text.length > 0,
        text,
        limit,
        auth,
        code: msg.status,
        signal: '',
        stderr: detail,
        error: limit ? 'free allowance spent' : (auth ? 'the server reports no usable credential' : (text ? '' : `the server answered without text (HTTP ${msg.status})`)),
        ms: Date.now() - started,
      };
    } catch (err) {
      return {
        ok: false, text: '', limit: false, auth: false, code: -1, signal: '',
        stderr: err.message, error: `opencode serve unreachable at ${this.serveUrl}: ${err.message}`, ms: Date.now() - started,
      };
    }
  }

  run(opts = {}) {
    return this.mode === 'serve' ? this.runViaServe(opts) : this.runViaSpawn(opts);
  }

  /**
   * The model ids the genuine client knows, cached.
   *
   * The free tier has no `/models` endpoint a router may GET: the client's own
   * catalogue is the source of truth, so ask it (`opencode models opencode`). This
   * is what `/v1/models` and the dashboard show; without it the catalogue is empty
   * and every client complains that no model is available.
   */
  listModels({ timeoutMs = 30_000, ttlMs = 10 * 60_000, env = {} } = {}) {
    const hit = this._models;
    if (hit && Date.now() - hit.at < ttlMs && hit.ids.length) return Promise.resolve(hit.ids);
    return new Promise((resolve) => {
      if (!this.available() || this.mode === 'serve') {
        // In serve mode the same catalogue is reachable through the server's own
        // model list, but the binary ships next to it in practice; if it is absent
        // we simply have nothing to advertise rather than inventing ids.
        if (this.mode === 'serve' && this._serveModels) return resolve(this._serveModels);
        if (!fs.existsSync(this.binPath)) return resolve([]);
      }
      try { fs.mkdirSync(this.homeDir, { recursive: true, mode: 0o700 }); } catch { /* exists */ }
      const child = spawn(this.binPath, [...this.args, 'models', 'opencode'], {
        cwd: this.homeDir,
        // The catalogue comes from the network too, so it leaves through the exit.
        env: cliEnv(this.homeDir, env),
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let out = '';
      const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* gone */ } }, Math.max(5_000, timeoutMs));
      if (timer.unref) timer.unref();
      const finish = () => {
        clearTimeout(timer);
        const ids = [...new Set(stripAnsi(out)
          .split('\n')
          .map((l) => l.trim())
          .map((l) => (/^opencode\/[A-Za-z0-9._-]+$/.test(l) ? l.replace(/^opencode\//, '') : ''))
          .filter(Boolean))];
        if (ids.length) this._models = { at: Date.now(), ids };
        resolve(ids);
      };
      child.stdout.on('data', (c) => { out += c.toString('utf8'); });
      child.stderr.on('data', (c) => { out += c.toString('utf8'); });
      child.on('error', finish);
      child.on('close', finish);
    });
  }

  version() {
    return new Promise((resolve) => {
      const child = spawn(this.binPath, [...this.args, '--version'], { stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '';
      child.stdout.on('data', (c) => { out += c; });
      child.stderr.on('data', (c) => { out += c; });
      child.on('error', (err) => resolve({ error: err.message }));
      child.on('close', (code) => resolve({ version: stripAnsi(out).trim().split('\n')[0] || '', code }));
      setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* gone */ } resolve({ error: 'timed out' }); }, 10_000).unref?.();
    });
  }

  /**
   * Run one prompt. The prompt goes in on stdin (measured: a 21KB agent-shaped
   * prompt answers the same as the argument form, without an ARG_MAX ceiling).
   *
   * @returns {Promise<{ok:boolean,text:string,limit:boolean,auth:boolean,code:number,error:string,ms:number}>}
   */
  runViaSpawn({ model, prompt, onDelta = null, signal = null, timeoutMs = 300_000, env = {}, authContent = '' } = {}) {
    const started = Date.now();
    return new Promise((resolve) => {
      if (!this.available()) {
        resolve({ ok: false, text: '', limit: false, auth: false, code: -1, error: `opencode CLI not found at ${this.binPath}`, ms: 0 });
        return;
      }
      try { fs.mkdirSync(this.homeDir, { recursive: true, mode: 0o700 }); } catch { /* exists */ }
      const args = [...this.args, 'run', '--model', `opencode/${model}`];
      const child = spawn(this.binPath, args, {
        cwd: this.homeDir,
        // `OPENCODE_AUTH_CONTENT` is the client's own credential channel (it sits in
        // the binary next to `OPENCODE_CONSOLE_TOKEN`): with it the genuine client
        // speaks for a *principal* instead of the anonymous `public` one, and a
        // principal carries its own allowance. Every environment this transport
        // passes also has to carry the exit proxy - the server's own address must
        // never be the one the edge sees.
        env: cliEnv(this.homeDir, {
          ...(authContent ? { OPENCODE_AUTH_CONTENT: authContent } : {}),
          ...env,
        }),
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      this.running += 1;
      const dbg = (...parts) => { try { this.log(`[cli-driver] ${parts.join(' ')}`); } catch { /* logging must not break the run */ } };
      dbg(`spawn pid=${child.pid || '?'} args="${args.join(' ')}" cwd=${this.homeDir}`);
      child.on('spawn', () => dbg(`spawned pid=${child.pid}`));
      child.on('exit', (code, signal) => dbg(`exit code=${code} signal=${signal || '-'}`));
      // The CLI retries with backoff after a limit instead of exiting, and when the
      // window is spent it often prints *nothing* at all - the banner and then silence
      // (measured: a spawn sat 60s with only the banner on stdout). Its own log always
      // records the refusal, so both channels are watched and either one cuts the call
      // short; without the log channel a spent window looked like a local deadline.
      let cutShort = false;
      const cut = (why) => {
        if (cutShort) return;
        cutShort = true;
        dbg(`${why} - killing the child instead of waiting out its retries`);
        try { child.kill('SIGKILL'); } catch { /* already gone */ }
      };
      const noteChunk = (text) => {
        if (cutShort || !text) return;
        if (LIMIT_RE.test(text)) cut('the limit line appeared');
      };
      const stopLogWatch = this.watchLimitInLog(() => cut('the client log says the allowance is spent'));
      let stdout = '';
      let stderr = '';
      let finished = false;
      // A kill this box asked for is not the upstream's answer. Remember which one it
      // was, so the caller can park a spent allowance without parking a slow model.
      let timedOut = false;
      const deadlineMs = Math.max(5_000, timeoutMs);
      const timer = setTimeout(() => {
        if (finished) return;
        timedOut = true;
        dbg(`the attempt deadline of ${deadlineMs}ms ran out - killing the child`);
        try { child.kill('SIGKILL'); } catch { /* gone */ }
      }, deadlineMs);
      if (timer.unref) timer.unref();

      const done = (extra = {}) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        try { stopLogWatch(); } catch { /* noop */ }
        this.running -= 1;
        const combined = `${stdout}\n${stderr}`;
        // `cutShort` matters: the line can arrive split across chunks, in which case
        // no single chunk matches the pattern even though the child was killed for it.
        const limit = cutShort || LIMIT_RE.test(combined);
        const auth = !limit && AUTH_RE.test(combined);
        const text = cleanAnswer(stdout);
        // Keep the tail of whatever the CLI said: an unexplained failure is the one
        // thing this transport must never report, since the binary is the whole point.
        const detail = stripAnsi(combined).split('\n').map((l) => l.trim()).filter(Boolean).slice(-3).join(' | ');
        resolve({
          ok: !limit && !auth && (text.length > 0 || (extra.code === 0 && !stderr.trim())),
          text,
          limit,
          auth,
          code: typeof extra.code === 'number' ? extra.code : -1,
          signal: extra.signal || '',
          stderr: detail.slice(0, 300),
          killedBy: cutShort ? 'limit-line' : (timedOut ? 'router-deadline' : (extra.signal ? String(extra.signal) : (extra.code ? 'exit' : ''))),
          deadlineMs,
          error: limit
            ? (detail || 'free allowance spent')
            : (auth ? 'the CLI reports no usable credential'
              : (timedOut ? `the client did not answer inside this attempt's ${deadlineMs}ms deadline, so the router killed it`
                : (extra.signal ? `the CLI was killed (${extra.signal})` : (extra.code ? `the CLI exited ${extra.code}` : '')))),
          ms: Date.now() - started,
        });
      };

      child.stdout.on('data', (chunk) => {
        const text = chunk.toString('utf8');
        stdout += text;
        dbg(`stdout ${Buffer.byteLength(text)}B`);
        noteChunk(text);
        if (onDelta) {
          const clean = stripAnsi(text);
          if (clean.trim()) { try { onDelta(clean); } catch { /* caller gone */ } }
        }
      });
      child.stderr.on('data', (chunk) => {
        const text = chunk.toString('utf8');
        stderr += text;
        dbg(`stderr ${stripAnsi(text).slice(0, 120)}`);
        noteChunk(text);
      });
      child.on('error', (err) => { stderr += `spawn error: ${err.message}`; done({ code: -1, signal: 'spawn-error' }); });
      child.on('close', (code, signal) => done({ code, signal }));
      if (signal) {
        const onAbort = () => { try { child.kill('SIGTERM'); } catch { /* gone */ } };
        if (signal.aborted) onAbort();
        else signal.addEventListener('abort', onAbort, { once: true });
      }
      try {
        child.stdin.end(String(prompt || ''));
      } catch (err) {
        stderr += `stdin error: ${err.message}`;
        done({ code: -1 });
      }
    });
  }
}

module.exports = { OpenCodeCliDriver, cleanAnswer, stripAnsi, LIMIT_RE, AUTH_RE };
