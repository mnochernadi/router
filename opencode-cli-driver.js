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
  constructor({ binPath, homeDir, args = [], log = () => {} } = {}) {
    this.binPath = binPath;
    this.homeDir = homeDir;
    this.args = args;
    this.log = log;
    this.running = 0;
  }

  available() {
    try { return fs.existsSync(this.binPath); } catch { return false; }
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
  run({ model, prompt, onDelta = null, signal = null, timeoutMs = 300_000, env = {} } = {}) {
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
        env: cliEnv(this.homeDir, env),
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      this.running += 1;
      const dbg = (...parts) => { try { this.log(`[cli-driver] ${parts.join(' ')}`); } catch { /* logging must not break the run */ } };
      dbg(`spawn pid=${child.pid || '?'} args="${args.join(' ')}" cwd=${this.homeDir}`);
      child.on('spawn', () => dbg(`spawned pid=${child.pid}`));
      child.on('exit', (code, signal) => dbg(`exit code=${code} signal=${signal || '-'}`));
      let stdout = '';
      let stderr = '';
      let finished = false;
      const timer = setTimeout(() => {
        if (finished) return;
        try { child.kill('SIGKILL'); } catch { /* gone */ }
      }, Math.max(5_000, timeoutMs));
      if (timer.unref) timer.unref();

      const done = (extra = {}) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        this.running -= 1;
        const combined = `${stdout}\n${stderr}`;
        const limit = LIMIT_RE.test(combined);
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
          error: limit
            ? (detail || 'free allowance spent')
            : (auth ? 'the CLI reports no usable credential' : (extra.signal ? `the CLI was killed (${extra.signal})` : (extra.code ? `the CLI exited ${extra.code}` : ''))),
          ms: Date.now() - started,
        });
      };

      child.stdout.on('data', (chunk) => {
        const text = chunk.toString('utf8');
        stdout += text;
        dbg(`stdout ${Buffer.byteLength(text)}B`);
        if (onDelta) {
          const clean = stripAnsi(text);
          if (clean.trim()) { try { onDelta(clean); } catch { /* caller gone */ } }
        }
      });
      child.stderr.on('data', (chunk) => {
        stderr += chunk.toString('utf8');
        dbg(`stderr ${stripAnsi(chunk.toString('utf8')).slice(0, 120)}`);
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
