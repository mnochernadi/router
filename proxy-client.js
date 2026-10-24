'use strict';

/**
 * ============================================================================
 *  proxy-client.js -- HTTP CONNECT tunnelling with zero dependencies
 * ============================================================================
 *
 *  Xray (or any HTTP proxy) listens on 127.0.0.1:<port> and speaks HTTP CONNECT.
 *  Node's global fetch cannot be pointed at a per-request proxy without the
 *  `undici` package, and http.request() cannot be *forced* onto an existing
 *  socket: with `agent: false` it ignores createConnection, and even with a
 *  custom Agent Node may open its own connection, which silently egresses from
 *  this host instead of the exit. That failure is invisible in a response body
 *  (the same URL answers either way), so this module writes the HTTP/1.1
 *  request onto the tunnel socket itself. There is no code path that can fall
 *  back to a direct connection.
 *
 *  Returns the same shape the router consumes from fetch:
 *      { status, ok, headers: { get(name) }, body: <web ReadableStream> }
 */

const http = require('node:http');
const net = require('node:net');
const tls = require('node:tls');
const { Readable, PassThrough, Transform } = require('node:stream');

const CONNECT_HEAD_LIMIT = 32 * 1024;
const RESPONSE_HEAD_LIMIT = 128 * 1024;

/** A deadline error. `stall` tells the caller the exit is at fault, not the peer. */
function stallError(message) {
  return Object.assign(new Error(message), { stall: true });
}

function headersShim(map) {
  return {
    get: (name) => (map.has(String(name).toLowerCase()) ? map.get(String(name).toLowerCase()) : null),
    has: (name) => map.has(String(name).toLowerCase()),
    forEach: (fn) => map.forEach((v, k) => fn(v, k)),
    toObject: () => Object.fromEntries(map),
  };
}

/** Decode `Transfer-Encoding: chunked` into a plain byte stream. */
function chunkedDecode() {
  let buf = Buffer.alloc(0);
  let done = false;
  return new Transform({
    transform(chunk, _enc, cb) {
      if (done) return cb();
      buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
      for (;;) {
        const nl = buf.indexOf('\r\n');
        if (nl === -1) return cb();
        const sizeLine = buf.subarray(0, nl).toString('latin1').split(';')[0].trim();
        const size = parseInt(sizeLine, 16);
        if (!Number.isFinite(size)) return cb(new Error(`bad chunk size "${sizeLine}"`));
        if (size === 0) {
          done = true;
          buf = Buffer.alloc(0);
          this.push(null);
          return cb();
        }
        if (buf.length < nl + 2 + size + 2) return cb();
        this.push(buf.subarray(nl + 2, nl + 2 + size));
        buf = buf.subarray(nl + 2 + size + 2);
      }
    },
    flush(cb) { if (!done) { this.push(null); } cb(); },
  });
}

/** Open a socket to `proxy` and CONNECT it to `target`; resolves with the tunnel. */
function openTunnel(proxy, target, timeoutMs) {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ host: proxy.host, port: proxy.port });
    let settled = false;
    let buf = Buffer.alloc(0);

    const fail = (err) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(err);
    };

    socket.setTimeout(timeoutMs, () => fail(new Error(`proxy ${proxy.host}:${proxy.port} connect timeout (${timeoutMs}ms)`)));
    socket.once('error', fail);

    const onData = (chunk) => {
      buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
      const end = buf.indexOf('\r\n\r\n');
      if (end === -1) {
        if (buf.length > CONNECT_HEAD_LIMIT) fail(new Error('proxy sent an oversized CONNECT response'));
        return;
      }
      socket.removeListener('data', onData);
      const head = buf.subarray(0, end).toString('latin1');
      const rest = buf.subarray(end + 4);
      const m = /^HTTP\/1\.[01]\s+(\d{3})/.exec(head);
      const status = m ? Number(m[1]) : 0;
      if (status !== 200) {
        fail(Object.assign(new Error(`proxy refused CONNECT to ${target.host}:${target.port} with HTTP ${status || '?'}`), { proxyStatus: status }));
        return;
      }
      if (rest.length) socket.unshift(rest);
      settled = true;
      socket.setTimeout(0);
      resolve(socket);
    };

    socket.once('connect', () => {
      const auth = proxy.auth ? `Proxy-Authorization: Basic ${Buffer.from(proxy.auth).toString('base64')}\r\n` : '';
      socket.write(
        `CONNECT ${target.host}:${target.port} HTTP/1.1\r\n`
        + `Host: ${target.host}:${target.port}\r\n`
        + `${auth}`
        + 'Proxy-Connection: keep-alive\r\n\r\n',
      );
    });
    socket.on('data', onData);
  });
}

/**
 * Write one HTTP/1.1 request onto `conn` and resolve the parsed response.
 * `Connection: close` keeps framing simple (one response per socket).
 *
 * Three deadlines, because they fail differently and only the first two are the
 * exit's fault:
 *   deadline  - the whole CONNECT + TLS + response-head path, armed by the caller
 *               before the handshake (a stalled handshake used to hang forever:
 *               the socket timeout was cleared when the tunnel opened).
 *   idleMs    - no byte of the body for this long after the head opened. A
 *               stream that keeps sending is never cut; a stalled one is.
 */
function requestOverTunnel(conn, { method, url, headers, body, headDeadline, idleMs = 0 }) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let head = Buffer.alloc(0);
    let idle = null;
    let opened = null;
    const stopIdle = () => { if (idle) { clearInterval(idle); idle = null; } };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      if (headDeadline) headDeadline.clear();
      stopIdle();
      try { conn.destroy(); } catch { /* noop */ }
      reject(err);
    };

    const onData = (chunk) => {
      if (opened) { opened.touch(); return; }
      head = head.length ? Buffer.concat([head, chunk]) : chunk;
      const end = head.indexOf('\r\n\r\n');
      if (end === -1) {
        if (head.length > RESPONSE_HEAD_LIMIT) fail(new Error('upstream sent an oversized response head'));
        return;
      }
      conn.removeListener('data', onData);
      settled = true;
      // The exit delivered its head: the exit deadline is met. What remains is
      // the body, which gets an idle deadline of its own instead.
      if (headDeadline) headDeadline.clear();
      conn.setTimeout(0);

      const headText = head.subarray(0, end).toString('latin1');
      const rest = head.subarray(end + 4);
      const lines = headText.split('\r\n');
      const m = /^HTTP\/1\.[01]\s+(\d{3})/.exec(lines[0] || '');
      const status = m ? Number(m[1]) : 0;
      const map = new Map();
      for (const line of lines.slice(1)) {
        const i = line.indexOf(':');
        if (i > 0) {
          const k = line.slice(0, i).trim().toLowerCase();
          const v = line.slice(i + 1).trim();
          map.set(k, map.has(k) ? `${map.get(k)}, ${v}` : v);
        }
      }

      // `close` framing: everything after the head is the body, until EOF.
      const out = new PassThrough();
      if (rest.length) out.write(rest);
      conn.pipe(out);
      conn.once('error', (err) => out.destroy(err));

      if (idleMs > 0) {
        // Reset on every byte: a long answer is fine, a dead one is not. The
        // underlying TLS socket's own setTimeout does not fire reliably once a
        // wrapper socket replaced it, so the timer is driven from `data`.
        let last = Date.now();
        const touch = () => { last = Date.now(); };
        opened = { touch };
        conn.on('data', touch);
        idle = setInterval(() => {
          if (Date.now() - last < idleMs) return;
          stopIdle();
          out.destroy(stallError(`upstream body stalled for ${Math.round(idleMs / 1000)}s (timeout)`));
        }, 1000);
        out.once('close', stopIdle);
      }

      const chunked = /\bchunked\b/i.test(map.get('transfer-encoding') || '');
      const bodyStream = chunked ? out.pipe(chunkedDecode()) : out;
      // pipe() does not forward errors, and an unhandled 'error' on the raw
      // PassThrough kills the process. Route it to whatever the caller reads.
      if (bodyStream !== out) out.on('error', (err) => bodyStream.destroy(err));
      else out.on('error', () => { /* the web reader carries it */ });

      resolve({
        status,
        ok: status >= 200 && status < 300,
        headers: headersShim(map),
        body: Readable.toWeb(bodyStream),
        node: bodyStream,
        tunnelled: true,
        cancel: () => { stopIdle(); try { out.destroy(); } catch { /* noop */ } try { conn.destroy(); } catch { /* noop */ } },
      });
    };

    conn.once('error', fail);
    conn.on('data', onData);

    const lines = [`${method} ${url.pathname}${url.search || ''} HTTP/1.1`, `Host: ${url.host}`];
    for (const [k, v] of Object.entries(headers || {})) {
      if (v == null || k.toLowerCase() === 'host' || k.toLowerCase() === 'connection' || k.toLowerCase() === 'content-length') continue;
      if (Array.isArray(v)) for (const vv of v) lines.push(`${k}: ${vv}`);
      else lines.push(`${k}: ${v}`);
    }
    if (body != null) lines.push(`Content-Length: ${Buffer.byteLength(body)}`);
    lines.push('Connection: close', '', '');
    conn.write(lines.join('\r\n'));
    if (body != null) conn.write(body);
  });
}

async function proxiedRequest(targetUrl, options = {}) {
  const {
    method = 'POST',
    headers = {},
    body = null,
    proxy,
    timeoutMs = 30_000,
    idleMs = 0,
    insecure = false,
    signal,
  } = options;

  if (!proxy || !proxy.host || !proxy.port) throw new Error('proxiedRequest requires a proxy with host and port');

  const url = new URL(targetUrl);
  const isTls = url.protocol === 'https:';
  const port = Number(url.port || (isTls ? 443 : 80));

  let conn = await openTunnel(proxy, { host: url.hostname, port }, timeoutMs);

  // Armed before the handshake and cleared when the head arrives. The tunnel's
  // own socket timeout is cleared on CONNECT 200, so without this a stalled TLS
  // handshake waits for the peer to give up (measured: 82s).
  const headDeadline = { timer: null, clear() { if (this.timer) { clearTimeout(this.timer); this.timer = null; } } };
  headDeadline.timer = setTimeout(
    () => { try { conn.destroy(stallError(`exit produced no response head in ${Math.round(timeoutMs / 1000)}s (timeout)`)); } catch { /* noop */ } },
    timeoutMs,
  );

  const onAbort = () => {
    headDeadline.clear();
    try { conn.destroy(Object.assign(new Error('aborted'), { stall: false })); } catch { /* noop */ }
  };
  if (signal) {
    if (signal.aborted) {
      headDeadline.clear();
      try { conn.destroy(); } catch { /* noop */ }
      throw new Error('aborted');
    }
    signal.addEventListener('abort', onAbort, { once: true });
  }

  if (isTls) {
    conn = await new Promise((resolve, reject) => {
      const t = tls.connect(
        { socket: conn, servername: url.hostname, rejectUnauthorized: !insecure, ALPNProtocols: ['http/1.1'] },
        () => resolve(t),
      );
      t.once('error', reject);
    });
  }

  // The listener stays attached after the head so the caller's deadline can
  // still cut a stalled body (a non-streaming client is buffering it).
  return requestOverTunnel(conn, { method, url, headers, body, headDeadline, idleMs });
}

/** GET a URL through a proxy and read a bounded body (used by the proxy tester). */
function proxiedGet(targetUrl, proxy, { timeoutMs = 8000, insecure = false, maxBytes = 64 * 1024 } = {}) {
  const started = Date.now();
  // The tester reads the whole body, so the idle guard is what stops it hanging
  // on an exit that hands over a head and then goes quiet.
  const res = proxiedRequest(targetUrl, { method: 'GET', headers: { accept: '*/*' }, proxy, timeoutMs, idleMs: timeoutMs, insecure });
  return res.then(async (r) => {
    let text = '';
    if (r.body) {
      const reader = Readable.fromWeb(r.body);
      try {
        for await (const chunk of reader) {
          text += chunk.toString('utf8');
          if (text.length > maxBytes) break;
        }
      } finally {
        try { reader.destroy(); } catch { /* noop */ }
      }
    }
    return { status: r.status, ok: r.ok, text, ms: Date.now() - started, headers: r.headers, tunnelled: r.tunnelled === true };
  });
}

module.exports = { proxiedRequest, proxiedGet, openTunnel, headersShim, chunkedDecode };
