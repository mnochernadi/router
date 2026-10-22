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
 */
function requestOverTunnel(conn, { method, url, headers, body, timeoutMs }) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let head = Buffer.alloc(0);
    const fail = (err) => {
      if (settled) return;
      settled = true;
      try { conn.destroy(); } catch { /* noop */ }
      reject(err);
    };

    const onData = (chunk) => {
      head = head.length ? Buffer.concat([head, chunk]) : chunk;
      const end = head.indexOf('\r\n\r\n');
      if (end === -1) {
        if (head.length > RESPONSE_HEAD_LIMIT) fail(new Error('upstream sent an oversized response head'));
        return;
      }
      conn.removeListener('data', onData);
      settled = true;
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

      const chunked = /\bchunked\b/i.test(map.get('transfer-encoding') || '');
      const bodyStream = chunked ? out.pipe(chunkedDecode()) : out;

      resolve({
        status,
        ok: status >= 200 && status < 300,
        headers: headersShim(map),
        body: Readable.toWeb(bodyStream),
        node: bodyStream,
        tunnelled: true,
        cancel: () => { try { out.destroy(); } catch { /* noop */ } try { conn.destroy(); } catch { /* noop */ } },
      });
    };

    conn.setTimeout(timeoutMs, () => fail(new Error(`proxy upstream timeout (${timeoutMs}ms)`)));
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
    insecure = false,
    signal,
  } = options;

  if (!proxy || !proxy.host || !proxy.port) throw new Error('proxiedRequest requires a proxy with host and port');

  const url = new URL(targetUrl);
  const isTls = url.protocol === 'https:';
  const port = Number(url.port || (isTls ? 443 : 80));

  let conn = await openTunnel(proxy, { host: url.hostname, port }, timeoutMs);
  if (isTls) {
    conn = await new Promise((resolve, reject) => {
      const t = tls.connect(
        { socket: conn, servername: url.hostname, rejectUnauthorized: !insecure, ALPNProtocols: ['http/1.1'] },
        () => resolve(t),
      );
      t.once('error', reject);
    });
  }

  if (signal) {
    if (signal.aborted) { try { conn.destroy(); } catch { /* noop */ } throw new Error('aborted'); }
    signal.addEventListener('abort', () => { try { conn.destroy(); } catch { /* noop */ } }, { once: true });
  }

  return requestOverTunnel(conn, { method, url, headers, body, timeoutMs });
}

/** GET a URL through a proxy and read a bounded body (used by the proxy tester). */
async function proxiedGet(targetUrl, proxy, { timeoutMs = 8000, insecure = false, maxBytes = 64 * 1024 } = {}) {
  const started = Date.now();
  const res = await proxiedRequest(targetUrl, { method: 'GET', headers: { accept: '*/*' }, proxy, timeoutMs, insecure });
  let text = '';
  if (res.body) {
    const reader = Readable.fromWeb(res.body);
    for await (const chunk of reader) {
      text += chunk.toString('utf8');
      if (text.length > maxBytes) break;
    }
    try { reader.destroy(); } catch { /* noop */ }
  }
  return { status: res.status, ok: res.ok, text, ms: Date.now() - started, headers: res.headers, tunnelled: res.tunnelled === true };
}

module.exports = { proxiedRequest, proxiedGet, openTunnel, headersShim, chunkedDecode };
