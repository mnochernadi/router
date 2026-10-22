'use strict';

/**
 * ============================================================================
 *  proxy-client.js -- HTTP CONNECT tunnelling with zero dependencies
 * ============================================================================
 *
 *  Xray (or any HTTP proxy) listens on 127.0.0.1:<port> and speaks the HTTP
 *  CONNECT method. Node's global fetch cannot be pointed at a per-request
 *  proxy without the `undici` package, so this module does the CONNECT
 *  handshake itself and reuses the resulting socket for a normal
 *  http/https request. The response is returned in the same shape the router
 *  already consumes from fetch:
 *
 *      { status, ok, headers: { get(name) }, body: <web ReadableStream> }
 *
 *  so relay() pipes it unchanged and nothing is buffered.
 */

const http = require('node:http');
const https = require('node:https');
const net = require('node:net');
const tls = require('node:tls');
const { Readable } = require('node:stream');

const CONNECT_HEAD_LIMIT = 32 * 1024;

function headersShim(rawHeaders) {
  const map = new Map();
  for (let i = 0; i + 1 < rawHeaders.length; i += 2) {
    const k = String(rawHeaders[i]).toLowerCase();
    const v = String(rawHeaders[i + 1]);
    map.set(k, map.has(k) ? `${map.get(k)}, ${v}` : v);
  }
  return {
    get: (name) => (map.has(String(name).toLowerCase()) ? map.get(String(name).toLowerCase()) : null),
    has: (name) => map.has(String(name).toLowerCase()),
    forEach: (fn) => map.forEach((v, k) => fn(v, k)),
    toObject: () => Object.fromEntries(map),
  };
}

function toWebBody(nodeReadable) {
  try { return Readable.toWeb(nodeReadable); } catch { return null; }
}

/**
 * Open a socket to `proxy` and CONNECT it to `target`.
 * Resolves with a live socket already positioned at the tunnel mouth.
 */
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
 * fetch()-shaped request through an HTTP proxy.
 * Returns { status, ok, headers, body, node, cancel() }.
 */
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

  const raw = await openTunnel(proxy, { host: url.hostname, port }, timeoutMs);

  let conn = raw;
  if (isTls) {
    conn = await new Promise((resolve, reject) => {
      const t = tls.connect(
        { socket: raw, servername: url.hostname, rejectUnauthorized: !insecure, ALPNProtocols: ['http/1.1'] },
        () => resolve(t),
      );
      t.once('error', reject);
    });
  }

  const mod = isTls ? https : http;
  const requestHeaders = { ...headers, host: url.host };

  return await new Promise((resolve, reject) => {
    // NOTE: `agent: false` makes Node ignore createConnection and dial
    // localhost:80 instead, so the agent is always explicit here. One socket
    // per request, no pooling (providers like freebuff reset pooled sockets).
    const agent = isTls
      ? new https.Agent({ keepAlive: false, maxSockets: 1, createConnection: () => conn })
      : new http.Agent({ keepAlive: false, maxSockets: 1, createConnection: () => conn });

    const req = mod.request({
      host: url.hostname,
      port,
      method,
      path: `${url.pathname}${url.search || ''}`,
      headers: requestHeaders,
      agent,
      servername: isTls ? url.hostname : undefined,
    });

    let done = false;
    const abort = () => {
      if (done) return;
      done = true;
      try { req.destroy(new Error('aborted')); } catch { /* noop */ }
      try { conn.destroy(); } catch { /* noop */ }
      try { agent.destroy(); } catch { /* noop */ }
    };
    if (signal) {
      if (signal.aborted) return abort();
      signal.addEventListener('abort', abort, { once: true });
    }

    req.setTimeout(timeoutMs, () => {
      if (!done) { done = true; req.destroy(new Error(`proxy upstream timeout (${timeoutMs}ms)`)); }
    });
    req.once('error', (err) => {
      if (done) return;
      done = true;
      try { agent.destroy(); } catch { /* noop */ }
      reject(err);
    });

    req.once('response', (res) => {
      done = true;
      req.setTimeout(0);
      resolve({
        status: res.statusCode,
        ok: res.statusCode >= 200 && res.statusCode < 300,
        headers: headersShim(res.rawHeaders),
        body: toWebBody(res),
        node: res,
        cancel: () => {
          try { res.destroy(); } catch { /* noop */ }
          try { conn.destroy(); } catch { /* noop */ }
          try { agent.destroy(); } catch { /* noop */ }
        },
      });
    });

    if (body != null) req.write(body);
    req.end();
  });
}

/** Small helper for the proxy tester: GET a URL through a proxy and read a bounded body. */
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
  return { status: res.status, ok: res.ok, text, ms: Date.now() - started, headers: res.headers };
}

module.exports = { proxiedRequest, proxiedGet, openTunnel, headersShim };
