#!/usr/bin/env node
/**
 * stub-upstream.js - a keyless, OpenAI-compatible upstream for tests.
 *
 * It exists so the router's routing / failover / cooldown / SSE paths can be
 * exercised end to end with NO provider key and NO network egress: every
 * assertion reads the model id and the credential the router actually put on the
 * wire, which the stub records and serves back at GET /__seen.
 *
 * USAGE
 *   STUB_PORT=11500 STUB_NAME=A node test/stub-upstream.js
 *
 * RUNTIME CONTROL (no restart needed)
 *   POST /__fail   {"status":500}   every later request answers that status
 *   POST /__fail   {"status":0}     back to 200
 *   GET  /__seen                    [{url, model, auth, stream}, ...]
 *   POST /__reset                   clear /__seen
 *
 * ENV
 *   STUB_PORT (default 11500), STUB_NAME (default 'stub'),
 *   STUB_FAIL (initial forced status, default 0),
 *   STUB_DELAY (ms before answering, default 0)
 */
'use strict';

const http = require('node:http');

const PORT = Number(process.env.STUB_PORT || 11500);
const NAME = process.env.STUB_NAME || 'stub';
const DELAY = Number(process.env.STUB_DELAY || 0);

let forced = Number(process.env.STUB_FAIL || 0);
let seen = [];

function readJson(req) {
  return new Promise((resolve) => {
    let raw = '';
    req.on('data', (c) => { raw += c; });
    req.on('end', () => {
      try { resolve(JSON.parse(raw || '{}')); } catch { resolve({}); }
    });
  });
}

function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/__seen' && req.method === 'GET') return json(res, 200, seen);
  if (req.url === '/__reset' && req.method === 'POST') { seen = []; return json(res, 200, { ok: true }); }
  if (req.url === '/__fail' && req.method === 'POST') {
    const b = await readJson(req);
    forced = Number(b.status || 0);
    return json(res, 200, { ok: true, forced });
  }

  const body = await readJson(req);
  seen.push({
    at: new Date().toISOString(),
    url: req.url,
    model: body.model || null,
    stream: !!body.stream,
    auth: req.headers.authorization || '',
    xkey: req.headers['x-api-key'] || '',
    referer: req.headers['http-referer'] || '',
    title: req.headers['x-title'] || '',
  });

  if (forced) {
    res.writeHead(forced, { 'content-type': 'application/json', 'retry-after': '3600' });
    return res.end(JSON.stringify({ error: { message: `stub ${NAME} forced ${forced}`, type: 'rate_limit_error' } }));
  }

  const text = `stub=${NAME} model=${body.model}`;
  const base = {
    id: 'chatcmpl-stub',
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: body.model || 'unknown',
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  };

  const answer = () => {
    if (body.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
      res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: { role: 'assistant', content: `stub=${NAME} ` }, finish_reason: null }] })}\n\n`);
      res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: { content: `model=${body.model}` }, finish_reason: 'stop' }] })}\n\n`);
      res.write('data: [DONE]\n\n');
      return res.end();
    }
    json(res, 200, { ...base, choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }] });
  };

  if (DELAY > 0) setTimeout(answer, DELAY);
  else answer();
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[stub ${NAME}] listening http://127.0.0.1:${PORT} forced=${forced || 'none'}`);
});