#!/usr/bin/env node
'use strict';
/**
 * A stand-in for the genuine `opencode` CLI, for the offline suite.
 *
 * It mimics only what the router's CLI transport relies on: `--version`, reading
 * the prompt from stdin, printing a banner and an answer on stdout, and - when the
 * prompt says so - printing the free-tier limit line and failing, which is how the
 * transport learns the allowance is spent.
 *
 *   prompt containing __LIMIT__   -> prints the limit text, exit 1
 *   prompt containing __EMPTY__   -> prints only the banner, exit 0
 *   anything else                 -> banner + "FAKE OK: <first words>", exit 0
 */
const ANSI = '\u001b[0m';

if (process.argv.includes('--version')) {
  process.stdout.write(`2.1.4\n`);
  process.exit(0);
}

let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => { input += chunk; });
process.stdin.on('end', () => {
  const model = (() => {
    const i = process.argv.indexOf('--model');
    return i === -1 ? 'unknown' : String(process.argv[i + 1] || '').replace(/^opencode\//, '');
  })();

  if (input.includes('__LIMIT__')) {
    process.stdout.write(`${ANSI}> build · ${model}${ANSI}\n`);
    process.stdout.write('Free usage limit reached. Subscribe to OpenCode Go to keep going: https://opencode.ai/go\n');
    process.exit(1);
  }

  process.stdout.write(`${ANSI}> build · ${model}${ANSI}\n`);
  if (!input.includes('__EMPTY__')) {
    const words = input.trim().split(/\s+/).slice(-6).join(' ').slice(0, 60);
    process.stdout.write(`FAKE OK: ${words}\n`);
  }
  process.exit(0);
});
