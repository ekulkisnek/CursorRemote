import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { chmodSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import express from 'express';
import { CursorAgentBridge, parseShellWords } from '../src/server/cursor-agent-bridge.js';

describe('cursor-agent bridge shell arg parsing', () => {
  it('parses quoted prompts as one argument', () => {
    assert.deepEqual(parseShellWords('-p "Reply with READY"'), ['-p', 'Reply with READY']);
  });

  it('supports single quotes and escaped spaces', () => {
    assert.deepEqual(parseShellWords("--flag 'one two' three\\ four"), ['--flag', 'one two', 'three four']);
  });

  it('preserves empty quoted arguments', () => {
    assert.deepEqual(parseShellWords('-p ""'), ['-p', '']);
  });

  it('rejects unclosed quotes', () => {
    assert.throws(() => parseShellWords('-p "unfinished'), /Unclosed/);
  });
});

describe('cursor-agent bridge HTTP endpoints', () => {
  it('supports async polling, since offsets, duplicate supersede, and cancel', async () => {
    const oldToken = process.env.CURSOR_BRIDGE_TOKEN;
    const oldBin = process.env.CURSOR_AGENT_BIN;
    const oldBaseArgs = process.env.CURSOR_AGENT_BASE_ARGS;
    const oldTimeout = process.env.CURSOR_AGENT_ASYNC_TIMEOUT_MS;
    const bin = createFakeCursorAgentBin();

    process.env.CURSOR_BRIDGE_TOKEN = 'test-token';
    process.env.CURSOR_AGENT_BIN = bin;
    process.env.CURSOR_AGENT_BASE_ARGS = '';
    process.env.CURSOR_AGENT_ASYNC_TIMEOUT_MS = '2000';

    const app = express();
    const bridge = new CursorAgentBridge();
    bridge.register(app);
    const server = app.listen(0);

    try {
      const address = server.address();
      assert.equal(typeof address, 'object');
      assert(address);
      const base = `http://127.0.0.1:${address.port}`;

      const completed = await submit(base, 'test-token', '--echo abcdef');
      await waitForStatus(base, 'test-token', completed, 'done');
      const incremental = await textFetch(`${base}/jobs/${completed}?token=test-token&since=3`);
      const incrementalStdout = incremental.split('--- stdout ---\n')[1]?.split('\n--- stderr ---')[0] ?? '';
      assert.match(incremental, /stdout_offset: 7/);
      assert.match(incrementalStdout, /def/);
      assert.doesNotMatch(incrementalStdout, /abcdef/);

      const blocker = await submit(base, 'test-token', '--sleep');
      await waitForStatus(base, 'test-token', blocker, 'running');

      const duplicateA = await submit(base, 'test-token', '--echo duplicate');
      const duplicateBResponse = await textFetch(`${base}/execute_async?token=test-token&cmd=${encodeURIComponent('--echo duplicate')}`);
      const duplicateB = extractJobId(duplicateBResponse);
      assert.match(duplicateBResponse, /Superseded 1 older queued duplicate job/);

      const oldDuplicate = await textFetch(`${base}/jobs/${duplicateA}?token=test-token`);
      assert.match(oldDuplicate, /status: cancelled/);
      assert.match(oldDuplicate, new RegExp(`Superseded by duplicate job ${duplicateB}`));

      const cancelResponse = await textFetch(`${base}/jobs/${blocker}/cancel?token=test-token`);
      assert.match(cancelResponse, new RegExp(`Cancelled ${blocker}`));
      await waitForStatus(base, 'test-token', blocker, 'cancelled');
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      restoreEnv('CURSOR_BRIDGE_TOKEN', oldToken);
      restoreEnv('CURSOR_AGENT_BIN', oldBin);
      restoreEnv('CURSOR_AGENT_BASE_ARGS', oldBaseArgs);
      restoreEnv('CURSOR_AGENT_ASYNC_TIMEOUT_MS', oldTimeout);
    }
  });
});

function createFakeCursorAgentBin(): string {
  const dir = mkdtempSync(join(tmpdir(), 'cursor-agent-bridge-'));
  const bin = join(dir, 'cursor-agent');
  writeFileSync(
    bin,
    [
      '#!/bin/sh',
      'if [ "$1" = "--sleep" ]; then',
      '  sleep 20',
      '  exit 0',
      'fi',
      'if [ "$1" = "--echo" ]; then',
      '  printf "%s\\n" "$2"',
      '  exit 0',
      'fi',
      'printf "%s\\n" "$*"',
    ].join('\n')
  );
  chmodSync(bin, 0o755);
  return bin;
}

async function submit(base: string, token: string, cmd: string): Promise<string> {
  const response = await textFetch(`${base}/execute_async?token=${token}&cmd=${encodeURIComponent(cmd)}`);
  return extractJobId(response);
}

function extractJobId(response: string): string {
  const match = response.match(/Job (j-[0-9a-f]+) submitted/);
  assert(match, response);
  return match[1];
}

async function waitForStatus(base: string, token: string, jobId: string, status: string): Promise<string> {
  const deadline = Date.now() + 3000;
  let body = '';
  while (Date.now() < deadline) {
    body = await textFetch(`${base}/jobs/${jobId}?token=${token}`);
    if (body.includes(`status: ${status}`)) return body;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  assert.fail(`Timed out waiting for ${jobId} to be ${status}. Last body:\n${body}`);
}

async function textFetch(url: string): Promise<string> {
  const response = await fetch(url);
  const body = await response.text();
  assert(response.ok, body);
  return body;
}

function restoreEnv(key: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[key];
  } else {
    process.env[key] = value;
  }
}
