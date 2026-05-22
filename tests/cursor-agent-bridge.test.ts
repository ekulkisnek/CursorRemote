import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseShellWords } from '../src/server/cursor-agent-bridge.js';

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
