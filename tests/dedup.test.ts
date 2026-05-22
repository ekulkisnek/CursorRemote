import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DedupCache, makeFingerprint, normalizeCommand } from '../src/server/dedup.js';

describe('command deduplication', () => {
  it('normalizes command text and scopes fingerprints by event', () => {
    assert.equal(normalizeCommand('  Send   THIS\nnow  '), 'send this now');
    assert.equal(
      makeFingerprint('command:send_message', normalizeCommand('Hello')),
      'command:send_message|hello'
    );
    assert.notEqual(
      makeFingerprint('command:send_message', normalizeCommand('Hello')),
      makeFingerprint('command:approve', normalizeCommand('Hello'))
    );
  });

  it('returns running and completed entries for duplicate fingerprints', () => {
    const cache = new DedupCache();
    try {
      const fingerprint = makeFingerprint('command:send_message', normalizeCommand('Fix it'));
      assert.equal(cache.lookup(fingerprint), null);

      const registered = cache.register(fingerprint);
      const running = cache.lookup(fingerprint);
      assert.equal(running?.jobId, registered.jobId);
      assert.equal(running?.status, 'running');

      const result = { commandId: 'cmd-1', ok: true };
      cache.complete(fingerprint, result);
      const completed = cache.lookup(fingerprint);
      assert.equal(completed?.jobId, registered.jobId);
      assert.equal(completed?.status, 'completed');
      assert.deepEqual(completed?.result, result);
    } finally {
      cache.destroy();
    }
  });

  it('evicts failed or retryable fingerprints', () => {
    const cache = new DedupCache();
    try {
      const fingerprint = makeFingerprint('command:send_message', normalizeCommand('Retry me'));
      cache.register(fingerprint);
      cache.evict(fingerprint);
      assert.equal(cache.lookup(fingerprint), null);
      assert.equal(cache.size, 0);
    } finally {
      cache.destroy();
    }
  });
});
