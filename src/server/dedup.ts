/**
 * Lightweight in-memory deduplication for socket command requests.
 *
 * Problem: Grok (and some other clients) frequently fire the same request
 * multiple times in rapid succession — e.g. sending the identical
 * "send_message" prompt twice within a few seconds.  Without dedup this
 * causes the same text to be typed into Cursor twice, creating duplicate
 * agent runs and confusing the UI state.
 *
 * Design
 * ──────
 * Each in-flight or recently-completed command is indexed by a **fingerprint**
 * derived from two stable fields:
 *
 *   fingerprint = "<socketEvent>|<normalizedText>"
 *
 * where `normalizedText` is the command text lowercased and whitespace-
 * collapsed, so minor formatting differences don't break dedup.
 *
 * When a duplicate arrives we return one of two canned responses:
 *   • still running  → { dedupStatus: "running",  jobId }
 *   • already done   → { dedupStatus: "completed", jobId, result }
 *
 * The cache entry expires after DEDUP_TTL_MS (default 30 s) to prevent
 * stale entries from blocking legitimate re-sends.
 */

export type DedupStatus = 'running' | 'completed';

export interface DedupEntry {
  jobId: string;
  fingerprint: string;
  status: DedupStatus;
  /** ISO timestamp when the entry was created */
  startedAt: string;
  /** Populated once the command finishes */
  result?: unknown;
  /** When the entry should be evicted (ms since epoch) */
  expiresAt: number;
}

/** How long to keep a completed-job entry before allowing a re-run (ms). */
const DEDUP_TTL_MS = 30_000;

/** How often the background sweep removes expired entries (ms). */
const SWEEP_INTERVAL_MS = 60_000;

let _jobCounter = 0;
function nextJobId(): string {
  return `job-${++_jobCounter}-${Date.now().toString(36)}`;
}

/**
 * Normalize a command string into a stable, comparison-safe key.
 * Collapses all whitespace and lower-cases so minor formatting differences
 * (extra spaces, mixed case) don't create false negatives.
 */
export function normalizeCommand(text: string): string {
  return text.trim().toLowerCase().replace(/\s+/g, ' ');
}

/**
 * Build the fingerprint key from the socket event name and command body.
 * The event name acts as the "endpoint" discriminator so that
 * `send_message("foo")` and `approve("foo")` are never conflated.
 */
export function makeFingerprint(eventName: string, normalizedText: string): string {
  return `${eventName}|${normalizedText}`;
}

export class DedupCache {
  private cache = new Map<string, DedupEntry>();
  private sweepTimer: ReturnType<typeof setInterval> | null = null;

  constructor() {
    // Periodically evict stale entries so the map doesn't grow unbounded.
    this.sweepTimer = setInterval(() => this.sweep(), SWEEP_INTERVAL_MS);
    // Don't let the timer prevent Node from exiting.
    this.sweepTimer.unref?.();
  }

  /**
   * Check whether a fingerprint is already known.
   *
   * Returns `null` if this is a new request (caller should proceed normally),
   * or a `DedupEntry` if it's a duplicate (caller should short-circuit).
   */
  lookup(fingerprint: string): DedupEntry | null {
    const entry = this.cache.get(fingerprint);
    if (!entry) return null;

    // Evict if the TTL has passed — treat as a fresh request.
    if (Date.now() > entry.expiresAt) {
      this.cache.delete(fingerprint);
      return null;
    }

    return entry;
  }

  /**
   * Register a new in-flight job.  Call this immediately before dispatching
   * the real command so that concurrent duplicates are blocked.
   */
  register(fingerprint: string): DedupEntry {
    const entry: DedupEntry = {
      jobId: nextJobId(),
      fingerprint,
      status: 'running',
      startedAt: new Date().toISOString(),
      expiresAt: Date.now() + DEDUP_TTL_MS,
    };
    this.cache.set(fingerprint, entry);
    return entry;
  }

  /**
   * Mark a job as completed and store its result.
   * The TTL is reset from *now* so callers that arrive shortly after
   * completion still get the cached result.
   */
  complete(fingerprint: string, result: unknown): void {
    const entry = this.cache.get(fingerprint);
    if (!entry) return;
    entry.status = 'completed';
    entry.result = result;
    entry.expiresAt = Date.now() + DEDUP_TTL_MS;
  }

  /**
   * Remove a job entry immediately (e.g. on error, so the next attempt isn't
   * blocked by a stale "running" entry).
   */
  evict(fingerprint: string): void {
    this.cache.delete(fingerprint);
  }

  /** Evict all entries whose TTL has elapsed. */
  private sweep(): void {
    const now = Date.now();
    for (const [key, entry] of this.cache) {
      if (now > entry.expiresAt) {
        this.cache.delete(key);
      }
    }
  }

  /** Size of the current cache (for diagnostics). */
  get size(): number {
    return this.cache.size;
  }

  destroy(): void {
    if (this.sweepTimer !== null) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    this.cache.clear();
  }
}
