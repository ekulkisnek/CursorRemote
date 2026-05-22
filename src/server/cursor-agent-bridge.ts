import type express from 'express';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { randomBytes } from 'crypto';

type JobStatus = 'queued' | 'running' | 'done' | 'error' | 'timeout' | 'cancelled';

interface BridgeJob {
  id: string;
  argsText: string;
  args: string[];
  status: JobStatus;
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
  exitCode?: number | null;
  signal?: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  error?: string;
  process?: ChildProcessWithoutNullStreams;
}

interface RunResult {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  signal: NodeJS.Signals | null;
  timedOut: boolean;
}

const DEFAULT_SYNC_TIMEOUT_MS = 10 * 60 * 1000;
const DEFAULT_ASYNC_TIMEOUT_MS = 60 * 60 * 1000;
const STALE_QUEUED_JOB_MS = 30 * 60 * 1000;
const MAX_BUFFER_CHARS = 500_000;
const MAX_LIST_LIMIT = 100;

export class CursorAgentBridge {
  private readonly token: string;
  private readonly bin: string;
  private readonly baseArgs: string[];
  private readonly asyncTimeoutMs: number;
  private jobs = new Map<string, BridgeJob>();
  private queue: BridgeJob[] = [];
  private runningJob: BridgeJob | null = null;

  constructor() {
    this.token =
      process.env.CURSOR_BRIDGE_TOKEN
      ?? process.env.GROK_BRIDGE_TOKEN
      ?? process.env.EXECUTE_TOKEN
      ?? process.env.TOKEN
      ?? '';
    this.bin = process.env.CURSOR_AGENT_BIN ?? 'cursor-agent';
    this.baseArgs = parseShellWords(process.env.CURSOR_AGENT_BASE_ARGS ?? '--model auto --trust');
    this.asyncTimeoutMs = parsePositiveInt(process.env.CURSOR_AGENT_ASYNC_TIMEOUT_MS, DEFAULT_ASYNC_TIMEOUT_MS);
  }

  get enabled(): boolean {
    return this.token.length > 0;
  }

  register(app: express.Application): void {
    app.get('/status', (req, res) => {
      if (!this.authorize(req, res)) return;
      const state = this.statusPayload();
      res.type('text').send(formatStatus(state));
    });

    app.get('/execute', async (req, res) => {
      if (!this.authorize(req, res)) return;
      let args: string[];
      let argsText: string;
      try {
        ({ args, argsText } = this.parseArgsRequest(req));
      } catch (err) {
        return res.status(400).type('text').send(errorMessage(err));
      }

      try {
        const result = await this.runCursorAgent(args, DEFAULT_SYNC_TIMEOUT_MS);
        res
          .status(result.exitCode === 0 && !result.timedOut ? 200 : 500)
          .type('text')
          .send(formatRunResult(argsText, result));
      } catch (err) {
        res.status(500).type('text').send(errorMessage(err));
      }
    });

    app.get('/execute_async', (req, res) => {
      if (!this.authorize(req, res)) return;
      let args: string[];
      let argsText: string;
      try {
        ({ args, argsText } = this.parseArgsRequest(req));
      } catch (err) {
        return res.status(400).type('text').send(errorMessage(err));
      }

      const job = this.createJob(argsText, args);
      const superseded = this.supersedeQueuedDuplicates(job);
      this.queue.push(job);
      this.pumpQueue();
      const base = `${req.protocol}://${req.get('host')}`;
      res.type('text').send(
        [
          `Job ${job.id} submitted.`,
          superseded > 0 ? `Superseded ${superseded} older queued duplicate job(s).` : '',
          `status_url: ${base}/jobs/${job.id}?token=${encodeURIComponent(this.token)}`,
          `$ ${this.renderCommand(args)}`,
          `Remember the job id; tell the user "Submitted as ${job.id}. Ask me 'check that job' anytime."`,
        ].filter(Boolean).join('\n')
      );
    });

    app.get('/jobs', (req, res) => {
      if (!this.authorize(req, res)) return;
      this.cancelStaleQueuedJobs();
      const limit = parseLimit(req.query.limit);
      const jobs = Array.from(this.jobs.values())
        .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
        .slice(0, limit);
      if (req.query.format === 'json') {
        return res.json({ jobs: jobs.map(safeJob) });
      }
      res.type('text').send(formatJobList(jobs));
    });

    app.get('/jobs/:id', (req, res) => {
      if (!this.authorize(req, res)) return;
      this.cancelStaleQueuedJobs();
      const job = this.jobs.get(req.params.id);
      if (!job) return res.status(404).type('text').send(`Job not found: ${req.params.id}`);
      if (req.query.format === 'json') {
        return res.json(sliceSafeJob(job, parseSince(req.query.since)));
      }
      res.type('text').send(formatJob(job, parseSince(req.query.since)));
    });

    app.get('/jobs/:id/cancel', (req, res) => {
      if (!this.authorize(req, res)) return;
      this.cancelStaleQueuedJobs();
      const job = this.jobs.get(req.params.id);
      if (!job) return res.status(404).type('text').send(`Job not found: ${req.params.id}`);
      const cancelled = this.cancelJob(job, 'Cancelled by /jobs/:id/cancel');
      res.type('text').send(
        cancelled
          ? `Cancelled ${job.id}.`
          : `Job ${job.id} is already ${job.status}.`
      );
    });

    app.get('/emergency/clear-queue', (req, res) => {
      if (!this.authorize(req, res)) return;
      const cleared = this.queue.splice(0);
      for (const job of cleared) {
        job.status = 'cancelled';
        job.finishedAt = new Date().toISOString();
        job.error = 'Cleared from queue by emergency/clear-queue';
      }
      res.type('text').send(`Cleared ${cleared.length} queued job(s). Running job: ${this.runningJob?.id ?? 'none'}`);
    });

    app.get('/emergency/restart', (req, res) => {
      if (!this.authorize(req, res)) return;
      const cleared = this.queue.splice(0);
      for (const job of cleared) {
        job.status = 'cancelled';
        job.finishedAt = new Date().toISOString();
        job.error = 'Cancelled by emergency/restart';
      }
      const killed = this.runningJob?.id ?? null;
      if (this.runningJob?.process) {
        this.runningJob.error = 'Killed by emergency/restart';
        this.runningJob.process.kill('SIGTERM');
      }
      res.type('text').send(`Restarted bridge worker. Cancelled queued: ${cleared.length}. Killed running: ${killed ?? 'none'}`);
    });

    console.log(
      this.enabled
        ? '[cursor-agent-bridge] Enabled HTTP endpoints: /execute, /execute_async, /jobs'
        : '[cursor-agent-bridge] Disabled; set CURSOR_BRIDGE_TOKEN to enable /execute endpoints'
    );
  }

  private authorize(req: express.Request, res: express.Response): boolean {
    if (!this.enabled) {
      res.status(503).type('text').send('Cursor agent bridge disabled. Set CURSOR_BRIDGE_TOKEN.');
      return false;
    }
    const supplied = typeof req.query.token === 'string' ? req.query.token : '';
    if (supplied !== this.token) {
      res.status(401).type('text').send('Unauthorized');
      return false;
    }
    return true;
  }

  private parseArgsRequest(req: express.Request): { args: string[]; argsText: string } {
    const cmd = typeof req.query.cmd === 'string' ? req.query.cmd : '';
    const b64 = typeof req.query.b64 === 'string' ? req.query.b64 : '';
    if (!cmd && !b64) throw new Error('Missing cmd or b64');
    const argsText = b64 ? Buffer.from(b64, 'base64').toString('utf-8') : cmd;
    const args = parseShellWords(argsText);
    if (args.length === 0) throw new Error('No cursor-agent args parsed');
    return { args, argsText };
  }

  private createJob(argsText: string, args: string[]): BridgeJob {
    const id = `j-${randomBytes(4).toString('hex')}`;
    const job: BridgeJob = {
      id,
      argsText,
      args,
      status: 'queued',
      createdAt: new Date().toISOString(),
      stdout: '',
      stderr: '',
    };
    this.jobs.set(id, job);
    return job;
  }

  private supersedeQueuedDuplicates(newJob: BridgeJob): number {
    let superseded = 0;
    this.queue = this.queue.filter(job => {
      if (job.status === 'queued' && sameArgs(job.args, newJob.args)) {
        job.status = 'cancelled';
        job.finishedAt = new Date().toISOString();
        job.error = `Superseded by duplicate job ${newJob.id}`;
        superseded += 1;
        return false;
      }
      return true;
    });
    return superseded;
  }

  private cancelStaleQueuedJobs(): void {
    const now = Date.now();
    this.queue = this.queue.filter(job => {
      const createdAt = Date.parse(job.createdAt);
      if (Number.isFinite(createdAt) && now - createdAt > STALE_QUEUED_JOB_MS) {
        job.status = 'cancelled';
        job.finishedAt = new Date().toISOString();
        job.error = 'Auto-cancelled after 30 minutes in queue';
        return false;
      }
      return true;
    });
  }

  private cancelJob(job: BridgeJob, reason: string): boolean {
    if (job.status === 'queued') {
      this.queue = this.queue.filter(queued => queued.id !== job.id);
      job.status = 'cancelled';
      job.finishedAt = new Date().toISOString();
      job.error = reason;
      return true;
    }
    if (job.status === 'running' && job.process) {
      job.status = 'cancelled';
      job.finishedAt = new Date().toISOString();
      job.error = reason;
      job.process.kill('SIGTERM');
      return true;
    }
    return false;
  }

  private pumpQueue(): void {
    this.cancelStaleQueuedJobs();
    if (this.runningJob || this.queue.length === 0) return;
    const job = this.queue.shift()!;
    this.runningJob = job;
    job.status = 'running';
    job.startedAt = new Date().toISOString();

    const child = spawn(this.bin, [...this.baseArgs, ...job.args], {
      env: process.env,
      cwd: process.cwd(),
    });
    job.process = child;
    const timer = setTimeout(() => {
      if (job.status === 'running') {
        job.status = 'timeout';
        job.error = `Timed out after ${this.asyncTimeoutMs}ms`;
        child.kill('SIGTERM');
      }
    }, this.asyncTimeoutMs);
    timer.unref?.();

    child.stdout.on('data', chunk => {
      job.stdout = appendBounded(job.stdout, chunk.toString());
    });
    child.stderr.on('data', chunk => {
      job.stderr = appendBounded(job.stderr, chunk.toString());
    });
    child.on('error', err => {
      clearTimeout(timer);
      job.status = 'error';
      job.error = err.message;
      job.finishedAt = new Date().toISOString();
      this.runningJob = null;
      this.pumpQueue();
    });
    child.on('close', (code, signal) => {
      clearTimeout(timer);
      job.exitCode = code;
      job.signal = signal;
      if (job.status !== 'error' && job.status !== 'cancelled' && job.status !== 'timeout') {
        job.status = code === 0 ? 'done' : 'error';
      }
      job.finishedAt = new Date().toISOString();
      job.process = undefined;
      this.runningJob = null;
      this.pumpQueue();
    });
  }

  private runCursorAgent(args: string[], timeoutMs: number): Promise<RunResult> {
    return new Promise((resolve, reject) => {
      let stdout = '';
      let stderr = '';
      let timedOut = false;
      const child = spawn(this.bin, [...this.baseArgs, ...args], {
        env: process.env,
        cwd: process.cwd(),
      });
      const timer = setTimeout(() => {
        timedOut = true;
        child.kill('SIGTERM');
      }, timeoutMs);
      timer.unref?.();

      child.stdout.on('data', chunk => {
        stdout = appendBounded(stdout, chunk.toString());
      });
      child.stderr.on('data', chunk => {
        stderr = appendBounded(stderr, chunk.toString());
      });
      child.on('error', err => {
        clearTimeout(timer);
        reject(err);
      });
      child.on('close', (exitCode, signal) => {
        clearTimeout(timer);
        resolve({ stdout, stderr, exitCode, signal, timedOut });
      });
    });
  }

  private statusPayload(): { enabled: boolean; running: string | null; queued: number; jobs: number } {
    return {
      enabled: this.enabled,
      running: this.runningJob?.id ?? null,
      queued: this.queue.length,
      jobs: this.jobs.size,
    };
  }

  private renderCommand(args: string[]): string {
    return [this.bin, ...this.baseArgs, ...args].map(shellQuote).join(' ');
  }
}

export function parseShellWords(input: string): string[] {
  const words: string[] = [];
  let cur = '';
  let quote: '"' | "'" | null = null;
  let escaping = false;
  let started = false;

  for (const ch of input) {
    if (escaping) {
      cur += ch;
      escaping = false;
      started = true;
      continue;
    }
    if (ch === '\\' && quote !== "'") {
      escaping = true;
      started = true;
      continue;
    }
    if ((ch === '"' || ch === "'") && quote === null) {
      quote = ch;
      started = true;
      continue;
    }
    if (ch === quote) {
      quote = null;
      started = true;
      continue;
    }
    if (/\s/.test(ch) && quote === null) {
      if (started) {
        words.push(cur);
        cur = '';
        started = false;
      }
      continue;
    }
    cur += ch;
    started = true;
  }

  if (escaping) cur += '\\';
  if (quote) throw new Error(`Unclosed ${quote} quote`);
  if (started) words.push(cur);
  return words;
}

function appendBounded(current: string, next: string): string {
  const combined = current + next;
  return combined.length > MAX_BUFFER_CHARS ? combined.slice(combined.length - MAX_BUFFER_CHARS) : combined;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function safeJob(job: BridgeJob): Omit<BridgeJob, 'process'> {
  const { process: _process, ...safe } = job;
  return safe;
}

function sliceSafeJob(job: BridgeJob, since: number): Omit<BridgeJob, 'process'> & { stdoutOffset: number; stderrOffset: number } {
  const safe = safeJob(job);
  return {
    ...safe,
    stdout: sliceUtf8ByByteOffset(job.stdout, since),
    stderr: sliceUtf8ByByteOffset(job.stderr, since),
    stdoutOffset: Buffer.byteLength(job.stdout),
    stderrOffset: Buffer.byteLength(job.stderr),
  };
}

function formatStatus(state: { enabled: boolean; running: string | null; queued: number; jobs: number }): string {
  return [
    'Cursor agent bridge status',
    `enabled: ${state.enabled}`,
    `running: ${state.running ?? 'none'}`,
    `queued: ${state.queued}`,
    `jobs: ${state.jobs}`,
  ].join('\n');
}

function formatJobList(jobs: BridgeJob[]): string {
  if (jobs.length === 0) return 'No jobs.';
  return jobs.map(job => `${job.id}\t${job.status}\t${job.createdAt}\t${job.argsText}`).join('\n');
}

function formatJob(job: BridgeJob, since = 0): string {
  const stdoutOffset = Buffer.byteLength(job.stdout);
  const stderrOffset = Buffer.byteLength(job.stderr);
  return [
    `Job ${job.id}`,
    `status: ${job.status}`,
    `created_at: ${job.createdAt}`,
    job.startedAt ? `started_at: ${job.startedAt}` : '',
    job.finishedAt ? `finished_at: ${job.finishedAt}` : '',
    job.exitCode !== undefined ? `exit_code: ${job.exitCode}` : '',
    job.signal ? `signal: ${job.signal}` : '',
    job.error ? `error: ${job.error}` : '',
    `stdout_offset: ${stdoutOffset}`,
    `stderr_offset: ${stderrOffset}`,
    `$ cursor-agent ${job.argsText}`,
    '--- stdout ---',
    sliceUtf8ByByteOffset(job.stdout, since),
    '--- stderr ---',
    sliceUtf8ByByteOffset(job.stderr, since),
  ].filter(line => line !== '').join('\n');
}

function formatRunResult(argsText: string, result: RunResult): string {
  return [
    `$ cursor-agent ${argsText}`,
    `exit_code: ${result.exitCode}`,
    result.signal ? `signal: ${result.signal}` : '',
    result.timedOut ? 'timed_out: true' : '',
    '--- stdout ---',
    result.stdout,
    '--- stderr ---',
    result.stderr,
  ].filter(line => line !== '').join('\n');
}

function shellQuote(word: string): string {
  if (/^[A-Za-z0-9_./:=@%+-]+$/.test(word)) return word;
  return `'${word.replace(/'/g, "'\\''")}'`;
}

function sameArgs(a: string[], b: string[]): boolean {
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseLimit(value: unknown): number {
  if (typeof value !== 'string') return 20;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return 20;
  return Math.min(parsed, MAX_LIST_LIMIT);
}

function parseSince(value: unknown): number {
  if (typeof value !== 'string') return 0;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function sliceUtf8ByByteOffset(value: string, since: number): string {
  if (since <= 0) return value;
  return Buffer.from(value, 'utf-8').slice(since).toString('utf-8');
}
