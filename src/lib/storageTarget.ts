import fs from 'node:fs';
import path from 'node:path';
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { Config } from './config.js';
import { LocalCsvSink, S3CsvSink, type CsvSink, type SinkStats } from './csvSink.js';
import { withRetry, type WarnFn } from './retry.js';
import { defaultState, loadState, parseState, saveState, type EtlState } from './state.js';

export type ManifestEntry = {
  storageId: string;
  total: number;
  written: number;
  sink: SinkStats;
};

export type Manifest = {
  schemaVersion: 1;
  runId: string;
  mode: 'full' | 'incremental';
  generatedAt: string;
  entries: ManifestEntry[];
  totals: { files: number; rows: number; bytes: number };
};

// A crashed run (e.g. SIGKILL mid-flight) leaves its lock behind; locks older
// than this are considered stale and stolen. Must exceed the longest plausible run.
export const LOCK_TTL_MS = 6 * 60 * 60 * 1000;

type RunLockBody = { runId: string; startedAt: string };

function lockIsStale(body: string | undefined): boolean {
  if (!body) return true;
  try {
    const lock = JSON.parse(body) as Partial<RunLockBody>;
    const startedAt = Date.parse(lock.startedAt ?? '');
    return !Number.isFinite(startedAt) || Date.now() - startedAt > LOCK_TTL_MS;
  } catch {
    return true;
  }
}

export function buildManifest(cfg: Pick<Config, 'runId' | 'mode'>, entries: ManifestEntry[]): Manifest {
  return {
    schemaVersion: 1,
    runId: cfg.runId,
    mode: cfg.mode,
    generatedAt: new Date().toISOString(),
    entries,
    totals: {
      files: entries.length,
      rows: entries.reduce((s, e) => s + e.written, 0),
      bytes: entries.reduce((s, e) => s + e.sink.bytes, 0),
    },
  };
}

export interface StorageTarget {
  // Local full loads resume by skipping completed canisters; S3 runs write to a
  // fresh runId prefix, so skipping would leave holes in that run's file set.
  readonly supportsResumeSkip: boolean;
  loadState(): Promise<EtlState>;
  saveState(state: EtlState): Promise<void>;
  createSink(fileName: string): CsvSink;
  writeManifest(manifest: Manifest): Promise<void>;
  acquireRunLock(): Promise<boolean>;
  releaseRunLock(): Promise<void>;
}

export class LocalTarget implements StorageTarget {
  readonly supportsResumeSkip = true;
  private readonly outDir: string;
  private readonly stateFile: string;
  private readonly lockFile: string;
  private readonly runId: string;

  constructor(cfg: Pick<Config, 'outDir' | 'stateFile' | 'runId'>) {
    this.outDir = cfg.outDir;
    this.stateFile = cfg.stateFile;
    this.lockFile = path.join(path.dirname(cfg.stateFile), 'run.lock');
    this.runId = cfg.runId;
    fs.mkdirSync(this.outDir, { recursive: true });
    fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
  }

  async loadState(): Promise<EtlState> {
    return loadState(this.stateFile);
  }

  async saveState(state: EtlState): Promise<void> {
    saveState(this.stateFile, state);
  }

  createSink(fileName: string): CsvSink {
    return new LocalCsvSink(path.join(this.outDir, fileName));
  }

  async writeManifest(manifest: Manifest): Promise<void> {
    const file = path.join(this.outDir, `manifest_${manifest.mode}_${manifest.runId}.json`);
    fs.writeFileSync(file, `${JSON.stringify(manifest, null, 2)}\n`);
  }

  async acquireRunLock(): Promise<boolean> {
    const body = JSON.stringify({ runId: this.runId, startedAt: new Date().toISOString() } satisfies RunLockBody);
    try {
      fs.writeFileSync(this.lockFile, body, { flag: 'wx' });
      return true;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
      const existing = fs.existsSync(this.lockFile) ? fs.readFileSync(this.lockFile, 'utf8') : undefined;
      if (!lockIsStale(existing)) return false;
      fs.writeFileSync(this.lockFile, body);
      return true;
    }
  }

  async releaseRunLock(): Promise<void> {
    fs.rmSync(this.lockFile, { force: true });
  }
}

export class S3Target implements StorageTarget {
  readonly supportsResumeSkip = false;
  private readonly bucket: string;
  private readonly prefix: string;
  private readonly mode: 'full' | 'incremental';
  private readonly runId: string;

  constructor(
    private readonly s3: S3Client,
    cfg: Pick<Config, 's3Bucket' | 's3Prefix' | 'mode' | 'runId'>,
    private readonly warn: WarnFn = console.warn,
  ) {
    if (!cfg.s3Bucket) throw new Error('S3Target requires s3Bucket');
    this.bucket = cfg.s3Bucket;
    this.prefix = cfg.s3Prefix;
    this.mode = cfg.mode;
    this.runId = cfg.runId;
  }

  private get stateKey(): string {
    return `${this.prefix}/state/state.json`;
  }

  private get lockKey(): string {
    return `${this.prefix}/locks/run.lock`;
  }

  private runKey(fileName: string): string {
    return `${this.prefix}/${this.mode}/${this.runId}/${fileName}`;
  }

  private async readText(key: string): Promise<string | undefined> {
    try {
      const res = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
      return await res.Body?.transformToString();
    } catch (err) {
      if ((err as { name?: string }).name === 'NoSuchKey') return undefined;
      throw err;
    }
  }

  async loadState(): Promise<EtlState> {
    const body = await this.readText(this.stateKey);
    return body ? parseState(body) : defaultState();
  }

  async saveState(state: EtlState): Promise<void> {
    const body = `${JSON.stringify(state, null, 2)}\n`;
    await withRetry(
      () =>
        this.s3.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: this.stateKey,
            Body: body,
            ContentType: 'application/json',
          })
        ),
      'saveState',
      this.warn
    );
  }

  createSink(fileName: string): CsvSink {
    const key = this.runKey(fileName);
    return new S3CsvSink(this.s3, this.bucket, key, `s3://${this.bucket}/${key}`);
  }

  async writeManifest(manifest: Manifest): Promise<void> {
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: this.runKey('manifest.json'),
        Body: `${JSON.stringify(manifest, null, 2)}\n`,
        ContentType: 'application/json',
      })
    );
  }

  async acquireRunLock(): Promise<boolean> {
    const body = JSON.stringify({ runId: this.runId, startedAt: new Date().toISOString() } satisfies RunLockBody);
    const putLock = (conditional: boolean) =>
      this.s3.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: this.lockKey,
          Body: body,
          ContentType: 'application/json',
          ...(conditional ? { IfNoneMatch: '*' } : {}),
        })
      );

    try {
      await putLock(true);
      return true;
    } catch (err) {
      const name = (err as { name?: string }).name;
      if (name !== 'PreconditionFailed' && name !== 'ConditionalRequestConflict') throw err;
      const existing = await this.readText(this.lockKey);
      if (!lockIsStale(existing)) return false;
      await putLock(false);
      return true;
    }
  }

  async releaseRunLock(): Promise<void> {
    try {
      await this.s3.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: this.lockKey }));
    } catch (err) {
      this.warn(`failed to release run lock: ${String((err as Error)?.message || err)}`);
    }
  }
}

export function createStorageTarget(cfg: Config, warn: WarnFn = console.warn): StorageTarget {
  return cfg.s3Bucket ? new S3Target(new S3Client({}), cfg, warn) : new LocalTarget(cfg);
}
