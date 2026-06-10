import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { S3Client } from '@aws-sdk/client-s3';
import type { Config } from '../src/lib/config.js';
import { defaultState } from '../src/lib/state.js';
import { LOCK_TTL_MS, LocalTarget, S3Target, buildManifest, type ManifestEntry } from '../src/lib/storageTarget.js';

function makeCfg(overrides: Partial<Config> = {}): Config {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icpswap-extractor-target-'));
  return {
    host: 'https://ic0.app',
    baseStorageCanisterId: 'g54jq-hiaaa-aaaag-qck5q-cai',
    pageSize: 1000,
    mode: 'full',
    outDir: dir,
    stateFile: path.join(dir, 'state.json'),
    logFile: path.join(dir, 'etl.log'),
    overlap: 50,
    concurrency: 5,
    s3Bucket: undefined,
    s3Prefix: 'icpswap',
    runId: 'run-1',
    ...overrides,
  };
}

type SentCommand = { name: string; input: Record<string, unknown> };

function makeFakeS3(handlers: Record<string, (input: Record<string, unknown>) => unknown> = {}) {
  const sent: SentCommand[] = [];
  const client = {
    send: async (cmd: { constructor: { name: string }; input: Record<string, unknown> }) => {
      const name = cmd.constructor.name;
      sent.push({ name, input: cmd.input });
      const handler = handlers[name];
      if (handler) return handler(cmd.input);
      return {};
    },
  };
  return { sent, client: client as unknown as S3Client };
}

function sampleEntries(): ManifestEntry[] {
  return [
    { storageId: 'a', total: 10, written: 10, sink: { location: 'x', rows: 10, bytes: 100, sha256: 'h1' } },
    { storageId: 'b', total: 5, written: 4, sink: { location: 'y', rows: 4, bytes: 50, sha256: 'h2' } },
  ];
}

test('buildManifest sums totals and stamps schemaVersion', () => {
  const manifest = buildManifest({ runId: 'run-1', mode: 'full' }, sampleEntries());

  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.runId, 'run-1');
  assert.equal(manifest.mode, 'full');
  assert.equal(manifest.totals.files, 2);
  assert.equal(manifest.totals.rows, 14);
  assert.equal(manifest.totals.bytes, 150);
  assert.ok(manifest.generatedAt);
});

test('LocalTarget round-trips state and reports missing state as default', async () => {
  const target = new LocalTarget(makeCfg());

  assert.deepEqual(await target.loadState(), defaultState());

  const state = defaultState();
  state.canisters['c1'] = { lastTotal: 9, recentHashes: ['h'], completed: false };
  await target.saveState(state);

  assert.deepEqual(await target.loadState(), state);
});

test('LocalTarget writes the manifest into outDir', async () => {
  const cfg = makeCfg();
  const target = new LocalTarget(cfg);

  await target.writeManifest(buildManifest(cfg, sampleEntries()));

  const file = path.join(cfg.outDir, 'manifest_full_run-1.json');
  const manifest = JSON.parse(fs.readFileSync(file, 'utf8'));
  assert.equal(manifest.totals.rows, 14);
});

test('S3Target saves state under <prefix>/state/state.json', async () => {
  const { sent, client } = makeFakeS3();
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  const state = defaultState();
  await target.saveState(state);

  assert.equal(sent.length, 1);
  assert.equal(sent[0].name, 'PutObjectCommand');
  assert.equal(sent[0].input.Bucket, 'bkt');
  assert.equal(sent[0].input.Key, 'icpswap/state/state.json');
  assert.deepEqual(JSON.parse(String(sent[0].input.Body)), state);
});

test('S3Target returns defaultState when the state object is missing', async () => {
  const { client } = makeFakeS3({
    GetObjectCommand: () => {
      throw Object.assign(new Error('no such key'), { name: 'NoSuchKey' });
    },
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.deepEqual(await target.loadState(), defaultState());
});

test('S3Target parses state fetched from S3', async () => {
  const stored = defaultState();
  stored.latestStorageId = 'zzz';
  const { client } = makeFakeS3({
    GetObjectCommand: () => ({ Body: { transformToString: async () => JSON.stringify(stored) } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  const loaded = await target.loadState();
  assert.equal(loaded.latestStorageId, 'zzz');
});

test('S3Target writes the manifest under the runId prefix', async () => {
  const { sent, client } = makeFakeS3();
  const cfg = makeCfg({ s3Bucket: 'bkt', mode: 'incremental' });
  const target = new S3Target(client, cfg);

  await target.writeManifest(buildManifest(cfg, sampleEntries()));

  assert.equal(sent[0].name, 'PutObjectCommand');
  assert.equal(sent[0].input.Key, 'icpswap/incremental/run-1/manifest.json');
});

test('LocalTarget lock: acquire, conflict, release, reacquire', async () => {
  const cfg = makeCfg();
  const first = new LocalTarget(cfg);
  const second = new LocalTarget(cfg);

  assert.equal(await first.acquireRunLock(), true);
  assert.equal(await second.acquireRunLock(), false);

  await first.releaseRunLock();
  assert.equal(await second.acquireRunLock(), true);
});

test('LocalTarget lock: steals a stale lock past the TTL', async () => {
  const cfg = makeCfg();
  const lockFile = path.join(path.dirname(cfg.stateFile), 'run.lock');
  fs.writeFileSync(
    lockFile,
    JSON.stringify({ runId: 'old', startedAt: new Date(Date.now() - LOCK_TTL_MS - 60_000).toISOString() })
  );

  const target = new LocalTarget(cfg);
  assert.equal(await target.acquireRunLock(), true);
});

test('S3Target lock: acquire uses a conditional create', async () => {
  const { sent, client } = makeFakeS3();
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.equal(await target.acquireRunLock(), true);
  assert.equal(sent[0].name, 'PutObjectCommand');
  assert.equal(sent[0].input.Key, 'icpswap/locks/run.lock');
  assert.equal(sent[0].input.IfNoneMatch, '*');
});

test('S3Target lock: fresh lock held by another run is respected', async () => {
  const freshLock = JSON.stringify({ runId: 'other', startedAt: new Date().toISOString() });
  const { client } = makeFakeS3({
    PutObjectCommand: (input) => {
      if (input.IfNoneMatch) throw Object.assign(new Error('held'), { name: 'PreconditionFailed' });
      return {};
    },
    GetObjectCommand: () => ({ Body: { transformToString: async () => freshLock } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.equal(await target.acquireRunLock(), false);
});

test('S3Target lock: stale lock is stolen with an unconditional overwrite', async () => {
  const staleLock = JSON.stringify({
    runId: 'old',
    startedAt: new Date(Date.now() - LOCK_TTL_MS - 60_000).toISOString(),
  });
  const { sent, client } = makeFakeS3({
    PutObjectCommand: (input) => {
      if (input.IfNoneMatch) throw Object.assign(new Error('held'), { name: 'PreconditionFailed' });
      return {};
    },
    GetObjectCommand: () => ({ Body: { transformToString: async () => staleLock } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.equal(await target.acquireRunLock(), true);
  const lastPut = sent.filter((c) => c.name === 'PutObjectCommand').at(-1)!;
  assert.equal(lastPut.input.IfNoneMatch, undefined);
});

test('S3Target lock: release deletes the lock object', async () => {
  const { sent, client } = makeFakeS3();
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  await target.acquireRunLock();
  await target.releaseRunLock();

  const last = sent.at(-1)!;
  assert.equal(last.name, 'DeleteObjectCommand');
  assert.equal(last.input.Key, 'icpswap/locks/run.lock');
});
