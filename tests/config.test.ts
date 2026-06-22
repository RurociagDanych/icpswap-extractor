import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseArgs } from '../src/lib/config.js';

// Make sure ambient env vars don't leak into default-value assertions.
delete process.env.S3_BUCKET;
delete process.env.S3_PREFIX;

test('parseArgs applies defaults', () => {
  const cfg = parseArgs([]);

  assert.equal(cfg.mode, 'sync');
  assert.equal(cfg.host, 'https://ic0.app');
  assert.equal(cfg.pageSize, 1000);
  assert.equal(cfg.overlap, 50);
  assert.equal(cfg.concurrency, 5);
  assert.equal(cfg.s3Bucket, undefined);
  assert.equal(cfg.s3Prefix, 'icpswap');
});

test('parseArgs reads CLI flags', () => {
  const cfg = parseArgs([
    '--mode', 'incremental',
    '--page-size', '500',
    '--overlap', '10',
    '--concurrency', '2',
    '--s3-bucket', 'my-bucket',
    '--s3-prefix', 'custom',
  ]);

  assert.equal(cfg.mode, 'incremental');
  assert.equal(cfg.pageSize, 500);
  assert.equal(cfg.overlap, 10);
  assert.equal(cfg.concurrency, 2);
  assert.equal(cfg.s3Bucket, 'my-bucket');
  assert.equal(cfg.s3Prefix, 'custom');
});

test('parseArgs rejects page-size outside 1..1000', () => {
  assert.throws(() => parseArgs(['--page-size', '0']), /page-size/);
  assert.throws(() => parseArgs(['--page-size', '1001']), /page-size/);
  assert.throws(() => parseArgs(['--page-size', 'abc']), /page-size/);
});

test('parseArgs rejects concurrency outside 1..20', () => {
  assert.throws(() => parseArgs(['--concurrency', '0']), /concurrency/);
  assert.throws(() => parseArgs(['--concurrency', '21']), /concurrency/);
});

test('parseArgs rejects negative overlap', () => {
  assert.throws(() => parseArgs(['--overlap', '-1']), /overlap/);
});

test('parseArgs reads REST flags and defaults mode to sync', () => {
  const cfg = parseArgs([]);
  assert.equal(cfg.mode, 'sync');
  assert.equal(cfg.restBaseUrl, 'https://api.icpswap.com/info');
  assert.equal(cfg.restPageSize, 100);
  assert.equal(cfg.backfillOverlapMs, 3600000);
  assert.equal(cfg.incrementalOverlapMs, 300000);
  assert.equal(cfg.actionTypes, 'Swap,AddLiquidity,DecreaseLiquidity,Claim');
  assert.equal(cfg.backfillFloor, undefined);
});

test('parseArgs parses explicit REST flags', () => {
  const cfg = parseArgs([
    '--mode', 'backfill',
    '--rest-page-size', '50',
    '--backfill-overlap-ms', '60000',
    '--backfill-floor', '1749254400000',
  ]);
  assert.equal(cfg.mode, 'backfill');
  assert.equal(cfg.restPageSize, 50);
  assert.equal(cfg.backfillOverlapMs, 60000);
  assert.equal(cfg.backfillFloor, 1749254400000);
});

test('parseArgs rejects rest-page-size above 100', () => {
  assert.throws(() => parseArgs(['--rest-page-size', '101']), /rest-page-size/);
});
