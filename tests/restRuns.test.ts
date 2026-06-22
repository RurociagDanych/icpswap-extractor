import assert from 'node:assert/strict';
import { test } from 'node:test';
import { runBackfill, runIncrementalRest, type RestDeps } from '../src/lib/restRuns.js';
import { defaultState, type EtlState } from '../src/lib/state.js';
import type { RestFindData, RestTx } from '../src/idl/restTx.js';

const cfg: any = {
  runId: 'r1', mode: 'backfill', restBaseUrl: 'http://x', restPageSize: 2,
  backfillOverlapMs: 0, incrementalOverlapMs: 0, actionTypes: 'Swap',
  backfillPagesPerFile: 50,
};

function fakeTarget() {
  const sinks: { columns: readonly string[]; rows: any[]; closed: boolean }[] = [];
  let saved: EtlState | undefined;
  const target: any = {
    supportsResumeSkip: false,
    async loadState() { return saved ?? defaultState(); },
    async saveState(s: EtlState) { saved = JSON.parse(JSON.stringify(s)); },
    createSink(_f: string, columns: readonly string[]) {
      const sink = { columns, rows: [] as any[], closed: false,
        async append(rs: any[]) { sink.rows.push(...rs); },
        async close() { sink.closed = true; return { location: 'l', rows: sink.rows.length, bytes: 1, sha256: 'x' }; } };
      sinks.push(sink); return sink;
    },
    async writeManifest() {},
  };
  return { target, sinks, getSaved: () => saved };
}

const tx = (h: string, t: number): RestTx => ({ txHash: h, txTime: t } as RestTx);
const logger: any = { info() {}, warn() {}, error() {} };

test('runBackfill pages newest->oldest and stops below floor', async () => {
  // canisterMaxTxTime=100, overlap=0 => floor=100
  const pages: Record<number, RestTx[]> = {
    1: [tx('a', 300), tx('b', 250)],
    2: [tx('c', 200), tx('d', 90)], // 90 < floor 100 -> stop after this page
  };
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => ({ totalElements: 4, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData),
  };
  const state = defaultState();
  state.canisterMaxTxTime = 100;
  const { target, sinks, getSaved } = fakeTarget();
  await runBackfill(cfg, target, logger, state, deps);

  assert.equal(sinks[0].rows.length, 4);             // all 4 rows emitted
  assert.equal(sinks[0].columns[0], 'tx_hash');      // REST headers used
  assert.equal(getSaved()?.rest?.backfillComplete, true);
  assert.equal(getSaved()?.rest?.incrementalWatermark, 300); // seeded to newest captured txTime
  assert.equal(sinks.length, 1); // 2 pages fit in one file at pagesPerFile=50
});

test('runBackfill rotates files and advances the cursor only after each commit', async () => {
  const pages: Record<number, RestTx[]> = {
    1: [tx('a', 300), tx('b', 250)],
    2: [tx('c', 200), tx('d', 90)], // 90 < floor 100 -> stop after this page
  };
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => ({ totalElements: 4, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData),
  };
  const state = defaultState();
  state.canisterMaxTxTime = 100;
  const { target, sinks, getSaved } = fakeTarget();
  await runBackfill({ ...cfg, backfillPagesPerFile: 1 }, target, logger, state, deps);

  assert.equal(sinks.length, 2);            // one file per page
  assert.equal(sinks[0].rows.length, 2);
  assert.equal(sinks[1].rows.length, 2);
  assert.equal(sinks[0].closed, true);      // each file committed
  assert.equal(getSaved()?.rest?.backfillComplete, true);
  assert.equal(getSaved()?.rest?.backfillCursor?.nextPage, 3); // cursor past both committed pages
});

test('runIncrementalRest emits only rows past the watermark and dedups', async () => {
  const pages: Record<number, RestTx[]> = {
    1: [tx('z', 500), tx('y', 400)],
    2: [tx('x', 300), tx('w', 50)], // 50 <= watermark(200)-0 -> stop
  };
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => ({ totalElements: 4, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData),
  };
  const state = defaultState();
  state.rest = { backfillComplete: true, incrementalWatermark: 200, recentTxHashes: ['x'] };
  const { target, sinks, getSaved } = fakeTarget();
  await runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, state, deps);

  // x is filtered (known hash); w is emitted as a new hash on the fetched page.
  assert.deepEqual(sinks[0].rows.map((r) => r.tx_hash), ['z', 'y', 'w']);
  assert.equal(getSaved()?.rest?.incrementalWatermark, 500);
});
