import { Actor, HttpAgent } from '@dfinity/agent';
import { baseStorageidlFactory, type BaseStorageActor } from './idl/baseStorage.js';
import { poolInfoidlFactory, type PoolInfoActor, type SwapTx } from './idl/poolInfo.js';
import { parseArgs, type Config } from './lib/config.js';
import { headers as canisterHeaders, rowFromTx } from './lib/csv.js';
import type { CsvSink, SinkStats } from './lib/csvSink.js';
import { createLogger, type Logger } from './lib/logger.js';
import { fetchTransactions } from './lib/restClient.js';
import { runBackfill, runIncrementalRest, runSync, type RestDeps } from './lib/restRuns.js';
import { withRetry, type WarnFn } from './lib/retry.js';
import {
  pushBounded,
  type CanisterState,
  type EtlState,
} from './lib/state.js';
import { buildManifest, createStorageTarget, type ManifestEntry, type StorageTarget } from './lib/storageTarget.js';

async function getActor<T>(agent: HttpAgent, canisterId: string, factory: unknown): Promise<T> {
  return Actor.createActor(factory as any, { agent, canisterId }) as T;
}

async function fetchCanisterFull(
  agent: HttpAgent,
  storageId: string,
  pageSize: number,
  sink: CsvSink,
  writeHeader: boolean,
  logger: Logger,
): Promise<{ written: number; total: number; recentHashes: string[]; sinkStats: SinkStats; maxTimestampNs: bigint }> {
  const storage = await getActor<PoolInfoActor>(agent, storageId, poolInfoidlFactory);
  const warn: WarnFn = (msg) => logger.warn(msg, { storageId });

  const head = await withRetry(
    () => storage.getBaseRecord(0n, 1n, []),
    `${storageId} head`,
    warn
  );
  const total = Number(head.totalElements);
  logger.info('canister total', { storageId, total });

  if (total === 0) {
    if (writeHeader) await sink.append([], true);
    const sinkStats = await sink.close();
    return { written: 0, total, recentHashes: [], sinkStats, maxTimestampNs: 0n };
  }

  let written = 0;
  let offset = 0;
  let headerPending = writeHeader;
  let maxTimestampNs = 0n;
  const recentHashes: string[] = [];

  while (offset < total) {
    const batchSize = Math.min(pageSize, total - offset);
    const page = await withRetry(
      () => storage.getBaseRecord(BigInt(offset), BigInt(batchSize), []),
      `${storageId} offset=${offset}`,
      warn
    );
    const txs = page.content || [];
    if (!txs.length) break;

    const rows = txs.map((tx: SwapTx) => rowFromTx(storageId, tx));
    await sink.append(rows, headerPending);
    headerPending = false;
    written += rows.length;

    pushBounded(recentHashes, txs.filter((tx) => tx.hash).map((tx) => tx.hash));
    for (const tx of txs) if (tx.timestamp > maxTimestampNs) maxTimestampNs = tx.timestamp;

    if (written % (pageSize * 5) === 0 || written >= total) {
      logger.info('canister progress', { storageId, written, total, pct: Number(((written / total) * 100).toFixed(1)) });
    }

    offset += txs.length;
  }

  const sinkStats = await sink.close();
  return { written, total, recentHashes, sinkStats, maxTimestampNs };
}

async function parallelLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let nextIdx = 0;

  async function worker() {
    while (nextIdx < tasks.length) {
      const idx = nextIdx++;
      results[idx] = await tasks[idx]();
    }
  }

  const workers = Array.from({ length: Math.min(limit, tasks.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

async function runFull(
  cfg: Config,
  agent: HttpAgent,
  target: StorageTarget,
  logger: Logger,
  state: EtlState,
  orderedStorages: string[],
  newestStorageId: string,
): Promise<void> {
  logger.info('starting full load', { canisters: orderedStorages.length, pageSize: cfg.pageSize, concurrency: cfg.concurrency });

  const manifestEntries: ManifestEntry[] = [];
  let maxCanisterTsNs = 0n;
  let saveQueue: Promise<void> = Promise.resolve();
  const queueStateSave = async () => {
    saveQueue = saveQueue.then(() => target.saveState(state));
    await saveQueue;
  };

  const tasks = orderedStorages.map((storageId, idx) => async () => {
    const fileName = `${String(idx).padStart(4, '0')}_${storageId}.csv`;
    const markedCompleted = state.canisters[storageId]?.completed === true;

    if (target.supportsResumeSkip && markedCompleted) {
      logger.info('skipping completed canister', { storageId });
      return;
    }

    logger.info('starting canister', { storageId, index: idx + 1, of: orderedStorages.length });
    const sink = target.createSink(fileName, canisterHeaders);

    const result = await fetchCanisterFull(agent, storageId, cfg.pageSize, sink, true, logger);

    const canState: CanisterState = {
      lastTotal: result.total,
      lastRun: new Date().toISOString(),
      recentHashes: result.recentHashes,
      completed: storageId !== newestStorageId,
    };
    state.canisters[storageId] = canState;
    state.lastRunAt = canState.lastRun;
    state.latestStorageId = newestStorageId;
    if (result.maxTimestampNs > maxCanisterTsNs) maxCanisterTsNs = result.maxTimestampNs;
    manifestEntries.push({
      storageId,
      total: result.total,
      written: result.written,
      sink: result.sinkStats,
    });
    await queueStateSave();
  });

  await parallelLimit(tasks, cfg.concurrency);
  const totalRows = manifestEntries.reduce((sum, entry) => sum + entry.written, 0);

  state.mode = 'full';
  state.latestStorageId = newestStorageId;
  state.lastRunAt = new Date().toISOString();
  // Record the canister↔REST seam (timestamps are nanoseconds) and mark the
  // one-time archive complete so future runs use only backfill + REST incremental.
  if (maxCanisterTsNs > 0n) state.canisterMaxTxTime = Number(maxCanisterTsNs / 1_000_000n);
  state.canisterArchiveComplete = true;
  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, manifestEntries));

  logger.info('full load done', { rows: totalRows, files: manifestEntries.length, newestStorageId, canisterMaxTxTime: state.canisterMaxTxTime });
}

async function run(): Promise<void> {
  const cfg = parseArgs();
  const logger = createLogger({
    base: { runId: cfg.runId, mode: cfg.mode },
    logFile: cfg.s3Bucket ? undefined : cfg.logFile,
  });
  const warn: WarnFn = (msg) => logger.warn(msg);
  const target = createStorageTarget(cfg, warn);

  process.on('SIGTERM', () => {
    logger.error('SIGTERM received - process terminated externally');
    process.exit(143);
  });
  process.on('SIGINT', () => {
    logger.error('SIGINT received');
    process.exit(130);
  });

  if (!(await target.acquireRunLock())) {
    logger.warn('another run holds the lock; exiting without work');
    return;
  }

  try {
    const state = await target.loadState();
    const restDeps: RestDeps = {
      fetchData: (p) => fetchTransactions(p, warn),
      now: () => Date.now(),
    };

    if (cfg.mode === 'canister' || cfg.mode === 'full') {
      const agent = new HttpAgent({ host: cfg.host });
      await agent.syncTime();
      const baseStorage = await getActor<BaseStorageActor>(agent, cfg.baseStorageCanisterId, baseStorageidlFactory);
      const storages = await withRetry(() => baseStorage.baseStorage(), 'baseStorage list', warn);
      if (!storages.length) throw new Error('No storage canisters found');
      logger.info('discovered storage canisters', { count: storages.length });
      const newestStorageId = storages[0];
      await runFull(cfg, agent, target, logger, state, [...storages].reverse(), newestStorageId);
    } else if (cfg.mode === 'backfill') {
      await runBackfill(cfg, target, logger, state, restDeps);
    } else if (cfg.mode === 'incremental') {
      await runIncrementalRest(cfg, target, logger, state, restDeps);
    } else {
      await runSync(cfg, target, logger, state, restDeps);
    }
  } finally {
    await target.releaseRunLock();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
