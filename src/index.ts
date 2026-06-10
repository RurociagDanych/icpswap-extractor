import { Actor, HttpAgent } from '@dfinity/agent';
import { baseStorageidlFactory, type BaseStorageActor } from './idl/baseStorage.js';
import { poolInfoidlFactory, type PoolInfoActor, type SwapTx } from './idl/poolInfo.js';
import { parseArgs, type Config } from './lib/config.js';
import { rowFromTx } from './lib/csv.js';
import type { CsvSink, SinkStats } from './lib/csvSink.js';
import { createLogger, type Logger } from './lib/logger.js';
import { withRetry, type WarnFn } from './lib/retry.js';
import {
  RECENT_HASHES_LIMIT,
  pushBounded,
  trimSetKeepLast,
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
): Promise<{ written: number; total: number; recentHashes: string[]; sinkStats: SinkStats }> {
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
    return { written: 0, total, recentHashes: [], sinkStats };
  }

  let written = 0;
  let offset = 0;
  let headerPending = writeHeader;
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

    if (written % (pageSize * 5) === 0 || written >= total) {
      logger.info('canister progress', { storageId, written, total, pct: Number(((written / total) * 100).toFixed(1)) });
    }

    offset += txs.length;
  }

  const sinkStats = await sink.close();
  return { written, total, recentHashes, sinkStats };
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
    const sink = target.createSink(fileName);

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
  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, manifestEntries));

  logger.info('full load done', { rows: totalRows, files: manifestEntries.length, newestStorageId });
}

async function runIncremental(
  cfg: Config,
  agent: HttpAgent,
  target: StorageTarget,
  logger: Logger,
  state: EtlState,
  activeId: string,
): Promise<void> {
  const warn: WarnFn = (msg) => logger.warn(msg, { storageId: activeId });
  const storage = await getActor<PoolInfoActor>(agent, activeId, poolInfoidlFactory);
  const head = await withRetry(() => storage.getBaseRecord(0n, 1n, []), `${activeId} head`, warn);
  const currentTotal = Number(head.totalElements);

  const prev = state.canisters[activeId];
  const previousTotal = prev?.lastTotal ?? 0;

  // switching: mark previous latest as completed archival
  if (state.latestStorageId && state.latestStorageId !== activeId && state.canisters[state.latestStorageId]) {
    state.canisters[state.latestStorageId].completed = true;
  }

  if (previousTotal >= currentTotal && previousTotal > 0) {
    if (previousTotal > currentTotal) {
      logger.warn('active canister total decreased - possible source reset, skipping without state damage', {
        storageId: activeId,
        previousTotal,
        currentTotal,
      });
    } else {
      logger.info('no new records on active canister', { storageId: activeId, currentTotal });
    }
    state.latestStorageId = activeId;
    state.lastRunAt = new Date().toISOString();
    await target.saveState(state);
    return;
  }

  const fetchFrom = Math.max(0, previousTotal - cfg.overlap);
  const known = new Set(prev?.recentHashes ?? []);
  const sink = target.createSink(`incremental_${activeId}.csv`);
  let offset = fetchFrom;
  let written = 0;
  let writeHeader = true;

  while (offset < currentTotal) {
    const batchSize = Math.min(cfg.pageSize, currentTotal - offset);
    const page = await withRetry(
      () => storage.getBaseRecord(BigInt(offset), BigInt(batchSize), []),
      `${activeId} incremental offset=${offset}`,
      warn
    );

    const txs = (page.content || []).filter((tx: SwapTx) => tx.hash && !known.has(tx.hash));
    const rows = txs.map((tx: SwapTx) => rowFromTx(activeId, tx));
    await sink.append(rows, writeHeader);
    writeHeader = false;
    written += rows.length;

    for (const tx of txs) known.add(tx.hash);
    // 2x the limit so previous-run hashes survive the overlap window even on big catch-up runs.
    trimSetKeepLast(known, RECENT_HASHES_LIMIT * 2);
    offset += batchSize;
  }
  const sinkStats = await sink.close();

  state.mode = 'incremental';
  state.latestStorageId = activeId;
  state.lastRunAt = new Date().toISOString();
  state.canisters[activeId] = {
    lastTotal: currentTotal,
    lastRun: state.lastRunAt,
    recentHashes: Array.from(known).slice(-RECENT_HASHES_LIMIT),
    completed: false,
  };

  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, [{ storageId: activeId, total: currentTotal, written, sink: sinkStats }]));
  logger.info('incremental done', { storageId: activeId, newRows: written });
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
    const agent = new HttpAgent({ host: cfg.host });
    await agent.syncTime();

    const baseStorage = await getActor<BaseStorageActor>(agent, cfg.baseStorageCanisterId, baseStorageidlFactory);
    const storages = await withRetry(() => baseStorage.baseStorage(), 'baseStorage list', warn);
    if (!storages.length) throw new Error('No storage canisters found');
    logger.info('discovered storage canisters', { count: storages.length });

    const newestStorageId = storages[0];
    const state = await target.loadState();

    if (cfg.mode === 'full') {
      await runFull(cfg, agent, target, logger, state, [...storages].reverse(), newestStorageId);
    } else {
      await runIncremental(cfg, agent, target, logger, state, newestStorageId);
    }
  } finally {
    await target.releaseRunLock();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
