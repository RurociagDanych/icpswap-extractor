import type { Config } from './config.js';
import type { Logger } from './logger.js';
import type { StorageTarget, ManifestEntry } from './storageTarget.js';
import { buildManifest } from './storageTarget.js';
import type { EtlState } from './state.js';
import { defaultRestState, pushBounded, RECENT_HASHES_LIMIT } from './state.js';
import type { FindParams } from './restClient.js';
import type { RestFindData } from '../idl/restTx.js';
import { restHeaders, rowFromRestTx } from './restCsv.js';
import {
  computeBackfillFloor, reachedFloor, isShortPage,
  reachedWatermark, maxTxTime, newTxs,
} from './restPaging.js';

export type RestDeps = {
  fetchData: (p: FindParams) => Promise<RestFindData>;
  now: () => number;
};

export async function runBackfill(cfg: Config, target: StorageTarget, logger: Logger, state: EtlState, deps: RestDeps): Promise<void> {
  const rest = state.rest ?? defaultRestState();
  const endSnapshot = rest.backfillCursor?.endSnapshot ?? deps.now();
  const floor = computeBackfillFloor(state.canisterMaxTxTime, cfg.backfillOverlapMs, cfg.backfillFloor);
  let page = rest.backfillCursor?.nextPage ?? 1;
  logger.info('starting backfill', {
    floor, endSnapshot, fromPage: page,
    restPageSize: cfg.restPageSize, pagesPerFile: cfg.backfillPagesPerFile,
  });

  const manifestEntries: ManifestEntry[] = [];
  let totalWritten = 0;
  let total = 0;
  let maxSeen = 0;
  let done = false;

  // Each batch is written to its own committed file BEFORE the durable cursor advances,
  // so an interrupted run never leaves the cursor past data that was not persisted.
  while (!done) {
    const startPage = page;
    const fileName = `transactions_backfill_${endSnapshot}_p${String(startPage).padStart(6, '0')}.csv`;
    let sink: ReturnType<StorageTarget['createSink']> | undefined;
    let fileRows = 0;
    let writeHeader = true;
    let pagesInFile = 0;
    let oldestTxTime: number | undefined;

    while (pagesInFile < cfg.backfillPagesPerFile) {
      const data = await deps.fetchData({
        baseUrl: cfg.restBaseUrl, page, limit: cfg.restPageSize,
        actionTypes: cfg.actionTypes, begin: floor, end: endSnapshot,
      });
      total = data.totalElements;
      const content = data.content;
      if (content.length === 0) { done = true; break; }

      if (!sink) sink = target.createSink(fileName, restHeaders);
      await sink.append(content.map(rowFromRestTx), writeHeader);
      writeHeader = false;
      fileRows += content.length;
      maxSeen = maxTxTime(content, maxSeen);
      oldestTxTime = content[content.length - 1]?.txTime;
      pagesInFile += 1;

      const stop = reachedFloor(content, floor) || isShortPage(content, cfg.restPageSize);
      logger.info('backfill page', {
        page, rows: content.length, fileRows, oldestTxTime,
        msAboveFloor: oldestTxTime !== undefined ? oldestTxTime - floor : undefined,
        totalElements: total,
      });
      page += 1;
      if (stop) { done = true; break; }
    }

    if (sink) {
      const stats = await sink.close(); // commits this file to S3
      manifestEntries.push({ storageId: `rest-backfill-p${startPage}`, total, written: fileRows, sink: stats } as ManifestEntry);
      totalWritten += fileRows;
      // Only now is the batch durable — advance the cursor past it.
      rest.backfillCursor = { endSnapshot, nextPage: page };
      rest.backfillFloor = floor;
      state.rest = rest;
      await target.saveState(state);
      logger.info('backfill batch committed', {
        file: stats.location, startPage, endPage: page - 1, rows: fileRows,
        oldestTxTime, cumulativeRows: totalWritten,
      });
    }
  }

  rest.backfillComplete = true;
  // Seed the incremental watermark to the newest record backfill captured (it pages
  // from the snapshot down to the floor, so page 1 holds the newest rows). Without
  // this, the first incremental run would rescan the entire REST history.
  const seed = maxSeen > 0 ? maxSeen : endSnapshot;
  if ((rest.incrementalWatermark ?? 0) < seed) rest.incrementalWatermark = seed;
  state.rest = rest;
  state.mode = 'backfill';
  state.lastRunAt = new Date().toISOString();
  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, manifestEntries));
  logger.info('backfill done', {
    files: manifestEntries.length, rows: totalWritten, floor, endSnapshot,
    watermark: rest.incrementalWatermark,
  });
}

export async function runIncrementalRest(cfg: Config, target: StorageTarget, logger: Logger, state: EtlState, deps: RestDeps): Promise<void> {
  const rest = state.rest ?? defaultRestState();
  const watermark = rest.incrementalWatermark ?? 0;
  const end = deps.now();
  const known = new Set(rest.recentTxHashes);
  let page = 1;
  logger.info('starting rest incremental', { watermark, end });

  const sink = target.createSink(`transactions_incremental_${cfg.runId}.csv`, restHeaders);
  let written = 0;
  let total = 0;
  let maxSeen = watermark;
  let writeHeader = true;

  for (;;) {
    const data = await deps.fetchData({
      baseUrl: cfg.restBaseUrl, page, limit: cfg.restPageSize,
      actionTypes: cfg.actionTypes, end,
    });
    total = data.totalElements;
    const content = data.content;
    if (content.length === 0) break;

    const fresh = newTxs(content, known);
    await sink.append(fresh.map(rowFromRestTx), writeHeader);
    writeHeader = false;
    written += fresh.length;
    for (const t of fresh) known.add(t.txHash);
    maxSeen = maxTxTime(content, maxSeen);

    if (reachedWatermark(content, watermark, cfg.incrementalOverlapMs) || isShortPage(content, cfg.restPageSize)) break;
    page += 1;
  }

  const stats = await sink.close();
  const recentTxHashes: string[] = [];
  pushBounded(recentTxHashes, Array.from(known), RECENT_HASHES_LIMIT);
  rest.incrementalWatermark = maxSeen;
  rest.recentTxHashes = recentTxHashes;
  state.rest = rest;
  state.mode = 'incremental';
  state.lastRunAt = new Date().toISOString();
  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, [{ storageId: 'rest-incremental', total, written, sink: stats } as ManifestEntry]));
  logger.info('rest incremental done', { written, watermark: maxSeen });
}

export async function runSync(cfg: Config, target: StorageTarget, logger: Logger, state: EtlState, deps: RestDeps): Promise<void> {
  if (state.rest?.backfillComplete !== true) {
    await runBackfill({ ...cfg, mode: 'backfill' }, target, logger, state, deps);
  }
  await runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, state, deps);
}
