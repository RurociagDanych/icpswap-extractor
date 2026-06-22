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
  logger.info('starting backfill', { floor, endSnapshot, fromPage: page });

  const sink = target.createSink(`transactions_backfill_${cfg.runId}.csv`, restHeaders);
  let written = 0;
  let total = 0;
  let maxSeen = 0;
  let writeHeader = true;

  for (;;) {
    const data = await deps.fetchData({
      baseUrl: cfg.restBaseUrl, page, limit: cfg.restPageSize,
      actionTypes: cfg.actionTypes, begin: floor, end: endSnapshot,
    });
    total = data.totalElements;
    const content = data.content;
    if (content.length === 0) break;

    await sink.append(content.map(rowFromRestTx), writeHeader);
    writeHeader = false;
    written += content.length;
    maxSeen = maxTxTime(content, maxSeen);

    rest.backfillCursor = { endSnapshot, nextPage: page + 1 };
    rest.backfillFloor = floor;
    state.rest = rest;
    await target.saveState(state);

    if (reachedFloor(content, floor) || isShortPage(content, cfg.restPageSize)) break;
    page += 1;
  }

  const stats = await sink.close();
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
  await target.writeManifest(buildManifest(cfg, [{ storageId: 'rest-backfill', total, written, sink: stats } as ManifestEntry]));
  logger.info('backfill done', { written, floor, endSnapshot });
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
