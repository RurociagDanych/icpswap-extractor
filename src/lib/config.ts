import path from 'node:path';

export type Config = {
  host: string;
  baseStorageCanisterId: string;
  pageSize: number;
  mode: 'canister' | 'backfill' | 'incremental' | 'sync' | 'full';
  outDir: string;
  stateFile: string;
  logFile: string;
  overlap: number;
  concurrency: number;
  s3Bucket?: string;
  s3Prefix: string;
  runId: string;
  restBaseUrl: string;
  restPageSize: number;
  backfillOverlapMs: number;
  incrementalOverlapMs: number;
  backfillFloor?: number;
  actionTypes: string;
};

export function parseArgs(args: string[] = process.argv.slice(2)): Config {
  const get = (name: string, fallback?: string) => {
    const idx = args.findIndex((a) => a === `--${name}`);
    if (idx === -1) return fallback;
    return args[idx + 1];
  };

  const mode = (get('mode', 'sync') || 'sync') as Config['mode'];
  const s3Bucket = get('s3-bucket', process.env.S3_BUCKET);
  const s3Prefix = get('s3-prefix', process.env.S3_PREFIX || 'icpswap')!;
  const runId = new Date().toISOString().replace(/[:.]/g, '-');

  const pageSize = Number(get('page-size', '1000'));
  const overlap = Number(get('overlap', '50'));
  const concurrency = Number(get('concurrency', '5'));

  const restPageSize = Number(get('rest-page-size', '100'));
  const backfillOverlapMs = Number(get('backfill-overlap-ms', '3600000'));
  const incrementalOverlapMs = Number(get('incremental-overlap-ms', '300000'));
  const backfillFloorRaw = get('backfill-floor');
  const backfillFloor = backfillFloorRaw === undefined ? undefined : Number(backfillFloorRaw);

  if (!Number.isFinite(pageSize) || pageSize < 1 || pageSize > 1000) {
    throw new Error(`Invalid --page-size=${pageSize}. Allowed range: 1..1000`);
  }
  if (!Number.isFinite(overlap) || overlap < 0) {
    throw new Error(`Invalid --overlap=${overlap}. Must be >= 0`);
  }
  if (!Number.isFinite(concurrency) || concurrency < 1 || concurrency > 20) {
    throw new Error(`Invalid --concurrency=${concurrency}. Allowed range: 1..20`);
  }
  if (!Number.isFinite(restPageSize) || restPageSize < 1 || restPageSize > 100) {
    throw new Error(`Invalid --rest-page-size=${restPageSize}. Allowed range: 1..100`);
  }
  if (!Number.isFinite(backfillOverlapMs) || backfillOverlapMs < 0) {
    throw new Error(`Invalid --backfill-overlap-ms=${backfillOverlapMs}. Must be >= 0`);
  }
  if (!Number.isFinite(incrementalOverlapMs) || incrementalOverlapMs < 0) {
    throw new Error(`Invalid --incremental-overlap-ms=${incrementalOverlapMs}. Must be >= 0`);
  }
  if (backfillFloor !== undefined && (!Number.isFinite(backfillFloor) || backfillFloor < 0)) {
    throw new Error(`Invalid --backfill-floor=${backfillFloorRaw}. Must be a non-negative epoch-ms`);
  }

  return {
    host: get('host', 'https://ic0.app')!,
    baseStorageCanisterId: get('base-storage', 'g54jq-hiaaa-aaaag-qck5q-cai')!,
    pageSize,
    mode,
    outDir: path.resolve(get('out-dir', './out')!),
    stateFile: path.resolve(get('state-file', './out/state.json')!),
    logFile: path.resolve(get('log-file', './out/etl.log')!),
    overlap,
    concurrency,
    s3Bucket: s3Bucket || undefined,
    s3Prefix,
    runId,
    restBaseUrl: get('rest-base-url', 'https://api.icpswap.com/info')!,
    restPageSize,
    backfillOverlapMs,
    incrementalOverlapMs,
    backfillFloor,
    actionTypes: get('action-types', 'Swap,AddLiquidity,DecreaseLiquidity,Claim')!,
  };
}
