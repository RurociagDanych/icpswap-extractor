import path from 'node:path';

export type Config = {
  host: string;
  baseStorageCanisterId: string;
  pageSize: number;
  mode: 'full' | 'incremental';
  outDir: string;
  stateFile: string;
  logFile: string;
  overlap: number;
  concurrency: number;
  s3Bucket?: string;
  s3Prefix: string;
  runId: string;
};

export function parseArgs(args: string[] = process.argv.slice(2)): Config {
  const get = (name: string, fallback?: string) => {
    const idx = args.findIndex((a) => a === `--${name}`);
    if (idx === -1) return fallback;
    return args[idx + 1];
  };

  const mode = (get('mode', 'full') || 'full') as 'full' | 'incremental';
  const s3Bucket = get('s3-bucket', process.env.S3_BUCKET);
  const s3Prefix = get('s3-prefix', process.env.S3_PREFIX || 'icpswap')!;
  const runId = new Date().toISOString().replace(/[:.]/g, '-');

  const pageSize = Number(get('page-size', '1000'));
  const overlap = Number(get('overlap', '50'));
  const concurrency = Number(get('concurrency', '5'));

  if (!Number.isFinite(pageSize) || pageSize < 1 || pageSize > 1000) {
    throw new Error(`Invalid --page-size=${pageSize}. Allowed range: 1..1000`);
  }
  if (!Number.isFinite(overlap) || overlap < 0) {
    throw new Error(`Invalid --overlap=${overlap}. Must be >= 0`);
  }
  if (!Number.isFinite(concurrency) || concurrency < 1 || concurrency > 20) {
    throw new Error(`Invalid --concurrency=${concurrency}. Allowed range: 1..20`);
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
  };
}
