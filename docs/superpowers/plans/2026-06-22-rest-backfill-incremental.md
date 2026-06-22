# REST Backfill + Incremental Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move ongoing ICPSwap transaction ingestion to the REST API (`/transaction/find`) via new `backfill` and `incremental` modes, while keeping the existing canister extraction as a one-time deep-history archive that is auto-skipped once complete.

**Architecture:** Three explicit run modes (`canister`, `backfill`, `incremental`) plus a default `sync` orchestrator that runs `backfill`-if-incomplete then `incremental` and never runs `canister`. REST and canister data land raw and separate in S3. Pagination is anchored to a fixed `end` snapshot for stability; the canister↔REST seam is covered by a configurable overlap. State (`state.json`) drives the one-time cutover.

**Tech Stack:** Node.js 20 + TypeScript (ESM, `.js` import specifiers), `@dfinity/agent` (canister path only), `@aws-sdk/client-s3` + `lib-storage`, `node:test` for tests, global `fetch` for REST.

## Global Constraints

- Language/runtime: TypeScript ESM; all relative imports MUST use the `.js` extension (e.g. `./state.js`).
- Tests: `node:test` + `node:assert/strict`, one suite per module under `tests/`, matching existing style. Run with `npm test`.
- Commits: author is `topek` / `zachara.arkadiusz@gmail.com`. NEVER add a Claude/AI co-author trailer or "Generated with" line. Use `git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit`.
- ELT principle: land raw, immutable, per-run. No cross-source dedup or schema unification in the extractor.
- REST facts (verified 2026-06-22): base `https://api.icpswap.com/info`, `GET /transaction/find`, `page` 1-indexed, `limit` capped at 100 server-side, results newest-first by `txTime` (ms), `begin`/`end` bound the window, response wrapped as `{ code, message, data: { totalElements, content, page, limit } }`.
- Reuse existing `withRetry` (max 5 attempts) for all network calls. Do not add new retry logic.

---

### Task 1: Extend state schema for REST + canister seam

**Files:**
- Modify: `src/lib/state.ts`
- Test: `tests/state.test.ts`

**Interfaces:**
- Consumes: existing `EtlState`, `defaultState`, `parseState`, `RECENT_HASHES_LIMIT`.
- Produces:
  - `type RestState = { backfillComplete?: boolean; backfillCursor?: { endSnapshot: number; nextPage: number }; backfillFloor?: number; incrementalWatermark?: number; recentTxHashes: string[] }`
  - `EtlState` gains: `canisterArchiveComplete?: boolean; canisterMaxTxTime?: number; rest?: RestState`
  - `function defaultRestState(): RestState` returning `{ recentTxHashes: [] }`

- [ ] **Step 1: Write the failing test**

Add to `tests/state.test.ts`:

```ts
import { defaultRestState } from '../src/lib/state.js';

test('parseState preserves rest + canister-seam fields', () => {
  const state = parseState(
    JSON.stringify({
      mode: 'incremental',
      canisters: {},
      canisterArchiveComplete: true,
      canisterMaxTxTime: 1749254400000,
      rest: {
        backfillComplete: false,
        backfillCursor: { endSnapshot: 1782127219000, nextPage: 137 },
        backfillFloor: 1749254400000,
        incrementalWatermark: 1782200000000,
        recentTxHashes: ['ttnzy-244431'],
      },
    })
  );

  assert.equal(state.canisterArchiveComplete, true);
  assert.equal(state.canisterMaxTxTime, 1749254400000);
  assert.equal(state.rest?.backfillCursor?.nextPage, 137);
  assert.deepEqual(state.rest?.recentTxHashes, ['ttnzy-244431']);
});

test('defaultRestState starts with an empty hash list', () => {
  assert.deepEqual(defaultRestState(), { recentTxHashes: [] });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `defaultRestState` is not exported / `rest` undefined.

- [ ] **Step 3: Implement the schema additions**

In `src/lib/state.ts`, extend the types and add the helper:

```ts
export type RestState = {
  backfillComplete?: boolean;
  backfillCursor?: { endSnapshot: number; nextPage: number };
  backfillFloor?: number;
  incrementalWatermark?: number;
  recentTxHashes: string[];
};

export type EtlState = {
  mode: 'full' | 'incremental' | 'canister' | 'backfill' | 'sync';
  lastRunAt?: string;
  latestStorageId?: string;
  canisters: Record<string, CanisterState>;
  canisterArchiveComplete?: boolean;
  canisterMaxTxTime?: number;
  rest?: RestState;
};

export const defaultRestState = (): RestState => ({ recentTxHashes: [] });
```

`parseState` already spreads `...parsed` over `defaultState()`, so the new optional fields round-trip with no further change. Leave the legacy-migration block untouched.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test`
Expected: PASS (all state tests).

- [ ] **Step 5: Commit**

```bash
git add src/lib/state.ts tests/state.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat(state): add REST + canister-seam fields to ETL state"
```

---

### Task 2: Add REST + mode config flags

**Files:**
- Modify: `src/lib/config.ts`
- Test: `tests/config.test.ts`

**Interfaces:**
- Consumes: existing `parseArgs`, `Config`.
- Produces, `Config` gains:
  - `mode: 'canister' | 'backfill' | 'incremental' | 'sync' | 'full'`
  - `restBaseUrl: string` (default `https://api.icpswap.com/info`)
  - `restPageSize: number` (default `100`, validated `1..100`)
  - `backfillOverlapMs: number` (default `3600000`)
  - `incrementalOverlapMs: number` (default `300000`)
  - `backfillFloor?: number` (from `--backfill-floor`, ms; undefined if absent)
  - `actionTypes: string` (default `Swap,AddLiquidity,DecreaseLiquidity,Claim`)

- [ ] **Step 1: Write the failing test**

Add to `tests/config.test.ts`:

```ts
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `cfg.mode` is `'full'`, REST fields undefined.

- [ ] **Step 3: Implement the flags**

In `src/lib/config.ts`, widen `Config.mode` and add the new fields to the type, then in `parseArgs`:

```ts
const mode = (get('mode', 'sync') || 'sync') as Config['mode'];

const restPageSize = Number(get('rest-page-size', '100'));
const backfillOverlapMs = Number(get('backfill-overlap-ms', '3600000'));
const incrementalOverlapMs = Number(get('incremental-overlap-ms', '300000'));
const backfillFloorRaw = get('backfill-floor');
const backfillFloor = backfillFloorRaw === undefined ? undefined : Number(backfillFloorRaw);

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
```

Add to the returned object:

```ts
    restBaseUrl: get('rest-base-url', 'https://api.icpswap.com/info')!,
    restPageSize,
    backfillOverlapMs,
    incrementalOverlapMs,
    backfillFloor,
    actionTypes: get('action-types', 'Swap,AddLiquidity,DecreaseLiquidity,Claim')!,
```

Keep the existing canister flags (`--page-size`, `--concurrency`, `--overlap`) as-is.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lib/config.ts tests/config.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat(config): add REST mode + pagination/overlap flags"
```

---

### Task 3: Make the CSV sink schema-driven

**Files:**
- Modify: `src/lib/csvSink.ts`
- Modify: `src/lib/storageTarget.ts`
- Modify: `src/index.ts` (canister call sites)
- Test: `tests/csv.test.ts`

**Interfaces:**
- Consumes: existing `CsvRow` from `csv.ts`, `headers` from `csv.ts`.
- Produces:
  - `type CsvValue = string | number; type CsvRecord = Record<string, CsvValue>;` (exported from `csvSink.ts`)
  - `CsvSink.append(rows: ReadonlyArray<CsvRecord>, writeHeaderIfNeeded?: boolean): Promise<void>`
  - `StorageTarget.createSink(fileName: string, columns: readonly string[]): CsvSink`

This generalization lets both canister rows and REST rows reuse the same streaming/checksum sink (DRY).

- [ ] **Step 1: Write the failing test**

Add to `tests/csv.test.ts` (a sink round-trip with a custom column set):

```ts
import os from 'node:os';
import fs from 'node:fs';
import path from 'node:path';
import { LocalCsvSink } from '../src/lib/csvSink.js';

test('LocalCsvSink writes arbitrary columns with header + checksum', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icpswap-sink-'));
  const file = path.join(dir, 'out.csv');
  const sink = new LocalCsvSink(file, ['a', 'b']);
  await sink.append([{ a: 'x', b: 1 }], true);
  const stats = await sink.close();

  assert.equal(fs.readFileSync(file, 'utf8'), 'a,b\nx,1\n');
  assert.equal(stats.rows, 1);
  assert.ok(stats.sha256.length === 64);
  fs.rmSync(dir, { recursive: true, force: true });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `LocalCsvSink` constructor takes 1 arg / `columns` unknown.

- [ ] **Step 3: Generalize the sink**

In `src/lib/csvSink.ts`: remove the `import { headers } from './csv.js'` dependency from the sink internals and make columns an instance field.

```ts
import type { CsvRow } from './csv.js'; // remove if unused after refactor

export type CsvValue = string | number;
export type CsvRecord = Record<string, CsvValue>;

function escapeCsvValue(value: CsvValue): string {
  const text = String(value ?? '');
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

export interface CsvSink {
  location: string;
  append(rows: ReadonlyArray<CsvRecord>, writeHeaderIfNeeded?: boolean): Promise<void>;
  close(): Promise<SinkStats>;
}
```

In `BaseCsvSink`, take columns in the constructor and use them for the header/row lines:

```ts
abstract class BaseCsvSink implements CsvSink {
  // ...existing private fields...
  constructor(public location: string, private readonly columns: readonly string[]) {}

  async append(rows: ReadonlyArray<CsvRecord>, writeHeaderIfNeeded = false): Promise<void> {
    this.ensureOpen();
    if (!this.headerWritten && writeHeaderIfNeeded) {
      await this.writeTrackedLine(`${this.columns.join(',')}\n`, true);
      this.headerWritten = true;
    }
    for (const row of rows) {
      const line = `${this.columns.map((c) => escapeCsvValue(row[c])).join(',')}\n`;
      await this.writeTrackedLine(line, false);
    }
  }
  // ...rest unchanged...
}
```

Update subclass constructors to forward `columns`:

```ts
export class LocalCsvSink extends BaseCsvSink {
  constructor(location: string, columns: readonly string[]) { super(location, columns); }
  // ...writeRaw/finish unchanged...
}

export class S3CsvSink extends StreamingCsvSink {
  constructor(s3: S3Client, bucket: string, key: string, location: string, columns: readonly string[]) {
    super(location, columns, async (stream) => { /* unchanged Upload body */ });
  }
}
```

And `StreamingCsvSink` constructor becomes `(location, columns, startUpload)` forwarding to `super(location, columns)`. Delete the now-unused `headerLine`/`rowLine` free functions.

- [ ] **Step 4: Update createSink + canister call sites**

In `src/lib/storageTarget.ts`, change the interface and both implementations:

```ts
createSink(fileName: string, columns: readonly string[]): CsvSink;
```

```ts
// LocalTarget
createSink(fileName: string, columns: readonly string[]): CsvSink {
  return new LocalCsvSink(path.join(this.outDir, fileName), columns);
}
// S3Target
createSink(fileName: string, columns: readonly string[]): CsvSink {
  const key = this.runKey(fileName);
  return new S3CsvSink(this.s3, this.bucket, key, `s3://${this.bucket}/${key}`, columns);
}
```

In `src/index.ts`, update the two canister `createSink` calls to pass the canister headers:

```ts
import { headers as canisterHeaders } from './lib/csv.js';
// ...
const sink = target.createSink(fileName, canisterHeaders);          // in runFull task
const sink = target.createSink(`incremental_${activeId}.csv`, canisterHeaders); // in runIncremental
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `npm test`
Expected: PASS (csv + storageTarget suites).

- [ ] **Step 6: Build to confirm types**

Run: `npm run build`
Expected: no TypeScript errors.

- [ ] **Step 7: Commit**

```bash
git add src/lib/csvSink.ts src/lib/storageTarget.ts src/index.ts tests/csv.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "refactor(sink): make CSV sink schema-driven for reuse"
```

---

### Task 4: REST record type + raw CSV mapping

**Files:**
- Create: `src/idl/restTx.ts`
- Create: `src/lib/restCsv.ts`
- Test: `tests/restCsv.test.ts`

**Interfaces:**
- Consumes: `CsvRecord` from `csvSink.ts`.
- Produces:
  - `restTx.ts`: `type RestTx = { ... }` (all 38 observed fields); `type RestFindData = { totalElements: number; content: RestTx[]; page: number; limit: number }`; `type RestResponse = { code: number; message: string | null; data: RestFindData }`.
  - `restCsv.ts`: `const restHeaders: readonly string[]`; `function rowFromRestTx(tx: RestTx): CsvRecord`.

- [ ] **Step 1: Create the REST types**

`src/idl/restTx.ts`:

```ts
export type RestTx = {
  poolId: string;
  poolFee: number;
  positionId: number;
  token0LedgerId: string;
  token0Price: string;
  token0Name: string;
  token0Symbol: string;
  token1LedgerId: string;
  token1Price: string;
  token1Name: string;
  token1Symbol: string;
  actionType: string;
  fromPrincipalId: string;
  fromSubaccount: string;
  fromAccountId: string;
  fromTextualId: string;
  fromAlias: string | null;
  toPrincipalId: string;
  toSubaccount: string;
  toAccountId: string;
  toTextualId: string;
  toAlias: string | null;
  token0AmountIn: string;
  token1AmountIn: string;
  token0AmountOut: string;
  token1AmountOut: string;
  token0Fee: string;
  token1Fee: string;
  sqrtPrice: string;
  tickLimit: string;
  tick: string;
  liquidity: string;
  currentLiquidity: string;
  txHash: string;
  txTime: number;
  token0TxValue: string;
  token1TxValue: string;
};

export type RestFindData = { totalElements: number; content: RestTx[]; page: number; limit: number };
export type RestResponse = { code: number; message: string | null; data: RestFindData };
```

- [ ] **Step 2: Write the failing mapping test**

`tests/restCsv.test.ts`:

```ts
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { restHeaders, rowFromRestTx } from '../src/lib/restCsv.js';
import type { RestTx } from '../src/idl/restTx.js';

const sample: RestTx = {
  poolId: 'ttnzy-lyaaa-aaaag-qj2bq-cai', poolFee: 3000, positionId: 0,
  token0LedgerId: 'ryjl3-tyaaa-aaaaa-aaaba-cai', token0Price: '2.27', token0Name: 'Internet Computer', token0Symbol: 'ICP',
  token1LedgerId: 'lkwrt-vyaaa-aaaaq-aadhq-cai', token1Price: '0.0008', token1Name: 'ORIGYN', token1Symbol: 'OGY',
  actionType: 'Swap',
  fromPrincipalId: 'tg3k5', fromSubaccount: '00', fromAccountId: '1ef5', fromTextualId: 'tg3k5', fromAlias: null,
  toPrincipalId: 'ttnzy', toSubaccount: '00', toAccountId: 'fd09', toTextualId: 'ttnzy', toAlias: 'ICPSwap:OGY/ICP',
  token0AmountIn: '0.29', token1AmountIn: '0', token0AmountOut: '0', token1AmountOut: '784.96',
  token0Fee: '0.0001', token1Fee: '0.002', sqrtPrice: '15451', tickLimit: '', tick: '-78749',
  liquidity: '0', currentLiquidity: '742335', txHash: 'ttnzy-lyaaa-aaaag-qj2bq-cai244431', txTime: 1782127219000,
  token0TxValue: '0.68', token1TxValue: '0.68',
};

test('rowFromRestTx maps every header column', () => {
  const row = rowFromRestTx(sample);
  for (const h of restHeaders) assert.ok(h in row, `missing column ${h}`);
  assert.equal(row.tx_hash, 'ttnzy-lyaaa-aaaag-qj2bq-cai244431');
  assert.equal(row.tx_time, 1782127219000);
  assert.equal(row.from_alias, ''); // null -> empty string
  assert.equal(row.action_type, 'Swap');
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `restCsv.js` not found.

- [ ] **Step 4: Implement the mapping**

`src/lib/restCsv.ts` (snake_case columns; nulls → empty string; preserve numeric `tx_time`/`pool_fee`/`position_id`, keep price/amount strings verbatim to avoid float loss):

```ts
import type { CsvRecord } from './csvSink.js';
import type { RestTx } from '../idl/restTx.js';

export const restHeaders = [
  'tx_hash','tx_time','action_type','pool_id','pool_fee','position_id',
  'from_principal_id','from_subaccount','from_account_id','from_textual_id','from_alias',
  'to_principal_id','to_subaccount','to_account_id','to_textual_id','to_alias',
  'token0_ledger_id','token0_name','token0_symbol','token0_price',
  'token1_ledger_id','token1_name','token1_symbol','token1_price',
  'token0_amount_in','token1_amount_in','token0_amount_out','token1_amount_out',
  'token0_fee','token1_fee','token0_tx_value','token1_tx_value',
  'sqrt_price','tick_limit','tick','liquidity','current_liquidity',
] as const;

const s = (v: string | null): string => v ?? '';

export function rowFromRestTx(tx: RestTx): CsvRecord {
  return {
    tx_hash: tx.txHash, tx_time: tx.txTime, action_type: tx.actionType,
    pool_id: tx.poolId, pool_fee: tx.poolFee, position_id: tx.positionId,
    from_principal_id: tx.fromPrincipalId, from_subaccount: tx.fromSubaccount,
    from_account_id: tx.fromAccountId, from_textual_id: tx.fromTextualId, from_alias: s(tx.fromAlias),
    to_principal_id: tx.toPrincipalId, to_subaccount: tx.toSubaccount,
    to_account_id: tx.toAccountId, to_textual_id: tx.toTextualId, to_alias: s(tx.toAlias),
    token0_ledger_id: tx.token0LedgerId, token0_name: tx.token0Name, token0_symbol: tx.token0Symbol, token0_price: tx.token0Price,
    token1_ledger_id: tx.token1LedgerId, token1_name: tx.token1Name, token1_symbol: tx.token1Symbol, token1_price: tx.token1Price,
    token0_amount_in: tx.token0AmountIn, token1_amount_in: tx.token1AmountIn,
    token0_amount_out: tx.token0AmountOut, token1_amount_out: tx.token1AmountOut,
    token0_fee: tx.token0Fee, token1_fee: tx.token1Fee, token0_tx_value: tx.token0TxValue, token1_tx_value: tx.token1TxValue,
    sqrt_price: tx.sqrtPrice, tick_limit: tx.tickLimit, tick: tx.tick,
    liquidity: tx.liquidity, current_liquidity: tx.currentLiquidity,
  };
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `npm test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/idl/restTx.ts src/lib/restCsv.ts tests/restCsv.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat(rest): add REST record type + raw CSV mapping"
```

---

### Task 5: REST client (`/transaction/find`)

**Files:**
- Create: `src/lib/restClient.ts`
- Test: `tests/restClient.test.ts`

**Interfaces:**
- Consumes: `RestResponse`, `RestFindData` from `restTx.js`; `withRetry`, `WarnFn` from `retry.js`.
- Produces:
  - `type FindParams = { baseUrl: string; page: number; limit: number; actionTypes: string; begin?: number; end?: number; poolId?: string; tokenId?: string; principal?: string }`
  - `function buildFindUrl(p: FindParams): string`
  - `type FetchLike = (url: string) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>`
  - `async function fetchTransactions(p: FindParams, warn: WarnFn, fetchImpl?: FetchLike): Promise<RestFindData>`

- [ ] **Step 1: Write the failing tests**

`tests/restClient.test.ts`:

```ts
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { buildFindUrl, fetchTransactions } from '../src/lib/restClient.js';

const base = { baseUrl: 'https://api.icpswap.com/info', page: 1, limit: 100, actionTypes: 'Swap' };

test('buildFindUrl includes required params and omits absent optionals', () => {
  const url = buildFindUrl(base);
  assert.match(url, /\/transaction\/find\?/);
  assert.match(url, /page=1/);
  assert.match(url, /limit=100/);
  assert.match(url, /actionTypes=Swap/);
  assert.doesNotMatch(url, /begin=/);
});

test('buildFindUrl includes begin/end when provided', () => {
  const url = buildFindUrl({ ...base, begin: 10, end: 20 });
  assert.match(url, /begin=10/);
  assert.match(url, /end=20/);
});

test('fetchTransactions returns data on code 200', async () => {
  const data = { totalElements: 1, content: [], page: 1, limit: 100 };
  const fake = async () => ({ ok: true, status: 200, json: async () => ({ code: 200, message: null, data }) });
  const result = await fetchTransactions(base, () => {}, fake);
  assert.deepEqual(result, data);
});

test('fetchTransactions retries then throws on non-200 wrapper code', async () => {
  let calls = 0;
  const fake = async () => { calls++; return { ok: true, status: 200, json: async () => ({ code: 500, message: 'boom', data: null }) }; };
  await assert.rejects(() => fetchTransactions(base, () => {}, fake), /boom/);
  assert.equal(calls, 5); // withRetry default attempts
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `restClient.js` not found.

- [ ] **Step 3: Implement the client**

`src/lib/restClient.ts`:

```ts
import type { RestFindData, RestResponse } from '../idl/restTx.js';
import { withRetry, type WarnFn } from './retry.js';

export type FindParams = {
  baseUrl: string;
  page: number;
  limit: number;
  actionTypes: string;
  begin?: number;
  end?: number;
  poolId?: string;
  tokenId?: string;
  principal?: string;
};

export type FetchLike = (url: string) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>;

export function buildFindUrl(p: FindParams): string {
  const qs = new URLSearchParams({
    page: String(p.page),
    limit: String(p.limit),
    actionTypes: p.actionTypes,
  });
  if (p.begin !== undefined) qs.set('begin', String(p.begin));
  if (p.end !== undefined) qs.set('end', String(p.end));
  if (p.poolId) qs.set('poolId', p.poolId);
  if (p.tokenId) qs.set('tokenId', p.tokenId);
  if (p.principal) qs.set('principal', p.principal);
  return `${p.baseUrl.replace(/\/$/, '')}/transaction/find?${qs.toString()}`;
}

export async function fetchTransactions(
  p: FindParams,
  warn: WarnFn,
  fetchImpl: FetchLike = (url) => fetch(url),
): Promise<RestFindData> {
  const url = buildFindUrl(p);
  return withRetry(async () => {
    const res = await fetchImpl(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    const body = (await res.json()) as RestResponse;
    if (body.code !== 200 || !body.data) {
      throw new Error(`API code ${body.code}: ${body.message ?? 'unknown'} for page ${p.page}`);
    }
    return body.data;
  }, `find page=${p.page}`, warn);
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test`
Expected: PASS (4 restClient tests).

- [ ] **Step 5: Commit**

```bash
git add src/lib/restClient.ts tests/restClient.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat(rest): add /transaction/find client with retry"
```

---

### Task 6: Backfill + incremental pure helpers

**Files:**
- Create: `src/lib/restPaging.ts`
- Test: `tests/restPaging.test.ts`

**Interfaces:**
- Consumes: `RestTx` from `restTx.js`.
- Produces:
  - `function computeBackfillFloor(canisterMaxTxTime: number | undefined, overlapMs: number, explicitFloor?: number): number` — explicit floor wins; else `canisterMaxTxTime - overlapMs`; throws if neither is available.
  - `function reachedFloor(content: RestTx[], floor: number): boolean` — true if the page's oldest (last) `txTime < floor`.
  - `function isShortPage(content: RestTx[], limit: number): boolean`
  - `function reachedWatermark(content: RestTx[], watermark: number, overlapMs: number): boolean` — true if oldest `txTime <= watermark - overlapMs`.
  - `function maxTxTime(content: RestTx[], fallback: number): number`
  - `function newTxs(content: RestTx[], known: Set<string>): RestTx[]` — filters out `txHash`es already in `known`.

- [ ] **Step 1: Write the failing tests**

`tests/restPaging.test.ts`:

```ts
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  computeBackfillFloor, reachedFloor, isShortPage,
  reachedWatermark, maxTxTime, newTxs,
} from '../src/lib/restPaging.js';
import type { RestTx } from '../src/idl/restTx.js';

const tx = (txHash: string, txTime: number) => ({ txHash, txTime } as RestTx);

test('computeBackfillFloor prefers explicit floor', () => {
  assert.equal(computeBackfillFloor(1000, 50, 200), 200);
});
test('computeBackfillFloor derives from canister max minus overlap', () => {
  assert.equal(computeBackfillFloor(1000, 50), 950);
});
test('computeBackfillFloor throws when nothing is available', () => {
  assert.throws(() => computeBackfillFloor(undefined, 50), /backfill floor/);
});

test('reachedFloor true once oldest row dips below floor', () => {
  assert.equal(reachedFloor([tx('a', 300), tx('b', 100)], 200), true);
  assert.equal(reachedFloor([tx('a', 300), tx('b', 250)], 200), false);
});

test('isShortPage detects a partial page', () => {
  assert.equal(isShortPage([tx('a', 1)], 100), true);
  assert.equal(isShortPage(new Array(100).fill(tx('a', 1)), 100), false);
});

test('reachedWatermark accounts for overlap', () => {
  assert.equal(reachedWatermark([tx('a', 500), tx('b', 90)], 100, 20), true);  // 90 <= 80? no -> false
  assert.equal(reachedWatermark([tx('a', 500), tx('b', 70)], 100, 20), true);  // 70 <= 80 -> true
});

test('maxTxTime returns the largest txTime or fallback', () => {
  assert.equal(maxTxTime([tx('a', 5), tx('b', 9)], 0), 9);
  assert.equal(maxTxTime([], 42), 42);
});

test('newTxs filters known hashes', () => {
  const known = new Set(['a']);
  assert.deepEqual(newTxs([tx('a', 1), tx('b', 2)], known).map((t) => t.txHash), ['b']);
});
```

Note: fix the first `reachedWatermark` expectation — `90 <= 100-20=80` is false, so expected `false`.

- [ ] **Step 2: Correct the test oracle**

Edit the first `reachedWatermark` assertion to `false`:

```ts
  assert.equal(reachedWatermark([tx('a', 500), tx('b', 90)], 100, 20), false);
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `restPaging.js` not found.

- [ ] **Step 4: Implement the helpers**

`src/lib/restPaging.ts`:

```ts
import type { RestTx } from '../idl/restTx.js';

export function computeBackfillFloor(canisterMaxTxTime: number | undefined, overlapMs: number, explicitFloor?: number): number {
  if (explicitFloor !== undefined) return explicitFloor;
  if (canisterMaxTxTime !== undefined) return canisterMaxTxTime - overlapMs;
  throw new Error('Cannot determine backfill floor: no canisterMaxTxTime in state and no --backfill-floor provided');
}

const oldest = (content: RestTx[]): RestTx | undefined => content[content.length - 1];

export function reachedFloor(content: RestTx[], floor: number): boolean {
  const last = oldest(content);
  return last !== undefined && last.txTime < floor;
}

export function isShortPage(content: RestTx[], limit: number): boolean {
  return content.length < limit;
}

export function reachedWatermark(content: RestTx[], watermark: number, overlapMs: number): boolean {
  const last = oldest(content);
  return last !== undefined && last.txTime <= watermark - overlapMs;
}

export function maxTxTime(content: RestTx[], fallback: number): number {
  return content.reduce((m, t) => (t.txTime > m ? t.txTime : m), fallback);
}

export function newTxs(content: RestTx[], known: Set<string>): RestTx[] {
  return content.filter((t) => !known.has(t.txHash));
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `npm test`
Expected: PASS (restPaging suite).

- [ ] **Step 6: Commit**

```bash
git add src/lib/restPaging.ts tests/restPaging.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat(rest): add backfill/incremental paging helpers"
```

---

### Task 7: Source-separated S3 paths + widened manifest mode

**Files:**
- Modify: `src/lib/storageTarget.ts`
- Test: `tests/storageTarget.test.ts`

**Interfaces:**
- Consumes: `Config['mode']` (widened in Task 2).
- Produces: S3 keys segmented by source — `canister/`, `rest/backfill/`, `rest/incremental/`. `Manifest.mode` widened to `Config['mode']`.
- Mapping: `function modePathSegment(mode: Config['mode']): string` exported for testing — `canister`/`full` → `canister`, `backfill` → `rest/backfill`, `incremental` → `rest/incremental`, `sync` → `rest/incremental` (sync's REST writes go through incremental runs; backfill runs set their own mode).

- [ ] **Step 1: Write the failing test**

Add to `tests/storageTarget.test.ts`:

```ts
import { modePathSegment } from '../src/lib/storageTarget.js';

test('modePathSegment separates sources', () => {
  assert.equal(modePathSegment('canister'), 'canister');
  assert.equal(modePathSegment('full'), 'canister');
  assert.equal(modePathSegment('backfill'), 'rest/backfill');
  assert.equal(modePathSegment('incremental'), 'rest/incremental');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `modePathSegment` not exported.

- [ ] **Step 3: Implement segment mapping + widen manifest types**

In `src/lib/storageTarget.ts`:

```ts
export function modePathSegment(mode: Config['mode']): string {
  switch (mode) {
    case 'backfill': return 'rest/backfill';
    case 'incremental': return 'rest/incremental';
    case 'sync': return 'rest/incremental';
    default: return 'canister'; // 'canister' | 'full'
  }
}
```

Change `Manifest.mode` and `S3Target.mode` types from `'full' | 'incremental'` to `Config['mode']`. Update `buildManifest`'s param type accordingly (it already uses `Pick<Config, 'runId' | 'mode'>`). In `S3Target`, replace the `runKey` segment:

```ts
private runKey(fileName: string): string {
  return `${this.prefix}/${modePathSegment(this.mode)}/${this.runId}/${fileName}`;
}
```

- [ ] **Step 4: Run tests + build**

Run: `npm test && npm run build`
Expected: PASS, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/lib/storageTarget.ts tests/storageTarget.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat(storage): source-separated S3 paths for canister vs REST"
```

---

### Task 8: Backfill run + incremental run + sync orchestrator + entrypoint

**Files:**
- Create: `src/lib/restRuns.ts`
- Modify: `src/index.ts`
- Test: `tests/restRuns.test.ts`

**Interfaces:**
- Consumes: `fetchTransactions`/`FindParams` (`restClient.js`), `rowFromRestTx`/`restHeaders` (`restCsv.js`), all helpers from `restPaging.js`, `StorageTarget` (`storageTarget.js`), `EtlState`/`defaultRestState`/`RECENT_HASHES_LIMIT`/`pushBounded` (`state.js`), `Config`, `Logger`, `RestFindData`.
- Produces:
  - `type RestDeps = { fetchData: (p: FindParams) => Promise<RestFindData>; now: () => number }`
  - `async function runBackfill(cfg, target, logger, state, deps): Promise<void>`
  - `async function runIncrementalRest(cfg, target, logger, state, deps): Promise<void>`
  - `async function runSync(cfg, target, logger, state, deps): Promise<void>` — calls `runBackfill` when `state.rest?.backfillComplete !== true`, then `runIncrementalRest`.

`RestDeps` is injected so tests drive paging without real HTTP. In `index.ts`, the real `deps` is `{ fetchData: (p) => fetchTransactions(p, warn), now: () => Date.now() }`.

- [ ] **Step 1: Write the failing test (backfill stops at floor, persists cursor)**

`tests/restRuns.test.ts`:

```ts
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { runBackfill, runIncrementalRest, type RestDeps } from '../src/lib/restRuns.js';
import { defaultState, type EtlState } from '../src/lib/state.js';
import type { RestFindData, RestTx } from '../src/idl/restTx.js';

const cfg: any = {
  runId: 'r1', mode: 'backfill', restBaseUrl: 'http://x', restPageSize: 2,
  backfillOverlapMs: 0, incrementalOverlapMs: 0, actionTypes: 'Swap',
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

  // x is in recentTxHashes -> filtered; w is below watermark window but on the stop page it is still emitted if > floor? No: only new & > watermark region
  assert.deepEqual(sinks[0].rows.map((r) => r.tx_hash), ['z', 'y', 'w']);
  assert.equal(getSaved()?.rest?.incrementalWatermark, 500);
});
```

Note: `w(50)` is emitted because it is a new hash on a fetched page; the stop check happens after appending the page. The watermark advances to the max seen (500).

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `restRuns.js` not found.

- [ ] **Step 3: Implement the runs**

`src/lib/restRuns.ts`:

```ts
import type { Config } from './config.js';
import type { Logger } from './logger.js';
import type { StorageTarget, ManifestEntry } from './storageTarget.js';
import { buildManifest } from './storageTarget.js';
import type { EtlState } from './state.js';
import { defaultRestState, pushBounded, RECENT_HASHES_LIMIT } from './state.js';
import type { FindParams } from './restClient.js';
import type { RestFindData, RestTx } from '../idl/restTx.js';
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

  const sink = target.createSink(`transactions_backfill_${cfg.runId}.csv`, restHeaders);
  let written = 0;
  let total = 0;
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

    rest.backfillCursor = { endSnapshot, nextPage: page + 1 };
    rest.backfillFloor = floor;
    state.rest = rest;
    await target.saveState(state);

    if (reachedFloor(content, floor) || isShortPage(content, cfg.restPageSize)) break;
    page += 1;
  }

  const stats = await sink.close();
  rest.backfillComplete = true;
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
```

- [ ] **Step 4: Wire the entrypoint dispatch**

In `src/index.ts`, import the runs and dispatch on mode. Replace the `if (cfg.mode === 'full') ... else ...` block inside `run()` with:

```ts
import { runBackfill, runIncrementalRest, runSync, type RestDeps } from './lib/restRuns.js';
import { fetchTransactions } from './lib/restClient.js';
// ...
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
  // canister archive complete: record seam + flag (see Step 5)
} else if (cfg.mode === 'backfill') {
  await runBackfill(cfg, target, logger, state, restDeps);
} else if (cfg.mode === 'incremental') {
  await runIncrementalRest(cfg, target, logger, state, restDeps);
} else {
  await runSync(cfg, target, logger, state, restDeps);
}
```

Move the agent/discovery code so it only runs in the canister branch (REST modes need no IC agent). Keep the run-lock acquire/release `try/finally` wrapper unchanged.

- [ ] **Step 5: Record the canister seam on archive completion**

In `runFull` (`src/index.ts`), after the loop completes and before the final `saveState`, set the cutover flag and seam timestamp. The canister `timestamp` is nanoseconds; convert to ms. Capture the max timestamp while iterating: in `fetchCanisterFull`, track `let maxTs = 0n;` updated per tx (`if (tx.timestamp > maxTs) maxTs = tx.timestamp;`) and return it as `maxTimestampNs`. Then in `runFull`:

```ts
// after parallelLimit(...) and before state.mode = 'full':
const maxNs = manifestEntries.reduce((m, e) => (e.maxTimestampNs && e.maxTimestampNs > m ? e.maxTimestampNs : m), 0n);
state.canisterMaxTxTime = Number(maxNs / 1_000_000n);
state.canisterArchiveComplete = true;
```

Add `maxTimestampNs?: bigint` to the local `manifestEntries.push({...})` object (it is internal bookkeeping; do not add it to the persisted `ManifestEntry` type). Keep `state.mode = 'full'` as-is for backward compatibility, or set `'canister'` — either is valid since the union now includes both.

- [ ] **Step 6: Run tests + build**

Run: `npm test && npm run build`
Expected: PASS, no type errors.

- [ ] **Step 7: Commit**

```bash
git add src/lib/restRuns.ts src/index.ts tests/restRuns.test.ts
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat: backfill/incremental REST runs + sync orchestrator + entrypoint dispatch"
```

---

### Task 9: Default scheduler to `sync` + docs

**Files:**
- Modify: `terraform/aws-compute/variables.tf` (or wherever the scheduled task command/`--mode` is set)
- Modify: `scripts/aws_build_push_and_run.sh`
- Modify: `README.md`
- Modify: `docs/RUNBOOK.md`

**Interfaces:** none (config + docs only).

- [ ] **Step 1: Point the schedule at `sync`**

Grep for the scheduled mode and update it:

Run: `grep -rn "mode" terraform/aws-compute scripts/aws_build_push_and_run.sh`

Set the EventBridge-scheduled container command / default `--mode` to `sync` (was `incremental`). In `scripts/aws_build_push_and_run.sh`, accept and pass through `--mode sync|canister|backfill|incremental` (default `sync`); the existing `--mode full` handling maps to `canister`.

- [ ] **Step 2: Update README CLI table + S3 layout**

In `README.md`, replace the modes description and CLI table to document `sync` (default), `canister`, `backfill`, `incremental`, and the new flags (`--rest-base-url`, `--rest-page-size`, `--backfill-overlap-ms`, `--incremental-overlap-ms`, `--backfill-floor`, `--action-types`). Update the S3 layout block to:

```
s3://<bucket>/<prefix>/canister/<runId>/<nnnn>_<canisterId>.csv
s3://<bucket>/<prefix>/rest/backfill/<runId>/transactions_backfill_<runId>.csv
s3://<bucket>/<prefix>/rest/incremental/<runId>/transactions_incremental_<runId>.csv
s3://<bucket>/<prefix>/state/state.json
```

Add a short "Cutover" note: run `--mode canister` once to archive deep history (sets `canisterArchiveComplete` + `canisterMaxTxTime`); scheduled `sync` then runs backfill-then-incremental against the REST API and never re-runs canister.

- [ ] **Step 3: Update RUNBOOK**

In `docs/RUNBOOK.md`, add the one-time canister archive step, the backfill/incremental/sync run commands, and how to inspect cutover state in `state.json`.

- [ ] **Step 4: Verify build + full test suite**

Run: `npm run build && npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add terraform/aws-compute scripts/aws_build_push_and_run.sh README.md docs/RUNBOOK.md
git -c user.name='topek' -c user.email='zachara.arkadiusz@gmail.com' commit -m "feat: default schedule to sync mode + document REST cutover"
```

---

## Self-Review

**Spec coverage:**
- Three modes + `sync` orchestrator that never runs canister → Tasks 2, 8. ✓
- State-flag cutover (`canisterArchiveComplete`, `canisterMaxTxTime`, `rest.*`) → Tasks 1, 8 (Step 5). ✓
- Raw, source-separated S3 layout → Task 7. ✓
- Stable pagination (fixed `end` snapshot, `begin=floor`) → Task 8 `runBackfill`. ✓
- Configurable seam overlap + explicit floor fallback → Tasks 2, 6, 8. ✓
- REST dedup on `txHash` + watermark → Tasks 6, 8 `runIncrementalRest`. ✓
- Reuse sink/state/manifest/retry/lock/logger → Tasks 3, 5, 7, 8. ✓
- Verified API facts (limit ≤ 100, 1-indexed, newest-first, wrapper) → Tasks 2, 4, 5. ✓
- Testing plan (mapping, paging stop conditions, watermark/dedup, ns→ms, client stub) → Tasks 1, 2, 4, 5, 6, 8. ✓
- Error handling via `withRetry`, non-200 wrapper as retryable → Task 5. ✓

**Placeholder scan:** No TBD/TODO/"handle edge cases" — every code step has concrete code. ✓

**Type consistency:** `Config['mode']` union (`canister|backfill|incremental|sync|full`) is defined in Task 2 and reused in Tasks 1, 7, 8. `CsvRecord`/`createSink(fileName, columns)` defined in Task 3, used in Tasks 4, 8. `RestTx`/`RestFindData`/`RestResponse` defined in Task 4, used in Tasks 5, 6, 8. `FindParams`/`fetchTransactions` defined in Task 5, used in Task 8 via `RestDeps`. Paging helper names (`computeBackfillFloor`, `reachedFloor`, `isShortPage`, `reachedWatermark`, `maxTxTime`, `newTxs`) defined in Task 6, used in Task 8. `modePathSegment` defined in Task 7. Consistent throughout. ✓
