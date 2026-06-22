# Design: REST backfill + incremental load with canister fallback

- **Date:** 2026-06-22
- **Status:** Approved (design); pending implementation plan
- **Author:** topek

## Problem

Since **2026-06-07** the live ICPSwap swap-record canisters stopped receiving new
transactions through the path this extractor uses (`baseStorage` registry →
per-canister `getBaseRecord`). The current `full` and `incremental` modes both read
those canisters, so incremental runs now emit nothing new.

ICPSwap publishes current data through a REST API
(`GET https://api.icpswap.com/info/transaction/find`). We must move ongoing
ingestion to that API while preserving the historical data that only the canisters
expose. Per the project owner, the REST API does **not** return the full deep
history, so the canister extraction path must be retained for a one-time archive.

## Goals

- Add a REST-based **backfill** and **incremental** load following the
  `/transaction/find` API.
- Keep the existing **canister** extraction as a one-time deep-history archive.
- After the canister archive exists on S3, run only **backfill + REST incremental** —
  never re-run the canister path.
- Guarantee no transaction is lost at the seam between the canister archive and the
  REST data, via a **configurable overlap**.
- Follow the project's existing ELT philosophy: land raw, immutable, per-run;
  model downstream.

## Non-goals

- Cross-source deduplication or schema unification. The canister and REST records have
  different schemas and identifiers; they land **raw and separate**, and any overlap
  band is resolved by downstream tooling.
- Backfilling REST history older than the canister seam (the canister archive already
  covers it).

## Verified API facts (probed 2026-06-22)

`GET https://api.icpswap.com/info/transaction/find`

Query params: `poolId`, `tokenId`, `principal`, `page` (**1-indexed**), `limit`,
`begin` (ms, inclusive), `end` (ms), `actionTypes` (`Swap,AddLiquidity,DecreaseLiquidity,Claim`).

- Response wrapper: `{ code, message, data: { totalElements, content: [...], page, limit } }`.
- **`limit` is capped at 100** server-side regardless of the requested value.
- Results are sorted **newest-first** by `txTime` (ms).
- `begin`/`end` narrow `totalElements` to the window — windowing bounds page depth.
- Deep page numbers (e.g. `page=5000`) are served normally.
- `totalElements` drifts run-to-run as new rows arrive — pagination must be anchored
  against a fixed `end` snapshot to stay stable.

Record fields (raw, landed as-is): `poolId`, `poolFee`, `positionId`,
`token0LedgerId`, `token0Price`, `token0Name`, `token0Symbol`, `token1LedgerId`,
`token1Price`, `token1Name`, `token1Symbol`, `actionType`, `fromPrincipalId`,
`fromSubaccount`, `fromAccountId`, `fromTextualId`, `fromAlias`, `toPrincipalId`,
`toSubaccount`, `toAccountId`, `toTextualId`, `toAlias`, `token0AmountIn`,
`token1AmountIn`, `token0AmountOut`, `token1AmountOut`, `token0Fee`, `token1Fee`,
`sqrtPrice`, `tickLimit`, `tick`, `liquidity`, `currentLiquidity`, `txHash`
(= `poolId+index`, stable & unique within REST), `txTime`, `token0TxValue`,
`token1TxValue`.

## Approach (chosen: A — three explicit modes + state-driven auto-skip)

Three load modes plus a default scheduled orchestrator. Each mode is small,
independently testable, and reuses the existing sink / state / manifest / retry / lock
machinery. Rejected alternatives: a single fused self-orchestrating job (long mixed
runs, harder to test/bound) and a REST-only deep-paging "full" (the API lacks the full
history; deep offset paging drifts).

### Modes

| Mode | Source | Purpose | Frequency |
|---|---|---|---|
| `canister` | IC canisters (today's `full` logic) | One-time deep-history archive | Once, manual |
| `backfill` | REST `/transaction/find` | Fill REST history down to the canister seam | Until complete, resumable |
| `incremental` | REST `/transaction/find` | New transactions since watermark | Every scheduled run |
| `sync` (default) | orchestrator | `backfill` (if incomplete) then `incremental` | Scheduled (EventBridge) |

`sync` is what EventBridge calls. It reads `state.json`, runs `backfill` when
`rest.backfillComplete !== true`, then runs `incremental`. It **never** runs
`canister`. The `canister` mode is a deliberate, manual one-time invocation; once
`canisterArchiveComplete` is set, nothing re-runs it.

## State schema additions

`state.json` keeps the existing per-canister section unchanged (so the archive stays
valid) and adds top-level canister-seam fields plus a `rest` section:

```jsonc
{
  "canisters": { /* unchanged */ },
  "canisterArchiveComplete": true,        // set when canister mode finishes a full pass
  "canisterMaxTxTime": 1749254400000,     // max txTime (ms) in canister data — the seam
  "rest": {
    "backfillComplete": false,
    "backfillCursor": { "endSnapshot": 1782127219000, "nextPage": 137 }, // resume point
    "backfillFloor": 1749254400000,       // canisterMaxTxTime - overlap; stop here
    "incrementalWatermark": 1782200000000,// max txTime emitted by incremental
    "recentTxHashes": ["ttnzy-...244431"] // bounded, for overlap dedup
  }
}
```

- `canisterMaxTxTime` is computed during the canister archive as the max record
  `timestamp`, converted ns→ms (canister timestamps are nanoseconds).
- If a pre-existing canister archive lacks `canisterMaxTxTime`, `backfill` **requires**
  an explicit `--backfill-floor` (or `--begin`) rather than guessing.
- `recentTxHashes` is bounded with the existing `RECENT_HASHES_LIMIT` helpers.

## Backfill algorithm (stable, bounded, resumable)

1. On first run, snapshot `endSnapshot = now`, persist it, and set
   `backfillFloor = canisterMaxTxTime - backfillOverlapMs` (or explicit `--backfill-floor`).
2. Query with `begin = backfillFloor`, `end = endSnapshot`, `page = nextPage`,
   `limit = 100`. The `begin`/`end` window bounds page depth to the seam→snapshot range;
   the fixed `end` keeps pages stable against front-arriving rows.
3. Map each record to a raw REST CSV row; stream to the sink. Persist `nextPage` after
   each page (resumable across restarts / task replacement).
4. Stop when a page returns fewer than `limit` rows or its oldest `txTime < backfillFloor`.
   Set `rest.backfillComplete = true`.

The overlap below the canister seam guarantees no gap; the duplicate band is resolved
downstream. (Implementation note: if a single window is very deep, backfill MAY chunk
into sub-windows by time to cap per-query page depth — same `endSnapshot`/`floor`
invariants apply. Decided during implementation based on observed window size.)

## Incremental algorithm

1. Read `incrementalWatermark` and `recentTxHashes`.
2. Page from `page = 1`, `end = now`, newest-first, `limit = 100`, skipping any record
   whose `txHash` is in `recentTxHashes`.
3. Stop when a page's oldest `txTime <= incrementalWatermark - incrementalOverlapMs`
   (or a short page is returned).
4. Emit new rows, update `incrementalWatermark` to the max `txTime` seen, and refresh the
   bounded `recentTxHashes`.

This mirrors the existing `runIncremental` overlap + hash-dedup pattern, keyed on REST
`txHash`/`txTime` instead of canister offset/`hash`.

## Modules

New (mirroring existing `src/lib` structure):

- `src/idl/restTx.ts` — REST record type + the response wrapper type.
- `src/lib/restClient.ts` — typed `fetch` wrapper over `/transaction/find`: builds query,
  parses the wrapper, retries via existing `withRetry`, exposes a page-iterator.
- `src/lib/restCsv.ts` — REST record → raw CSV row + header list (native REST columns,
  no lossy mapping).
- REST run logic (`runBackfill`, `runIncrementalRest`) and the `sync` orchestrator — in
  `src/index.ts` or a new `src/lib/orchestrator.ts`, following the current `runFull`/
  `runIncremental` style.

Reused unchanged: `csvSink`, `storageTarget`, `state` (extended), `retry`, `logger`,
manifest, run lock.

## Output layout (source-separated)

```
s3://<bucket>/<prefix>/canister/<runId>/<nnnn>_<canisterId>.csv     # existing canister archive
s3://<bucket>/<prefix>/rest/backfill/<runId>/transactions_<pageRange>.csv
s3://<bucket>/<prefix>/rest/incremental/<runId>/transactions.csv
s3://<bucket>/<prefix>/state/state.json
```

Each run still writes a `manifest.json` (rows/bytes/sha256 per file). The existing
`full`/`incremental` S3 path segments are renamed conceptually to `canister`/`rest`;
the canister archive's own per-canister filenames are unchanged.

## Config additions

| Flag | Default | Notes |
|---|---|---|
| `--mode` | `sync` | `sync` \| `canister` \| `backfill` \| `incremental` |
| `--rest-base-url` | `https://api.icpswap.com/info` | REST host base |
| `--rest-page-size` | `100` | validated `1..100` (server cap) |
| `--backfill-overlap-ms` | (e.g. `3600000`) | configurable seam overlap below canister cutoff |
| `--incremental-overlap-ms` | (e.g. `300000`) | re-fetch window for incremental dedup |
| `--backfill-floor` | (none) | explicit floor when `canisterMaxTxTime` is absent |
| `--action-types` | `Swap,AddLiquidity,DecreaseLiquidity,Claim` | API filter |

The existing canister flags (`--page-size`, `--concurrency`, etc.) remain for `canister`
mode. `--mode full` is retained as an alias for `canister` for backward compatibility (or
removed — decided in the plan).

## Error handling

- All REST calls go through `withRetry` (bounded exponential backoff, max 5 attempts),
  matching canister queries.
- Non-200 `code` in the response wrapper is treated as a retryable error with the
  `message` logged.
- Backfill persists its cursor after every page, so a killed task resumes without
  re-emitting or skipping pages.
- A shrinking `totalElements` mid-window is expected (drift); the fixed `endSnapshot`
  makes it harmless. Incremental logs (not fails) if the newest `txTime` is older than the
  stored watermark (possible source anomaly), consistent with the existing
  "shrinking total" warning behavior.
- Run lock, SIGTERM/SIGINT handling, and structured JSON logging are unchanged.

## Testing

Pure-logic unit tests (`node:test`, matching existing suites):

- REST record → CSV row mapping (field coverage, types as strings).
- Backfill cursor/stop conditions (floor boundary, short page, resume from `nextPage`).
- Incremental watermark + `txHash` dedup over an overlap window.
- ns→ms seam computation for `canisterMaxTxTime`.
- Config parsing/validation for the new flags (page-size cap, overlap >= 0, floor
  fallback rule).
- `restClient` against a stubbed `fetch` (query construction, wrapper parsing, retry on
  non-200).

## Open implementation decisions (not blocking design)

- Whether backfill chunks deep windows into time sub-windows (depends on observed depth).
- Keep `--mode full` as an alias vs. rename outright.
- Whether `sync`/REST logic lives in `index.ts` or a dedicated `orchestrator.ts`.

These are resolved in the implementation plan, not re-litigated in design.
