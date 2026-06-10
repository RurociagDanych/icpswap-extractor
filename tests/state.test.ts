import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  RECENT_HASHES_LIMIT,
  defaultState,
  loadState,
  parseState,
  pushBounded,
  saveState,
  trimSetKeepLast,
} from '../src/lib/state.js';

test('parseState parses the current schema', () => {
  const state = parseState(
    JSON.stringify({
      mode: 'incremental',
      lastRunAt: '2026-06-01T00:00:00.000Z',
      latestStorageId: 'aaaaa-aa',
      canisters: {
        'aaaaa-aa': { lastTotal: 42, recentHashes: ['h1', 'h2'], completed: false },
      },
    })
  );

  assert.equal(state.mode, 'incremental');
  assert.equal(state.latestStorageId, 'aaaaa-aa');
  assert.equal(state.canisters['aaaaa-aa'].lastTotal, 42);
  assert.deepEqual(state.canisters['aaaaa-aa'].recentHashes, ['h1', 'h2']);
});

test('parseState migrates the legacy single-canister schema', () => {
  const state = parseState(
    JSON.stringify({
      mode: 'incremental',
      lastRunAt: '2026-01-01T00:00:00.000Z',
      latestStorageId: 'bbbbb-bb',
      latestStorageTotal: 7,
      recentHashes: ['x'],
    })
  );

  assert.equal(state.canisters['bbbbb-bb'].lastTotal, 7);
  assert.deepEqual(state.canisters['bbbbb-bb'].recentHashes, ['x']);
  assert.equal(state.canisters['bbbbb-bb'].lastRun, '2026-01-01T00:00:00.000Z');
});

test('saveState/loadState round-trips through a file', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icpswap-extractor-state-'));
  const file = path.join(dir, 'state.json');

  const state = defaultState();
  state.mode = 'full';
  state.canisters['ccccc-cc'] = { lastTotal: 100, recentHashes: ['a'], completed: true };

  saveState(file, state);
  const loaded = loadState(file);

  assert.deepEqual(loaded, state);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('loadState returns defaultState for a missing file', () => {
  const loaded = loadState(path.join(os.tmpdir(), 'does-not-exist-icpswap-extractor', 'state.json'));
  assert.deepEqual(loaded, defaultState());
});

test('pushBounded keeps only the most recent entries', () => {
  const target = ['a', 'b'];
  pushBounded(target, ['c', 'd', 'e'], 3);
  assert.deepEqual(target, ['c', 'd', 'e']);
  pushBounded(target, ['f'], 3);
  assert.deepEqual(target, ['d', 'e', 'f']);
});

test('pushBounded leaves arrays under the limit untouched', () => {
  const target = ['a'];
  pushBounded(target, ['b'], 5);
  assert.deepEqual(target, ['a', 'b']);
});

test('trimSetKeepLast removes oldest-inserted entries first', () => {
  const set = new Set(['a', 'b', 'c', 'd']);
  trimSetKeepLast(set, 2);
  assert.deepEqual(Array.from(set), ['c', 'd']);
});

test('RECENT_HASHES_LIMIT is exported for callers', () => {
  assert.equal(RECENT_HASHES_LIMIT, 5000);
});
