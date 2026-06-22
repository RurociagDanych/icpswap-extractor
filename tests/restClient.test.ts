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
