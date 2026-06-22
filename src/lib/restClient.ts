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
