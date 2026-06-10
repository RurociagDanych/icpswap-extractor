import fs from 'node:fs';

export type CanisterState = {
  lastTotal: number;
  lastRun?: string;
  recentHashes: string[];
  completed?: boolean; // archival/full-complete marker
};

export type EtlState = {
  mode: 'full' | 'incremental';
  lastRunAt?: string;
  latestStorageId?: string;
  canisters: Record<string, CanisterState>;
};

export const defaultState = (): EtlState => ({
  mode: 'full',
  canisters: {},
});

// Cap on remembered tx hashes: must comfortably exceed the largest --overlap
// anyone would pass, while keeping state.json and run memory small.
export const RECENT_HASHES_LIMIT = 5000;

export function pushBounded(target: string[], values: string[], limit = RECENT_HASHES_LIMIT): void {
  for (const value of values) target.push(value);
  if (target.length > limit) target.splice(0, target.length - limit);
}

export function trimSetKeepLast(set: Set<string>, limit = RECENT_HASHES_LIMIT): void {
  if (set.size <= limit) return;
  const oldestFirst = set.values();
  while (set.size > limit) set.delete(oldestFirst.next().value!);
}

export function parseState(text: string): EtlState {
  const parsed = JSON.parse(text) as Partial<EtlState>;
  const state: EtlState = {
    ...defaultState(),
    ...parsed,
    canisters: parsed.canisters ?? {},
  };

  // backward compatibility from old schema
  const anyParsed = parsed as any;
  if (!parsed.canisters && anyParsed?.latestStorageId) {
    state.canisters[anyParsed.latestStorageId] = {
      lastTotal: Number(anyParsed.latestStorageTotal ?? 0),
      recentHashes: Array.isArray(anyParsed.recentHashes) ? anyParsed.recentHashes : [],
      lastRun: anyParsed.lastRunAt,
    };
  }

  return state;
}

export function loadState(path: string): EtlState {
  if (!fs.existsSync(path)) return defaultState();
  return parseState(fs.readFileSync(path, 'utf8'));
}

export function saveState(path: string, state: EtlState): void {
  fs.writeFileSync(path, `${JSON.stringify(state, null, 2)}\n`, 'utf8');
}
