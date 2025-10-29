type CacheEntry = { value: any; ts: number };
const store = new Map<string, CacheEntry>();

// Persist counters on globalThis during development so they survive hot-reloads.
// Use a unique key to avoid colliding with other globals.
const GLOBAL_KEY = '__MY_APP_CACHE_STATS__' as const;
function getGlobalCounters() {
  const g: any = globalThis as any;
  // Only persist on global in non-production environments
  if (process.env.NODE_ENV === 'production') {
    return { hits: 0, misses: 0, prewarm: 0, inFlightDedupeHits: 0 } as any;
  }
  if (!g[GLOBAL_KEY]) {
    g[GLOBAL_KEY] = { hits: 0, misses: 0, prewarm: 0, inFlightDedupeHits: 0 };
  }
  return g[GLOBAL_KEY] as { hits: number; misses: number; prewarm: number; inFlightDedupeHits: number };
}

let hitCount = getGlobalCounters().hits;
let missCount = getGlobalCounters().misses;
let prewarmCount = getGlobalCounters().prewarm ?? 0;
let inFlightDedupeHits = getGlobalCounters().inFlightDedupeHits ?? 0;

/**
 * Return cached value only if it is fresh within ttlMs, otherwise null.
 */
export function getIfFresh<T>(key: string, ttlMs: number): T | null {
  const entry = store.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts < ttlMs) return entry.value as T;
  return null;
}

// wrapped versions that track hits/misses
export function getIfFreshTracked<T>(key: string, ttlMs: number): T | null {
  const v = getIfFresh<T>(key, ttlMs);
  const g = getGlobalCounters();
  if (v != null) {
    hitCount++;
    g.hits = hitCount;
  } else {
    missCount++;
    g.misses = missCount;
  }
  return v;
}

/**
 * Return cached value regardless of age (useful as a stale fallback).
 */
export function getAny<T>(key: string): T | null {
  const entry = store.get(key);
  return entry ? (entry.value as T) : null;
}

export function getCacheStats() {
  const g = getGlobalCounters();
  // Keep local values in sync in case someone mutated global directly
  hitCount = g.hits;
  missCount = g.misses;
  prewarmCount = g.prewarm ?? prewarmCount;
  inFlightDedupeHits = g.inFlightDedupeHits ?? inFlightDedupeHits;
  return { hits: hitCount, misses: missCount, prewarm: prewarmCount, inFlightDedupeHits, keys: store.size };
}

export function setCached(key: string, value: any): void {
  store.set(key, { value, ts: Date.now() });
}

export function clearCache(): void {
  store.clear();
}

// Dev helpers to increment extra counters
export function incrPrewarmCount(amount = 1) {
  const g = getGlobalCounters();
  prewarmCount += amount;
  if (process.env.NODE_ENV !== 'production') g.prewarm = prewarmCount;
}

export function incrInFlightDedupeHits(amount = 1) {
  const g = getGlobalCounters();
  inFlightDedupeHits += amount;
  if (process.env.NODE_ENV !== 'production') g.inFlightDedupeHits = inFlightDedupeHits;
}
