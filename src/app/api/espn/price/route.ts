import axios from 'axios';
import { NextResponse } from 'next/server';
import * as cache from '@/lib/cache';
import historyStore from '@/lib/historyStore';
import { computeInitialPriceFromStats, computePerformanceScore, computePerformanceFactor, computePriceFromPerformance } from '@/lib/pricing';

// Season adjustment tuning: multiplier applied to (ratio - 1) before clamping.
// Configurable via env: SEASON_ADJ_MULTIPLIER (default 0.5). Range is expected
// to be small (e.g., 0.1..1.0). Smaller values reduce season influence.
const SEASON_ADJ_MULTIPLIER = Number(process.env.SEASON_ADJ_MULTIPLIER ?? process.env.NEXT_PUBLIC_SEASON_ADJ_MULTIPLIER ?? 0.5);
// Season stats cache TTL in milliseconds. Configurable via env: SEASON_STATS_TTL_MS
const SEASON_STATS_TTL_MS = Number(process.env.SEASON_STATS_TTL_MS ?? process.env.NEXT_PUBLIC_SEASON_STATS_TTL_MS ?? 10 * 60 * 1000);
// Season adjustment cap (absolute). Default ±0.10 (10%). Configurable via env: SEASON_ADJ_CAP or NEXT_PUBLIC_SEASON_ADJ_CAP
const SEASON_ADJ_CAP = Number(process.env.SEASON_ADJ_CAP ?? process.env.NEXT_PUBLIC_SEASON_ADJ_CAP ?? 0.1);

// Simple in-memory TTL cache for season stats: espnId -> { expires: number, value: any }
const seasonStatsCache = new Map<string, { expires: number; value: any }>();

// Simple dev-only automatic prewarm: when this module loads in development,
// probe localhost ports to find the running dev server, fetch the first page
// of players, then call /api/espn/price for the first visible players in
// parallel to warm per-event summaries and price caches.
async function runDevPrewarm() {
  try {
    if (process.env.NODE_ENV === 'production') return;
    const portsToTry = [3000, 3001, 3002, 3003];
    let baseUrl: string | null = null;
    for (const p of portsToTry) {
      try {
        const url = `http://localhost:${p}/api/espn/players?team=all&limit=50`;
        const res = await axios.get(url, { timeout: 1500 });
        if (res?.data?.response && Array.isArray(res.data.response)) {
          baseUrl = `http://localhost:${p}`;
          // we found a working dev server
          const ids = res.data.response.slice(0, 12).map((pl: any) => pl.id).filter(Boolean);
          if (ids.length) {
            console.log('[dev-prewarm] warming prices for', ids.length, 'players from', url);
              // increment prewarm counter
              cache.incrPrewarmCount(ids.length);
            await Promise.all(ids.map((id: string) => axios.get(`${baseUrl}/api/espn/price?espnId=${encodeURIComponent(id)}&currentPrice=100`).catch((e) => ({ error: String(e?.message ?? e) }))));
            console.log('[dev-prewarm] done');
          }
          break;
        }
      } catch (e) {
        // ignore and try next port
      }
    }
    if (!baseUrl) {
      console.log('[dev-prewarm] no local dev server detected on ports', portsToTry.join(', '));
    }
  } catch (e) {
    console.error('[dev-prewarm] error', e);
  }
}

// Kick off a delayed prewarm in development so it doesn't block module load.
if (process.env.NODE_ENV !== 'production') {
  setTimeout(() => void runDevPrewarm(), 1500);
}

// In-flight dedupe map for event summary fetches
const inFlightSummaries = new Map<string, Promise<any>>();

// In-memory price history map for dev: espnId/name -> PricePoint[]
const PRICE_HISTORY_MAP = new Map<string, { t: string; p: number }[]>();
// Load persisted history into the in-memory map (only affects dev/local)
// Load persisted history into in-memory map (dev/backends). If the store
// supports a backend it will populate PRICE_HISTORY_MAP.
void (async () => {
  try {
    await historyStore.loadInto(PRICE_HISTORY_MAP);
     
    console.log('[history] loaded', PRICE_HISTORY_MAP.size, 'keys from store');
  } catch (e) {
    // ignore
  }
})();
// Expose to global so other route modules can read it in the same server process
// (useful in dev where modules may be hot-reloaded).
// eslint-disable-next-line @typescript-eslint/no-explicit-any
;(globalThis as any).__PRICE_HISTORY_MAP__ = PRICE_HISTORY_MAP;

async function fetchSummaryWithDedupe(id: string, ttlMs: number) {
  const cacheKey = `espn:summary:${id}`;
  const getIfFreshRuntime: <T = any>(key: string, ttlMs?: number) => T | undefined = (cache as any).getIfFreshTracked || (cache as any).getIfFresh;
  const cached = getIfFreshRuntime<any>(cacheKey, ttlMs);
  if (cached) return cached;

  // If there's an in-flight fetch, reuse its promise
  const existing = inFlightSummaries.get(id);
    if (existing) {
    // track that we reused an existing in-flight fetch (dedupe hit)
    cache.incrInFlightDedupeHits(1);
    return existing;
  }

  const p = (async () => {
    try {
      const summaryUrl = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=${id}`;
      const resp = await axios.get(summaryUrl);
      const data = resp.data;
      cache.setCached(cacheKey, data);
      return data;
    } finally {
      // clear in-flight regardless of success/failure to avoid leaks
      inFlightSummaries.delete(id);
    }
  })();

  inFlightSummaries.set(id, p);
  return p;
}

// Simple heuristic: fetch today's scoreboard, find games, fetch summary/boxscore
// for each game and search for the provided espnId (or player name). Compute a
// naive score from yards/tds/turnovers and return a price delta suggestion.

function safeNum(v: any) {
  if (v == null) return 0;
  const cleaned = String(v).replace(/[^0-9.\-]/g, '');
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : 0;
}

function computeDeltaFromStats(stats: { yards?: number; tds?: number; ints?: number; fumbles?: number }) {
  const yards = stats.yards ?? 0;
  const tds = stats.tds ?? 0;
  const ints = stats.ints ?? 0;
  const fumbles = stats.fumbles ?? 0;

  // weights chosen to be modest; this is just a demo formula.
  const score = yards * 0.02 + tds * 6 - (ints + fumbles) * 3;
  return Number(score.toFixed(2));
}

// Try to fetch season-level stats for a player id. Returns a small normalized
// stats object or null. This attempts a couple ESPN endpoints and does a
// best-effort extraction of common stat fields (yards, rec, rush, tds, ints, fumbles, position).
async function fetchSeasonStatsForPlayer(espnId: string) {
  if (!espnId) return null;
  // Check cache first
  try {
    const cached = seasonStatsCache.get(espnId);
    if (cached && cached.expires > Date.now()) return cached.value;
    if (cached) seasonStatsCache.delete(espnId);
  } catch (e) {
    // ignore cache errors
  }
  const tried = [] as string[];
  const candidates = [
    `https://site.api.espn.com/apis/common/v3/sports/football/nfl/athletes/${encodeURIComponent(espnId)}/statistics`,
    `https://site.api.espn.com/apis/common/v3/sports/football/nfl/athletes/${encodeURIComponent(espnId)}`,
    `https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/${encodeURIComponent(espnId)}/profile`,
  ];
  // Helper to extract fields heuristically from an object
  function extract(o: any) {
    if (!o || typeof o !== 'object') return null;
    const maybe = {
      yards: safeNum(o.yards ?? o.yds ?? o.receivingYards ?? o.rushingYards ?? o.passingYards ?? o.statYards ?? o.totalYards),
      rec: safeNum(o.rec ?? o.receptions ?? o.receiving ?? o.catches),
      rush: safeNum(o.rush ?? o.rushing ?? o.rushingAttempts),
      tds: safeNum(o.tds ?? o.touchdowns ?? o.touchdown ?? o.touchdownsTotal),
      ints: safeNum(o.ints ?? o.interceptions ?? o.int),
      fumbles: safeNum(o.fumbles ?? o.fumblesLost ?? o.fum),
      position: (o.position && (o.position.abbreviation || o.position.displayName)) || o.position || null,
    };
    // If we found any non-zero numeric stat, return it
    if (maybe.yards || maybe.rec || maybe.rush || maybe.tds || maybe.ints || maybe.fumbles) return maybe;
    return null;
  }

  for (const url of candidates) {
    try {
      // Try local advanced stats API first (fast, file-backed) to get enriched metrics
      try {
        const advResp = await axios.get(`${process.env.NEXT_PUBLIC_BASE_URL ?? ''}/api/advanced/player?espnId=${encodeURIComponent(espnId)}`, { timeout: 800 }).catch(() => null);
        if (advResp && advResp.data && advResp.data.ok && advResp.data.data) {
          const adv = advResp.data.data;
          // merge basic extracted fields with advanced metrics
          const merged = { ...adv };
          try { seasonStatsCache.set(espnId, { expires: Date.now() + SEASON_STATS_TTL_MS, value: merged }); } catch (er) {}
          return merged;
        }
      } catch (e) {
        // ignore local advanced API failures
      }
      tried.push(url);
      const resp = await axios.get(url, { timeout: 2500 });
      const data = resp?.data;
      // shallow attempts: if data has a splits/rows array, scan it
      if (Array.isArray(data?.splits)) {
        for (const s of data.splits) {
          const e = extract(s);
          if (e) {
            try { seasonStatsCache.set(espnId, { expires: Date.now() + SEASON_STATS_TTL_MS, value: e }); } catch (er) {}
            return e;
          }
        }
      }
      // if contains athlete or person
      if (data?.athlete) {
        const e = extract(data.athlete);
        if (e) {
          try { seasonStatsCache.set(espnId, { expires: Date.now() + SEASON_STATS_TTL_MS, value: e }); } catch (er) {}
          return e;
        }
      }
      // deep scan: walk top-level keys and try to extract
      for (const k of Object.keys(data || {})) {
        const v = (data as any)[k];
        const e = extract(v);
        if (e) {
          try { seasonStatsCache.set(espnId, { expires: Date.now() + SEASON_STATS_TTL_MS, value: e }); } catch (er) {}
          return e;
        }
        if (Array.isArray(v)) {
          for (const item of v) {
            const ei = extract(item);
            if (ei) {
              try { seasonStatsCache.set(espnId, { expires: Date.now() + SEASON_STATS_TTL_MS, value: ei }); } catch (er) {}
              return ei;
            }
          }
        }
      }
    } catch (e) {
      // ignore and continue
    }
  }
  return null;
}

// Recursively search an object for a player matching espnId or name. Returns
// a small normalized stat summary when found.
function findPlayerStats(obj: any, espnId?: string, name?: string): any | null {
  if (!obj || typeof obj !== 'object') return null;

  // direct match by id or person.id
  const id = obj.id ?? obj.playerId ?? obj.person?.id ?? obj.uid ?? null;
  const displayName = (obj.displayName || obj.fullName || obj.person?.fullName || obj.person?.displayName || obj.name) ?? null;
  if (espnId && id && String(id) === String(espnId)) {
    // Try to extract obvious stat fields
    const yards = safeNum(obj.yards ?? obj.yds ?? obj.rushingYards ?? obj.receivingYards ?? obj.passingYards ?? obj.statYards);
    const tds = safeNum(obj.touchdowns ?? obj.td ?? obj.tds);
    const ints = safeNum(obj.interceptions ?? obj.int ?? obj.ints);
    const fumbles = safeNum(obj.fumblesLost ?? obj.fumbles ?? obj.fum);
    return { id, name: displayName, yards, tds, ints, fumbles, raw: obj };
  }

  if (name && displayName && String(displayName).toLowerCase().includes(String(name).toLowerCase())) {
    const yards = safeNum(obj.yards ?? obj.yds ?? obj.rushingYards ?? obj.receivingYards ?? obj.passingYards ?? obj.statYards);
    const tds = safeNum(obj.touchdowns ?? obj.td ?? obj.tds);
    const ints = safeNum(obj.interceptions ?? obj.int ?? obj.ints);
    const fumbles = safeNum(obj.fumblesLost ?? obj.fumbles ?? obj.fum);
    return { id, name: displayName, yards, tds, ints, fumbles, raw: obj };
  }

  // Dive into arrays/objects
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (Array.isArray(v)) {
      for (const item of v) {
        const found = findPlayerStats(item, espnId, name);
        if (found) return found;
      }
    } else if (v && typeof v === 'object') {
      const found = findPlayerStats(v, espnId, name);
      if (found) return found;
    }
  }

  return null;
}

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const espnId = url.searchParams.get('espnId') || url.searchParams.get('id') || undefined;
    const name = url.searchParams.get('name') || undefined;
    const currentPrice = Number(url.searchParams.get('currentPrice') ?? url.searchParams.get('price') ?? 100);

    if (!espnId && !name) {
      return NextResponse.json({ ok: false, error: 'expected espnId or name query param' }, { status: 400 });
    }

    // 1) Fetch today's scoreboard and find live/ongoing games
    const boardResp = await axios.get('https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard');
    const events = Array.isArray(boardResp.data?.events) ? boardResp.data.events : [];

    let foundStats: any = null;
    const debug: any = { checkedGames: 0, events: events.length };

    // Prepare event summary cache keys and check cache first
    const SUMMARY_TTL_MS = 15 * 1000; // 15 seconds per-event cache
    const eventIds: string[] = [];
    for (const ev of events) {
      const id = ev?.id ?? ev?.uid ?? ev?.shortName ?? null;
      if (id) eventIds.push(String(id));
    }

    // First, search cached summaries for a match
    const missingIds: string[] = [];
    const summaries: Record<string, any> = {};
    for (const id of eventIds) {
      const cacheKey = `espn:summary:${id}`;
        const getIfFreshRuntime: <T = any>(key: string, ttlMs?: number) => T | undefined = (cache as any).getIfFreshTracked || (cache as any).getIfFresh;
        const cached = getIfFreshRuntime<any>(cacheKey, SUMMARY_TTL_MS);
        if (cached) {
        summaries[id] = cached;
        const found = findPlayerStats(cached, espnId, name);
        debug.checkedGames++;
        if (found) {
          foundStats = { ...found, eventId: id };
          debug.lastEvent = id;
          break;
        }
      } else {
        missingIds.push(id);
      }
    }

    // If not found in cache, fetch missing summaries in parallel (but stop early if found)
    if (!foundStats && missingIds.length > 0) {
      // Fetch summaries using the dedupe helper in parallel
      const fetchPromises = missingIds.map((id) => fetchSummaryWithDedupe(id, SUMMARY_TTL_MS).then((data) => ({ id, ok: true, data })).catch((e) => ({ id, ok: false, error: String(e?.message ?? e) })));
      const results = await Promise.allSettled(fetchPromises);
      for (const r of results) {
        if (r.status !== 'fulfilled') continue;
        const payload = (r as PromiseFulfilledResult<any>).value;
        const { id, ok, data, error } = payload;
        debug.checkedGames++;
        if (!ok) {
          debug[`error_event_${id}`] = error;
          continue;
        }
        summaries[id] = data;
        const found = findPlayerStats(data, espnId, name);
        if (found) {
          foundStats = { ...found, eventId: id };
          debug.lastEvent = id;
          break;
        }
      }
    }

    if (!foundStats) {
      // No live stats found — simulate a small price movement based on
      // recent season fantasy production (if available) and a small random volatility.
      try {
        // Try to fetch season-level stats to produce a fantasyScore baseline
        let seasonStats = null as any;
        if (espnId) {
          try {
            seasonStats = await fetchSeasonStatsForPlayer(String(espnId));
          } catch (e) {
            seasonStats = null;
          }
        }

        function getNumField(o: any, keys: string[]) {
          for (const k of keys) {
            if (o && typeof o === 'object' && (o[k] !== undefined && o[k] !== null)) return safeNum(o[k]);
          }
          return 0;
        }

        // compute fantasyScore from seasonStats when available
        const seasonFantasy = seasonStats
          ? (
              (getNumField(seasonStats, ['passing_yards', 'passingYards', 'passYards', 'passingYardsTotal']) * 0.04) +
              (getNumField(seasonStats, ['passing_tds', 'passingTDs', 'pass_tds']) * 4) +
              (getNumField(seasonStats, ['interceptions', 'ints']) * -2) +
              (getNumField(seasonStats, ['rushing_yards', 'rushYards', 'rush']) * 0.1) +
              (getNumField(seasonStats, ['rushing_tds', 'rush_tds']) * 6) +
              (getNumField(seasonStats, ['receptions', 'rec', 'catches', 'rec_y']) * 0.5) +
              (getNumField(seasonStats, ['receiving_yards', 'receivingYards', 'rec_yards']) * 0.1) +
              (getNumField(seasonStats, ['receiving_tds', 'rec_tds']) * 6)
            )
          : 0;

        // Use cached last fantasy score (if any) to compute delta; store for next time.
        const lastKey = espnId ? `lastFantasyScore:${String(espnId)}` : null;
        const lastFantasy = lastKey ? (cache.getAny<number>(lastKey) ?? seasonFantasy) : seasonFantasy;
        const fantasyDelta = seasonFantasy - (lastFantasy ?? 0);
        if (lastKey) cache.setCached(lastKey, seasonFantasy);

  // Random volatility between 3% and 5% to make market move feel alive
  const baseVol = 0.03;
  const extra = Math.random() * 0.02; // 0..0.02
  const volatility = baseVol + extra; // 0.03 .. 0.05
  // ±volatility random component plus ±3% per 10 fantasy points delta
  const randomComp = (Math.random() - 0.5) * 2 * volatility;
        const deltaComp = (fantasyDelta / 10) * 0.03;
        const change = randomComp + deltaComp;
        const newPrice = Math.max(0.01, Number((currentPrice * (1 + change)).toFixed(2)));

        // Append to in-memory history so frontend charts show movement
        const key = espnId ? String(espnId) : `name:${String(name || '').toLowerCase()}`;
        const nowIso = new Date().toISOString();
        const arr = PRICE_HISTORY_MAP.get(key) || [];
        const last = arr[arr.length - 1];
        const todayDate = nowIso.slice(0, 10);
        if (last && last.t && last.t.slice(0, 10) === todayDate) {
          last.t = nowIso;
          last.p = newPrice;
        } else {
          arr.push({ t: nowIso, p: newPrice });
          if (arr.length > 240) arr.shift();
        }
        PRICE_HISTORY_MAP.set(key, arr);
        try { void historyStore.appendPoint(key, { t: nowIso, p: newPrice }); } catch (e) {}

        debug.simulated = { fantasyDelta, seasonFantasy, randomComp: Number(randomComp.toFixed(4)), deltaComp: Number(deltaComp.toFixed(4)), change: Number(change.toFixed(4)) };
        return NextResponse.json({ ok: true, found: false, simulated: true, newPrice, change, _debug: debug });
      } catch (e) {
        // fallback to previous behavior: seed synthetic history point
        try {
          const key = espnId ? String(espnId) : `name:${String(name || '').toLowerCase()}`;
          const nowIso = new Date().toISOString();
          const arr = PRICE_HISTORY_MAP.get(key) || [];
          const last = arr[arr.length - 1];
          const today = nowIso.slice(0, 10);
          if (last && last.t && last.t.slice(0, 10) === today) {
            last.t = nowIso;
            last.p = currentPrice;
          } else {
            arr.push({ t: nowIso, p: currentPrice });
            if (arr.length > 240) arr.shift();
          }
          PRICE_HISTORY_MAP.set(key, arr);
        } catch (e) {}
        return NextResponse.json({ ok: true, found: false, message: 'no live stats found for player in current scoreboard', _debug: debug });
      }
    }

  // Observed absolute delta (units of currency) from naive stat formula
  const absDelta = computeDeltaFromStats({ yards: foundStats.yards, tds: foundStats.tds, ints: foundStats.ints, fumbles: foundStats.fumbles });

    // Convert to percent change relative to currentPrice
    const observedPct = currentPrice > 0 ? absDelta / currentPrice : 0;

    // Smoothing & caps configuration
    const MAX_CHANGE_PCT = 0.10; // ±10% per update
    const SMOOTHING_ALPHA = 0.3; // EMA alpha
    const MIN_DELTA_THRESHOLD = 0.005; // 0.5% -> ignore small moves

  // Use cache to persist previous smoothed pct per player (or name)
    const keyBase = foundStats.id ? `price-smoothed:${foundStats.id}` : `price-smoothed:name:${(name || foundStats.name || '').toLowerCase()}`;
  const prev = cache.getAny<number>(keyBase) ?? 0;

    // EMA smoothing
    const newSmoothed = SMOOTHING_ALPHA * observedPct + (1 - SMOOTHING_ALPHA) * prev;

    // If the smoothed move is below the min threshold, treat as zero
    const effectivePct = Math.abs(newSmoothed) < MIN_DELTA_THRESHOLD ? 0 : newSmoothed;

    // Cap per-update pct
    const appliedPct = Math.max(-MAX_CHANGE_PCT, Math.min(MAX_CHANGE_PCT, effectivePct));

  // Persist smoothed value for next call
  cache.setCached(keyBase, newSmoothed);

    // Try to fetch season stats and compute a season-derived baseline. We'll
    // use this to slightly bias the appliedPct: strong season performance
    // increases sensitivity, poor season reduces it. This is a small nudge —
    // we don't want season totals to cause large jumps during live updates.
    let seasonStats: any = null;
    let seasonBaselinePrice: number | null = null;
    try {
      if (foundStats.id) {
        seasonStats = await fetchSeasonStatsForPlayer(String(foundStats.id));
        if (seasonStats) {
          // Use computeInitialPriceFromStats with a modest base to get a season score
          seasonBaselinePrice = computeInitialPriceFromStats(
            { yards: seasonStats.yards, rec: seasonStats.rec, rush: seasonStats.rush, tds: seasonStats.tds, ints: seasonStats.ints, fumbles: seasonStats.fumbles },
            seasonStats.position || undefined,
            80
          );
          debug.seasonBaseline = seasonBaselinePrice;
        }
      }
    } catch (e) {
      // ignore season fetch errors
    }

    // Compute final price. If we have richer seasonStats (with advanced metrics),
    // use the user-supplied performance formulas to compute a normalized performance
    // factor and map it through tanh scaling to derive the new price. Otherwise
    // fall back to the previous appliedPct logic which nudges price by recent events.
    let newPrice: number;
    const finalAppliedPct = appliedPct;
    try {
      const hasAdvanced = seasonStats && (seasonStats.EPA_per_play != null || seasonStats.CPOE != null || seasonStats.RushYardsOverExpected_per_Att != null || seasonStats.YardsPerRouteRun != null || seasonStats.EPA_Allowed_per_Play != null);
      if (hasAdvanced) {
        // Compute position-aware performance score
        const score = computePerformanceScore(seasonStats.position || undefined, seasonStats);
        const leagueAvg = Number(process.env.LEAGUE_AVG_POSITION_SCORE ?? 1) || 1;
        const sensitivity = Number(process.env.PERF_SENSITIVITY ?? 1) || 1;
        const perfFactor = computePerformanceFactor(score, leagueAvg);
        const candidatePrice = computePriceFromPerformance(currentPrice, perfFactor, sensitivity);
        // Limit changes per update to MAX_CHANGE_PCT
        const pctFromPerf = currentPrice > 0 ? (candidatePrice - currentPrice) / currentPrice : 0;
        const cappedPct = Math.max(-MAX_CHANGE_PCT, Math.min(MAX_CHANGE_PCT, pctFromPerf));
        newPrice = Math.max(0.01, Number((currentPrice * (1 + cappedPct)).toFixed(2)));
        debug.performance = { score: Number(score.toFixed(4)), leagueAvg, perfFactor: Number(perfFactor.toFixed(4)), sensitivity, pctFromPerf: Number(pctFromPerf.toFixed(4)), cappedPct };
      } else {
        // If no advanced stats, use season baseline to nudge appliedPct like before
        let finalPct = appliedPct;
        if (seasonBaselinePrice && currentPrice > 0) {
          const ratio = seasonBaselinePrice / currentPrice; // e.g. 1.2 if baseline > current
          const adjRaw = (ratio - 1) * SEASON_ADJ_MULTIPLIER;
          const adj = Math.max(-SEASON_ADJ_CAP, Math.min(SEASON_ADJ_CAP, adjRaw));
          finalPct = Math.max(-MAX_CHANGE_PCT, Math.min(MAX_CHANGE_PCT, appliedPct + adj));
          debug.seasonAdj = { ratio: Number(ratio.toFixed(3)), multiplier: SEASON_ADJ_MULTIPLIER, cap: SEASON_ADJ_CAP, adj: Number(adj.toFixed(4)), finalAppliedPct: finalPct };
        }
        newPrice = Math.max(0.01, Number((currentPrice * (1 + finalPct)).toFixed(2)));
      }
    } catch (e) {
      // in case of any failure, fallback to the simple appliedPct
      newPrice = Math.max(0.01, Number((currentPrice * (1 + appliedPct)).toFixed(2)));
    }

    const response = {
      ok: true,
      found: true,
      player: { id: foundStats.id, name: foundStats.name },
      stats: { yards: foundStats.yards, tds: foundStats.tds, ints: foundStats.ints, fumbles: foundStats.fumbles },
      absDelta,
      observedPct,
      prevSmoothedPct: prev,
      newSmoothedPct: newSmoothed,
      appliedPct,
      newPrice,
      _debug: { ...debug, maxChangePct: MAX_CHANGE_PCT, smoothingAlpha: SMOOTHING_ALPHA, minDeltaThreshold: MIN_DELTA_THRESHOLD },
    };

    // Append to in-memory price history (use espn id if present, otherwise name key)
    try {
      const key = foundStats.id ? String(foundStats.id) : `name:${String(foundStats.name || name || '').toLowerCase()}`;
      const nowIso = new Date().toISOString();
  const arr = PRICE_HISTORY_MAP.get(key) || [];
      // If last point is same-day, replace it to avoid duplicate days
      const last = arr[arr.length - 1];
      const todayDate = nowIso.slice(0, 10);
      if (last && last.t && last.t.slice(0, 10) === todayDate) {
        last.t = nowIso;
        last.p = newPrice;
      } else {
        arr.push({ t: nowIso, p: newPrice });
        // limit to 240 points (~daily for ~8 months or high-frequency for demos)
        if (arr.length > 240) arr.shift();
      }
      PRICE_HISTORY_MAP.set(key, arr);
      // append to persistent store (backend will choose Redis/SQLite/file)
      try {
        void historyStore.appendPoint(key, { t: nowIso, p: newPrice });
      } catch (e) {
        // ignore
      }
    } catch (e) {
      // non-fatal; continue
    }

    return NextResponse.json(response);
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}
