import axios from "axios";
import { MOCK_ATHLETES } from "@/lib/mockData";
import type { Athlete, PricePoint } from '@/lib/types';
import * as cache from '@/lib/cache';
import { computeInitialPriceFromStats, posMultiplier } from '@/lib/pricing';

// Simple in-memory cache for athlete stats (server-only). TTL = 5 minutes.
const ATHLETE_STATS_TTL_MS = 5 * 60 * 1000;

// ✅ Working API call — single athlete stats with caching
export async function getAthleteStats(id: number) {
  const getIfFreshRuntime: <T = any>(key: string, ttlMs?: number) => T | undefined = (cache as any).getIfFreshTracked || (cache as any).getIfFresh;
  const cached = getIfFreshRuntime<any>(String(id), ATHLETE_STATS_TTL_MS) as any;
  if (cached) return cached;
  // Call our server-side proxy instead of calling RapidAPI directly. The proxy
  // attaches the RapidAPI key server-side and implements filesystem caching.
  const base =
    process.env.NEXT_PUBLIC_BASE_URL || (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'http://localhost:3000');
  const proxyPath = `/api/rapid-proxy?path=${encodeURIComponent(`/nfl-ath-stats?id=${id}`)}`;
  const proxyUrl = `${base.replace(/\/$/, '')}${proxyPath}`;

  try {
    const response = await axios.get(proxyUrl, { timeout: 15_000 });
    const data = response.data;
    cache.setCached(String(id), data);
    console.log('✅ Athlete data (fresh via proxy):', data);
    return data;
  } catch (error: any) {
    console.error('❌ API/proxy error in getAthleteStats:', error?.message ?? error);
    // If we have stale cached data, return it as a best-effort fallback
    const stale = cache.getAny<any>(String(id));
    if (stale) {
      console.warn('⚠️ Returning stale cached athlete data due to API/proxy error');
      return stale;
    }
    return null;
  }
}

// 💡 Temporary mock data (until real team endpoint exists)
export async function getPlayers(teamAbbr?: string) {
  // If caller provided a team abbreviation, prefer fetching a roster from a
  // reliable public source (ESPN). RapidAPI's `/nfl-team-roster` appears to be
  // unreliable for team queries (returns an error body), so use ESPN's public
  // roster endpoint as a stable fallback.
  if (teamAbbr) {
    const match = String(teamAbbr).toUpperCase();
    try {
      // Fetch the list of NFL teams from ESPN and find the requested team id
      const teamsResp = await axios.get(
        "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
      );
      // ESPN sometimes returns different shapes. Normalize to an array of team entries.
      const extractTeamsArray = (d: any): any[] => {
        if (!d) return [];
        if (Array.isArray(d)) return d;
        if (Array.isArray(d.teams)) return d.teams;
        if (Array.isArray(d.sports)) {
          for (const s of d.sports) {
            if (Array.isArray(s?.leagues)) {
              for (const l of s.leagues) {
                if (Array.isArray(l?.teams)) return l.teams;
              }
            }
          }
        }
        return [];
      };

      const teamsList: any[] = extractTeamsArray(teamsResp.data);
      let teamId: number | null = null;
      for (const t of teamsList) {
        const abbr = (t?.abbreviation || t?.team?.abbreviation || "").toString().toUpperCase();
        const slug = (t?.slug || t?.team?.slug || "").toString().toLowerCase();
        const id = Number(t?.id ?? t?.team?.id ?? NaN);
        if (abbr === match || String(id) === match || slug.includes(String(teamAbbr).toLowerCase())) {
          teamId = id;
          break;
        }
      }

  if (teamId) {
        const rosterUrl = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/${teamId}/roster`;
        const rosterResp = await axios.get(rosterUrl);
        const players = normalizePlayersFromEspnRoster(rosterResp.data, match);
        // Provide lightweight debug metadata so the UI can show what happened
        const debug = {
          teamId,
          rosterKeys: Object.keys(rosterResp.data || {}),
          rosterItemsLength: Array.isArray(rosterResp.data?.items) ? rosterResp.data.items.length : undefined,
          normalizedCount: players.length,
        };
        return { response: players, _debug: debug } as any;
      }
      // If we couldn't locate the team, fall through to a generic attempt below
    } catch (err: any) {
      console.warn("⚠️ ESPN roster fetch failed, falling back to provider/demo path:", err?.message || err);
      // Fall through to provider/demo path below
    }
  }

  // Default/demo path: use local mock athletes to avoid RapidAPI calls during
  // development or when ESPN lookup fails. This prevents wasting RapidAPI
  // quota and keeps the UI stable.
  const normalized = MOCK_ATHLETES;
  return { response: normalized, _debug: { source: 'mock', normalizedCount: normalized.length } } as any;
}

type Player = {
  id: string | number | null;
  name: string;
  team?: { name?: string; abbreviation?: string };
  imageUrl?: string;
  position?: string;
  currentPrice?: number;
};

export function normalizePlayersFromNflAthStats(data: any): Player[] {
  if (!data || typeof data !== "object") return [];

  // Common top-level arrays
  const candidateArrays = [
    data.players,
    data.athletes,
    data.roster,
    data.rosters,
    data.results,
    data.data,
  ].filter(Boolean) as any[];

  const players: Player[] = [];
  const seen = new Set<string>();

  const isNumericId = (v: any) => {
    if (v == null) return false;
    if (typeof v === "number") return Number.isFinite(v);
    if (typeof v === "string") return /^\d+$/.test(v);
    return false;
  };

  const extractName = (p: any) => {
    return (
      p?.displayName || p?.fullName || p?.name ||
      (p?.firstName && p?.lastName ? `${p.firstName} ${p.lastName}` : null) ||
      (p?.person && (p.person.fullName || p.person.displayName)) ||
      null
    );
  };

  const pushPlayer = (p: any, teamName?: string) => {
    if (!p || typeof p !== "object") return;
    const rawId = p.id ?? p.uid ?? p.guid ?? (p.person && p.person.id) ?? null;
    if (!isNumericId(rawId)) return; // require numeric id to be confident it's a player
    const id = typeof rawId === "string" && /^\d+$/.test(rawId) ? Number(rawId) : rawId;
    const name = extractName(p);
    if (!name || typeof name !== "string" || name.trim().length === 0) return;
    const key = `${id}:${name}`;
    if (seen.has(key)) return;
    seen.add(key);
    const team = p.team
      ? { name: p.team.name || p.team.displayName || undefined, abbreviation: p.team.abbreviation }
      : teamName
      ? { name: teamName }
      : undefined;
    players.push({ id, name: name.trim(), team });
  };

  // If we found obvious arrays, use them
  for (const arr of candidateArrays) {
    if (Array.isArray(arr)) {
      for (const item of arr) pushPlayer(item);
    }
  }

  // Heuristic searches: inspect obvious arrays only, avoid walking into
  // huge statistic blobs and team metadata which caused false positives.
  const arraysToScan = [] as any[];
  for (const key of ["players", "athletes", "roster", "rosters", "results", "data"]) {
    if (Array.isArray((data as any)[key])) arraysToScan.push((data as any)[key]);
  }

  // If the payload includes a teams map with nested rosters, scan those rosters
  if (data.teams && typeof data.teams === "object") {
    for (const [slug, teamObj] of Object.entries(data.teams)) {
      const teamName = (teamObj as any)?.displayName || (teamObj as any)?.name || undefined;
      // look for roster-like arrays
      for (const k of ["roster", "players", "athletes"]) {
        if (Array.isArray((teamObj as any)[k])) {
          arraysToScan.push({ arr: (teamObj as any)[k], teamName });
        }
      }
    }
  }

  for (const entry of arraysToScan) {
    if (!entry) continue;
    if (Array.isArray(entry)) {
      for (const item of entry) pushPlayer(item);
    } else if (entry.arr && Array.isArray(entry.arr)) {
      for (const item of entry.arr) pushPlayer(item, entry.teamName);
    }
  }

  return players;

  return players;
}

// Normalize an ESPN roster response into Player[]
export function normalizePlayersFromEspnRoster(data: any, teamAbbr?: string): Player[] {
  if (!data || typeof data !== 'object') return [];
  const players: Player[] = [];
  const seen = new Set<string>();

  // ESPN roster payload typically has `items` grouped by position, or a
  // top-level `athletes`/`items` array. Also includes `team` metadata at the
  // end.
  const teamObj = data.team || (data?.items && data.items.team) || null;
  const teamName = teamObj?.displayName || teamObj?.name || undefined;
  const teamAbbreviation = (teamObj?.abbreviation || teamAbbr || undefined) as any;

  const push = (raw: any) => {
    if (!raw || typeof raw !== 'object') return;
    const id = raw.id ?? raw.uid ?? raw.playerId ?? raw.person?.id ?? null;
  const name = raw.fullName || raw.displayName || raw.person?.fullName || raw.person?.displayName || raw.name || (raw.firstName && raw.lastName ? `${raw.firstName} ${raw.lastName}` : null);
    if (!id || !name) return;
    const key = `${id}:${name}`;
    if (seen.has(key)) return;
    seen.add(key);

  // Attempt to extract a headshot/photo from common ESPN shapes
  let imageUrl = raw.imageUrl || raw.headshot?.href || raw.photo || raw.person?.headshot?.href || raw.person?.photo || undefined;

    // position may live at different keys depending on payload
  const position = raw.position?.abbreviation || raw.position?.displayName || raw.positionName || raw.position?.name || undefined;

    // Try to compute a stats-influenced initial price. ESPN payloads sometimes
    // include recent stat summaries; if not available, fall back to a modest
    // deterministic base using the id hash.
    const stats = {
      yards: Number(raw.yards ?? raw.yds ?? raw.passingYards ?? raw.rushingYards ?? raw.receivingYards ?? 0),
      tds: Number(raw.touchdowns ?? raw.td ?? raw.tds ?? 0),
      ints: Number(raw.interceptions ?? raw.int ?? raw.ints ?? 0),
      fumbles: Number(raw.fumblesLost ?? raw.fumbles ?? raw.fum ?? 0),
    };

    // Base price influenced by stats and position. If stats produce a weak
    // price, fall back to a deterministic id-hash adjusted by position so
    // different positions and players vary from the uniform $80 baseline.
    let initialPrice = computeInitialPriceFromStats(stats, position, 80);
    if (!initialPrice || initialPrice <= 0) {
      const idStr = String(id);
      let h = 0;
      for (let i = 0; i < idStr.length; i++) h = (h * 31 + idStr.charCodeAt(i)) >>> 0;
      const base = 50 + (h % 150); // wider spread up to ~200
      const mult = posMultiplier(position);
      initialPrice = Math.max(5, Math.min(2000, Math.round(base * mult)));
    }

    // If no explicit headshot found, use ESPN CDN pattern which works for most NFL players
    try {
      const sid = String(id ?? '').replace(/[^0-9]/g, '');
      if ((!imageUrl || imageUrl.length === 0) && sid) {
        imageUrl = `https://a.espncdn.com/i/headshots/nfl/players/full/${sid}.png`;
      }
    } catch (e) {
      // ignore fallback construction errors
    }

    players.push({ id, name: String(name).trim(), team: teamAbbreviation ? { name: teamName, abbreviation: String(teamAbbreviation).toUpperCase() } : undefined, imageUrl, position, currentPrice: Math.round(initialPrice) });
  };

  // Common shapes: data.items is array of position groups with `items`
  if (Array.isArray(data.items)) {
    for (const group of data.items) {
      if (Array.isArray(group.items)) {
        for (const p of group.items) push(p);
      }
    }
  }

  // Sometimes roster lives at data.athletes or data.players
  const alt = data.athletes || data.players || data.items;
  if (Array.isArray(alt)) {
    for (const entry of alt) {
      // Some ESPN responses use an array of position groups where each
      // group contains `items` (the player objects). Other times the
      // array is a flat list of player objects. Handle both.
      if (entry && typeof entry === 'object' && Array.isArray(entry.items)) {
        for (const p of entry.items) push(p);
      } else {
        push(entry);
      }
    }
  }

  return players;
}

// Map a normalized player (the lightweight Player shape returned by
// normalizePlayersFromEspnRoster) to the project's `Athlete` type. This is a
// conservative mapping that fills missing numeric values with sensible
// defaults so front-end code can rely on the shape.
export function mapNormalizedToAthlete(p: any): Athlete {
  const safeString = (v: any) => (v == null ? '' : String(v).trim());
  const id = safeString(p.id ?? p.uid ?? p.playerId ?? (p.person && p.person.id) ?? '');
  const name = safeString(p.name ?? p.fullName ?? (p.person && (p.person.fullName || p.person.displayName)) ?? 'Unknown');
  const team = safeString(p.team?.abbreviation ?? p.team?.name ?? p.team ?? '');
  const position = safeString(p.position ?? p.positionName ?? p.position?.abbreviation ?? 'Unknown');
  let imageUrl = safeString(p.imageUrl ?? p.headshot?.href ?? p.photo ?? p.person?.headshot?.href) || undefined;
  // fallback to ESPN CDN by id when possible
  if (!imageUrl) {
    const sid = String(p.id ?? p.uid ?? p.playerId ?? '')?.replace(/[^0-9]/g, '');
    if (sid) imageUrl = `https://a.espncdn.com/i/headshots/nfl/players/full/${sid}.png`;
  }

  // Default numeric values keep the UI predictable. Consumers can update
  // these later when real market data is available.
  const defaultNumber = 0;
  const priceHistory: PricePoint[] = Array.isArray(p.priceHistory) ? p.priceHistory : [];

  return {
    id,
    name,
    team,
    sport: 'Football',
    position: position || 'Unknown',
    currentPrice: Number(p.currentPrice ?? p.price ?? defaultNumber),
    previousPrice: Number(p.previousPrice ?? p.prevPrice ?? defaultNumber),
    marketCap: Number(p.marketCap ?? defaultNumber),
    sharesOwned: Number(p.sharesOwned ?? 0),
    totalShares: Number(p.totalShares ?? 0),
    priceHistory,
    imageUrl,
  };
}

// Convenience: normalize an ESPN roster and map to Athlete[] in one step.
export function normalizeAndMapPlayersFromEspnRoster(data: any, teamAbbr?: string): Athlete[] {
  const normalized = normalizePlayersFromEspnRoster(data, teamAbbr);

  // Team-level normalization: compute team average price and amplify deviations
  // so elite players stand out more. This keeps league-wide base similar but
  // increases intra-team variance.
  const TEAM_SCALE = 1.2; // >1 amplifies differences, <1 mutes
  try {
  const prices = normalized.map((p: any) => Number(p.currentPrice ?? 0)).filter((v: number) => Number.isFinite(v) && v > 0);
    if (prices.length > 0) {
      const teamAvg = prices.reduce((a, b) => a + b, 0) / prices.length;
      for (const p of normalized) {
  const cur = Number(p.currentPrice ?? 0) || 0;
        const adjusted = teamAvg + (cur - teamAvg) * TEAM_SCALE;
        p.currentPrice = Math.max(5, Math.min(2000, Math.round(adjusted)));
      }
    }
  } catch (e) {
    // Non-fatal; if normalization fails, leave original prices intact.
    // Keep this silent in production but useful during dev.
    if (process.env.NODE_ENV !== 'production') console.warn('[pricing] team normalization failed', e);
  }

  // Attach priceHistory to each normalized player. Prefer a matching mock
  // athlete by id or name. If none found, synthesize a simple 14-day history
  // based on the player's currentPrice so the UI can render charts.
  const findMock = (p: any) => {
    if (!p) return undefined;
    const pid = String(p.id ?? p.playerId ?? p.uid ?? '').toLowerCase();
    const name = String(p.name ?? p.fullName ?? '').toLowerCase();
    return MOCK_ATHLETES.find((m) => {
      if (!m) return false;
      const mid = String(m.id ?? '').toLowerCase();
      const mname = String(m.name ?? '').toLowerCase();
      return mid && pid && mid === pid || (mname && name && mname === name);
    });
  };

  const synthHistory = (base: number) => {
    const now = new Date();
    const days = (n: number) => {
      const d = new Date(now);
      d.setDate(d.getDate() - n);
      return d.toISOString().slice(0, 10);
    };
    return Array.from({ length: 14 }, (_, i) => {
      const drift = Math.sin(i / 2) * 4;
      const noise = (Math.random() * 3 - 1.5);
      return { t: days(13 - i), p: +(base + drift + noise).toFixed(2) } as any;
    });
  };

  for (const p of normalized) {
    // if already has priceHistory, skip
    if (Array.isArray((p as any).priceHistory) && (p as any).priceHistory.length > 0) continue;
    const mock = findMock(p);
    if (mock && Array.isArray(mock.priceHistory)) {
      (p as any).priceHistory = mock.priceHistory;
    } else {
      const base = Number(p.currentPrice ?? 100) || 100;
      (p as any).priceHistory = synthHistory(base);
    }
  }

  return normalized.map(mapNormalizedToAthlete);
}
