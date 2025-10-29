import fs from 'fs';
import path from 'path';
import { parse as csvParseSync } from 'csv-parse/sync';
import axios from 'axios';
import { NextResponse } from 'next/server';
import { normalizePlayersFromEspnRoster, normalizeAndMapPlayersFromEspnRoster } from '@/lib/api';
import * as cache from '@/lib/cache';

// Cache TTL = 5 minutes
const TTL_MS = 5 * 60 * 1000;

function extractTeamsArray(d: any): any[] {
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
}

function filterAndPaginate(players: any[], q: string | null, page: number, limit: number) {
  let filtered = players;
  if (q && q.trim().length > 0) {
    const ql = q.toLowerCase();
    filtered = players.filter((p: any) => String(p.name ?? '').toLowerCase().includes(ql));
  }
  const normalizedCount = filtered.length;
  const start = Math.max(0, (page - 1) * limit);
  const paginated = filtered.slice(start, start + limit);
  return { paginated, normalizedCount };
}

function loadCleanedProfiles() {
  const profilesPath = path.join(process.cwd(), 'data/player_profiles_cleaned.csv');
  const byEspn: Record<string, any> = {};
  const byName: Record<string, any> = {};
  if (!fs.existsSync(profilesPath)) return { byEspn, byName };
  try {
    const raw = fs.readFileSync(profilesPath, 'utf8');
    const recs = csvParseSync(raw, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
    for (const r of recs) {
      const esp = String(r.espnId ?? r.espnid ?? r.player_id ?? r.playerid ?? '').trim();
      const name = String(r.player ?? r.player_name ?? r.name ?? '').trim();
      const team = String(r.team ?? r.team_name ?? r.team_abbr ?? '').trim();
      const position = String(r.position ?? r.pos ?? r.position_name ?? '').trim().toUpperCase();
      const entry = { espnId: esp || '', name: name || '', team: team || '', position };
      if (esp) byEspn[esp] = entry;
      if (name) byName[String(name).toLowerCase()] = entry;
    }
  } catch (e) {
    // ignore parse errors and return empty maps
  }
  return { byEspn, byName };
}

function mergeProfilesIntoAthletes(athletes: any[], byEspn: Record<string, any>, byName: Record<string, any>) {
  return athletes.map((a) => {
    try {
      const idKey = String(a.id ?? a.uid ?? a.playerId ?? '').trim();
      const nameKey = String(a.name ?? '').trim();
      let profile: any = null;
      if (idKey && byEspn[idKey]) profile = byEspn[idKey];
      if (!profile && nameKey && byName[nameKey.toLowerCase()]) profile = byName[nameKey.toLowerCase()];
      if (profile) {
        // merge fields, preferring profile where present
        a.name = profile.name || a.name;
        if (profile.position) a.position = profile.position;
        if (profile.team) {
          // if athlete has team as object or string, prefer profile.team string
          a.team = profile.team;
        }
        a.espnId = profile.espnId || (a.espnId || idKey || '');
      } else {
        // ensure espnId exists on the returned object for consumers
        a.espnId = a.espnId || (a.id ? String(a.id) : '');
      }
    } catch (e) {
      // ignore
    }
    return a;
  });
}

export async function GET(req: Request) {
  try {
    // Resolve tracked or basic cache getter at request-time to avoid module-eval-time
    // errors when tests partially mock the cache module.
    const getIfFreshRuntime: <T = any>(key: string, ttlMs?: number) => T | undefined = (cache as any).getIfFreshTracked || (cache as any).getIfFresh;

  const url = new URL(req.url);
    const teamParam = url.searchParams.get('team');
    const q = url.searchParams.get('q');
    const page = parseInt(url.searchParams.get('page') || '1', 10) || 1;
    const limit = parseInt(url.searchParams.get('limit') || '50', 10) || 50;

    // Dev-only: clear the in-memory cache instantly via ?clearCache=1
    const clear = url.searchParams.get('clearCache');
    if (clear === '1') {
      if (process.env.NODE_ENV === 'production') {
        return NextResponse.json({ error: 'Cache clear disabled in production' }, { status: 403 });
      }
  cache.clearCache();
      return NextResponse.json({ ok: true, message: 'Cache cleared' });
    }

  // If no team param is provided, return aggregated players for all teams.
    // This is useful for listing all players across the league.
  // Support `teams` param (comma-separated abbreviations) to fetch multiple specific teams
  const teamsParam = url.searchParams.get('teams');
  const wantTeams: string[] | null = teamsParam ? teamsParam.split(',').map(s => String(s).trim().toUpperCase()).filter(Boolean) : null;
  const wantTeamsSet: Set<string> | null = wantTeams ? new Set(wantTeams) : null;

    // Load cleaned profiles so we can enrich ESPN responses consistently
    const { byEspn, byName } = loadCleanedProfiles();
    // If profiles are missing, warn in dev so maintainers notice missing enrichment
    if (process.env.NODE_ENV !== 'production' && Object.keys(byEspn).length === 0 && Object.keys(byName).length === 0) {
      console.warn('DEV WARNING: data/player_profiles_cleaned.csv not found — ESPN player responses will not be enriched with local profile data');
    }

    if (!teamParam && !wantTeams) {
      const allKey = 'all-teams';
  const freshAll = getIfFreshRuntime<any[]>(allKey, TTL_MS);
      if (freshAll) {
        const { paginated, normalizedCount } = filterAndPaginate(freshAll, q, page, limit);
        const merged = mergeProfilesIntoAthletes(paginated, byEspn, byName);
        return NextResponse.json({ response: merged, _debug: { source: 'espn', normalizedCount, cacheHit: true } });
      }

      // Fetch teams list then fetch each roster in parallel (best-effort).
      try {
        const teamsResp = await axios.get('https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams');
          // (no-op) removed debug logging for cleaner dev console
        const teamsList: any[] = extractTeamsArray(teamsResp.data);
  const teamIds: number[] = [];
        for (const t of teamsList) {
          const id = Number(t?.id ?? t?.team?.id ?? NaN);
          const abbr = (t?.abbreviation || t?.team?.abbreviation || '').toString().toUpperCase();
          if (wantTeamsSet) {
            if (wantTeamsSet.has(abbr) && Number.isFinite(id)) teamIds.push(id);
          } else {
            if (Number.isFinite(id)) teamIds.push(id);
          }
        }

        // Fetch rosters in parallel with Promise.allSettled so one failing team doesn't abort everything.
        const rosterPromises = teamIds.map((tid) => axios.get(`https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/${tid}/roster`).then(r => r.data).catch(() => null));
        const rosterResults = await Promise.allSettled(rosterPromises);
        const aggregated: any[] = [];
        for (const r of rosterResults) {
          if (r.status === 'fulfilled' && r.value) {
            // map to full Athlete shape so UI gets imageUrl and currentPrice
            const mapped = normalizeAndMapPlayersFromEspnRoster(r.value, undefined);
            for (const p of mapped) aggregated.push(p);
          }
        }

        // Deduplicate by id:name key
        const seen = new Set<string>();
        const deduped = [] as any[];
        for (const p of aggregated) {
          const key = `${p.id}:${p.name}`;
          if (!seen.has(key)) { seen.add(key); deduped.push(p); }
        }

        cache.setCached(allKey, deduped);
        const { paginated, normalizedCount } = filterAndPaginate(deduped, q, page, limit);
        const merged = mergeProfilesIntoAthletes(paginated, byEspn, byName);
        return NextResponse.json({ response: merged, _debug: { source: 'espn', normalizedCount } });
      } catch (err: any) {
        return NextResponse.json({ response: [], _debug: { source: 'espn', error: `teams lookup failed: ${err?.message ?? err}` } }, { status: 200 });
      }
    }

    // Resolve team id from ESPN teams endpoint
    let teamId: number | null = null;
    try {
      const teamsResp = await axios.get('https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams');
      // Debug: print top-level keys from ESPN teams response to help diagnose
      // normalization issues during development.
      // (no-op) removed debug logging for cleaner dev console
      const teamsList: any[] = extractTeamsArray(teamsResp.data);
      const match = String(teamParam).toUpperCase();
      for (const t of teamsList) {
        const abbr = (t?.abbreviation || t?.team?.abbreviation || '').toString().toUpperCase();
        const slug = (t?.slug || t?.team?.slug || '').toString().toLowerCase();
        const id = Number(t?.id ?? t?.team?.id ?? NaN);
        if (abbr === match || String(id) === match || slug.includes(String(teamParam).toLowerCase())) {
          teamId = id;
          break;
        }
      }
    } catch (err: any) {
      // If teams list lookup fails, return a helpful debug message
      return NextResponse.json({ response: [], _debug: { source: 'espn', error: `teams lookup failed: ${err?.message ?? err}` } }, { status: 200 });
    }

    if (!teamId) {
      console.log('DEBUG: team not found for param', teamParam);
      return NextResponse.json({ response: [], _debug: { source: 'espn', error: 'team not found' } }, { status: 200 });
    }

    const cacheKey = String(teamId);
    const fresh = getIfFreshRuntime<any[]>(cacheKey, TTL_MS);
    if (fresh) {
      const { paginated, normalizedCount } = filterAndPaginate(fresh, q, page, limit);
      const merged = mergeProfilesIntoAthletes(paginated, byEspn, byName);
      return NextResponse.json({ response: merged, _debug: { source: 'espn', normalizedCount, cacheHit: true } });
    }

    // Fetch roster and normalize
    try {
      const rosterUrl = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/${teamId}/roster`;
      const rosterResp = await axios.get(rosterUrl);
      // (no-op) removed debug logging for cleaner dev console
  const normalized = normalizeAndMapPlayersFromEspnRoster(rosterResp.data, teamParam || undefined);
  cache.setCached(cacheKey, normalized);
  const { paginated, normalizedCount } = filterAndPaginate(normalized, q, page, limit);
  const merged = mergeProfilesIntoAthletes(paginated, byEspn, byName);
  return NextResponse.json({ response: merged, _debug: { source: 'espn', normalizedCount } });
    } catch (err: any) {
      return NextResponse.json({ response: [], _debug: { source: 'espn', error: `roster fetch failed: ${err?.message ?? err}` } }, { status: 200 });
    }
  } catch (err: any) {
    return NextResponse.json({ response: [], _debug: { source: 'espn', error: `unexpected error: ${err?.message ?? err}` } }, { status: 500 });
  }
}
