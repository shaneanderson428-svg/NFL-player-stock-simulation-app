import fs from "fs";
import path from "path";
import { NextResponse } from "next/server";
import { parse as csvParseSync } from "csv-parse/sync";
import { readCSV } from '@/lib/readCSV';
import { getOrInitPlayer } from '@/lib/playerCache';

type CacheEntry = {
  mtimeMs: number;
  rows: Array<Record<string, any>>;
};

const cache: Record<string, CacheEntry> = {};

function coerceValue(v: string) {
  if (v === null || v === undefined) return v;
  const t = v.trim();
  if (t === "") return null;
  if (/^-?\d+$/.test(t)) return parseInt(t, 10);
  if (/^-?\d+\.\d+$/.test(t)) return parseFloat(t);
  return t;
}

export async function GET(request: Request) {
  try {
    const filePath = path.join(process.cwd(), "data/player_stock_summary.csv");

    if (!fs.existsSync(filePath)) {
      return NextResponse.json({ ok: false, error: 'player_stock_summary.csv not found' }, { status: 404 });
    }

    const st = fs.statSync(filePath);
    const mtimeMs = st.mtimeMs;

  const normalizeNameToKey = (s: string) => {
    if (!s) return '';
    try {
      const ns = String(s)
        .normalize('NFKD')
        .replace(/\p{Diacritic}/gu, '')
        .replace(/\b(JR|SR|II|III|IV)\.?$/i, '')
        .replace(/\./g, '')
        .trim()
        .toLowerCase();
      const compact = ns.replace(/\s+/g, ' ');
      const withHyphens = compact.replace(/\s+/g, '-').replace(/[^a-z0-9\-]/g, '');
      return withHyphens;
    } catch (e) {
      return String(s).toLowerCase().replace(/[^a-z0-9]/g, '');
    }
  };

  // try to load per-player history CSV if present
    const histPath = path.join(process.cwd(), "data/player_stock_history.csv");
    let histMtime = 0;
    let historyMap: Record<string, Array<Record<string, any>>> = {};
    if (fs.existsSync(histPath)) {
      const hst = fs.statSync(histPath);
      histMtime = hst.mtimeMs;
      try {
        const rawh = fs.readFileSync(histPath, 'utf8');
        const hrecords = csvParseSync(rawh, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
        // Build maps keyed by player name and espnId (if present) for robust matching
        const histByName: Record<string, Array<Record<string, any>>> = {};
        const histByEspn: Record<string, Array<Record<string, any>>> = {};
        for (const hr of hrecords) {
          const pname = String(hr.player || '').trim();
          const entry: Record<string, any> = {};
    // support several possible history column names: week/stock or timestamp/price
    if ('week' in hr) entry.week = coerceValue(String(hr.week));
    if ('stock' in hr) entry.stock = coerceValue(String(hr.stock));
    if ('price' in hr) entry.stock = coerceValue(String(hr.price));
    if ('p' in hr) entry.stock = coerceValue(String(hr.p));
    if ('confidence' in hr) entry.confidence = coerceValue(String(hr.confidence));
    // expose a normalized timestamp field when available (timestamp, date, game_date)
    if ('timestamp' in hr) entry.t = String(hr.timestamp);
    if ('date' in hr) entry.t = String(hr.date);
    if ('game_date' in hr) entry.t = String(hr.game_date);
          // include z_* fields if present on the row
          Object.entries(hr).forEach(([k, v]) => {
            if (k.startsWith('z_')) entry[k] = coerceValue(String(v));
          });
          if (pname) {
            if (!histByName[pname]) histByName[pname] = [];
            histByName[pname].push(entry);
            const key = normalizeNameToKey(pname);
            if (key) {
              if (!histByName[key]) histByName[key] = [];
              histByName[key].push(entry);
            }
          }
          // accept several possible ESPN id header names from CSVs
          const he = hr['espnId'] || hr['espn_id'] || hr['espn'] || hr['espnid'] || hr['playerId'] || hr['player_id'] || hr['playerid'] || '';
          if (he) {
            const key = String(he).trim();
            if (!histByEspn[key]) histByEspn[key] = [];
            histByEspn[key].push(entry);
          }
        }
        // sort histories: prefer numeric week when present, otherwise fall back to timestamp parsing
        const sortKey = (item: any) => {
          try {
            if (item && item.week !== undefined && item.week !== null && item.week !== '') {
              const n = Number(item.week);
              if (!Number.isNaN(n)) return n;
            }
            const t = item && (item.t || item.timestamp || item.date);
            if (t) {
              const ms = Date.parse(String(t));
              if (!Number.isNaN(ms)) return ms;
            }
          } catch (e) {
            // ignore and return zero
          }
          return 0;
        };
        Object.keys(histByName).forEach((k) => histByName[k].sort((a, b) => sortKey(a) - sortKey(b)));
        Object.keys(histByEspn).forEach((k) => histByEspn[k].sort((a, b) => sortKey(a) - sortKey(b)));
        // store combined maps into historyMap with special keys
        historyMap = { __byName: histByName, __byEspn: histByEspn } as any;
      } catch (e) {
        // ignore history parse errors and continue without history
        historyMap = {};
        histMtime = 0;
      }
    }

    // try to load cleaned player profiles (optional)
    const profilesPath = path.join(process.cwd(), 'data/player_profiles_cleaned.csv');
    // warn in dev when file is missing so devs notice missing enrichment
    if (process.env.NODE_ENV !== 'production' && !fs.existsSync(profilesPath)) {
      console.warn('DEV WARNING: data/player_profiles_cleaned.csv not found — nfl/stocks API responses will not be enriched with profile data');
    }
    let profilesMtime = 0;
    const profilesByEspn: Record<string, Record<string, any>> = {};
    const profilesByName: Record<string, Record<string, any>> = {};
    
    if (fs.existsSync(profilesPath)) {
      try {
        const pst = fs.statSync(profilesPath);
        profilesMtime = pst.mtimeMs;
        const rawp = fs.readFileSync(profilesPath, 'utf8');
        const precords = csvParseSync(rawp, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
        for (const pr of precords) {
          const esp = String(pr.espnId ?? pr.espnid ?? pr.player_id ?? pr.playerid ?? '').trim();
          const name = String(pr.player ?? pr.player_name ?? pr.name ?? '').trim();
          const team = pr.team ?? pr.team_name ?? pr.team_abbr ?? '';
          const position = pr.position ?? pr.pos ?? pr.position_name ?? '';
          const entry = { espnId: esp || '', name: name || '', team: team || '', position: (position || '').toUpperCase() };
          if (esp) profilesByEspn[esp] = entry;
          if (name) {
            profilesByName[String(name).toLowerCase()] = entry;
            const key = normalizeNameToKey(name);
            if (key) profilesByName[key] = entry;
          }
        }
      } catch (e) {
        // ignore profile parse errors
      }
    }

    const combinedMtime = mtimeMs + histMtime + profilesMtime;
    const cached = cache[filePath];
    // read the CSV using helper which coerces numbers
    const records = await readCSV('data/player_stock_summary.csv');

    // build normalized rows
    const rows = records.map((r) => {
      const out: Record<string, any> = {};
      Object.entries(r).forEach(([k, v]) => {
        out[k] = v;
      });
      // normalize various espn id column names into a canonical `espnId` field
      const maybeEspn = out.espnId ?? out.espn_id ?? out.espn ?? out.espnid ?? out.playerId ?? out.player_id ?? out.playerid ?? null;
      if (maybeEspn !== null && maybeEspn !== undefined && String(maybeEspn).trim() !== '') {
        out.espnId = String(maybeEspn);
      }
      // attach history: prefer espnId-based lookup, fallback to name
      let hist: Array<Record<string, any>> = [];
      try {
        const byEspn = (historyMap && (historyMap as any).__byEspn) || {};
        const byName = (historyMap && (historyMap as any).__byName) || {};
        // prefer canonical espnId, then any known aliases
        const esp = out.espnId || out.espn_id || out.espn || out.espnid || out.playerId || out.player_id || out.playerid || null;
        if (esp && String(esp).trim() && byEspn[String(esp).trim()]) {
          hist = byEspn[String(esp).trim()];
        } else {
          const pname = String(out.player || '');
          hist = byName[pname] || byName[pname.trim()] || [];
        }
      } catch (e) {
        hist = [];
      }
      out.history = hist || [];
      // enrich with profile info when available
      try {
  const rawName = String(out.player ?? '').trim();
  const rawEsp = String(out.espnId ?? out.espn_id ?? out.espn ?? out.espnid ?? out.playerId ?? out.player_id ?? out.playerid ?? '').trim();
  let profile = null as Record<string, any> | null;
  if (rawEsp && profilesByEspn[rawEsp]) profile = profilesByEspn[rawEsp];
  // try direct lowercase name
  if (!profile && rawName && profilesByName[rawName.toLowerCase()]) profile = profilesByName[rawName.toLowerCase()];
  // try normalized slug keys (handles slugs like 'a-rodgers' or 'tj-hockenson')
  if (!profile) {
    const nKey = normalizeNameToKey(rawName || rawEsp || '');
    if (nKey && profilesByName[nKey]) profile = profilesByName[nKey];
    // also try if espn id itself is a slug
    const espSlug = normalizeNameToKey(rawEsp || '');
    if (!profile && espSlug && profilesByName[espSlug]) profile = profilesByName[espSlug];
  }
        if (profile) {
          out.name = profile.name || out.player || '';
          out.position = profile.position || out.position || '';
          // keep existing team on the row if present; otherwise use profile
          out.team = out.team || profile.team || '';
          // prefer profile espnId when available
          out.espnId = profile.espnId || out.espnId || rawEsp || '';
        } else {
          // ensure fields exist
          out.name = out.player || '';
          out.position = out.position || '';
          out.team = out.team || '';
        }
      } catch (e) {
        out.name = out.player || '';
        out.position = out.position || '';
        out.team = out.team || '';
      }
      return out;
    });

    // ensure every row has a stable espnId: prefer existing espnId, otherwise synthesize
    const slugify = (s: string) => {
      const k = normalizeNameToKey(String(s || ''));
      return k || '';
    };

    const rowsWithIds: Array<Record<string, any>> = rows.map((p, i) => {
      const pid = p.espnId ?? p.espn_id ?? p.espn ?? p.espnid ?? p.playerId ?? p.player_id ?? p.playerid ?? null;
      if (pid && String(pid).trim() !== '') {
        return { ...p, espnId: String(pid) };
      }
      const name = String(p.player ?? '').trim() || `player_${i}`;
      const slug = slugify(name);
      const synth = slug ? `${slug}` : `player${i}`;
      return { ...p, espnId: synth };
    });

    // Filter players using position-specific starter thresholds (plays column)
    // and then optionally keep only the top player per team by avg_epa.
    // This mirrors the user's requested logic:
    //  - Keep only players whose `plays` meet a minimum by position
    //  - Group by `team` and keep the player with highest `avg_epa` per team
    const starterThresholds: Record<string, number> = {
      QB: 150,
      RB: 100,
      WR: 100,
      TE: 80,
    };
    const minPassAttempts = 20;

    // normalize position to uppercase and coerce plays to number
    const starters = rowsWithIds.filter((row: Record<string, any>) => {
      try {
        const pos = (row.position || '').toString().toUpperCase();
        const threshold = starterThresholds[pos] ?? 120;
        const playsRaw = row.plays ?? row.plays_count ?? row.playsCount ?? null;
        const playsNum = playsRaw !== null && playsRaw !== undefined ? Number(playsRaw) : NaN;
        // Enforce minimum pass attempts for QBs to avoid including non-QBs
        if (pos === 'QB') {
          const rawAttempts = row.pass_attempts ?? row['pass_attempts'] ?? row.passAttempts ?? row['passAttempts'] ?? row.z_pass_attempts ?? null;
          const attemptsNum = rawAttempts !== null && rawAttempts !== undefined ? Number(rawAttempts) : NaN;
          // require a numeric attempts count and at least minPassAttempts
          if (Number.isNaN(attemptsNum) || attemptsNum < minPassAttempts) return false;
        }
        if (!Number.isNaN(playsNum)) {
          return playsNum >= threshold;
        }
        // if plays isn't available, fall back to previous heuristics:
        const rawAttempts = row.pass_attempts ?? row['pass_attempts'] ?? row.passAttempts ?? row['passAttempts'] ?? null;
        const attemptsNum = rawAttempts !== null && rawAttempts !== undefined ? Number(rawAttempts) : NaN;
        if (!Number.isNaN(attemptsNum)) return attemptsNum >= 20;
        const z = Number(row.z_pass_attempts ?? row['z_pass_attempts'] ?? 0);
        return !Number.isNaN(z) && z >= 0.5;
      } catch (e) {
        return false;
      }
    });

    // group by team and keep top avg_epa per team
    const groupedByTeam: Record<string, Record<string, any>> = {};
    for (const row of starters) {
      const teamKey = String(row.team || '').trim() || `__no_team__:${String(row.espnId || row.espn_id || row.player || '')}`;
      const curr = groupedByTeam[teamKey];
      const rowAvg = Number(row.avg_epa ?? row.avgEPA ?? row.avg_epa_per_play ?? NaN);
      if (!curr) {
        groupedByTeam[teamKey] = row;
      } else {
        const currAvg = Number(curr.avg_epa ?? curr.avgEPA ?? curr.avg_epa_per_play ?? NaN);
        if (Number.isNaN(currAvg) && !Number.isNaN(rowAvg)) {
          groupedByTeam[teamKey] = row;
        } else if (!Number.isNaN(rowAvg) && rowAvg > currAvg) {
          groupedByTeam[teamKey] = row;
        }
      }
    }

    let filtered = Object.values(groupedByTeam);

    // Edge case: if grouping produced no results (e.g., no plays column),
    // fall back to the original pass-attempts / z-score filter so we don't
    // return an empty list.
    if (!filtered || filtered.length === 0) {
      filtered = rowsWithIds.filter((p: Record<string, any>) => {
        // accept multiple possible column spellings for raw counts
        const rawAttempts = p.pass_attempts ?? p['pass_attempts'] ?? p.passAttempts ?? p['passAttempts'] ?? p.z_pass_attempts ?? null;
        const attemptsNum = rawAttempts !== null && rawAttempts !== undefined ? Number(rawAttempts) : NaN;
        if (!Number.isNaN(attemptsNum)) {
          // enforce min pass attempts for QBs
          const pos = (p.position || '').toString().toUpperCase();
          if (pos === 'QB' && attemptsNum < minPassAttempts) return false;
          if (attemptsNum >= minPassAttempts) return true;
          return false;
        }

        // fallback to z-score heuristic when raw counts aren't present
        const z = Number(p.z_pass_attempts ?? p['z_pass_attempts'] ?? 0);
        return !Number.isNaN(z) && z >= 0.5;
      });
    }

    // If filtering heuristics produce nothing (e.g., non-standard CSV),
    // fall back to including all rows so consumers can still see data.
    if (!filtered || filtered.length === 0) {
      filtered = rowsWithIds.slice();
    }

    cache[filePath] = { mtimeMs: combinedMtime, rows: filtered };

    // Convert filtered rows into structured player objects and merge with in-memory cache
    // (starterThresholds already declared above; don't redeclare to avoid TS errors)

    // blended fantasy + efficiency stock calculator
    function calculateStock(row: Record<string, any>) {
      const efficiencyScore = (Number(row.avg_epa || row.avgEPA || 0) * 100) + (Number(row.avg_cpoe || row.avgCpoe || 0) || 0);
      const fantasyScore =
        (Number(row.passing_yards || row.pass_yards || 0) || 0) * 0.04 +
        (Number(row.passing_tds || row.pass_tds || 0) || 0) * 4 +
        (Number(row.interceptions || row.ints || 0) || 0) * -2 +
        (Number(row.rushing_yards || row.rush_yards || 0) || 0) * 0.1 +
        (Number(row.rushing_tds || row.rush_tds || 0) || 0) * 6 +
        (Number(row.receptions || row.rec || row.targets || 0) || 0) * 0.5 +
        (Number(row.receiving_yards || row.rec_yards || 0) || 0) * 0.1 +
        (Number(row.receiving_tds || row.rec_tds || 0) || 0) * 6;
      return Math.round(efficiencyScore * 0.3 + fantasyScore * 0.7);
    }

    const players: Array<Record<string, any>> = filtered.map((row) => {
      const pos = String(row.position || '').toUpperCase();
      const plays = Number(row.plays ?? row.plays_count ?? row.playsCount ?? row.plays ?? 0);
      const avg_epa = Number(row.avg_epa ?? row.avgEPA ?? row.avg_epa_per_play ?? 0) || 0;
      const avg_cpoe = Number(row.avg_cpoe ?? row.avgCpoe ?? 0) || 0;
      const espnId = String(row.espnId || row.espnid || row.playerId || row.player_id || row.playerid || row.player || '').trim();

      const playerBase = {
        id: espnId || String(row.player || row.name || '').replace(/\s+/g, '-').toLowerCase(),
        name: row.name || row.player || '',
        team: row.team || '',
        position: pos || '',
        avg_epa,
        avg_cpoe,
        plays,
        // compute blended stock (fantasy + efficiency)
        stock: calculateStock(row),
        history: Array.isArray(row.history) ? [...row.history] : [],
        espnId: espnId || undefined,
        // preserve raw row for debugging/fields
        _raw: row,
      } as Record<string, any>;

      // merge with in-memory cache (keep existing history if present)
      const merged = getOrInitPlayer(playerBase.espnId || playerBase.id, playerBase) as any;
      // return a lightweight player object for consumers
      return {
        id: merged.espnId || merged.id || playerBase.id,
        name: merged.name || playerBase.name,
        team: merged.team || playerBase.team,
        position: merged.position || playerBase.position,
        position_profile: merged.position_profile || playerBase.position_profile || undefined,
        avg_epa: avg_epa,
        avg_cpoe: avg_cpoe,
        plays: plays,
        stock: merged.stock ?? playerBase.stock,
        history: merged.history || playerBase.history || [],
        espnId: merged.espnId || playerBase.espnId,
      } as Record<string, any>;
    });

    // Derive a numeric `priceHistory` (newest -> oldest) from available `history` objects
    const playersWithPrice: Array<Record<string, any>> = players.map((p) => {
      const rawHist = Array.isArray(p.history) ? p.history : [];
      const pts: Array<{ t: string; p: number }> = rawHist.map((h: any) => {
        if (h == null) return null as any;
        if (typeof h === 'object') {
          if ('stock' in h) {
            const t = h.t ?? (h.week !== undefined ? String(h.week) : h.date ?? '');
            const pval = Number(h.stock ?? h.p ?? h.price ?? NaN);
            return { t: String(t), p: pval };
          }
          if ('p' in h || 'price' in h) {
            const t = h.t ?? h.date ?? '';
            const pval = Number(h.p ?? h.price ?? NaN);
            return { t: String(t), p: pval };
          }
          // fallback: try week/first value
          const t = h.week !== undefined ? String(h.week) : (h.t ?? '');
          const pval = Number(h.stock ?? h.p ?? h.price ?? NaN);
          return { t: String(t), p: pval };
        }
        if (typeof h === 'number') return { t: '', p: h };
        return null as any;
      }).filter(Boolean as any) as Array<{ t: string; p: number }>;
      const numeric = pts.filter(pt => typeof pt.p === 'number' && !Number.isNaN(pt.p));
      // CSV/history arrays are usually oldest->newest; consumers expect newest->oldest for sparklines
      const priceHistory = numeric.length ? numeric.slice().reverse() : [];
      return { ...p, priceHistory };
    });

  // optional unique teams list
  const teams = Array.from(new Set(playersWithPrice.map((p) => String(p.team || '').trim()).filter(Boolean)));

    // support slug query param: /api/nfl/stocks?slug=dak-prescott
    try {
      const url = new URL(request.url);
      const slugParam = (url.searchParams.get('slug') || '').trim();
      if (slugParam) {
        const normalizeIncoming = (s: string) => normalizeNameToKey(s || '').toLowerCase();
        const target = normalizeIncoming(slugParam.replace(/\s+/g, '-'));
        // find by normalized name, espnId, or raw name
        const found = filtered.find((p: Record<string, any>) => {
          try {
            const candidateNames = [String(p.name || ''), String(p.player || ''), String(p.player_name || ''), String(p.espnId || '')];
            // parts of the incoming slug (e.g. dak-prescott -> ['dak','prescott'])
            const targetParts = target.split('-').filter(Boolean);
            const targetFirst = targetParts[0] || '';
            const targetLast = targetParts[targetParts.length - 1] || '';

            for (const cn of candidateNames) {
              if (!cn) continue;
              const candidate = normalizeIncoming(cn.replace(/\s+/g, '-'));
              // exact match
              if (candidate === target) return true;
              // espnId synthesized as slug
              if (String(p.espnId || '') === target) return true;
              // if incoming has both first+last, prefer those both present in candidate
              if (targetParts.length >= 2) {
                const hasFirst = candidate.startsWith(targetFirst) || candidate.includes(`${targetFirst}-`);
                const hasLast = candidate.endsWith(targetLast) || candidate.includes(`-${targetLast}`) || candidate.includes(targetLast);
                if (hasFirst && hasLast) return true;
              }
              // match by last name alone (e.g. dak-prescott vs D.Prescott)
              if (targetLast && candidate.includes(targetLast)) {
                // optionally confirm first initial matches if candidate provides an initial
                const candParts = candidate.split('-').filter(Boolean);
                if (candParts.length === 1) {
                  // candidate like 'dprescott' -> check last name substring
                  if (candidate.endsWith(targetLast)) return true;
                } else {
                  // candidate like 'd-prescott' or 'dprescott'
                  const candFirst = candParts[0] || '';
                  if (candFirst.length === 1 && targetFirst && candFirst === targetFirst.charAt(0)) return true;
                  if (candParts[candParts.length - 1] === targetLast) return true;
                }
              }
            }
          } catch (e) {
            // ignore
          }
          return false;
        });

        if (!found) {
          return NextResponse.json({ ok: false, slug: target, error: 'player not found' }, { status: 404 });
        }

        // build response shape
        const player = {
          name: found.name || found.player || '',
          team: found.team || '',
          position: found.position || '',
          position_profile: found.position_profile || '',
          espnId: found.espnId || ''
        };
        // pick some stock metrics
        const stock = {
          price: found.price ?? found.current_price ?? found.last_price ?? null,
          change: found.change ?? found.delta ?? null,
          z_epa: found.z_epa ?? null,
          z_cpoe: found.z_cpoe ?? null,
          // include any z_ fields present
          ...Object.fromEntries(Object.entries(found).filter(([k]) => k.startsWith('z_')))
        };

        const history = Array.isArray(found.history) ? found.history : [];
        // derive numeric priceHistory for this single-player response as well
        const pts = Array.isArray(history) ? history.map((h: any) => {
          if (!h) return null as any;
          if (typeof h === 'object') {
            if ('stock' in h) return { t: h.t ?? (h.week !== undefined ? String(h.week) : ''), p: Number(h.stock ?? h.p ?? h.price ?? NaN) };
            if ('p' in h || 'price' in h) return { t: h.t ?? h.date ?? '', p: Number(h.p ?? h.price ?? NaN) };
            return { t: h.t ?? (h.week !== undefined ? String(h.week) : ''), p: Number(h.stock ?? h.p ?? h.price ?? NaN) };
          }
          if (typeof h === 'number') return { t: '', p: h };
          return null as any;
        }).filter(Boolean as any) : [];
        const priceHistory = pts.filter(pt => typeof pt.p === 'number' && !Number.isNaN(pt.p)).slice().reverse();

        return NextResponse.json({ ok: true, slug: target, player, stock, history, priceHistory });
      }
    } catch (e) {
      // ignore URL parsing errors and fall back to list response
    }

  // include both `rows` (legacy) and `players` (consumer-friendly) keys
  return NextResponse.json({ ok: true, rows: filtered, players: playersWithPrice, teams });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: err.message }, { status: 500 });
  }
}
