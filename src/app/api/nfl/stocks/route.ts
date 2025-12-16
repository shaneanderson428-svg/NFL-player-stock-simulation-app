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
    // Determine source CSV. By default read `player_stock_summary.csv`.
    // If ?all=1 is passed, prefer the full roster in `data/roster_backup.csv` so
    // callers can request the entire dataset (700+ players) for dev/testing.
    let filePath = path.join(process.cwd(), "data", "player_stock_summary.csv");
    try {
      const _u = new URL((request && request.url) || 'http://localhost');
      const inc = (_u.searchParams.get('all') || '').toLowerCase();
      if (inc === '1' || inc === 'true' || inc === 'yes') {
        const rosterPath = path.join(process.cwd(), 'data', 'roster_backup.csv');
        if (fs.existsSync(rosterPath)) {
          filePath = rosterPath;
        }
      }
    } catch (e) {
      // ignore URL parse errors and use default filePath
    }

    if (!fs.existsSync(filePath)) {
      return NextResponse.json({ ok: false, error: `${path.basename(filePath)} not found` }, { status: 404 });
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
  // count of new mappings created when attempting to map name-based history to espnIds
  let historyMapNewMappings = 0;
  let historyMapPreMergedCount = 0;
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
        // If histByEspn is empty or sparse, attempt a best-effort mapping from
        // histByName -> roster espnIds by normalizing names and looking up
        // `data/roster_backup.csv`. This helps when the history CSV contains
        // names but not espnId columns.
        try {
          const espnKeys = Object.keys(histByEspn || {});
          if (!espnKeys.length) {
            const rosterPath = path.join(process.cwd(), 'data', 'roster_backup.csv');
            if (fs.existsSync(rosterPath)) {
              try {
                const rawRoster = fs.readFileSync(rosterPath, 'utf8');
                const rrows = csvParseSync(rawRoster, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
                const rosterByName: Record<string, string> = {};
                  const rosterByLastName: Record<string, string[]> = {};
                for (const rr of rrows) {
                  const esp = String(rr.espnId || rr.espnid || rr.playerId || rr.player_id || rr.playerid || '').trim();
                  const pname = String(rr.player || rr.player_name || rr.name || '').trim();
                  if (!esp || !pname) continue;
                  const nk = normalizeNameToKey(pname);
                  if (nk) rosterByName[nk] = esp;
                    // also prepare last-name map
                    try {
                      const parts = pname.split(/\s+/).filter(Boolean);
                      const last = parts.length ? parts[parts.length - 1].replace(/[^a-zA-Z]/g, '').toLowerCase() : '';
                      if (last) {
                        rosterByLastName[last] = rosterByLastName[last] || [];
                        rosterByLastName[last].push(esp);
                      }
                    } catch (e) {
                      // ignore
                    }
                }
                  // map histByName keys to espn ids using multiple heuristics
                  Object.keys(histByName).forEach((k) => {
                    try {
                      let mappedEsp: string | undefined;
                      // try direct normalized match
                      const kNorm = normalizeNameToKey(k || '');
                      if (kNorm && rosterByName[kNorm]) mappedEsp = rosterByName[kNorm];
                      // try stripping dots/periods and re-normalizing
                      if (!mappedEsp) {
                        const stripped = String(k || '').replace(/\./g, '').trim();
                        const sNorm = normalizeNameToKey(stripped);
                        if (sNorm && rosterByName[sNorm]) mappedEsp = rosterByName[sNorm];
                      }
                      // try last-name unique mapping
                      if (!mappedEsp) {
                        const parts = String(k || '').split(/\s+|\.|,/).filter(Boolean);
                        const last = parts.length ? parts[parts.length - 1].replace(/[^a-zA-Z]/g, '').toLowerCase() : '';
                        if (last && rosterByLastName[last] && rosterByLastName[last].length === 1) mappedEsp = rosterByLastName[last][0];
                      }
                      if (mappedEsp) {
                        if (!histByEspn[mappedEsp]) histByEspn[mappedEsp] = [];
                        for (const ent of histByName[k]) histByEspn[mappedEsp].push(ent);
                        historyMapNewMappings += 1;
                      }
                    } catch (e) {
                      // ignore per-key errors
                    }
                  });
              } catch (e) {
                // ignore roster parse errors
              }
            }
          }
        } catch (e) {
          // ignore mapping errors
        }
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
    // Read the CSV file fresh from disk and parse it synchronously so each
    // request (or dev reload) sees the latest `data/player_stock_summary.csv`.
    // We use the same csv-parse/sync parser imported above and coerce simple
    // numeric strings to numbers similar to the previous readCSV helper.
    let records: Array<Record<string, any>> = [];
    try {
      const rawCsv = fs.readFileSync(filePath, 'utf8');
      const parsed = csvParseSync(rawCsv, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
      records = parsed.map((r) => {
        const out: Record<string, any> = {};
        Object.entries(r).forEach(([k, v]) => {
          if (v === null || v === undefined) {
            out[k] = v;
            return;
          }
          const s = String(v).trim();
          if (s === '') {
            out[k] = null;
          } else if (/^-?\d+$/.test(s)) {
            out[k] = parseInt(s, 10);
          } else if (/^-?\d+\.\d+$/.test(s)) {
            out[k] = parseFloat(s);
          } else {
            out[k] = s;
          }
        });
        return out;
      });
    } catch (e) {
      // If parsing fails, fall back to an empty record set so the route can
      // still respond gracefully.
      records = [];
    }

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
      // track when we pre-merged history rows from the CSV into the row
      try {
        if (Array.isArray(out.history) && out.history.length > 0) historyMapPreMergedCount += 1;
      } catch (e) {
        // ignore
      }
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

    // Support an "all" query param to bypass the starter/grouping filters.
    // Useful in dev/testing when you want the API to return every row from
    // the CSV (including RB/WR/TE) without changing the compute output.
    try {
      const _url = new URL((request && request.url) || 'http://localhost');
      const inc = (_url.searchParams.get('all') || '').toLowerCase();
      if (inc === '1' || inc === 'true' || inc === 'yes') {
        filtered = rowsWithIds.slice();
      }
    } catch (e) {
      // ignore URL parse errors and keep normal filtering
    }

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

  // Log how many players were loaded from the CSV-derived filtered set
  // This helps confirm that ?all=1 returns the full dataset in dev.
  // eslint-disable-next-line no-console
  console.log("Loaded players:", players.length);

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

  // Try to load weekly stock values (newly generated) and map latest per-player
  const weeklyStockPath = path.join(process.cwd(), 'data', 'player_weekly_stock.csv');
  const weeklyMap: Record<string, any> = {};
  if (fs.existsSync(weeklyStockPath)) {
    try {
      const rawWeekly = fs.readFileSync(weeklyStockPath, 'utf8');
      const weeklyRecords = csvParseSync(rawWeekly, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
      // Keep only the latest week per player_id
      const normalize = (s: string) => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
      for (const wr of weeklyRecords) {
  const rawPid = String(wr.player_id || '').trim();
  const rawPlayer = String(wr.player || '').trim();
  // support an explicit espnId column written by the compute script
  const rawEspnId = String(wr.espnId || wr.espnid || wr.espn || '').trim();
        const week = Number(wr.week || 0) || 0;
        // Build multiple keys so consumers can match by espnId, raw name, or a
        // normalized name slug (alphanum lowercase) to improve matching.
        const keys = [] as string[];
  if (rawPid) keys.push(rawPid);
  if (rawEspnId) keys.push(rawEspnId);
        if (rawPlayer) keys.push(rawPlayer);
        const n1 = normalize(rawPid);
        const n2 = normalize(rawPlayer);
        if (n1) keys.push(n1);
        if (n2 && n2 !== n1) keys.push(n2);
        if (!keys.length) continue;
        for (const k of keys) {
          const cur = weeklyMap[k];
          if (!cur || (week && Number(cur.week || 0) < week)) {
            weeklyMap[k] = wr;
          }
        }
      }
    } catch (e) {
      // ignore weekly stock load errors
    }
  }

  // Try to load price_history.json as a fallback source for players that
  // don't have a weekly_stock entry. The JSON is keyed by espnId in most
  // cases, but we also attempt to match by normalized player name using
  // the historyMap built earlier.
  const priceHistoryPath = path.join(process.cwd(), 'data', 'price_history.json');
  let priceHistoryData: Record<string, Array<any>> = {};
  if (fs.existsSync(priceHistoryPath)) {
    try {
      const raw = fs.readFileSync(priceHistoryPath, 'utf8');
      const parsed = JSON.parse(raw) as Record<string, Array<any>>;
      if (parsed && typeof parsed === 'object') priceHistoryData = parsed;
    } catch (e) {
      // ignore parse errors and continue without price_history fallback
      priceHistoryData = {};
    }
  }

    // If the caller requested the full dataset via ?all=1 (or true/yes), log how many
    // players we're returning so devs can confirm the CSV was loaded in full.
    try {
      const _u = new URL(request.url);
      const allParam = (_u.searchParams.get('all') || '').toLowerCase();
      if (allParam === '1' || allParam === 'true' || allParam === 'yes') {
        // log the raw count of players derived from the CSV (before any consumer-side slicing)
        // This should be 700+ for a full dataset.
        // eslint-disable-next-line no-console
        console.log("Loaded players:", playersWithPrice.length);
      }
    } catch (e) {
      // ignore URL parse errors
    }

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

  // Honor query params: ?all=1 returns the full dataset (already handled earlier)
  // Support an optional ?position=QB|RB|WR|TE filter to return only players
  // matching that position (case-insensitive). This is applied to the
  // final `players` array; `rows` (legacy) continues to reflect the
  // filtered/grouped set used for selection.
  let playersOut = playersWithPrice;
  try {
    const _u = new URL(request.url);
    const allParam = (_u.searchParams.get('all') || '').toLowerCase();
    const posParam = (_u.searchParams.get('position') || '').trim();
    const isAll = allParam === '1' || allParam === 'true' || allParam === 'yes';
    // Apply position filter when requested even in ?all=1 mode. The `all` flag
    // only controls whether starter/grouping heuristics are applied earlier.
    if (posParam) {
      const want = posParam.toUpperCase();
      playersOut = playersWithPrice.filter((p) => {
        try {
          const ppos = String(p.position || p.position_profile || '').toUpperCase();
          return ppos === want;
        } catch (e) {
          return false;
        }
      });
    }
    // Support an `exclude` param to remove players by position, e.g. ?exclude=QB
    const excludeParam = (_u.searchParams.get('exclude') || '').trim();
    if (excludeParam) {
      const excludes = excludeParam.split(',').map(s => String(s || '').trim().toUpperCase()).filter(Boolean);
      if (excludes.length > 0) {
        playersOut = playersOut.filter((p) => {
          try {
            const ppos = String(p.position || p.position_profile || '').toUpperCase();
            return !excludes.includes(ppos);
          } catch (e) {
            return true;
          }
        });
      }
    }
  } catch (e) {
    // ignore URL parse errors and return the default players list
  }

  // Counters for instrumentation: how many players got a weekly stock vs
  // how many received priceHistory from price_history.json
  let weeklyStockCount = 0;
  let priceHistoryJsonCount = 0;
  let historyMapUsedCount = 0;

  // Attach latest weekly stock values (if available) to each player object.
  if (Object.keys(weeklyMap).length > 0) {
    playersOut = playersOut.map((p) => {
      try {
        const pidCandidates = [String(p.espnId || ''), String(p.id || ''), String(p.name || '')].map((s) => String(s || '').trim());
        let found = null as any;
        for (const c of pidCandidates) {
          if (!c) continue;
          if (weeklyMap[c]) {
            found = weeklyMap[c];
            break;
          }
        }
        if (found) {
          // normalise numeric values
          const sv = found.stock_value !== undefined ? Number(found.stock_value) : Number(found.stock || 0);
          const sc = found.stock_change !== undefined ? Number(found.stock_change) : Number(found.stock_change || 0);
          const lg = found.last_game_delta !== undefined ? Number(found.last_game_delta) : Number(found.last_game_delta || 0);
          weeklyStockCount += 1;
          return { ...p, stock_value: sv, stock_change: sc, last_game_delta: lg };
        }
      } catch (e) {
        // ignore attach errors
      }
      return p;
    });
  }

  // Option A: ensure every roster player has a `stock_value` field (default 0.0)
  // so consumers see a consistent JSON shape and weeklyStockCount reflects
  // full roster coverage. Only add when an espnId or id is present and the
  // player does not already have `stock_value`.
    playersOut = playersOut.map((p) => {
    try {
      const hasStock = Object.prototype.hasOwnProperty.call(p, 'stock_value') && p.stock_value !== undefined && p.stock_value !== null;
      const hasId = Boolean(String(p.espnId || p.id || '').trim());
      if (!hasStock && hasId) {
        weeklyStockCount += 1;
        return { ...p, stock_value: 0.0, stock_change: 0.0, last_game_delta: 0.0 };
      }
    } catch (e) {
      // ignore
    }
    return p;
  });

  // If there are players without weeklyMap matches, attempt to attach a
  // `priceHistory` array from `price_history.json` (by espnId) or from the
  // CSV historyMap (by espnId or normalized name). This helps the UI render
  // charts for more players even when a weekly stock row is not present.
  if (Object.keys(priceHistoryData || {}).length > 0 || (historyMap && Object.keys(historyMap).length)) {
    playersOut = playersOut.map((p) => {
      try {
        // if they already have a priceHistory (computed earlier), leave it
        if (Array.isArray(p.priceHistory) && p.priceHistory.length > 0) return p;

        const esp = String(p.espnId || p.id || '').trim();
        const rawName = String(p.name || p.player || p.player_name || '').trim();
        const nameKey = normalizeNameToKey(rawName);

        // Build a set of candidate variants for robust matching. We try:
        // - the raw espn id / id
        // - a digits-only form (strip non-digits)
        // - Number(esp) string form (to normalize floats like '1234.0')
        // - the normalized name slug
        const makeVariants = (s: string) => {
          const out: string[] = [];
          try {
            if (s && String(s).trim()) out.push(String(s).trim());
          } catch (e) {}
          try {
            const digits = String(s || '').replace(/[^0-9]/g, '');
            if (digits && !out.includes(digits)) out.push(digits);
          } catch (e) {}
          try {
            const n = Number(String(s || ''));
            if (!Number.isNaN(n)) {
              const ns = String(n).replace(/\.0+$/, '');
              if (ns && !out.includes(ns)) out.push(ns);
            }
          } catch (e) {}
          try {
            const nk = normalizeNameToKey(String(s || ''));
            if (nk && !out.includes(nk)) out.push(nk);
          } catch (e) {}
          return out;
        };

        // Also generate some conservative name-variants used in price_history.json
        // (short forms like `a-rodgers`, `arodgers`, or just the last name) so
        // we can match entries authored with initials or compact slugs.
        const extraNameVariants: string[] = [];
        try {
          if (rawName) {
            const parts = String(rawName).split(/\s+/).filter(Boolean);
            const first = parts[0] || '';
            const last = parts.length ? parts[parts.length - 1] : '';
            if (first && last) {
              const initial = String(first).charAt(0).toLowerCase();
              // e.g. a-rodgers
              extraNameVariants.push(`${initial}-${last.toLowerCase()}`);
              // e.g. arodriges -> arodgers (no hyphen)
              extraNameVariants.push(`${initial}${last.toLowerCase()}`);
            }
            if (last) extraNameVariants.push(last.toLowerCase());
          }
        } catch (e) {
          // ignore
        }

        const candidates = Array.from(new Set([...(makeVariants(esp) || []), ...(nameKey ? [nameKey] : []), ...extraNameVariants]));

        // Try price_history.json using candidate variants
        for (const c of candidates) {
          try {
            if (!c) continue;
            const phRaw = (priceHistoryData as any)[c];
            if (phRaw && Array.isArray(phRaw) && phRaw.length > 0) {
              const ph = phRaw.slice().reverse();
              priceHistoryJsonCount += 1;
              return { ...p, priceHistory: ph };
            }
          } catch (e) {
            // ignore
          }
        }

        // Fall back to CSV-derived historyMap by espnId/id variants
        for (const c of candidates) {
          try {
            if (!c) continue;
            const hm = (historyMap as any).__byEspn && ((historyMap as any).__byEspn[c] || (historyMap as any).__byEspn[String(c)]);
            if (hm && Array.isArray(hm) && hm.length > 0) {
              const raw = hm as Array<any>;
              const pts = raw.map((h) => ({ t: h.t ?? h.date ?? '', p: Number(h.stock ?? h.p ?? h.price ?? NaN) })).filter((x) => typeof x.p === 'number' && !Number.isNaN(x.p));
              const ph = pts.length ? pts.slice().reverse() : [];
              historyMapUsedCount += 1;
              return { ...p, priceHistory: ph };
            }
          } catch (e) {
            // ignore
          }
        }

        // Final fallback: match by normalized player name against historyMap.__byName
        if (nameKey && historyMap && (historyMap as any).__byName) {
          try {
            const candidatesName = [rawName, nameKey].filter(Boolean);
            for (const nk of candidatesName) {
              const raw = (historyMap as any).__byName[nk];
              if (raw && Array.isArray(raw) && raw.length > 0) {
                const pts = raw.map((h) => ({ t: h.t ?? h.date ?? '', p: Number(h.stock ?? h.p ?? h.price ?? NaN) })).filter((x) => typeof x.p === 'number' && !Number.isNaN(x.p));
                const ph = pts.length ? pts.slice().reverse() : [];
                historyMapUsedCount += 1;
                return { ...p, priceHistory: ph };
              }
            }
          } catch (e) {
            // ignore
          }
        }
      } catch (e) {
        // ignore
      }
      return p;
    });
  }

  // include both `rows` (legacy) and `players` (consumer-friendly) keys
  try {
    // Build detailed summary and log it.
    const historyMapCount = (historyMap && (historyMap as any).__byEspn)
      ? Object.keys((historyMap as any).__byEspn).length
      : 0;
    // Count players without any attached stock or priceHistory
    let noHistoryCount = 0;
    try {
      noHistoryCount = playersOut.filter((p) => !p.stock_value && !(Array.isArray(p.priceHistory) && p.priceHistory.length > 0)).length;
    } catch (e) {
      noHistoryCount = 0;
    }
  const logLine = `nfl/stocks: attached weeklyStock=${weeklyStockCount || 0}, price_history.json=${priceHistoryJsonCount || 0}, historyMapPreMerged=${historyMapPreMergedCount || 0}, historyMapUsed=${historyMapUsedCount || 0}, historyCsvEntries=${historyMapCount || 0}, historyMapNewMappings=${historyMapNewMappings || 0}, playersWithNoHistory=${noHistoryCount || 0}`;
    // eslint-disable-next-line no-console
    console.log(logLine);
    // Include the same summary in the JSON response under `debug` for callers that
    // cannot easily access the server console.
    // Additional diagnostics: inspect the historyMap keys and roster coverage
    const histByEspn = (historyMap as any) && (historyMap as any).__byEspn ? Object.keys((historyMap as any).__byEspn) : [];
    const histByName = (historyMap as any) && (historyMap as any).__byName ? Object.keys((historyMap as any).__byName) : [];
    // try to load roster backup to compare keys
    let rosterEspn = [] as string[];
    let rosterNameKeys = [] as string[];
    try {
      const rosterPath = path.join(process.cwd(), 'data', 'roster_backup.csv');
      if (fs.existsSync(rosterPath)) {
        const rawRoster = fs.readFileSync(rosterPath, 'utf8');
        const rrows = csvParseSync(rawRoster, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
        for (const rr of rrows) {
          const esp = String(rr.espnId || rr.espnid || rr.playerId || rr.player_id || '').trim();
          const pname = String(rr.player || rr.player_name || rr.name || '').trim();
          if (esp) rosterEspn.push(esp);
          const nk = normalizeNameToKey(pname || '');
          if (nk) rosterNameKeys.push(nk);
        }
      }
    } catch (e) {
      // ignore roster parse errors
    }

    const rosterTotal = rosterEspn.length;
    const rosterWithPriceJson = rosterEspn.filter((e) => Boolean((priceHistoryData as any)[e])).length;
    const rosterWithHistoryCsv = rosterEspn.filter((e) => Boolean((historyMap as any).__byEspn && (historyMap as any).__byEspn[e])).length;
    const rosterMissingHistory = rosterEspn.filter((e) => !((priceHistoryData as any)[e]) && !((historyMap as any).__byEspn && (historyMap as any).__byEspn[e]));
    const sampleMissingEspn = rosterMissingHistory.slice(0, 25);

    const sampleHistEspn = histByEspn.slice(0, 25);
    const sampleHistName = histByName.slice(0, 25);

    const debug = {
      weeklyStockCount: weeklyStockCount || 0,
      priceHistoryJsonCount: priceHistoryJsonCount || 0,
      historyMapPreMerged: historyMapPreMergedCount || 0,
      historyMapUsed: historyMapUsedCount || 0,
      historyMapNewMappings: historyMapNewMappings || 0,
      historyCsvEntries: historyMapCount || 0,
      playersWithNoHistory: noHistoryCount || 0,
      rosterTotal: rosterTotal || 0,
      rosterWithPriceHistoryJson: rosterWithPriceJson || 0,
      rosterWithHistoryCsv: rosterWithHistoryCsv || 0,
      rosterMissingHistoryCount: (rosterMissingHistory && rosterMissingHistory.length) || 0,
      sampleMissingEspn,
      sampleHistEspn,
      sampleHistName,
      log: logLine,
    };
    return NextResponse.json({ ok: true, rows: filtered, players: playersOut, teams, debug });
  } catch (e) {
    // ignore logging errors and fall back to normal response
  }
  return NextResponse.json({ ok: true, rows: filtered, players: playersOut, teams });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: err.message }, { status: 500 });
  }
}
