import fs from "fs";
import path from "path";
import { NextResponse } from "next/server";
import { parse as csvParseSync } from "csv-parse/sync";

type CacheEntry = {
  mtimeMs: number;
  payload?: Record<string, any>;
  rows?: Array<Record<string, any>>;
};

const cache: Record<string, CacheEntry> = {};

function coerceValue(v: string) {
  if (v === null || v === undefined) return v;
  const t = v.trim();
  if (t === '') return null;
  // try integer
  if (/^-?\d+$/.test(t)) return parseInt(t, 10);
  // try float
  if (/^-?\d+\.\d+$/.test(t)) return parseFloat(t);
  // otherwise string
  return t;
}

export async function GET() {
  try {
    const filePath = path.join(process.cwd(), "data/epa_cpoe_summary_2025.csv");

    const st = fs.statSync(filePath);
    const mtimeMs = st.mtimeMs;

    const cached = cache[filePath];
    if (cached && cached.mtimeMs === mtimeMs && cached.payload) {
      return NextResponse.json(cached.payload);
    }

    const raw = fs.readFileSync(filePath, "utf8");
    // use csv-parse to handle quoted fields and commas
    const records = csvParseSync(raw, {
      columns: true,
      skip_empty_lines: true,
    }) as Array<Record<string, string>>;

    // coerce values
    const rows = records.map((r) => {
      const out: Record<string, any> = {};
      Object.entries(r).forEach(([k, v]) => {
        out[k] = coerceValue(String(v));
      });
      return out;
    });

    // try to load cleaned player profiles so we can enrich rows with full names
    const profilesPath = path.join(process.cwd(), 'data', 'player_profiles_cleaned.csv');
    const profilesByEspn: Record<string, { name: string; team?: string }> = {};
    const profilesByName: Record<string, { name: string; team?: string }> = {};
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
        return ns.replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
      } catch (e) {
        return String(s).toLowerCase().replace(/[^a-z0-9]/g, '-');
      }
    };

    if (fs.existsSync(profilesPath)) {
      try {
        const rawp = fs.readFileSync(profilesPath, 'utf8');
        const precords = csvParseSync(rawp, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
        for (const pr of precords) {
          const esp = String(pr.espnId ?? pr.espnid ?? pr.player_id ?? pr.playerid ?? '').trim();
          const name = String(pr.player ?? pr.player_name ?? pr.name ?? '').trim();
          const team = String(pr.team ?? pr.team_name ?? pr.team_abbr ?? '').trim();
          if (esp) profilesByEspn[esp] = { name, team };
          if (name) {
            profilesByName[name.toLowerCase()] = { name, team };
            const k = normalizeNameToKey(name);
            if (k) profilesByName[k] = { name, team };
          }
        }
      } catch (e) {
        // ignore profile parse errors
      }
    }

    // Try to supplement with roster_backup.csv (sometimes espnId formats differ)
    const rosterPath = path.join(process.cwd(), 'data', 'roster_backup.csv');
    let rosterRecords: Array<Record<string, string>> = [];
    if (fs.existsSync(rosterPath)) {
      try {
        const rawr = fs.readFileSync(rosterPath, 'utf8');
        const rrecords = csvParseSync(rawr, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
        rosterRecords = rrecords;
        for (const rr of rrecords) {
          let esp = String(rr.espnId ?? rr.espnid ?? rr.player_id ?? rr.playerid ?? rr.id ?? '').trim();
          const name = String(rr.player ?? rr.player_name ?? rr.name ?? rr.full_name ?? '').trim();
          const team = String(rr.team ?? rr.team_abbr ?? rr.team_name ?? '').trim();
          if (!esp && rr.espn && String(rr.espn).trim()) {
            // sometimes column is named 'espn'
            esp = String(rr.espn).trim();
          }
          if (esp) {
            // store multiple key variants for robustness (e.g. '3045146.0' vs '3045146')
            profilesByEspn[esp] = profilesByEspn[esp] || { name, team };
            try {
              const nnum = Number(esp);
              if (!isNaN(nnum)) profilesByEspn[String(nnum)] = profilesByEspn[String(nnum)] || { name, team };
              const noDot = String(esp).replace(/\.0$/, '');
              if (noDot) profilesByEspn[noDot] = profilesByEspn[noDot] || { name, team };
            } catch (e) {}
          }
          if (name) {
            const ln = name.split(' ').slice(-1)[0].toLowerCase();
            if (ln) {
              profilesByName[ln] = profilesByName[ln] || { name, team };
            }
            const k = normalizeNameToKey(name);
            if (k) profilesByName[k] = profilesByName[k] || { name, team };
          }
        }
      } catch (e) {
        // ignore roster parse errors
      }
    }

    // Enrich rows with profile names where missing. Build a simplified players array
    // with a consistent shape for consumers.
  let enrichedCount = 0;
  let filledMissingNameCount = 0;
  const unmatchedSamples: Array<Record<string, any>> = [];
    const players = rows.map((r) => {
      // candidate id fields (including empty-header key '')
      const idCandidates = [r.espnId, r.espn_id, r.espn, r.espnid, r.playerId, r.player_id, r.playerid, r.player, r.passer, r[''], r.id];
      let rawId: string | null = null;
      for (const c of idCandidates) {
        if (c !== undefined && c !== null && String(c).toString().trim() !== '') {
          rawId = String(c).trim();
          break;
        }
      }

      // candidate name fields
      const rawName = (r.passer_player_name ?? r.player ?? r.name ?? r.passer ?? '') as string;

      // find profile by espn id first, then by name variants
      let profile: { name: string; team?: string } | undefined;
      if (rawId) {
        // try direct match
        if (profilesByEspn[rawId]) profile = profilesByEspn[rawId];
        // try numeric-normalized variants (e.g. '3045146.0' -> '3045146')
        if (!profile) {
          const asNum = Number(rawId);
          if (!isNaN(asNum) && profilesByEspn[String(asNum)]) profile = profilesByEspn[String(asNum)];
        }
        if (!profile) {
          const stripped = String(rawId).replace(/\.0$/, '');
          if (profilesByEspn[stripped]) profile = profilesByEspn[stripped];
        }
      }

      // If the row has no name but does have an ID-like value, try to fill the
      // missing name by looking up the espnId/roster maps. This handles CSVs
      // where the name column is blank but an ID column exists.
      if (!profile && (!rawName || String(rawName).trim() === '') && rawId) {
        const cand = String(rawId).trim();
        // direct espn map
        if (profilesByEspn[cand]) {
          profile = profilesByEspn[cand];
        }
        // numeric normalization
        if (!profile) {
          const num = Number(cand);
          if (!isNaN(num) && profilesByEspn[String(num)]) profile = profilesByEspn[String(num)];
        }
        // strip trailing .0
        if (!profile) {
          const s = cand.replace(/\.0$/, '');
          if (profilesByEspn[s]) profile = profilesByEspn[s];
        }
        // try last-name map: profilesByName may contain last-name keys
        if (!profile) {
          try {
            const last = String(cand).split(' ').slice(-1)[0].toLowerCase();
            if (last && profilesByName[last]) profile = profilesByName[last];
          } catch (e) {}
        }
        // If still not found, check whether the rawId is a numeric index that maps
        // into our roster backup (some summary CSVs use positional indices as the
        // first column). Map 1-based index -> rosterRecords array index.
        if (!profile) {
          try {
            const asn = Number(cand);
            if (!isNaN(asn) && asn > 0 && rosterRecords && rosterRecords.length >= asn) {
              const rr = rosterRecords[asn - 1];
              const mappedEspn = String(rr.espnId ?? rr.espnid ?? rr.player_id ?? rr.playerid ?? rr.id ?? '').trim();
              const mappedName = String(rr.player ?? rr.player_name ?? rr.name ?? rr.full_name ?? '').trim();
              if (mappedEspn) {
                profile = profilesByEspn[mappedEspn] || { name: mappedName || mappedEspn, team: String(rr.team ?? '') };
              } else if (mappedName) {
                profile = { name: mappedName, team: String(rr.team ?? '') };
              }
            }
          } catch (e) {}
        }
        if (profile) {
          filledMissingNameCount += 1;
        }
      }
      if (!profile && rawName) {
        // Enhanced normalization for matching: case-insensitive and punctuation-agnostic
        const normalizeForMatch = (s: string) => {
          if (!s) return '';
          try {
            // remove diacritics, strip Jr/Sr suffixes, remove punctuation and whitespace,
            // and lowercase for robust matching
            return String(s)
              .normalize('NFKD')
              .replace(/\p{Diacritic}/gu, '')
              .replace(/\b(JR|SR|II|III|IV)\.?$/i, '')
              .replace(/[^a-z0-9]/gi, '')
              .toLowerCase()
              .trim();
          } catch (e) {
            return String(s).toLowerCase().replace(/[^a-z0-9]/g, '').trim();
          }
        };

        // Small Levenshtein distance implementation for fuzzy matching
        const levenshtein = (a: string, b: string) => {
          if (a === b) return 0;
          const al = a.length;
          const bl = b.length;
          if (al === 0) return bl;
          if (bl === 0) return al;
          const matrix: number[][] = [];
          for (let i = 0; i <= bl; i++) {
            matrix[i] = [i];
          }
          for (let j = 0; j <= al; j++) {
            matrix[0][j] = j;
          }
          for (let i = 1; i <= bl; i++) {
            for (let j = 1; j <= al; j++) {
              const cost = a[j - 1] === b[i - 1] ? 0 : 1;
              matrix[i][j] = Math.min(
                matrix[i - 1][j] + 1,
                matrix[i][j - 1] + 1,
                matrix[i - 1][j - 1] + cost
              );
            }
          }
          return matrix[bl][al];
        };

        const rawNorm = normalizeForMatch(String(rawName));

        // try direct normalized match against profilesByName keys
        for (const [k, v] of Object.entries(profilesByName)) {
          if (!k) continue;
          const kNorm = k.toLowerCase().replace(/[^a-z0-9]/g, '');
          if (!kNorm) continue;
          if (kNorm === rawNorm) {
            profile = v;
            break;
          }
        }

        // try last-name match (normalized)
        if (!profile) {
          try {
            const rawLast = String(rawName).split(' ').slice(-1)[0] || '';
            const rawLastNorm = normalizeForMatch(rawLast);
            for (const [k, v] of Object.entries(profilesByName)) {
              const kNorm = k.toLowerCase().replace(/[^a-z0-9]/g, '');
              if (kNorm.endsWith(rawLastNorm) && rawLastNorm.length >= 2) {
                profile = v;
                break;
              }
            }
          } catch (e) {
            // ignore
          }
        }

        // fuzzy Levenshtein fallback: allow small edit distance or substring matches
        if (!profile) {
          let best: { score: number; v?: { name: string; team?: string } } = { score: Infinity };
          for (const [k, v] of Object.entries(profilesByName)) {
            if (!k) continue;
            const kNorm = k.toLowerCase().replace(/[^a-z0-9]/g, '');
            if (!kNorm) continue;
            // cheap substring check first
            if (kNorm.includes(rawNorm) || rawNorm.includes(kNorm)) {
              profile = v;
              break;
            }
            const dist = levenshtein(kNorm, rawNorm);
            // relative threshold: allow small edits up to 20% of length, with absolute cap 3
            const thresh = Math.max(1, Math.min(3, Math.floor(Math.max(kNorm.length, rawNorm.length) * 0.2)));
            if (dist <= thresh && dist < best.score) {
              best = { score: dist, v };
            }
          }
          if (!profile && best.v) profile = best.v;
        }
      }

      const name = profile ? profile.name : (rawName ? String(rawName) : rawId ? String(rawId) : 'Unknown');
      const team = profile ? profile.team || '' : String(r.team ?? '');

      // detect if we enriched a missing name
      if ((!rawName || String(rawName).trim() === '') && profile && profile.name) enrichedCount += 1;
      // capture a small sample of unmatched rows for diagnostics
      if (!profile) {
        if (unmatchedSamples.length < 10) {
          unmatchedSamples.push({ id: rawId || null, rawName: rawName || null, keys: Object.keys(r).slice(0,5) });
        }
      }

      // preserve numeric fields when present
      const stock_value = r.stock_value !== undefined ? Number(r.stock_value) : (r.stock !== undefined ? Number(r.stock) : null);
      const stock_change = r.stock_change !== undefined ? Number(r.stock_change) : (r.stock_change !== undefined ? Number(r.stock_change) : null);
      const last_game_delta = r.last_game_delta !== undefined ? Number(r.last_game_delta) : (r.last_game_delta !== undefined ? Number(r.last_game_delta) : null);

      return {
        id: rawId || String(name).replace(/\s+/g, '-').toLowerCase(),
        name: String(name),
        team: String(team || ''),
        // keep these fields even when null to ensure a stable shape
        stock_value: stock_value !== null ? stock_value : null,
        stock_change: stock_change !== null ? stock_change : null,
        last_game_delta: last_game_delta !== null ? last_game_delta : null,
        // expose raw row for debugging / backwards compatibility
        _raw: r,
      } as Record<string, any>;
    });

  // cache full payload so subsequent requests return the same shape
  const payload = { ok: true, rows, players, enriched: { enrichedCount, filledMissingNameCount, total: players.length } };
  cache[filePath] = { mtimeMs, rows, payload };

  // log enrichment summary
  // eslint-disable-next-line no-console
  console.log(`Enriched ${enrichedCount}/${players.length} players with names from profile CSV`);
  // eslint-disable-next-line no-console
  console.log(`Filled ${filledMissingNameCount} missing names via espnId/roster lookup`);
    if (unmatchedSamples.length) {
      // eslint-disable-next-line no-console
      console.log('Leaderboard enrichment unmatched samples:', JSON.stringify(unmatchedSamples, null, 2));
    }

  return NextResponse.json({ ok: true, rows, players, enriched: { enrichedCount, filledMissingNameCount, total: players.length } });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: err.message }, { status: 500 });
  }
}
