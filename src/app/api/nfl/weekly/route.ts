import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import { NextResponse } from 'next/server';
import { parse as csvParseSync } from 'csv-parse/sync';

// API: /api/nfl/weekly
// Reads the latest `external/tank01/player_stats_week_<WEEK>.(csv|json)` and the
// optional `external/advanced/advanced_metrics_week_<WEEK>.csv` and returns a
// normalized list of player objects suitable for server-side rendering.

function coerceNumeric(v: any) {
  if (v === null || v === undefined) return v;
  const s = String(v).trim();
  if (s === '') return null;
  if (/^-?\d+$/.test(s)) return parseInt(s, 10);
  if (/^-?\d+\.\d+$/.test(s)) return parseFloat(s);
  return v;
}

function normalizePlayerId(obj: Record<string, any>) {
  const cand = obj.playerID ?? obj.espnId ?? obj.espn_id ?? obj.espn ?? obj.playerId ?? obj.player_id ?? obj.playerid;
  if (cand === undefined || cand === null) return null;
  return String(cand);
}

async function findLatestWeekFile(dir: string) {
  try {
    const files = await fs.readdir(dir);
    // match player_stats_week_<WEEK>.(csv|json)
    const re = /player_stats_week_(\d+)\.(csv|json)$/i;
    let best: { week: number; file: string } | null = null;
    for (const f of files) {
      const m = f.match(re);
      if (m) {
        const wk = Number(m[1]);
        if (!best || wk > best.week) best = { week: wk, file: f };
      }
    }
    return best;
  } catch (e) {
    return null;
  }
}

export async function GET() {
  try {
    const tankDir = path.join(process.cwd(), 'external', 'tank01');
    const advDir = path.join(process.cwd(), 'external', 'advanced');

    if (!fsSync.existsSync(tankDir)) {
      return NextResponse.json({ ok: false, error: 'external/tank01 directory not found' }, { status: 404 });
    }

    const latest = await findLatestWeekFile(tankDir);
    if (!latest) {
      return NextResponse.json({ ok: false, error: 'no player_stats_week_<WEEK> file found' }, { status: 404 });
    }

    const week = latest.week;
    const statsPath = path.join(tankDir, latest.file);

    // load stats (csv or json)
    let statsRows: Array<Record<string, any>> = [];
    if (latest.file.toLowerCase().endsWith('.json')) {
      const raw = await fs.readFile(statsPath, 'utf8');
      const parsed = JSON.parse(raw || '[]');
      // if file contains { rows: [...] } or { ok: true, rows: [...] }
      if (Array.isArray(parsed)) statsRows = parsed as any[];
      else if (parsed && Array.isArray(parsed.rows)) statsRows = parsed.rows;
      else statsRows = [];
    } else {
      const raw = await fs.readFile(statsPath, 'utf8');
      const parsed = csvParseSync(raw, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
      statsRows = parsed.map((r) => {
        const out: Record<string, any> = {};
        Object.entries(r).forEach(([k, v]) => {
          out[k] = coerceNumeric(v);
        });
        return out;
      });
    }

    // attempt to load advanced metrics for same week
    const advFilenameCsv = `advanced_metrics_week_${week}.csv`;
    const advPath = path.join(advDir, advFilenameCsv);
    let advRows: Record<string, Record<string, any>> = {};
    if (fsSync.existsSync(advPath)) {
      try {
        const raw = await fs.readFile(advPath, 'utf8');
        const parsed = csvParseSync(raw, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
        for (const r of parsed) {
          const out: Record<string, any> = {};
          Object.entries(r).forEach(([k, v]) => (out[k] = coerceNumeric(v)));
          const id = normalizePlayerId(out) || String(out.player || out.playerName || out.playerNameLong || '');
          if (id) advRows[id] = out;
        }
      } catch (e) {
        // ignore advanced parse errors — UI can function without advancedMetrics
        advRows = {};
      }
    }

  // Build normalized objects
    const players = statsRows.map((r) => {
      const id = normalizePlayerId(r) || String(r.player || r.playerName || r.player_name || '') || '';
      const playerId = id;
      const name = r.playerName || r.longName || r.player || r.displayName || '';
      const position = (r.position || r.pos || '').toString().toUpperCase();
      const team = r.team || r.Team || r.team_abbr || '';
      const stats = { ...r };
      // remove redundant fields from stats
      delete stats.playerID;
      delete stats.espnId;
      delete stats.espn_id;
      delete stats.player;

      const advancedMetrics = advRows[playerId] ?? advRows[String(r.espnID ?? r.espnId ?? r.playerID ?? '')] ?? null;

      return {
        playerId,
        name,
        position,
        team,
        week,
        stats,
        advancedMetrics,
      };
    });

    // Log which week is being rendered (server-side) for operational visibility
    try {
      // eslint-disable-next-line no-console
      console.log(`/api/nfl/weekly rendering week ${week} with ${players.length} players`);
    } catch (e) {
      // ignore logging errors
    }

    return NextResponse.json({ ok: true, week, players });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: String(e?.message ?? e) }, { status: 500 });
  }
}
