import { NextResponse } from 'next/server';
import fs from 'fs/promises';
import path from 'path';

const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');
const indexPath = path.join(DATA_DIR, 'index.json');

const cache = new Map<string, { expires: number; value: any }>();
const TTL_MS = Number(process.env.ADVANCED_STATS_TTL_MS ?? 60 * 60 * 1000); // 1 hour

async function readJsonSafe(p: string) {
  try {
    const raw = await fs.readFile(p, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const teamParam = url.searchParams.get('team');
    if (!teamParam) return NextResponse.json({ ok: false, error: 'team required' }, { status: 400 });

    const key = `team:${teamParam.toUpperCase()}`;
    const cached = cache.get(key);
    if (cached && cached.expires > Date.now()) return NextResponse.json({ ok: true, data: cached.value });

    const idx = await readJsonSafe(indexPath);
    if (!idx) return NextResponse.json({ ok: true, data: [] });

    const candidates: Array<{ espnId: string; file?: string }> = [];

    // support index being a map (espnId -> filename) or an array of players
    if (typeof idx === 'object' && !Array.isArray(idx)) {
      // object map
      for (const [espnId, file] of Object.entries(idx)) {
        candidates.push({ espnId: String(espnId), file: String(file) });
      }
    }
    if (Array.isArray(idx.players)) {
      for (const p of idx.players) {
        if (p && (p.espnId || p.file)) candidates.push({ espnId: String(p.espnId), file: p.file });
      }
    }

    // If nothing in index, fall back to scanning all files in DATA_DIR (best-effort)
    let filesToCheck: string[] = [];
    if (candidates.length > 0) {
      filesToCheck = candidates.map((c) => path.join(DATA_DIR, c.file || `${c.espnId}.json`));
    } else {
      try {
        const all = await fs.readdir(DATA_DIR);
        filesToCheck = all.filter((f) => f.endsWith('.json')).map((f) => path.join(DATA_DIR, f));
      } catch (e) {
        filesToCheck = [];
      }
    }

    const teamUpper = teamParam.toUpperCase();
    const results: any[] = [];

    await Promise.all(filesToCheck.map(async (fp) => {
      const d = await readJsonSafe(fp);
      if (!d) return;
      // d may contain metrics or { player, metrics }
      const meta = d.player ?? d.metadata ?? null;
      const espnId = (d.player && d.player.espnId) || d.espnId || d.id || null;
      const playerTeam = (meta && (meta.team || meta.teamAbbrev || meta.team?.abbreviation)) || d.team || null;
      if (!playerTeam) return;
      // compare by uppercased abbreviation or team string
      if (String(playerTeam).toUpperCase() === teamUpper || String(playerTeam).toUpperCase().includes(teamUpper)) {
        results.push({ espnId: String(espnId || ''), data: d });
      }
    }));

    cache.set(key, { expires: Date.now() + TTL_MS, value: results });
    return NextResponse.json({ ok: true, data: results });
  } catch (e) {
    return NextResponse.json({ ok: false, error: String(e) }, { status: 500 });
  }
}
