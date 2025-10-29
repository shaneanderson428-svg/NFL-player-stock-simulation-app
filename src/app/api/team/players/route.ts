import { NextResponse } from 'next/server';
import { getPlayers, normalizeAndMapPlayersFromEspnRoster } from '@/lib/api';
import path from 'path';
import fs from 'fs/promises';
import historyStore from '@/lib/historyStore';

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const team = url.searchParams.get('team') || url.searchParams.get('abbr') || 'SEA';

    // Fetch roster (ESPN or mock)
    const res = await getPlayers(team);
    const players = Array.isArray(res?.response) ? res.response : [];

    // Map to Athlete shape via existing helper
    const athletes = normalizeAndMapPlayersFromEspnRoster({ items: players }, team);

    // Load persisted history map once
    const histMap = await historyStore.loadMap();

    // For each athlete, attempt to load advanced stats from data/advanced/<espnId>.json
    const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');

    const out = await Promise.all(athletes.map(async (a: any) => {
      const espnId = String(a.id || a.espnId || a.playerId || '');
      const advPath = path.join(DATA_DIR, `${espnId}.json`);
      let advanced = null;
      try {
        const raw = await fs.readFile(advPath, 'utf8');
        advanced = JSON.parse(raw);
      } catch (e) {
        advanced = null;
      }

      // Attach persisted history if present
      const hist = histMap.get(espnId) || histMap.get(String(Number(espnId))) || null;

      return {
        ...a,
        espnId,
        advanced: advanced?.metrics ? advanced.metrics : advanced,
        advancedMeta: advanced ? { player: advanced.player, position: advanced.position } : null,
        persistedHistory: Array.isArray(hist) ? hist : (a.priceHistory || []),
      };
    }));

    return NextResponse.json({ ok: true, team: String(team).toUpperCase(), players: out, _debug: res?._debug ?? null });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}
