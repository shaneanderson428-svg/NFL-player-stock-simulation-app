import React from 'react';
import PlayerCard from '@/app/components/PlayerCard';
import PlayersVirtualGrid from './PlayersVirtualGrid';
import LeaderboardChart from '@/app/components/LeaderboardChart';
import { renderSparkline as renderSparklineShared } from '@/lib/sparkline';
import historyStore from '@/lib/historyStore';
import path from 'path';
import fs from 'fs/promises';

export const revalidate = 10;

async function readJson(p: string) {
  try {
    const raw = await fs.readFile(p, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

export default async function Page({ searchParams }: { searchParams?: { position?: string; all?: string; page?: string; limit?: string } }) {
  const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');
  const index = await readJson(path.join(DATA_DIR, 'index.json'));
  // index.json can be either { players: [{ espnId, file }, ...], lastUpdated: "..." }
  // or a flat map produced by the compute script: { "3045146.0": "3045146.0.json", ... }
  let playersIndex: Array<{ espnId: number; file: string }> = [];
  if (index) {
    if (Array.isArray(index.players)) {
      playersIndex = index.players;
    } else if (typeof index === 'object' && !Array.isArray(index)) {
      // Detect flat-map shape (keys are espnIds, values are filenames)
      const keys = Object.keys(index);
      const isFlatMap = keys.length > 0 && typeof index[keys[0]] === 'string';
      if (isFlatMap) {
        playersIndex = keys.map((k) => ({ espnId: Number(k), file: String(index[k]) }));
      }
    }
  }

  const histMap = await historyStore.loadMap();

  // Load team map file (optional). If present, it should be a map of espnId -> { abbreviation, name }
  const TEAM_MAP_PATH = path.join(process.cwd(), 'data', 'team-map.json');
  const teamMap = (await readJson(TEAM_MAP_PATH)) || {};

  // Load each advanced file
  const items = await Promise.all(playersIndex.map(async (p) => {
    const fp = path.join(DATA_DIR, p.file || `${p.espnId}.json`);
    const d = await readJson(fp);
    const espnId = String(p.espnId || d?.espnId || '');
    const persisted = histMap.get(espnId) || histMap.get(String(Number(espnId))) || null;
    const team = d?.team || teamMap[String(p.espnId)] || null;
    return {
      espnId,
      name: d?.player ?? d?.playerName ?? d?.player ?? null,
      position: d?.position ?? null,
      team,
      advanced: d?.metrics ?? d ?? null,
      persistedHistory: Array.isArray(persisted) ? persisted : null,
      raw: d ?? null,
    };
  }));

  // Optional position filter from querystring, e.g. /players?position=WR
  // `searchParams` can be treated as async in some Next.js versions; resolve it first
  const sp = await Promise.resolve(searchParams);
  const positionFilter = String((sp?.position) || '').trim().toUpperCase();

  // If caller requested the full roster (all=1) then fetch the API server-side
  // and render those players instead of the advanced index. This enables
  // viewing all 700+ players (and their priceHistory) e.g. /players?all=1&position=WR
  const allParam = String((sp?.all) || '').trim().toLowerCase();
  let filteredItems = items;
  if (allParam === '1' || allParam === 'true' || allParam === 'yes') {
    try {
      // fetch the API server-side — Next's fetch to a relative path works in server components
      const posQuery = positionFilter ? `&position=${encodeURIComponent(positionFilter)}` : '';
      const res = await fetch(`/api/nfl/stocks?all=1${posQuery}`, { cache: 'no-store' });
      const json = await res.json();
      const players = Array.isArray(json?.players) ? json.players : [];
      // Map API players into the same shape expected by the PlayerCard mapping below
      filteredItems = players.map((p: any) => ({
        espnId: String(p.espnId || p.id || ''),
        name: p.name || p.player || '',
        position: p.position || p.position_profile || '',
        team: p.team || '',
        raw: p || null,
        // include server-side priceHistory so PlayerCard can render sparklines immediately
        persistedHistory: Array.isArray(p.priceHistory) ? p.priceHistory : (Array.isArray(p.history) ? p.history : []),
        // also expose stock/confidence for immediate display via PlayerCard prop synth
        stock: p.stock,
        confidence: p.confidence,
        history: Array.isArray(p.history) ? p.history : undefined,
      }));
    } catch (e) {
      // on failure fall back to advanced index items
      filteredItems = items;
    }
  } else {
    filteredItems = positionFilter
      ? items.filter((it) => String(it.position || '').toUpperCase() === positionFilter)
      : items;
  }

  return (
    <div style={{ padding: 18 }}>
      <div className="players-header">
        <div>
          <h1 style={{ color: '#fff', margin: 0 }}>All advanced players</h1>
        </div>
      </div>

      <LeaderboardChart minPlays={50} topN={10} />

      <div style={{ marginTop: 12, marginBottom: 8, position: 'relative', zIndex: 20 }}>
        <a href="/players" className={`player-meta ${(!positionFilter && allParam !== '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>Advanced</a>
        <a href="/players?all=1" className={`player-meta ${(allParam === '1' && !positionFilter) ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>Full roster</a>
        <a href="/players?all=1&position=WR" className={`player-meta ${(positionFilter === 'WR' && allParam === '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>WRs (all)</a>
        <a href="/players?all=1&position=RB" className={`player-meta ${(positionFilter === 'RB' && allParam === '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>RBs (all)</a>
        <a href="/players?all=1&position=TE" className={`player-meta ${(positionFilter === 'TE' && allParam === '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>TEs (all)</a>
        <a href="/players?position=QB" className={`player-meta ${(positionFilter === 'QB' && allParam !== '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>QBs (advanced)</a>
        <span className="player-meta">Showing {filteredItems.length} players{positionFilter ? ` (position=${positionFilter})` : ''}</span>
      </div>

      {/* If this page was rendered with the `all=1` query we prefer a virtualized client grid
          to avoid rendering 700+ cards during server-side hydration. The PlayersVirtualGrid
          client component uses react-window and accepts the same minimal player shapes. */}
      { (allParam === '1' || allParam === 'true' || allParam === 'yes') ? (
        // For the full-roster view we intentionally let the client component fetch
        // the roster itself to avoid serializing a large players array into the
        // initial HTML. This reduces hydration risk and improves perceived load.
        <PlayersVirtualGrid />
      ) : (
        <div className="players-grid grid gap-4 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
          {filteredItems.map((p) => (
            <div key={p.espnId}>
              <a href={`/players/${p.espnId}`} aria-label={`Open ${p.name ?? 'player'} details`}>
              <PlayerCard player={{
                id: p.espnId,
                name: p.name ?? p.raw?.player ?? 'Unknown',
                position: p.position,
                espnId: p.espnId,
                team: p.team,
                // priceHistory / history: prefer persistedHistory (from API) then raw
                priceHistory: p.persistedHistory ?? p.raw?.priceHistory ?? [],
                // expose computed stock/confidence/history so PlayerCard can show stock immediately
                stock: (p as any).stock,
                confidence: (p as any).confidence,
                history: (p as any).history ?? (p as any).persistedHistory ?? p.raw?.history,
                // keep raw payload for debugging
                _raw: p.raw ?? p.raw,
              }} />
              </a>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
