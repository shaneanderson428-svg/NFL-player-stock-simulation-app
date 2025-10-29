import React from 'react';
import PlayerCard from '@/app/components/PlayerCard';
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

export default async function Page() {
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

  return (
    <div style={{ padding: 18 }}>
      <div className="players-header">
        <div>
          <h1 style={{ color: '#fff', margin: 0 }}>All advanced players</h1>
          <div className="player-meta">Showing players with advanced metrics from data/advanced (index.lastUpdated: {index?.lastUpdated ?? 'n/a'})</div>
        </div>
      </div>

      <LeaderboardChart minPlays={50} topN={10} />

      <div className="players-grid grid gap-4 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
        {items.map((p) => (
          <div key={p.espnId}>
            <a href={`/players/${p.espnId}`} aria-label={`Open ${p.name ?? 'player'} details`}>
            <PlayerCard player={{ id: p.espnId, name: p.name ?? p.raw?.player ?? 'Unknown', position: p.position, espnId: p.espnId, team: p.team, priceHistory: p.persistedHistory ?? p.raw?.priceHistory ?? [] }} />
            </a>
          </div>
        ))}
      </div>
    </div>
  );
}
