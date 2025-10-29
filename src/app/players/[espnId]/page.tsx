import React from 'react';
import path from 'path';
import fs from 'fs/promises';
import PlayerCard from '@/app/components/PlayerCard';
import PlayerStatsChart from '@/components/PlayerStatsChart';
import historyStore from '@/lib/historyStore';

export const revalidate = 10;

async function readJson(p: string) {
  try {
    const raw = await fs.readFile(p, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

export default async function Page({ params }: any) {
  const espnId = String(params?.espnId ?? '');
  const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');
  const idx = await readJson(path.join(DATA_DIR, 'index.json'));
  let fileName = `${espnId}.json`;
  if (idx) {
    if (Array.isArray(idx.players)) {
      const found = idx.players.find((p: any) => String(p.espnId) === String(espnId));
      if (found) fileName = found.file || fileName;
    } else if (idx[espnId]) {
      fileName = idx[espnId];
    }
  }

  const playerJson = await readJson(path.join(DATA_DIR, fileName));
  const histMap = await historyStore.loadMap();
  const persisted = histMap.get(String(espnId)) || null;

  const player = {
    id: espnId,
    espnId: espnId,
    name: playerJson?.player ?? playerJson?.playerName ?? playerJson?.name ?? null,
    position: playerJson?.position ?? null,
    team: playerJson?.team ?? null,
    priceHistory: Array.isArray(persisted) ? persisted : playerJson?.priceHistory ?? [],
    raw: playerJson ?? null,
  };

  return (
    <div style={{ padding: 18 }}>
      <div style={{ display: 'flex', gap: 24 }}>
        <div style={{ flex: '0 0 360px' }}>
          <PlayerCard player={player} />
        </div>
        <div style={{ flex: 1 }}>
          {/* PlayerStatsChart accepts defaultPlayer to preselect the current player */}
          {/* @ts-ignore server->client prop bridging is fine for primitive strings */}
          <PlayerStatsChart defaultPlayer={player.name ?? null} />
        </div>
      </div>
    </div>
  );
}
