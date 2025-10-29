import React from 'react';
import PlayerCard from '@/app/components/PlayerCard';
import { getPlayers, normalizeAndMapPlayersFromEspnRoster } from '@/lib/api';
import historyStore from '@/lib/historyStore';
import path from 'path';
import fs from 'fs/promises';

export const revalidate = 10; // ISR-ish for dev

async function loadAdvancedFor(espnId: string) {
  const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');
  try {
    const raw = await fs.readFile(path.join(DATA_DIR, `${espnId}.json`), 'utf8');
    const parsed = JSON.parse(raw);
    return parsed?.metrics ?? parsed ?? null;
  } catch (e) {
    return null;
  }
}

export default async function Page() {
  // Fetch roster (ESPN or mock)
  const res = await getPlayers('SEA');
  const players = Array.isArray(res?.response) ? res.response : [];

  // Normalize to Athlete[] shape
  const normalized = normalizeAndMapPlayersFromEspnRoster({ items: players }, 'SEA');

  // Load persisted history once
  const histMap = await historyStore.loadMap();

  // Attach advanced metrics and persisted history
  const enriched = await Promise.all(normalized.map(async (a: any) => {
    const espnId = String(a.id || a.espnId || '');
    const advanced = await loadAdvancedFor(espnId);
    const persisted = histMap.get(espnId) || histMap.get(String(Number(espnId))) || null;
    return { ...a, espnId, advanced, persistedHistory: Array.isArray(persisted) ? persisted : a.priceHistory };
  }));

  return (
    <div style={{ padding: 18 }}>
      <h1 style={{ color: '#fff' }}>Seattle Seahawks — Players</h1>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(360px, 1fr))', gap: 12, marginTop: 12 }}>
        {enriched.map((p: any) => (
          // PlayerCard is a client component; it expects a Player/Athlete shape
          // pass through persistedHistory as priceHistory so the chart shows seeded series
          <div key={p.espnId}>
            {/* eslint-disable-next-line @next/next/no-async-static */}
            <PlayerCard player={{ ...p, priceHistory: p.persistedHistory, espnId: p.espnId }} />
          </div>
        ))}
      </div>
    </div>
  );
}
