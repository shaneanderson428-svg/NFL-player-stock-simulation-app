import React from 'react';
import PlayerCard from '@/app/components/PlayerCard';
import { renderSparkline as renderSparklineShared } from '@/lib/sparkline';
import { getPlayers, normalizeAndMapPlayersFromEspnRoster } from '@/lib/api';
import historyStore from '@/lib/historyStore';
import path from 'path';
import fs from 'fs/promises';

export const revalidate = 10; // ISR-ish for dev

// Keep a per-file fallback (used only if bulk API fails)
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

export default async function Page(props: any) {
  // Next.js may provide `params` as an async proxy — await it before reading.
  const params = await props.params;
  const teamParam = String(params?.team || 'SEA').toUpperCase();

  // Fetch roster (ESPN or mock)
  const res = await getPlayers(teamParam);
  const players = Array.isArray(res?.response) ? res.response : [];

  // Normalize to Athlete[] shape
  const normalized = normalizeAndMapPlayersFromEspnRoster({ items: players }, teamParam);

  // Load persisted history once
  const histMap = await historyStore.loadMap();

  // Attach advanced metrics and persisted history
  const espnIds = normalized.map((a: any) => String(a.id || a.espnId || ''));

  // Try bulk advanced API first (server-side fetch). If it fails, fall back to per-file reads.
  let advancedMap: Record<string, any> = {};
  try {
    const base = process.env.NEXT_PUBLIC_BASE_URL ?? '';
    const res = await fetch(`${base}/api/advanced/bulk?espnId=${encodeURIComponent(espnIds.join(','))}`, { cache: 'no-store' });
    if (res.ok) {
      const json = await res.json();
      advancedMap = json?.data ?? {};
    }
  } catch (e) {
    // ignore and fall back
    advancedMap = {};
  }

  const enriched = await Promise.all(normalized.map(async (a: any) => {
    const espnId = String(a.id || a.espnId || '');
    let advanced = advancedMap && advancedMap[espnId] ? (advancedMap[espnId]?.metrics ?? advancedMap[espnId]) : null;
    if (!advanced) {
      // fallback to file read (best-effort)
      advanced = await loadAdvancedFor(espnId);
    }
    const persisted = histMap.get(espnId) || histMap.get(String(Number(espnId))) || null;
    return { ...a, espnId, advanced, persistedHistory: Array.isArray(persisted) ? persisted : a.priceHistory };
  }));

  return (
    <div style={{ padding: 18 }}>
      <h1 style={{ color: '#fff' }}>{teamParam} — Players</h1>
  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: 12, marginTop: 12 }}>
        {enriched.map((p: any) => (
          <div key={p.espnId}>
            {/* Server-rendered sparkline so the page shows charts without client libs */}
            <div style={{ marginBottom: 8 }}>
              {renderSparklineShared(p.persistedHistory || p.priceHistory, 320, 48, '#22c55e')}
            </div>
            <PlayerCard player={{ ...p, priceHistory: p.persistedHistory, espnId: p.espnId }} />
          </div>
        ))}
      </div>
    </div>
  );
}

// Uses shared `renderSparkline` from `src/lib/sparkline.tsx` above
