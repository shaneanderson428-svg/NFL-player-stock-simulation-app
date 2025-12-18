"use client";

import React, { useEffect, useState } from 'react';
import PlayerCard from '@/app/components/PlayerCard';
import PlayerStatsChart from '@/components/PlayerStatsChart';
import WeeklyPriceChartWrapper from '@/app/components/WeeklyPriceChartWrapper.client';

type Props = { espnId: string };

export default function PlayerClient({ espnId }: Props) {
  const [playerJson, setPlayerJson] = useState<any | null>(null);
  const [historyJson, setHistoryJson] = useState<any | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);

    (async () => {
      try {
        const [pRes, hRes] = await Promise.all([
          fetch(`/api/advanced/player?espnId=${encodeURIComponent(String(espnId))}`),
          fetch(`/api/player-history?id=${encodeURIComponent(String(espnId))}`),
        ]);

        const pJson = pRes.ok ? await pRes.json() : null;
        const hJson = hRes.ok ? await hRes.json() : null;
        if (!mounted) return;

        setPlayerJson(pJson?.data ?? null);
        setHistoryJson(hJson ?? null);
      } catch (e: any) {
        if (!mounted) return;
        setError(String(e?.message ?? e));
      } finally {
        if (!mounted) return;
        setLoading(false);
      }
    })();

    return () => {
      mounted = false;
    };
  }, [espnId]);

  // Build a unified `player` object expected by PlayerCard
  const player = React.useMemo(() => {
    const raw = playerJson ?? null;
    const hist = historyJson ?? null;

    const name = raw?.player ?? raw?.playerName ?? raw?.name ?? (hist?.playerName ?? null) ?? null;
    const position = raw?.position ?? hist?.position ?? null;
    const team = raw?.team ?? null;

    // weeklyHistory from API is [{week,price}], map to {t,p}
    let priceHistory: any[] = [];
    if (Array.isArray(hist?.weeklyHistory)) {
      priceHistory = hist.weeklyHistory.map((h: any) => ({ t: `W${h.week}`, p: Number(h.price) }));
    }

    return {
      id: espnId,
      espnId,
      name,
      position,
      team,
      priceHistory,
      raw,
    };
  }, [playerJson, historyJson, espnId]);

  if (loading) return <div style={{ padding: 18 }}>Loading player…</div>;
  if (error) return <div style={{ padding: 18 }} className="text-red-500">Error: {error}</div>;

  return (
    <div style={{ padding: 18 }}>
      <div style={{ display: 'flex', gap: 24 }}>
        <div style={{ flex: '0 0 360px' }}>
          <PlayerCard player={player} />
        </div>
        <div style={{ flex: 1 }}>
          <PlayerStatsChart defaultPlayer={player.name ?? null} />
          <WeeklyPriceChartWrapper history={player.priceHistory ?? []} />
        </div>
      </div>
    </div>
  );
}
