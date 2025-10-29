"use client";
import React, { useEffect, useState, Suspense } from 'react';
// Recharts is client-only; lazy-load the inner component to avoid server evaluation errors
const LeaderboardChartInner = React.lazy(() => import('./LeaderboardChartInner'));

type Row = {
  passer_player_name?: string;
  avg_epa?: number;
  avg_cpoe?: number;
  plays?: number;
  [k: string]: any;
};

export default function LeaderboardChart({ minPlays = 50, topN = 10 }: { minPlays?: number; topN?: number }) {
  const [data, setData] = useState<Row[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    fetch('/api/nfl/leaderboard')
      .then((r) => r.json())
      .then((json) => {
        if (!mounted) return;
        if (!json.ok) throw new Error(json.error || 'API error');
        const rows: Row[] = json.rows || [];
        const filtered = rows
          .filter((r) => (typeof r.plays === 'number' ? r.plays >= minPlays : Number(r.plays || 0) >= minPlays))
          .sort((a, b) => (b.avg_epa || 0) - (a.avg_epa || 0))
          .slice(0, topN)
          .map((r) => ({
            passer_player_name: r.passer_player_name ?? r.passer ?? r.player ?? 'Unknown',
            avg_epa: typeof r.avg_epa === 'number' ? r.avg_epa : Number(r.avg_epa || 0),
            avg_cpoe: typeof r.avg_cpoe === 'number' ? r.avg_cpoe : Number(r.avg_cpoe || 0),
            plays: typeof r.plays === 'number' ? r.plays : Number(r.plays || 0),
          }));
        setData(filtered);
        setLoading(false);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(String(err?.message || err));
        setLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, [minPlays, topN]);

  if (loading) return <div className="mb-4 text-[#9aa]">Loading leaderboard chart…</div>;
  if (error) return <div className="mb-4 text-red-400">Error loading chart: {error}</div>;
  if (!data || data.length === 0) return <div className="mb-4 text-[#9aa]">No data for chart (increase timeframe or lower minPlays)</div>;

  return (
    <Suspense fallback={<div className="mb-4 text-[#9aa]">Rendering chart…</div>}>
      <LeaderboardChartInner data={data} />
    </Suspense>
  );
}
