"use client";
import React, { useEffect, useState } from 'react';
import dynamic from 'next/dynamic';
// Recharts is client-only; use next/dynamic with ssr:false to avoid server evaluation/bundling issues
const LeaderboardChartInner = dynamic(() => import('./LeaderboardChartInner'), { ssr: false, loading: () => <div className="mb-4 text-[#9aa]">Rendering chart…</div> });

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
    (async () => {
      try {
        // helper: fetch with simple retry/backoff
        const fetchWithRetry = async (url: string, attempts = 3, delayMs = 400) => {
          let lastErr: any = null;
          for (let i = 0; i < attempts; i++) {
            try {
              const rr = await fetch(url);
              if (!rr.ok) {
                const txt = await rr.text();
                throw new Error(`Status ${rr.status}: ${txt.substring(0,200)}`);
              }
              return rr;
            } catch (e) {
              lastErr = e;
              // exponential backoff
              await new Promise((res) => setTimeout(res, delayMs * Math.pow(2, i)));
            }
          }
          throw lastErr;
        };

        const r = await fetchWithRetry('/api/nfl/leaderboard', 3, 300);
        // If server returned a non-2xx status, attempt to read text for a helpful
        // error message instead of blindly calling `r.json()` which can throw
        // when the server returns HTML (dev overlay) or an error page.
        if (!r.ok) {
          const txt = await r.text();
          const msg = `Server error ${r.status}: ${txt.substring(0, 200)}`;
          // Log full details for debugging, but don't throw - set UI error
          console.error('Leaderboard fetch non-OK response', { status: r.status, body: txt });
          if (!mounted) return;
          setError(msg);
          setLoading(false);
          return;
        }
        // Defensive: check content-type before parsing as JSON
        const ct = r.headers.get('content-type') || '';
        let json: any = null;
        if (ct.includes('application/json')) {
          try {
            json = await r.json();
          } catch (e) {
            const txt = await r.text();
            const msg = `Invalid JSON response: ${String((e as any)?.message || e)}. Body: ${txt.substring(0, 200)}`;
            console.error('Leaderboard invalid JSON', { err: e, body: txt });
            if (!mounted) return;
            setError(msg);
            setLoading(false);
            return;
          }
        } else {
          // not JSON (likely HTML when dev overlay is shown) — we'll attempt a CSV/API fallback
          const txt = await r.text();
          console.error('Leaderboard non-JSON response', { contentType: ct, body: txt.substring(0, 200) });
          // attempt a fallback read from the stocks API which reads local CSVs on the server
          try {
            const f2 = await fetchWithRetry('/api/nfl/stocks?all=1', 2, 300);
            const ct2 = f2.headers.get('content-type') || '';
            if (ct2.includes('application/json')) {
              const j2 = await f2.json();
              if (j2 && Array.isArray(j2.players)) {
                // map players -> rows compatible with leaderboard inner component
                const rowsFallback = j2.players.map((p: any) => ({
                  passer_player_name: p.name || p.player || p.player_name || p.id,
                  avg_epa: Number(p.avg_epa || p.avg_epa || 0),
                  avg_cpoe: Number(p.avg_cpoe || 0),
                  plays: Number(p.plays || p.plays || 0),
                }));
                if (!mounted) return;
                setData(rowsFallback);
                setLoading(false);
                return;
              }
            }
          } catch (fallbackErr) {
            // swallow and fall through to set error below
            console.error('Leaderboard fallback failed', fallbackErr);
          }

          const msg = `Expected JSON but got ${ct || 'non-JSON'} response. Body start: ${txt.substring(0,200)}`;
          if (!mounted) return;
          setError(msg);
          setLoading(false);
          return;
        }

        if (!mounted) return;
        if (!json.ok) {
          const msg = json.error || 'API error';
          console.error('Leaderboard API returned ok=false', json);
          if (!mounted) return;
          setError(msg);
          setLoading(false);
          return;
        }
        const rows: Row[] = json.rows || [];
        const filtered = rows
          .filter((r) => (typeof r.plays === 'number' ? r.plays >= minPlays : Number(r.plays || 0) >= minPlays))
          .sort((a, b) => (b.avg_epa || 0) - (a.avg_epa || 0))
          .slice(0, topN)
          .map((r) => {
            // Try several fallbacks for the player name because CSVs sometimes
            // have different column names (or an empty first column header).
            const fallbackName =
              r.passer_player_name ??
              r.passer ??
              r.player ??
              // csv-parse may produce an empty-string header for the first column
              // so try that too
              (r[''] as any) ??
              (r['player_name'] as any) ??
              (r['name'] as any) ??
              'Unknown';

            return {
              passer_player_name: String(fallbackName),
              avg_epa: typeof r.avg_epa === 'number' ? r.avg_epa : Number(r.avg_epa || 0),
              avg_cpoe: typeof r.avg_cpoe === 'number' ? r.avg_cpoe : Number(r.avg_cpoe || 0),
              plays: typeof r.plays === 'number' ? r.plays : Number(r.plays || 0),
            };
          });
        setData(filtered);
        setLoading(false);
      } catch (err: any) {
        if (!mounted) return;
        // Provide a concise error message for the UI while logging the full
        // error to the console for debugging.
        console.error('Leaderboard fetch error', err);
        setError(String(err?.message || err));
        setLoading(false);
      }
    })();
    return () => {
      mounted = false;
    };
  }, [minPlays, topN]);

  if (loading) return <div className="mb-4 text-[#9aa]">Loading leaderboard chart…</div>;
  if (error) return <div className="mb-4 text-red-400">Error loading chart: {error}</div>;
  if (!data || data.length === 0) return <div className="mb-4 text-[#9aa]">No data for chart (increase timeframe or lower minPlays)</div>;

  return <LeaderboardChartInner data={data} />;
}
