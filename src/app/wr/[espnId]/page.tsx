"use client";
import React, { useEffect, useMemo, useState } from 'react';
import dynamic from 'next/dynamic';
import PriceHistoryChart from '@/components/PriceHistoryChart';
import { Sparkline } from '@/lib/sparkline';

const WRChart = dynamic(() => import('@/components/WRChart'), { ssr: false });

export default function PlayerPage({ params }: { params: { espnId: string } }) {
  const { espnId } = params;
  const [row, setRow] = useState<Record<string, any> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<{ week: number; price: number }[] | null>(null);

  useEffect(() => {
    const ac = new AbortController();
    setLoading(true);
    setError(null);

    (async () => {
      try {
        const res = await fetch(`/api/tank01/wr?espnId=${encodeURIComponent(String(espnId))}`, { signal: ac.signal });
        const json = await res.json();
        if (!res.ok) throw new Error(json?.error || 'Failed to load');
        const rows = Array.isArray(json.rows) ? json.rows : [];
        const found = rows.find((rr: any) => String(rr.playerID) === String(espnId) || String(rr.espnID) === String(espnId));
        if (found) setRow(found);
        else setError('Player not found');
      } catch (e: any) {
        if (e?.name === 'AbortError') return;
        setError(String(e?.message ?? e));
      } finally {
        setLoading(false);
      }
    })();

    return () => ac.abort();
  }, [espnId]);

  // Fetch persisted history after row is known
  useEffect(() => {
    if (!row) return;
    const ac = new AbortController();
    (async () => {
      try {
        // Determine numeric player id from common keys
        const pid = row.playerID ?? row.id ?? row.playerId ?? row.uid ?? row.espnID ?? (row.person && row.person.id) ?? null;
        if (!pid) {
          setHistory([]);
          return;
        }
        const res = await fetch(`/api/player-history?id=${encodeURIComponent(String(pid))}`, { signal: ac.signal });
        const json = await res.json();
        if (!res.ok) {
          // log api response for debugging as requested
          // eslint-disable-next-line no-console
          console.error('[player page] player-history API responded with non-OK:', res.status, json);
          setHistory([]);
          return;
        }
        // New API shape returns weeklyHistory: [{week, price}, ...]
        const hist = Array.isArray(json.weeklyHistory) ? json.weeklyHistory : [];
        setHistory(hist);
        // attach to row so other components can use it (sparkline/team pages expect persistedHistory/priceHistory)
        setRow((prev) => {
          if (!prev) return prev;
          const persisted = hist.map((h: any) => ({ t: `W${h.week}`, p: Number(h.price) }));
          // if row name/position are missing, try to fill from API response
          const name = prev.longName ?? prev.player ?? json.playerName ?? prev.longName ?? '';
          const pos = prev.position ?? json.position ?? prev.position ?? '';
          return { ...prev, persistedHistory: persisted, priceHistory: persisted, longName: name, player: name, position: pos };
        });
      } catch (e: any) {
        if (e?.name === 'AbortError') return;
        // eslint-disable-next-line no-console
        console.error('[player page] failed to fetch player-history', e);
        setHistory([]);
      }
    })();
    return () => ac.abort();
  }, [row]);

  const last = useMemo(() => {
    if (!row) return null;
    const hist = Array.isArray(row.priceHistory) ? row.priceHistory : [];
    return hist.length ? hist[hist.length - 1] : null;
  }, [row]);

  if (loading) return <div style={{ padding: 18 }}>Loading…</div>;
  if (error) return <div style={{ padding: 18 }} className="text-red-500">Error: {error}</div>;
  if (!row) return <div style={{ padding: 18 }}>Player not found</div>;

  const name = row.longName ?? row.player ?? '';
  const team = row.team ?? '';
  const headshot = row.espnHeadshot || row.headshot || row.imageUrl || undefined;

  return (
    <div style={{ padding: 18 }}>
      <div className="flex items-center gap-4">
        {headshot ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img src={headshot} alt={String(name)} className="w-20 h-20 rounded-full object-cover" />
        ) : (
          <div className="w-20 h-20 rounded-full bg-gray-700 flex items-center justify-center">{String(name).split(' ').map(s => s[0]).slice(0, 2).join('')}</div>
        )}
        <div>
          <h1 className="text-2xl font-bold">{name}</h1>
          <div className="text-sm text-[#9aa]">{team}</div>
        </div>
      </div>

      <div className="mt-6">
        <WRChart row={row} />
      </div>

      <div className="mt-6">
        <h2 className="text-lg font-semibold">Price history</h2>
        <div className="mt-3">
          {history === null ? (
            <div>Loading history…</div>
          ) : history.length === 0 ? (
            <div className="text-sm text-gray-400">No weekly history available</div>
          ) : (
            <>
              <PriceHistoryChart history={history} />
              <div className="mt-2 w-40">
                {/* sparkline uses the same history data (as t/p pairs) */}
                {/** renderSparkline expects objects with p or price */}
                <Sparkline history={history.map(h => ({ t: `W${h.week}`, p: Number(h.price) }))} width={200} height={40} />
              </div>
            </>
          )}
        </div>
      </div>

      <div className="mt-6">
        <h2 className="text-lg font-semibold">Advanced metrics</h2>
        <div className="grid grid-cols-3 gap-4 mt-3">
          <div className="p-3 rounded bg-[#08111a]">
            <div className="text-sm text-gray-400">YOE</div>
            <div className="font-bold">{last?.yoe ?? row.yoe ?? '—'}</div>
          </div>
          <div className="p-3 rounded bg-[#08111a]">
            <div className="text-sm text-gray-400">ROE</div>
            <div className="font-bold">{last?.roe ?? row.roe ?? '—'}</div>
          </div>
          <div className="p-3 rounded bg-[#08111a]">
            <div className="text-sm text-gray-400">TOE</div>
            <div className="font-bold">{last?.toe ?? row.toe ?? '—'}</div>
          </div>
          <div className="p-3 rounded bg-[#08111a]">
            <div className="text-sm text-gray-400">UER</div>
            <div className="font-bold">{last?.uer ?? row.uer ?? '—'}</div>
          </div>
          <div className="p-3 rounded bg-[#08111a]">
            <div className="text-sm text-gray-400">PIS</div>
            <div className="font-bold">{last?.pis ?? row.pis ?? '—'}</div>
          </div>
          <div className="p-3 rounded bg-[#08111a]">
            <div className="text-sm text-gray-400">ITS</div>
            <div className="font-bold">{last?.its ?? row.its ?? '—'}</div>
          </div>
        </div>
      </div>
    </div>
  );
}
