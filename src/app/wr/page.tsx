"use client";
import React, { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import PlayerCard from '@/app/components/PlayerCard';
import StockChartSmall from '@/app/components/StockChartSmall';

// Force dynamic so client polling + router.refresh works reliably
export const dynamic = 'force-dynamic';

// Client-only WR list page: fetches /api/tank01/wr and renders a searchable list

type Row = Record<string, any>;

export default function Page() {
  const router = useRouter();
  const [rows, setRows] = useState<Row[]>([]);
  const [q, setQ] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // initial load
  useEffect(() => {
    const ac = new AbortController();
    setError(null);
    (async () => {
      try {
        const res = await fetch('/api/tank01/wr', { signal: ac.signal });
        const json = await res.json();
        if (!res.ok) throw new Error(json?.error || 'Failed to load WR data');
        if (json?.ok && Array.isArray(json.rows)) {
          setRows(json.rows as Row[]);
        } else if (Array.isArray(json)) {
          // some routes return a raw array
          setRows(json as Row[]);
        } else if (Array.isArray(json?.rows)) {
          setRows(json.rows as Row[]);
        } else {
          setError('Failed to load WR data');
        }
      } catch (e: any) {
        if (e?.name === 'AbortError') return;
        setError(String(e?.message ?? e));
      } finally {
        setLoading(false);
      }
    })();
    return () => ac.abort();
  }, []);

  // poll the live ingestion endpoint every 10 minutes and refresh the route
  useEffect(() => {
    const id = setInterval(() => {
      fetch('/api/live/wr')
        .then(() => {
          try {
            router.refresh();
          } catch (e) {
            // ignore
          }
        })
        .catch(() => {
          // swallow errors — the next tank01 fetch will recover
        });
    }, 600000);
    return () => clearInterval(id);
  }, [router]);

  const filtered = useMemo(() => {
    const ql = q.trim().toLowerCase();
    if (!ql) return rows;
    return rows.filter((r) => {
      const name = String(r.longName ?? r.player ?? r.name ?? '').toLowerCase();
      const team = String(r.team ?? '').toLowerCase();
      return name.includes(ql) || team.includes(ql);
    });
  }, [rows, q]);

  const computeDelta = (hist: any[]) => {
    if (!hist || hist.length === 0) return null;
    try {
      const withTs = hist.map((h: any) => ({ ...(h || {}), tms: h?.t ? Date.parse(String(h.t)) : NaN, p: Number(h?.p ?? h?.price ?? h?.value ?? 0) }));
      const hasTs = withTs.some((w: any) => Number.isFinite(w.tms));
      let newest = null as any;
      let prev = null as any;
      if (hasTs) {
        withTs.sort((a: any, b: any) => (Number(a.tms) || 0) - (Number(b.tms) || 0));
        newest = withTs[withTs.length - 1];
        prev = withTs[withTs.length - 2] ?? null;
      } else {
        newest = withTs[withTs.length - 1];
        prev = withTs[withTs.length - 2] ?? null;
      }
      if (!newest || !prev) return null;
      const latest = Number(newest.p ?? 0);
      const prior = Number(prev.p ?? 0);
      if (!Number.isFinite(latest) || !Number.isFinite(prior) || prior === 0) return null;
      const pct = ((latest - prior) / Math.abs(prior)) * 100;
      return { pct: +pct.toFixed(2), latest, prior };
    } catch (e) {
      return null;
    }
  };

  return (
    <div style={{ padding: 18 }}>
      <h1 className="text-2xl font-bold mb-4">Wide Receivers — latest stat-containing game</h1>
      <div className="mb-4">
        <input className="input" placeholder="Search name or team" value={q} onChange={(e) => setQ(e.target.value)} />
      </div>
      {loading ? (
        <div>Loading WR data…</div>
      ) : error ? (
        <div className="text-red-500">Error: {error}</div>
      ) : (
        <div className="grid gap-3">
          {filtered.map((r) => {
            const espnId = String(r.playerID ?? r.espnID ?? r.espnid ?? '');
            const name = r.longName ?? r.player ?? '';
            const team = r.team ?? '';
            // prefer headshot fields from flattened CSV
            const headshot = r.espnHeadshot || r.headshot || r.imageUrl || undefined;
            // stat fields: try several known flattened keys
            const receptions = r['Receiving.receptions'] ?? r['receiving.receptions'] ?? r.receptions ?? r.rec ?? r.Rec ?? '';
            const yards = r['Receiving.recYds'] ?? r['receiving.recYds'] ?? r.yards ?? r.yds ?? '';
            const tds = r['Receiving.recTD'] ?? r['receiving.recTD'] ?? r.td ?? r.touchdown ?? '';

            const history = Array.isArray(r.priceHistory) ? r.priceHistory : [];
            const apiPct = r.pricePercentChange ?? (r.pricePercentChange === 0 ? 0 : null);
            const delta = apiPct != null ? { pct: Number(apiPct), latest: null, prior: null } : computeDelta(history);

            return (
              <div key={espnId} className="flex items-center gap-3 p-3 rounded-md bg-[#0b1220]">
                <div style={{ flex: '0 0 80px' }}>
                  {headshot ? (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img src={headshot} alt={String(name)} className="w-16 h-16 rounded-full object-cover" />
                  ) : (
                    <div className="w-16 h-16 rounded-full bg-gray-700 flex items-center justify-center">{String(name).split(' ').map(s=>s[0]).slice(0,2).join('')}</div>
                  )}
                </div>

                <div className="text-sm text-[#9aa]">{team}</div>
                <div style={{ width: 200, textAlign: 'right' }}>
                  <div>Yds: <strong>{yards}</strong></div>
                  <div>Rec: <strong>{receptions}</strong></div>
                  <div>TD: <strong>{tds}</strong></div>
                </div>
                <div style={{ width: 160, textAlign: 'right' }}>
                  {/* Advanced metrics badges: PIS, ITS, UER */}
                  {(() => {
                    const last = Array.isArray(history) && history.length ? history[history.length - 1] : null;
                    const pis = last?.pis ?? r.pis ?? null;
                    const its = last?.its ?? r.its ?? null;
                    const uer = last?.uer ?? r.uer ?? null;
                    const badgeClass = (v: number | null) => {
                      if (v == null) return 'text-gray-400 bg-gray-800';
                      if (v > 0) return 'text-green-400 bg-opacity-10 bg-green-900';
                      if (v < 0) return 'text-red-400 bg-opacity-10 bg-red-900';
                      return 'text-gray-300 bg-gray-800';
                    };
                    return (
                      <div className="flex flex-col items-end gap-1 mb-2">
                        <div className={`px-2 py-1 rounded-md text-xs ${badgeClass(pis)}`}><strong>PIS:</strong> {pis != null ? String(pis) : '—'}</div>
                        <div className={`px-2 py-1 rounded-md text-xs ${badgeClass(its)}`}><strong>ITS:</strong> {its != null ? String(its) : '—'}</div>
                        <div className={`px-2 py-1 rounded-md text-xs ${badgeClass(uer)}`}><strong>UER:</strong> {uer != null ? String(uer) : '—'}</div>
                      </div>
                    );
                  })()}

                </div>
                <div style={{ width: 120, textAlign: 'right' }}>
                  {history && history.length > 0 ? (
                    <div className="flex items-center justify-end gap-2">
                      <div style={{ width: 80 }}>
                        <StockChartSmall data={history} width={80} height={36} />
                      </div>
                      <div style={{ width: 40, textAlign: 'right' }}>
                        {delta ? (
                          <div className={`text-sm ${delta.pct > 0 ? 'text-green-400' : delta.pct < 0 ? 'text-red-400' : 'text-gray-300'}`}>
                            {delta.pct > 0 ? '+' : ''}{delta.pct}%
                          </div>
                        ) : (
                          <div className="text-sm text-gray-400">—</div>
                        )}
                      </div>
                    </div>
                  ) : null}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
