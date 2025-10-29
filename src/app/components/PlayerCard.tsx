"use client";
import React, { useEffect, useState, useRef, useMemo } from 'react';
import useSWR from 'swr';
import PriceChart from './PriceChart';
import dynamic from 'next/dynamic';
const StockChartSmall = dynamic(() => import('./StockChartSmall'), { ssr: false });
const fetcher = (url: string) => fetch(url).then((r) => r.json());
import type { Athlete } from '@/lib/types';

type Props = {
  player: Athlete | any;
};

export default function PlayerCard({ player }: Props) {
  const name = (player?.name ?? player?.fullName ?? 'Unknown') as string;
  // team can be a string or { abbreviation, name }
  const teamRaw = player?.team;
  const team = typeof teamRaw === 'string' ? teamRaw : (teamRaw?.abbreviation ?? teamRaw?.name) as string | undefined;
  const position = (player?.position ?? player?.positionName) as string | undefined;
  const imageUrlFromPlayer = (player?.imageUrl || player?.headshot || player?.photo) as string | undefined;
  const espnId = String(player?.espnId || player?.id || player?.uid || '');
  // prefer local avatar under /public/avatars/{espnId}.png or .jpg, fallback to provided imageUrl
  const localPng = espnId ? `/avatars/${espnId}.png` : undefined;
  const localJpg = espnId ? `/avatars/${espnId}.jpg` : undefined;
  const imageUrl = imageUrlFromPlayer || localPng || localJpg;
  const [resolvedImage, setResolvedImage] = useState<string | undefined>(imageUrl);

  const [loadingPrice, setLoadingPrice] = useState(false);
  const [priceInfo, setPriceInfo] = useState<any | null>(null);
  const [priceError, setPriceError] = useState<string | null>(null);
  const [showDebug, setShowDebug] = useState(false);
  const [showChart, setShowChart] = useState(false);
  const [compactChart, setCompactChart] = useState(false);
  const [range, setRange] = useState<'spark'|'1d'|'7d'|'30d'>('spark');
  const [history, setHistory] = useState<any[] | null>(() => Array.isArray(player?.priceHistory) ? player.priceHistory : null);
  const acRef = useRef<AbortController | null>(null);

  const fetchPrice = async () => {
    try {
      acRef.current?.abort();
      const ac = new AbortController();
      acRef.current = ac;
      setLoadingPrice(true);
      setPriceError(null);
      // Try to use espn id if present, otherwise search by name
      const espnId = player?.espnId || player?.id || player?.uid || '';
      const q = espnId ? `espnId=${encodeURIComponent(espnId)}` : `name=${encodeURIComponent(name)}`;
      const res = await fetch(`/api/espn/price?${q}&currentPrice=${encodeURIComponent(player?.currentPrice ?? 100)}`, { signal: ac.signal });
      const json = await res.json();
      if (!res.ok) throw new Error(json?.error || 'price fetch failed');
      setPriceInfo(json);
      // if we got a newPrice, append to local history for charting
      if (json && typeof json.newPrice === 'number') {
        const nowIso = new Date().toISOString().slice(0, 10);
        setHistory((prev) => {
          const base = Array.isArray(prev) ? [...prev] : [];
          // avoid duplicate date entries by checking last point
          const last = base[base.length - 1];
          if (last && last.t === nowIso) {
            // replace last
            base[base.length - 1] = { t: nowIso, p: +(json.newPrice).toFixed(2) };
          } else {
            base.push({ t: nowIso, p: +(json.newPrice).toFixed(2) });
            // keep a rolling window of 30 points
            if (base.length > 30) base.shift();
          }
          return base;
        });
      }
    } catch (err: any) {
      if (err?.name === 'AbortError') return;
      setPriceError(String(err?.message ?? err));
      setPriceInfo(null);
    } finally {
      setLoadingPrice(false);
    }
  };

  const formatCurrency = (v: number | string | undefined) => {
    if (typeof v === 'number') return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(v);
    return v ?? '—';
  };

  useEffect(() => {
    // If a local history was provided, avoid an immediate price API call on mount —
    // the server-rendered sparkline and client chart already show data.
    if (!Array.isArray(history) || history.length === 0) {
      fetchPrice();
    }
    return () => acRef.current?.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [player?.id]);

  // Fetch stock summary (cached via SWR). This will be shared among all PlayerCard instances.
  const { data: stocksData } = useSWR('/api/nfl/stocks', fetcher, { refreshInterval: 15000 });

  const stockRow = useMemo(() => {
    try {
      const rows = stocksData?.rows || [];
      if (!Array.isArray(rows) || !rows.length) return null;
      const nameNorm = (name || '').toLowerCase();
      const espnIdStr = String(espnId || '').toLowerCase();
      // Try matching by espnId first (if API ever includes espnId in player field), otherwise fuzzy name match
      for (const r of rows) {
        if (!r) continue;
        // prefer explicit espnId match
        const rEspn = String(r.espnId ?? r.espn_id ?? r.espnid ?? '').toLowerCase();
        if (rEspn && espnIdStr && rEspn === espnIdStr) return r;
      }
      // then try exact player name match
      for (const r of rows) {
        if (!r) continue;
        const rp = String(r.player || '').toLowerCase();
        if (rp === nameNorm) return r;
      }
      // fallback: match by last name substring
      const last = (name || '').split(/\s+/).slice(-1)[0]?.toLowerCase() || '';
      const found = rows.find((r: any) => String(r.player || '').toLowerCase().includes(last));
      return found || null;
    } catch (e) {
      return null;
    }
  }, [stocksData, name, espnId]);

  // Try to fetch a cached headshot from our server-side fetcher
  useEffect(() => {
    let mounted = true;
    if (!espnId) return;
    // If a provided imageUrl exists, prefer it and skip our fetch
    if (imageUrlFromPlayer) return;
    (async () => {
      try {
        const res = await fetch(`/api/avatars/fetch?espnId=${encodeURIComponent(espnId)}`);
        if (!mounted) return;
        if (!res.ok) return;
        const json = await res.json();
        if (json && json.ok && json.url) {
          setResolvedImage(String(json.url));
        }
      } catch (e) {
        // ignore fetch failures
      }
    })();
    return () => { mounted = false; };
     
  }, [espnId, imageUrlFromPlayer]);

  // If no history passed in, try to load persisted history from server
  useEffect(() => {
    if (Array.isArray(history) && history.length > 0) return;
    const espnId = player?.espnId || player?.id || player?.uid || '';
    if (!espnId) return;
    let mounted = true;
    (async () => {
      try {
        const res = await fetch(`/api/espn/history?espnId=${encodeURIComponent(espnId)}`);
        if (!res.ok) return;
        const json = await res.json();
        if (!mounted) return;
        if (json && Array.isArray(json.history)) setHistory(json.history as any[]);
      } catch (e) {
        // ignore
      }
    })();
    return () => { mounted = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [player?.id]);

  // Polling: refresh price every 30 seconds while the document is visible.
  useEffect(() => {
    const espnId = player?.espnId || player?.id || player?.uid || '';
    if (!espnId) return;
    let mounted = true;
    const intervalMs = 30_000; // 30 seconds

    const shouldPoll = () => typeof document !== 'undefined' ? document.visibilityState === 'visible' : true;

    const tick = async () => {
      if (!mounted) return;
      if (!shouldPoll()) return;
      try {
        // reuse fetchPrice logic but avoid aborting user's manual requests
        const ac = new AbortController();
        acRef.current = ac;
        const q = `espnId=${encodeURIComponent(espnId)}`;
        const res = await fetch(`/api/espn/price?${q}&currentPrice=${encodeURIComponent(player?.currentPrice ?? 100)}`, { signal: ac.signal });
        const json = await res.json();
        if (res.ok) {
          setPriceInfo(json);
          if (json && typeof json.newPrice === 'number') {
            const nowIso = new Date().toISOString().slice(0, 10);
            setHistory((prev) => {
              const base = Array.isArray(prev) ? [...prev] : [];
              const last = base[base.length - 1];
              if (last && last.t === nowIso) {
                base[base.length - 1] = { t: nowIso, p: +(json.newPrice).toFixed(2) };
              } else {
                base.push({ t: nowIso, p: +(json.newPrice).toFixed(2) });
                if (base.length > 30) base.shift();
              }
              return base;
            });
          }
        }
      } catch (err: any) {
        if (err?.name === 'AbortError') return;
        // don't escalate polling errors; keep silent
      }
    };

    const id = setInterval(tick, intervalMs);
    // Run an initial tick after interval to stagger with other requests
    const initialTimer = setTimeout(tick, Math.min(5000, intervalMs));

    const onVisibility = () => {
      if (shouldPoll()) tick();
    };
    if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onVisibility);

    return () => {
      mounted = false;
      clearInterval(id);
      clearTimeout(initialTimer);
      if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', onVisibility);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [player?.id]);

  return (
    <div role="article" aria-label={name} className="player-card-clean bg-[#0f111b] rounded-xl p-5 shadow-sm transform transition-all duration-200 ease-in-out hover:shadow-lg hover:scale-[1.02] hover:-translate-y-0.5" tabIndex={0}>
      <div className="flex gap-3 items-center">
  <div className="player-avatar mr-2" aria-hidden>
          {resolvedImage ? (
              <img
              src={resolvedImage}
              alt={name}
              loading="lazy"
              onError={(e) => {
                const img = e.target as HTMLImageElement;
                // If the src failed, try swapping extension from .png -> .jpg -> placeholder
                const src = img.src || '';
                if (src.endsWith('.png')) {
                  img.src = src.replace(/\.png$/, '.jpg');
                  return;
                }
                if (src.endsWith('.jpg')) {
                  img.src = '/avatars/placeholder.svg';
                  img.alt = `${name} avatar`;
                  return;
                }
                if (!src.endsWith('/avatars/placeholder.svg')) {
                  img.src = '/avatars/placeholder.svg';
                  img.alt = `${name} avatar`;
                } else {
                  img.style.display = 'none';
                }
              }}
              className="w-12 h-12 rounded-full object-cover border border-[rgba(255,255,255,0.03)]"
            />
          ) : (
            <div className="player-initials">{name ? name.split(' ').map(s=>s[0]).slice(0,2).join('') : '—'}</div>
          )}
        </div>

          <div className="flex-1 min-w-0">
          <div className="font-bold text-[15px] leading-[1.05] overflow-hidden text-ellipsis line-clamp-2 text-white">{name}</div>
          <div className="player-meta text-[12px] text-[#9aa]">{
            (() => {
              const posText = position ?? '—';
              // provenance: explicit profile value (from cleaned profiles) vs inferred
              const profilePos = (player?.position_profile || player?._raw?.position_profile) as string | undefined;
              const inferredPos = (player?.position_inferred || player?._raw?.position_inferred) as string | undefined;
              const overwritten = Boolean(player?.position_overwritten_from_profile || player?._raw?.position_overwritten_from_profile);

              // Display the canonical position, and show profile separately when available.
              // If a profile overwrite flag is present, show a clearer provenance badge.
              return (
                <>
                  <span>{posText}</span>
                  {profilePos && String(profilePos).trim() !== '' && String(profilePos).toUpperCase() !== String(posText).toUpperCase() ? (
                    <span style={{ marginLeft: 8, fontSize: 11, color: '#cbd5e1' }}>{String(profilePos).toUpperCase()}</span>
                  ) : null}

                  {/* provenance badge: prefer explicit overwritten flag */}
                  {overwritten ? (
                    <span style={{ marginLeft: 8, fontSize: 11, color: '#9ee6c2', background: 'rgba(0,0,0,0.25)', padding: '2px 6px', borderRadius: 6, textTransform: 'uppercase' }}>📊 profile</span>
                  ) : profilePos && String(profilePos).trim() !== '' ? (
                    <span style={{ marginLeft: 8, fontSize: 11, color: '#cbd5e1', background: 'rgba(255,255,255,0.03)', padding: '2px 6px', borderRadius: 6, textTransform: 'uppercase' }}>profile</span>
                  ) : inferredPos ? (
                    <span style={{ marginLeft: 8, fontSize: 11, color: '#cbd5e1', background: 'rgba(255,255,255,0.03)', padding: '2px 6px', borderRadius: 6, textTransform: 'uppercase' }}>⚙️ inferred</span>
                  ) : null}

                  <span style={{ marginLeft: 8 }}>•</span>
                  <span style={{ marginLeft: 6 }}>{team ?? '—'}</span>
                </>
              );
            })()
          }</div>
          {espnId ? (
            <div className="text-[11px] text-[#6b7280] mt-0.5">ID: <span className="font-mono text-[11px]">{espnId}</span></div>
          ) : null}
        </div>

  {/* Price box area */}
  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 6 }}>
          <div className="min-w-[64px] text-right">
            {loadingPrice ? (
              <div className="flex justify-end">
                <div className="w-20 h-4 rounded-md skeleton-gradient" aria-hidden />
              </div>
            ) : priceError ? (
              <div className="text-[#fa8072] text-[12px]">err</div>
            ) : priceInfo && priceInfo.found ? (
              <div className="flex gap-2 items-center justify-end">
                <div className="relative inline-block">
                  <div className="text-[12px] text-[#9aa] text-right">
                    <div className="text-[10px] text-[#9aa] mb-1">Price</div>
                    <div className="font-bold text-[13px] bg-[#061424] px-2 py-1 rounded-full inline-block">
                      {formatCurrency(typeof priceInfo.newPrice === 'number' ? priceInfo.newPrice : (typeof player?.currentPrice === 'number' ? player.currentPrice : (priceInfo.newPrice ?? player?.currentPrice)))}
                    </div>
                  </div>
                    {/* Dev-only compact debug badge near price */}
                    {typeof window !== 'undefined' && process.env.NODE_ENV !== 'production' && priceInfo?._debug ? (
                      <div className="absolute left-0 -bottom-6 bg-[#071827] text-[#9ee6c2] px-2 py-0.5 rounded-md text-[11px]">{priceInfo._debug.source ?? 'api'}{priceInfo._debug.cacheHit ? ' • cache' : ''}</div>
                    ) : null}
                  {/* Tooltip (dev-only) */}
                      {typeof window !== 'undefined' && priceInfo?._debug ? (
                    <>
                      {/* Performance badge */}
                      {priceInfo._debug.performance && priceInfo._debug.performance.score ? (
                        <div style={{ position: 'absolute', left: -8, top: -18, background: '#072', color: '#bfffcf', padding: '2px 6px', borderRadius: 6, fontSize: 11, fontWeight: 700 }} title="Performance score">{Number(priceInfo._debug.performance.score).toFixed(2)}</div>
                      ) : null}
                      <button className="pc-debug-target" onClick={() => setShowDebug((s) => !s)} title="Toggle debug details">Details</button>
                      <div className={`absolute right-0 top-full mt-1.5 ${showDebug ? 'block' : 'hidden'} bg-[#0b1220] text-[#ccc] p-2 rounded-md shadow-[0_6px_18px_rgba(0,0,0,0.6)] w-[320px] text-[12px] pc-debug-panel`}>
                          <div className="flex justify-between items-center mb-1.5">
                            <div className="text-[12px] text-[#ddd]">Price debug</div>
                            <button onClick={() => { navigator.clipboard?.writeText(JSON.stringify(priceInfo._debug, null, 2)); }} className="bg-transparent text-[#9aa] cursor-pointer text-[12px]" title="Copy debug JSON">Copy</button>
                          </div>
                          <pre className="max-h-[200px] overflow-auto m-0 whitespace-pre-wrap">{JSON.stringify(priceInfo._debug, null, 2)}</pre>
                        </div>
                    </>
                  ) : null}
                </div>
                {/* Prefer showing computed stock & confidence from /api/nfl/stocks when available */}
                {stockRow ? (
                  <>
                    <div className="text-[11px] text-[#9aa]">Stock</div>
                    {typeof stockRow.stock === 'number' ? (
                      <div className={`font-bold ${stockRow.stock - 100 > 0 ? 'text-[#23c55e]' : stockRow.stock - 100 < 0 ? 'text-[#fb7185]' : 'text-[#9aa]'}`}>
                        {` ${(stockRow.stock - 100) >= 0 ? '+' : ''}${(stockRow.stock - 100).toFixed(1)}%`}
                      </div>
                    ) : (
                      <div className="font-bold text-[#9aa]">—</div>
                    )}
                    <div className="text-[11px] text-[#7aa]">{typeof stockRow.confidence === 'number' ? `C: ${(stockRow.confidence * 100).toFixed(0)}%` : ''}</div>
                  </>
                ) : (
                  <>
                    <div className="text-[11px] text-[#9aa]">Stock</div>
                    <div className={`font-bold ${ (priceInfo.appliedPct ?? priceInfo.delta) > 0 ? 'text-[#23c55e]' : (priceInfo.appliedPct ?? priceInfo.delta) < 0 ? 'text-[#fb7185]' : 'text-[#9aa]'}`}>
                      {typeof priceInfo.appliedPct === 'number'
                        ? `${(priceInfo.appliedPct * 100).toFixed(1)}%`
                        : priceInfo.delta >= 0
                        ? `+${priceInfo.delta}`
                        : priceInfo.delta}
                    </div>
                  </>
                )}
                <style>{`
                  .pc-debug-tooltip {
                    pointer-events: none;
                    opacity: 0;
                    transform: translateY(4px);
                    transition: opacity 160ms ease, transform 160ms ease;
                  }
                  .pc-debug-tooltip.visible {
                    display: block;
                    opacity: 1;
                    transform: translateY(0);
                  }
                  .pc-debug-target:hover + .pc-debug-tooltip,
                  .pc-debug-target:focus + .pc-debug-tooltip {
                    display: block;
                    opacity: 1;
                    transform: translateY(0);
                  }
                `}</style>
              </div>
              ) : (
                <div className="player-meta">—</div>
              )}
        </div>
      </div>

      <div style={{ marginTop: 8, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <button onClick={() => fetchPrice()} disabled={loadingPrice} className="bg-transparent text-[#9aa] border-none cursor-pointer text-[12px]" title="Refresh price">⟳</button>
        <button onClick={() => setShowChart(s => !s)} className="bg-transparent text-[#9aa] border-none cursor-pointer text-[12px]" title="Toggle price chart">{showChart ? 'Hide' : 'Chart'}</button>
        <button onClick={() => setCompactChart(s => !s)} className="bg-transparent text-[#9aa] border-none cursor-pointer text-[12px]" title="Toggle compact chart">{compactChart ? 'Full' : 'Spark'}</button>
      </div>
    </div>
        {/* Small client-side sparkline when chart is hidden */}
        {!showChart && history && Array.isArray(history) ? (
          <div className="mt-2">
            <div aria-hidden>
              {
                (() => {
                  // Prefer using stockRow to determine sparkline color; fall back to history or priceInfo
                  let color = '#9aa';
                  try {
                    // Prefer weekly_change (percent) when available on the stockRow
                    if (stockRow && stockRow.weekly_change !== undefined && stockRow.weekly_change !== null) {
                      const wc = Number(stockRow.weekly_change) || 0;
                      color = wc > 0 ? '#22c55e' : wc < 0 ? '#fb7185' : '#9aa';
                    } else if (stockRow && typeof stockRow.stock === 'number') {
                      const delta = stockRow.stock - 100;
                      color = delta > 0 ? '#22c55e' : delta < 0 ? '#fb7185' : '#9aa';
                    } else if (Array.isArray(history) && history.length >= 2) {
                      const first = Number(history[0]?.p ?? history[0]?.price ?? 0);
                      const last = Number(history[history.length - 1]?.p ?? history[history.length - 1]?.price ?? 0);
                      if (!Number.isNaN(first) && !Number.isNaN(last)) {
                        color = last > first ? '#22c55e' : last < first ? '#fb7185' : '#9aa';
                      }
                    } else if (priceInfo && typeof priceInfo.appliedPct === 'number') {
                      color = priceInfo.appliedPct > 0 ? '#22c55e' : priceInfo.appliedPct < 0 ? '#fb7185' : '#9aa';
                    }
                  } catch (e) {
                    color = '#9aa';
                  }

                  // If the stockRow contains a small history/series, prefer that shape for the sparkline
                  let sparkData = history;
                  if (stockRow && Array.isArray(stockRow.history) && stockRow.history.length) {
                    // normalize possible shapes into {t, p} — accept {week, stock}, {t,p}, numeric arrays
                    sparkData = stockRow.history.map((d: any, i: number) => {
                      if (d && typeof d === 'object') {
                        // prefer stock/week shape produced by API history
                        if ('stock' in d) return { t: d.week !== undefined ? String(d.week) : (d.t ?? String(i)), p: Number(d.stock ?? 0) };
                        if ('p' in d || 'price' in d) return { t: d.t ?? String(i), p: Number(d.p ?? d.price ?? 0) };
                        // some history rows may use {week, stock, confidence, ...}
                        if ('week' in d && ('stock' in d || 'price' in d || 'p' in d)) return { t: String(d.week), p: Number(d.stock ?? d.p ?? d.price ?? 0) };
                      }
                      if (typeof d === 'number') return { t: String(i), p: d };
                      return { t: String(i), p: 0 };
                    });
                    // route/history CSVs are typically oldest->newest; StockChartSmall expects newest->oldest
                    sparkData = Array.isArray(sparkData) ? sparkData.slice().reverse() : sparkData;
                  }
                  // also normalize any raw `history` passed directly via props (e.g., player.priceHistory)
                  if (!sparkData || !sparkData.length) {
                    if (Array.isArray(history) && history.length) {
                      // accept {t,p} or {week,stock} shapes and normalize to newest->oldest
                      const hnorm = history.map((d: any, i: number) => {
                        if (d && typeof d === 'object') {
                          if ('p' in d || 'price' in d) return { t: d.t ?? String(i), p: Number(d.p ?? d.price ?? 0) };
                          if ('stock' in d) return { t: d.week !== undefined ? String(d.week) : (d.t ?? String(i)), p: Number(d.stock ?? 0) };
                        }
                        if (typeof d === 'number') return { t: String(i), p: d };
                        return { t: String(i), p: 0 };
                      });
                      sparkData = hnorm.slice().reverse();
                    }
                  }

                  // make the sparkline slightly larger for visual balance inside the card
                  return <StockChartSmall data={Array.isArray(sparkData) ? sparkData : []} color={color} width={200} height={50} />;
                })()
              }
            </div>
          </div>
        ) : null}

        {showChart && history && Array.isArray(history) ? (
          <div className="mt-3">
      <div className="flex gap-2 items-center mb-2">
        <div className="text-[12px] text-[#9aa]">Range</div>
                {/* Range buttons: accessible, keyboard-focusable, with small labels/icons */}
                {([
                  ['spark', 'Spark', 'S'],
                  ['1d', '1 Day', '1D'],
                  ['7d', '7 Days', '7D'],
                  ['30d', '30 Days', '30D'],
                ] as Array<[typeof range, string, string]>).map(([val, title, label]) => {
                  const active = range === val;
                  const compactPad = compactChart ? '4px 6px' : '6px 8px';
                  const btnClasses = `inline-flex items-center justify-center rounded-md border px-2 ${compactChart ? 'py-1 px-1.5 text-[11px]' : 'py-1.5 px-2 text-[12px]'} ${active ? 'bg-[#111827] text-white' : 'bg-transparent text-[#9aa]'} border-[rgba(255,255,255,0.03)]`;

                  return (
                    <button
                      key={String(val)}
                      onClick={() => setRange(val)}
                      title={title}
                      aria-pressed={active}
                      aria-label={`Range ${title}`}
                      className={btnClasses}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          setRange(val);
                        }
                      }}
                    >
                      <span className={`font-extrabold mr-1 ${compactChart ? 'text-[10px]' : 'text-[11px]'}`}>{label}</span>
                      <span className={`${compactChart ? 'hidden' : 'inline-block'} text-[11px] ${active ? 'text-[#ddd]' : 'text-[#9aa]'}`}>{title}</span>
                    </button>
                  );
                })}
              </div>
            <PriceChart data={history} compact={compactChart} range={range} seasonBaseline={priceInfo?._debug?.seasonBaseline ?? null} />
          </div>
        ) : null}
    </div>
  );
}

// Use shared sparkline helper from `src/lib/sparkline.tsx` (imported above)
