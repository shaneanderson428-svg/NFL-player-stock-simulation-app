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

  // Compute diagnostics (priceChange, momentum, expectationGap) from fetched history
  const diagnostics = React.useMemo(() => {
    try {
      const rawHist = historyJson ?? null;
      const weekly = Array.isArray(rawHist?.weeklyHistory) ? rawHist.weeklyHistory : [];

      let explicitPriceChange: number | null = null;
      let explicitExpectationGap: number | null = null;
      let explicitMomentum: number | null = null;

      if (weekly.length > 0) {
        const last = weekly[weekly.length - 1] as any;
        explicitPriceChange = (last?.price_change_pct ?? last?.price_change ?? last?.change_pct) ?? null;
        explicitExpectationGap = (last?.expectation_gap ?? null);
        explicitMomentum = (last?.momentum ?? null);
      }

      // Fallback: compute from last two prices
      let computedPriceChange: number | null = null;
      let computedMomentum: number | null = null;
      if (weekly.length >= 2) {
        const a = Number(weekly[weekly.length - 1].price);
        const b = Number(weekly[weekly.length - 2].price);
        if (Number.isFinite(a) && Number.isFinite(b) && Math.abs(b) > 1e-9) {
          computedPriceChange = (a - b) / Math.abs(b);
          computedMomentum = computedPriceChange;
        }
      }

      const priceChange = (explicitPriceChange !== null && explicitPriceChange !== undefined) ? Number(explicitPriceChange) : computedPriceChange;
      const momentum = (explicitMomentum !== null && explicitMomentum !== undefined) ? Number(explicitMomentum) : computedMomentum;
      const expectationGap = (explicitExpectationGap !== null && explicitExpectationGap !== undefined) ? Number(explicitExpectationGap) : null;

      return { priceChange: Number.isFinite(priceChange) ? priceChange : null, momentum: Number.isFinite(momentum) ? momentum : null, expectationGap: Number.isFinite(expectationGap as number) ? expectationGap : null };
    } catch (e) {
      return { priceChange: null, momentum: null, expectationGap: null };
    }
  }, [historyJson]);

  // Derive market status label and color
  const marketStatus = React.useMemo(() => {
    const { priceChange, momentum, expectationGap } = diagnostics;
    let label: string | null = null;
    let color = '#9ca3af';
    let tooltip: string | null = null;

    if (expectationGap !== null && expectationGap > 0.15 && momentum !== null && momentum > 0) {
      label = 'BREAKOUT';
      color = '#4ade80';
      tooltip = 'Exceeded expectations and positive momentum amplified the price move.';
    } else if (momentum !== null && momentum > 0.1) {
      label = 'HOT';
      color = '#4ade80';
      tooltip = 'Strong positive market momentum is pushing the price up.';
    } else if (momentum !== null && momentum < -0.1) {
      label = 'COLD';
      color = '#ef4444';
      tooltip = 'Negative market momentum is pulling the price down.';
    } else if (priceChange !== null && priceChange < -0.1) {
      label = 'SELL-OFF';
      color = '#ef4444';
      tooltip = 'Price fell sharply due to negative market sentiment.';
    } else if (priceChange !== null && Math.abs(priceChange) < 0.02) {
      label = 'STABLE';
      color = '#9ca3af';
      tooltip = 'Price has been stable with little recent change.';
    }

    return { label, color, tooltip };
  }, [diagnostics]);

  // Compute price streak (consecutive up/down weeks) from weekly history
  const priceStreak = React.useMemo(() => {
    try {
      const rawHist = historyJson ?? null;
      const weekly = Array.isArray(rawHist?.weeklyHistory) ? rawHist.weeklyHistory : [];
      if (!Array.isArray(weekly) || weekly.length < 2) return null;

      const pts = weekly
        .map((w: any) => ({ week: Number(w.week), price: Number(w.price) }))
        .filter((p: any) => Number.isFinite(p.week) && Number.isFinite(p.price));
      if (pts.length < 2) return null;
      pts.sort((a: any, b: any) => a.week - b.week);

      let i = pts.length - 1;
      let last = pts[i].price;
      let count = 1;
      let direction: 'up' | 'down' | null = null;

      for (let j = i - 1; j >= 0; j--) {
        const cur = pts[j].price;
        if (last > cur) {
          if (direction === null) direction = 'up';
          if (direction !== 'up') break;
          count++;
        } else if (last < cur) {
          if (direction === null) direction = 'down';
          if (direction !== 'down') break;
          count++;
        } else {
          break;
        }
        last = cur;
      }

      if (count >= 2 && direction) return { direction, count };
      return null;
    } catch (e) {
      return null;
    }
  }, [historyJson]);

  if (loading) return <div style={{ padding: 18 }}>Loading player…</div>;
  if (error) return <div style={{ padding: 18 }} className="text-red-500">Error: {error}</div>;

  return (
    <div style={{ padding: 18 }}>
      <div style={{ display: 'flex', gap: 24 }}>
        <div style={{ flex: '0 0 360px' }}>
          {/* Market status pill near player name / price */}
          {marketStatus.label ? (
            <div style={{ marginBottom: 8 }}>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <span title={marketStatus.tooltip ?? ''} aria-label={marketStatus.tooltip ?? ''} style={{ display: 'inline-block', padding: '4px 8px', borderRadius: 9999, fontSize: 12, fontWeight: 700, color: '#000', background: marketStatus.color }}>
                  {marketStatus.label}
                </span>
                {priceStreak ? (
                  <span style={{ color: '#9ca3af', fontSize: 12 }}>
                    {priceStreak.direction === 'up' ? `📈 Up ${priceStreak.count} weeks in a row` : `📉 Down ${priceStreak.count} weeks in a row`}
                  </span>
                ) : null}
              </div>
            </div>
          ) : null}
          <PlayerCard player={player} />
        </div>
        <div style={{ flex: 1 }}>
          <PlayerStatsChart defaultPlayer={player.name ?? null} />
          <WeeklyPriceChartWrapper history={player.priceHistory ?? []} />

          {/* "Why did this move?" explanation box — UI-only logic using available diagnostics.
              Show only when we can derive or read at least one diagnostic (price change or momentum
              or expectation gap). Keep copy short and non-technical. */}
          {(() => {
            const rawHist = historyJson ?? null;
            const weekly = Array.isArray(rawHist?.weeklyHistory) ? rawHist.weeklyHistory : [];

            let explicitPriceChange: number | null = null;
            let explicitExpectationGap: number | null = null;
            let explicitMomentum: number | null = null;

            if (weekly.length > 0) {
              const last = weekly[weekly.length - 1] as any;
              explicitPriceChange = last?.price_change_pct ?? last?.price_change ?? last?.change_pct ?? null;
              explicitExpectationGap = last?.expectation_gap ?? null;
              explicitMomentum = last?.momentum ?? null;
            }

            // Fallback: compute price_change_pct and momentum from the last two price points if available
            let computedPriceChange: number | null = null;
            let computedMomentum: number | null = null;
            if (weekly.length >= 2) {
              try {
                const a = Number(weekly[weekly.length - 1].price);
                const b = Number(weekly[weekly.length - 2].price);
                if (Number.isFinite(a) && Number.isFinite(b) && Math.abs(b) > 1e-9) {
                  computedPriceChange = (a - b) / Math.abs(b);
                  computedMomentum = computedPriceChange;
                }
              } catch (e) {
                // ignore
              }
            }

            const priceChange = explicitPriceChange ?? computedPriceChange;
            const momentum = explicitMomentum ?? computedMomentum;
            const expectationGap = explicitExpectationGap ?? null;

            const hasDiagnostics = (priceChange !== null && Number.isFinite(priceChange)) || (momentum !== null && Number.isFinite(momentum)) || (expectationGap !== null && Number.isFinite(expectationGap));
            if (!hasDiagnostics) return null;

            let explanation: string | null = null;
            if (expectationGap !== null && Number.isFinite(expectationGap) && expectationGap > 0.15 && momentum !== null && Number.isFinite(momentum) && momentum > 0) {
              explanation = 'Price increased because the player exceeded expectations and positive market momentum amplified the move.';
            } else if (expectationGap !== null && Number.isFinite(expectationGap) && expectationGap > 0.15) {
              explanation = 'Price increased because the player exceeded recent expectations.';
            } else if (momentum !== null && Number.isFinite(momentum) && momentum > 0.1) {
              explanation = 'Price increased due to strong positive market momentum.';
            } else if (priceChange !== null && Number.isFinite(priceChange) && priceChange < -0.1) {
              explanation = 'Price dropped due to underperformance and negative market sentiment.';
            } else if (priceChange !== null && Number.isFinite(priceChange) && Math.abs(priceChange) < 0.02) {
              explanation = 'Price remained stable as performance matched market expectations.';
            } else {
              explanation = 'Price moved based on a combination of recent performance and market momentum.';
            }

            if (!explanation) return null;

            return (
              <div style={{ marginTop: 12 }}>
                <div style={{ background: 'rgba(255,255,255,0.03)', padding: 12, borderRadius: 8, maxWidth: 720 }}>
                  <div style={{ fontWeight: 700, marginBottom: 6 }}>Why did this move?</div>
                  <div style={{ color: '#d1d5db', fontSize: 14 }}>{explanation}</div>
                </div>
              </div>
            );
          })()}
        </div>
      </div>
    </div>
  );
}
