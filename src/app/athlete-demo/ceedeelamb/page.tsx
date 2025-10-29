"use client";
import React, { useEffect, useState } from 'react';
import PriceChart from '@/app/components/PriceChart';

export default function DemoCeeDee() {
  const [series, setSeries] = useState<any[] | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const res = await fetch('/api/espn/season-price-synth?espnId=4241389&startPrice=100');
        const json = await res.json();
        if (!res.ok) throw new Error(json?.error || 'fetch failed');
        if (mounted) setSeries(json.series || []);
      } catch (e: any) {
        if (mounted) setErr(String(e?.message ?? e));
      }
    })();
    return () => { mounted = false; };
  }, []);

  return (
    <div style={{ padding: 20 }}>
      <h2>CeeDee Lamb — Season Price (synthetic)</h2>
      {err ? <div style={{ color: 'salmon' }}>{err}</div> : null}
      {series ? (
        <div style={{ width: 640, maxWidth: '100%' }}>
          <PriceChart data={series} color="#3b82f6" range="30d" />
        </div>
      ) : (
        <div>Loading…</div>
      )}
    </div>
  );
}
