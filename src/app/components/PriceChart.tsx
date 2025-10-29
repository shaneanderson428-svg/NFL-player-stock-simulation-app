"use client";

import React, { Suspense, useMemo, lazy } from 'react';
import type { PricePoint } from '@/lib/types';
import { filterPricePoints } from '@/lib/historyUtils';
function formatPrice(v: number) {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(v);
}

function CustomTooltip({ active, payload }: any) {
  if (!active || !payload || !payload.length) return null;
  const p = payload[0].payload as PricePoint;
  const date = p?.t ? new Date(p.t).toLocaleString() : '';
  return (
    <div style={{ background: 'rgba(11,17,32,0.95)', color: '#fff', padding: 8, borderRadius: 6, boxShadow: '0 6px 18px rgba(0,0,0,0.5)', fontSize: 12 }}>
      <div style={{ fontSize: 12, color: '#9aa' }}>{date}</div>
      <div style={{ fontWeight: 700, marginTop: 4 }}>{formatPrice(Number(p?.p ?? 0))}</div>
    </div>
  );
}

export default function PriceChart({ data, compact = false, color = '#22c55e', range = 'spark', seasonBaseline = null }: { data: PricePoint[]; compact?: boolean; color?: string; range?: 'spark' | '1d' | '7d' | '30d'; seasonBaseline?: number | null }) {
  const height = compact ? 48 : 160;
  const showAxes = !compact;
  const gradientId = `grad-${String(color).replace('#', '')}`;

  // Ensure data sorts by time if possible
  const sorted = filterPricePoints(Array.isArray(data) ? data : [], range);

  // Lazy-load the client-only wrapper. Because the wrapper file uses "use client" and
  // imports 'recharts' directly, this import will only execute in the browser and
  // will never cause the server to bundle Recharts.
  const ClientChart = useMemo(() => lazy(() => import('./PriceChart.client')) , []);

  return (
    <Suspense fallback={<div style={{ width: '100%', height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>Loading chart…</div>}>
      <ClientChart
        data={sorted}
        compact={compact}
        color={color}
        height={height}
        showAxes={showAxes}
        gradientId={gradientId}
        range={range}
        seasonBaseline={seasonBaseline}
      />
    </Suspense>
  );
}
