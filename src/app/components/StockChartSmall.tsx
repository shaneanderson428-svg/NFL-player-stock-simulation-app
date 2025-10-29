"use client";
import React from 'react';
import { ResponsiveContainer, Tooltip, AreaChart, Area, XAxis, YAxis } from 'recharts';

type Point = { t: string; p: number };

export default function StockChartSmall({ data, width = 200, height = 50, color = '#22c55e' }: { data: Point[]; width?: number; height?: number; color?: string }) {
  // unique id for gradients to avoid collisions when many charts render on the page
  const uidBase = typeof React.useId === 'function' ? React.useId() : `s-${Math.random().toString(36).slice(2, 9)}`;
  const gradId = `grad-${uidBase}`;

  const formatted = (data || []).map((d, i) => ({ x: i, y: Number(d?.p ?? 0), t: d?.t }));

  if (!formatted || formatted.length === 0) {
    return (
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} xmlns="http://www.w3.org/2000/svg" aria-hidden>
        <rect width="100%" height="100%" fill="transparent" />
      </svg>
    );
  }

  // compute a small padding for the y-domain so the line doesn't touch the edges
  const ys = formatted.map((f) => f.y);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const range = Math.max(0.0001, maxY - minY);
  const pad = Math.max(range * 0.08, 0.5);
  const domain: [number, number] = [minY - pad, maxY + pad];

  // Determine sparkline color from newest -> oldest point: green if newest is higher than oldest, red if lower.
  // Be robust to ordering by checking timestamps when available.
  let sparkColor = color;
  try {
    if (formatted.length >= 2) {
      // attempt to determine oldest/newest via timestamp parsing
      const withTs = formatted.map((f) => ({ ...f, tms: f.t ? Date.parse(String(f.t)) : NaN }));
      const hasValidTs = withTs.some((f) => Number.isFinite(f.tms));
      let newestVal: number | null = null;
      let oldestVal: number | null = null;
      if (hasValidTs) {
        let newest = withTs[0];
        let oldest = withTs[0];
        for (const pt of withTs) {
          if (!Number.isFinite(pt.tms)) continue;
          if (!Number.isFinite(oldest.tms) || pt.tms < oldest.tms) oldest = pt;
          if (!Number.isFinite(newest.tms) || pt.tms > newest.tms) newest = pt;
        }
        newestVal = (newest as any).v ?? newest.y;
        oldestVal = (oldest as any).v ?? oldest.y;
      } else {
        // fall back to assuming chronological ascending (oldest -> newest)
        oldestVal = formatted[0].y;
        newestVal = formatted[formatted.length - 1].y;
      }

      if (newestVal != null && oldestVal != null) {
        if (newestVal > oldestVal) sparkColor = '#34d399';
        else if (newestVal < oldestVal) sparkColor = '#fb7185';
        else sparkColor = '#94a3b8';
      }
    }
  } catch (e) {
    // ignore and keep provided color
  }

  const labelFormatter = (label: any) => {
    const p = formatted.find((f) => String(f.x) === String(label));
    return p ? p.t : String(label);
  };

  // pick a strokeWidth that reads well on dark backgrounds
  const strokeWidth = sparkColor === '#94a3b8' ? 2 : 3;

  return (
    <div style={{ width, height, background: 'transparent' }}>
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={formatted} margin={{ top: 2, right: 0, left: 0, bottom: 2 }}>
          <defs>
            <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={sparkColor} stopOpacity={0.32} />
              <stop offset="100%" stopColor={sparkColor} stopOpacity={0.06} />
            </linearGradient>
          </defs>
          <XAxis dataKey="x" hide />
          <YAxis hide domain={domain as any} />
          <Tooltip
            contentStyle={{ background: '#0b1220', border: 'none', color: '#ddd' }}
            labelFormatter={labelFormatter}
            formatter={(v: any) => (typeof v === 'number' ? v.toFixed(2) : v)}
          />
          {/* Area provides fill, add an explicit Line for a crisp stroke */}
          <Area type="monotone" dataKey="y" stroke={sparkColor} fill={`url(#${gradId})`} strokeWidth={strokeWidth} dot={false} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
