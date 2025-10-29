"use client";

import React, { useRef, useEffect } from 'react';
import type { PricePoint } from '@/lib/types';
import { AreaChart, Area, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, ReferenceLine } from 'recharts';

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
      {p?.e ? (
        <div style={{ marginTop: 6, fontSize: 12, color: '#ffd', display: 'flex', gap: 8, alignItems: 'center' }}>
          <div style={{ width: 8, height: 8, borderRadius: 8, background: '#f59e0b', boxShadow: '0 0 0 3px rgba(245,158,11,0.08)' }} />
          <div>
            <div style={{ fontWeight: 700, fontSize: 12 }}>{String(p.e.type).replace(/_/g, ' ')}</div>
            <div style={{ color: '#9aa', fontSize: 11 }}>{p.e.impact ? `${p.e.impact}% impact` : ''}</div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

// Custom dot renderer: only render a visible marker for points that carry an event (p.e)
function EventDot(props: any) {
  const { cx, cy, payload } = props || {};
  if (cx == null || cy == null || !payload) return null;
  if (!payload.e) return null;
  return (
    <circle cx={cx} cy={cy} r={3.75} fill="#f59e0b" stroke="#071226" strokeWidth={1.25} style={{ pointerEvents: 'auto' }} />
  );
}

type Props = {
  data: PricePoint[];
  compact?: boolean;
  color?: string;
  height?: number;
  showAxes?: boolean;
  gradientId?: string;
  range?: string;
  seasonBaseline?: number | null;
};

export default function PriceChartClient(props: Props) {
  const { data, compact = false, color = '#22c55e', height = 160, showAxes = true, gradientId = 'grad', seasonBaseline = null } = props;

  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    // start hidden in DOM, then trigger transition
    requestAnimationFrame(() => {
      el.style.opacity = '1';
      el.style.transform = 'translateY(0px)';
    });
  }, []);

  const containerStyle: React.CSSProperties = {
    width: '100%',
    height,
    opacity: 0,
    transform: 'translateY(4px)',
    transition: 'opacity 240ms ease, transform 240ms ease',
  };

  // derive a trend color from data (newest vs oldest) so the chart can be green/red by trend
  let trendColor = color;
  try {
    if (Array.isArray(data) && data.length >= 2) {
      const pts = data
        .map((p: any) => ({ t: p?.t ? Date.parse(String(p.t)) : NaN, v: Number(p?.p ?? 0) }))
        .filter((p: any) => Number.isFinite(p.t));
      if (pts.length >= 2) {
        let oldest = pts[0];
        let newest = pts[0];
        for (const pt of pts) {
          if (pt.t < oldest.t) oldest = pt;
          if (pt.t > newest.t) newest = pt;
        }
        if (newest.v > oldest.v) trendColor = '#34d399';
        else if (newest.v < oldest.v) trendColor = '#fb7185';
        else trendColor = '#94a3b8';
      }
    }
  } catch (e) {
    // ignore and fall back to provided color
  }

  return (
    <div data-pc-chart ref={containerRef} style={containerStyle}>
      {/* guard: if no data, render an empty transparent box so layout doesn't jump */}
      {!data || !Array.isArray(data) || data.length === 0 ? (
        <svg width="100%" height={height} viewBox={`0 0 100 ${height}`} preserveAspectRatio="none" aria-hidden>
          <rect width="100%" height="100%" fill="transparent" />
        </svg>
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 6, right: 8, left: 0, bottom: 6 }}>
            <defs>
              <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={trendColor} stopOpacity={0.22} />
                <stop offset="60%" stopColor={trendColor} stopOpacity={0.08} />
                <stop offset="100%" stopColor={trendColor} stopOpacity={0.02} />
              </linearGradient>
            </defs>
            {!compact ? <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.06} /> : null}
            {showAxes ? <XAxis dataKey="t" tick={{ fontSize: 12 }} /> : null}
            {showAxes ? <YAxis tick={{ fontSize: 12 }} domain={["auto", "auto"]} /> : null}
            <Tooltip content={<CustomTooltip />} cursor={{ stroke: '#ffffff22', strokeWidth: 1 }} />
            <Area type="monotone" dataKey="p" stroke={trendColor} strokeWidth={2} fill={`url(#${gradientId})`} dot={false} />
            <Line type="monotone" dataKey="p" stroke={trendColor} strokeWidth={2.5} dot={false} activeDot={{ r: 4 }} />
            {/* custom dot renderer to show event markers */}
            <Line type="monotone" dataKey="p" stroke="none" dot={<EventDot />} activeDot={false} />
            {seasonBaseline ? (
              <ReferenceLine y={seasonBaseline} stroke="#9aa" strokeDasharray="4 4" strokeWidth={1} label={{ position: 'right', value: `Season: ${formatPrice(seasonBaseline)}`, fill: '#9aa', fontSize: 11 }} />
            ) : null}
          </AreaChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
