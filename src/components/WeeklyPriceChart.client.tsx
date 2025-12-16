"use client";

import React, { useMemo, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';

type Point = { t: string; p: number; [key: string]: any };

function isoWeekNumber(date: Date) {
  // Copy date so don't modify original
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  // Set to nearest Thursday: current date + 4 - current day number
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  // Year of the Thursday
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil(((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7);
  return { year: d.getUTCFullYear(), week: weekNo };
}

function formatWeekLabel(item: any) {
  if (!item) return '';
  const { year, week } = item;
  return `W${week}` + (year ? `/${String(year).slice(-2)}` : '');
}

function buildSeries(points: Point[]) {
  // Map each point to ISO week number; keep the earliest and latest week bounds
  const mapped: Array<{ year: number; week: number; t: string; p: number; [key: string]: any }> = [];
  for (const pt of points || []) {
    try {
      const d = new Date(String(pt.t));
      if (Number.isNaN(d.getTime())) continue;
      const w = isoWeekNumber(d);
      // preserve any additional stat fields that may be present on the point
      mapped.push({ ...pt, year: w.year, week: w.week, t: pt.t, p: Number(pt.p ?? 0) });
    } catch (e) {
      continue;
    }
  }

  if (!mapped.length) return { series: [], minWeek: 0, maxWeek: 0 };

  // Find numeric range across (year,week) ordered lexicographically
  const keys = mapped.map((m) => ({ key: m.year * 100 + m.week, ...m }));
  keys.sort((a, b) => a.key - b.key);
  const minKey = keys[0].key;
  const maxKey = keys[keys.length - 1].key;

  // Build full series from minKey..maxKey; insert nulls where missing
  const series: Array<{ x: number; year: number; week: number; p: number | null; t?: string; pct?: number | null }> = [];
  // Build a map for quick lookup
  // Map key -> full mapped object (so any extra fields are preserved)
  const lookup = new Map<number, any>();
  for (const k of keys) lookup.set(k.key, k);

  // Instead, construct a list of unique sorted (year,week) values between bounds using dates
  // Find the start date from the earliest mapped point
  const start = new Date(keys[0].t);
  const end = new Date(keys[keys.length - 1].t);
  // iterate by 7 days
  const cur = new Date(start);
  // normalize to Monday of that ISO week
  // push series until cur > end
  let prevP: number | null = null;
  while (cur <= end) {
    const w = isoWeekNumber(cur);
    const key = w.year * 100 + w.week;
    const hit = lookup.has(key) ? lookup.get(key) : null;
    const pVal = hit ? Number(hit.p ?? null) : null;
    const entry: any = { x: key, year: w.year, week: w.week, p: pVal, t: cur.toISOString().slice(0, 10), pct: null };
    // If the mapped point had extra stat fields, copy a few common ones
    if (hit) {
      for (const k of ["targets", "receptions", "receiving_yards", "rec_yards", "yards", "tds", "receiving_tds"]) {
        if (k in hit) entry[k] = hit[k];
      }
      // copy optional performance metadata if present on the history point
      if ("weekly_pct_change" in hit) entry.weekly_pct_change = hit.weekly_pct_change;
      if ("performance_score" in hit) entry.performance_score = hit.performance_score;
    }
    series.push(entry);
    // advance 7 days
    cur.setUTCDate(cur.getUTCDate() + 7);
  }

  // compute percent change per week where possible
  for (let i = 0; i < series.length; i++) {
    const s = series[i];
    const prev = i > 0 ? series[i - 1] : null;
    if (s.p == null) {
      s.pct = null;
    } else if (prev && prev.p != null) {
      s.pct = prev.p !== 0 ? Math.round(((s.p - prev.p) / Math.abs(prev.p)) * 100 * 100) / 100 : null;
    } else {
      s.pct = null;
    }
  }

  return { series, minWeek: keys[0].week, maxWeek: keys[keys.length - 1].week };
}

function TooltipContent({ active, payload, view }: any) {
  if (!active || !payload || !payload.length) return null;
  const item = payload[0].payload;
  const price = item.p;
  // Prefer persisted weekly_pct_change if present; fall back to computed pct
  const pctVal = item.weekly_pct_change != null ? Number(item.weekly_pct_change) : (item.pct != null ? Number(item.pct) : null);
  const perf = item.performance_score != null ? Number(item.performance_score) : null;

  const formatSignedPercent = (v: number) => (v >= 0 ? `+${v.toFixed(1)}%` : `${v.toFixed(1)}%`);
  const formatPerf = (v: number) => v.toFixed(2);

  return (
    <div style={{ background: '#071226', color: '#fff', padding: 8, borderRadius: 6, fontSize: 12 }}>
      <div style={{ fontSize: 12, color: '#9aa' }}>{formatWeekLabel({ year: item.year, week: item.week })}</div>
      {/* Primary value depends on selected view */}
      {view === 'price' ? (
        <div style={{ fontWeight: 700, marginTop: 4 }}>{price != null ? `$${price.toFixed(2)}` : 'No data'}</div>
      ) : (
        <div style={{ fontWeight: 700, marginTop: 4 }}>{pctVal != null ? formatSignedPercent(pctVal) : 'No data'}</div>
      )}
      {view !== 'price' && pctVal != null ? <div style={{ color: pctVal >= 0 ? '#34d399' : '#fb7185' }}>{/* extra color stripe, redundant with primary */}</div> : null}
      {/* show a few key stat fields if present */}
      <div style={{ marginTop: 6, color: '#cbd5e1' }}>
        {item.targets != null ? <div>Targets: {item.targets}</div> : null}
        {item.receptions != null ? <div>Receptions: {item.receptions}</div> : null}
        {item.receiving_yards != null ? <div>Rec Yds: {item.receiving_yards}</div> : null}
        {item.tds != null ? <div>TDs: {item.tds}</div> : null}
        {perf != null ? <div style={{ marginTop: 6 }}>Performance: {formatPerf(perf)}</div> : null}
      </div>
    </div>
  );
}

export default function WeeklyPriceChart({ history }: { history: Point[] }) {
  const { series } = useMemo(() => buildSeries(history || []), [history]);
  const [view, setView] = useState<'price' | 'pct'>('price');

  if (!series || series.length === 0) {
    return (
      <div style={{ width: '100%', height: 240, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#94a3b8' }}>No weekly history available</div>
    );
  }

  // Helper to get persisted or computed percent value for a series row
  const getPctForRow = (s: any) => {
    if (!s) return null;
    if (s.weekly_pct_change != null) return Number(s.weekly_pct_change);
    if (s.pct != null) return Number(s.pct);
    return null;
  };

  // Determine overall trend color based on selected view (price vs pct)
  const first = series.find((s: any) => (view === 'price' ? s.p != null : getPctForRow(s) != null));
  const last = [...series].reverse().find((s: any) => (view === 'price' ? s.p != null : getPctForRow(s) != null));
  const trendUp = last && first ? ((view === 'price' ? (last.p ?? 0) - (first.p ?? 0) : (getPctForRow(last) ?? 0) - (getPctForRow(first) ?? 0)) >= 0) : true;
  const lineColor = trendUp ? '#34d399' : '#fb7185';

  const dataKey = view === 'price' ? 'p' : 'pct';
  // Build three overlay datasets (positive / negative / zero) so segments can be colored
  const { posData, negData, zeroData, lastNonNullIndex } = useMemo(() => {
    const n = series.length;
  const pos = series.map((s) => ({ ...s, displayValue: null as number | null }));
  const neg = series.map((s) => ({ ...s, displayValue: null as number | null }));
  const zer = series.map((s) => ({ ...s, displayValue: null as number | null }));

    const getPct = (s: any) => {
      if (s.weekly_pct_change != null) return Number(s.weekly_pct_change);
      if (s.pct != null) return Number(s.pct);
      return null;
    };

    for (let i = 0; i < n; i++) {
      const s: any = series[i];
      const pctVal = getPct(s);
      // For plotting value, prefer persisted weekly_pct_change when in pct view
      const val = view === 'price' ? (s.p ?? null) : (s.weekly_pct_change != null ? Number(s.weekly_pct_change) : (s.pct != null ? Number(s.pct) : null));
      if (val == null || pctVal == null) continue;

      // assign current and previous index so the Line draws the segment between them
      if (pctVal > 0) {
        pos[i].displayValue = val;
        if (i > 0 && pos[i - 1].displayValue == null) {
          const prevVal = view === 'price' ? (series[i - 1].p ?? null) : ((series[i - 1] as any).weekly_pct_change != null ? Number((series[i - 1] as any).weekly_pct_change) : (series[i - 1].pct != null ? Number(series[i - 1].pct) : null));
          pos[i - 1].displayValue = prevVal;
        }
      } else if (pctVal < 0) {
        neg[i].displayValue = val;
        if (i > 0 && neg[i - 1].displayValue == null) {
          const prevVal = view === 'price' ? (series[i - 1].p ?? null) : ((series[i - 1] as any).weekly_pct_change != null ? Number((series[i - 1] as any).weekly_pct_change) : (series[i - 1].pct != null ? Number(series[i - 1].pct) : null));
          neg[i - 1].displayValue = prevVal;
        }
      } else {
        zer[i].displayValue = val;
        if (i > 0 && zer[i - 1].displayValue == null) {
          const prevVal = view === 'price' ? (series[i - 1].p ?? null) : ((series[i - 1] as any).weekly_pct_change != null ? Number((series[i - 1] as any).weekly_pct_change) : (series[i - 1].pct != null ? Number(series[i - 1].pct) : null));
          zer[i - 1].displayValue = prevVal;
        }
      }
    }

    // compute last non-null index for the currently-selected view (used to emphasize final point)
    let lastIdx = -1;
    for (let i = series.length - 1; i >= 0; i--) {
      const s = series[i];
  const v = view === 'price' ? (s.p ?? null) : (((s as any).weekly_pct_change != null) ? Number((s as any).weekly_pct_change) : (s.pct != null ? Number(s.pct) : null));
      if (v != null) {
        lastIdx = i;
        break;
      }
    }

    return { posData: pos, negData: neg, zeroData: zer, lastNonNullIndex: lastIdx };
  }, [series, view]);

  // custom dot renderer to emphasize the most recent (non-null) point
  const CustomDot = (dotProps: any) => {
    const { cx, cy, index } = dotProps;
    const isLast = index === lastNonNullIndex;
    const r = isLast ? 7 : 3;
    const fill = isLast ? lineColor : '#0f172a';
    const stroke = isLast ? '#fff' : lineColor;
    return (
      <circle cx={cx ?? 0} cy={cy ?? 0} r={r} fill={fill} stroke={stroke} strokeWidth={isLast ? 2 : 1} />
    );
  };

  // find the latest series point that contains a performance_score (if any)
  const latestPerfPoint = [...series].reverse().find((s: any) => (s as any).performance_score != null);

  return (
    <div style={{ width: '100%', height: 320 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 12px' }}>
        <div style={{ fontSize: 14, color: '#0f172a', fontWeight: 600 }}>Weekly Price</div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={() => setView('price')} style={{ padding: '6px 10px', borderRadius: 6, border: view === 'price' ? `1px solid ${lineColor}` : '1px solid #e2e8f0', background: view === 'price' ? lineColor : '#fff', color: view === 'price' ? '#042f2b' : '#0f172a' }}>Price</button>
          <button onClick={() => setView('pct')} style={{ padding: '6px 10px', borderRadius: 6, border: view === 'pct' ? `1px solid ${lineColor}` : '1px solid #e2e8f0', background: view === 'pct' ? lineColor : '#fff', color: view === 'pct' ? '#042f2b' : '#0f172a' }}>Weekly %</button>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={series} margin={{ top: 6, right: 16, left: 8, bottom: 12 }}>
          <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.06} />
          <XAxis dataKey="week" tickFormatter={(w: any) => `W${w}`} />
          <YAxis domain={["auto", "auto"]} tickFormatter={(v: any) => (view === 'pct' ? `${Number(v).toFixed(1)}%` : String(v))} />
          <Tooltip content={<TooltipContent view={view} />} />
          {/* base continuous line so price history is visible even when weekly_pct_change/pct is missing */}
          <Line type="monotone" data={series} dataKey={dataKey} stroke={lineColor} strokeWidth={1.5} dot={false} connectNulls={true} isAnimationActive={false} opacity={0.85} />
          {/* colored segments achieved by overlaying three lines that only populate displayValue when the segment sign matches */}
          <Line type="monotone" data={posData} dataKey={'displayValue'} stroke={'#34d399'} strokeWidth={2} dot={CustomDot} connectNulls={false} isAnimationActive={false} />
          <Line type="monotone" data={negData} dataKey={'displayValue'} stroke={'#fb7185'} strokeWidth={2} dot={CustomDot} connectNulls={false} isAnimationActive={false} />
          <Line type="monotone" data={zeroData} dataKey={'displayValue'} stroke={'#94a3b8'} strokeWidth={2} dot={CustomDot} connectNulls={false} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
      {/* Caption noting the latest performance score when available */}
      {latestPerfPoint ? (
        <div style={{ padding: '8px 12px', fontSize: 12, color: '#475569' }}>
          {`${formatWeekLabel({ year: latestPerfPoint.year, week: latestPerfPoint.week })} price reflects performance score of ${Number((latestPerfPoint as any).performance_score).toFixed(2)}`}
        </div>
      ) : null}
    </div>
  );
}
