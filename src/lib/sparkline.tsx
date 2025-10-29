import React from 'react';

type PriceLike = { t?: string; p?: number; price?: number; value?: number };

// Server-side renderer: returns an SVG element for a compact sparkline.
export function renderSparkline(history: PriceLike[] | undefined | null, width = 160, height = 36, stroke = '#22c55e') {
  const hist = Array.isArray(history) ? history : [];
  if (hist.length === 0) {
    return (
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} xmlns="http://www.w3.org/2000/svg" aria-hidden>
        <rect x="0" y="0" width="100%" height="100%" fill="#0b0b0b" />
      </svg>
    );
  }

  const vals = hist.map((pt) => Number(pt?.p ?? pt?.price ?? pt?.value ?? 0)).filter((v) => Number.isFinite(v));
  if (vals.length === 0) return null;

  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const range = max - min || 1;
  const step = width / Math.max(1, vals.length - 1);
  const points: string[] = [];
  vals.forEach((v, i) => {
    const x = (i * step);
    const y = height - ((v - min) / range) * (height - 4) - 2;
    points.push(`${x.toFixed(2)},${y.toFixed(2)}`);
  });
  const pathD = `M ${points.join(' L ')}`;
  const last = vals[vals.length - 1];

  return (
    <svg viewBox={`0 0 ${width} ${height}`} xmlns="http://www.w3.org/2000/svg" aria-hidden className={`w-full h-10 opacity-40`}>
      <rect x="0" y="0" width="100%" height="100%" fill="#0b0b0b" />
      <path d={pathD} fill="none" stroke={stroke} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={((vals.length - 1) * step).toFixed(2)} cy={((height - ((last - min) / range) * (height - 4) - 2)).toFixed(2)} r="2" fill={stroke} />
      {/* Last-price label */}
      <text x={6} y={14} className="fill-[#9ca3af] text-[11px]">{String(Math.round(last * 100) / 100)}</text>
    </svg>
  );
}

// Client-side React component wrapper. Accepts the same props and renders the SVG markup.
export function Sparkline({ history, width = 160, height = 36, stroke = '#22c55e' }: { history?: PriceLike[]; width?: number; height?: number; stroke?: string }) {
  return <>{renderSparkline(history, width, height, stroke)}</>;
}

export default Sparkline;
