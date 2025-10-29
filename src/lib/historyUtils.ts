export type PricePoint = { t: string; p: number };

function parseTime(t: string) {
  // Support date-only (YYYY-MM-DD) and full ISO timestamps
  const n = Date.parse(t);
  if (!isNaN(n)) return n;
  // Last-resort: try replacing space with T
  const alt = Date.parse(t.replace(' ', 'T'));
  return isNaN(alt) ? null : alt;
}

export function filterPricePoints(points: PricePoint[] | undefined | null, range: 'spark' | '1d' | '7d' | '30d' = 'spark') {
  if (!Array.isArray(points) || points.length === 0) return [];
  const sorted = [...points].sort((a, b) => (parseTime(a.t) || 0) - (parseTime(b.t) || 0));
  const now = Date.now();
  if (range === 'spark') {
    // last 30 points
    return sorted.slice(Math.max(0, sorted.length - 30));
  }
  let ms = 0;
  if (range === '1d') ms = 24 * 60 * 60 * 1000;
  if (range === '7d') ms = 7 * 24 * 60 * 60 * 1000;
  if (range === '30d') ms = 30 * 24 * 60 * 60 * 1000;
  if (ms <= 0) return sorted;
  const cutoff = now - ms;
  return sorted.filter((pt) => {
    const ts = parseTime(pt.t);
    return ts !== null && ts >= cutoff;
  });
}

export default { filterPricePoints };
