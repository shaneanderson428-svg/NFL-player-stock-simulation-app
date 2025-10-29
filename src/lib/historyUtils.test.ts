import { describe, it, expect } from 'vitest';
import { filterPricePoints } from './historyUtils';

function makePoint(ts: number, price = 100) {
  return { t: new Date(ts).toISOString(), p: price };
}

describe('historyUtils.filterPricePoints', () => {
  it('returns last 30 points for spark', () => {
    const now = Date.now();
    const points = Array.from({ length: 40 }).map((_, i) => makePoint(now - (40 - i) * 60_000, 80 + i));
    const out = filterPricePoints(points, 'spark');
    expect(out.length).toBe(30);
    // last element should match last original
    expect(out[out.length - 1].p).toBe(80 + 39);
  });

  it('filters 1d window', () => {
    const now = Date.now();
    const oneDay = 24 * 60 * 60 * 1000;
    const points = [
      makePoint(now - oneDay * 2, 1),
      makePoint(now - oneDay * 1.5, 2),
      makePoint(now - oneDay * 0.5, 3),
      makePoint(now, 4),
    ];
    const out = filterPricePoints(points, '1d');
    expect(out.every(p => new Date(p.t).getTime() >= now - oneDay)).toBe(true);
    expect(out.map(p => p.p)).toEqual([3, 4]);
  });

  it('filters 7d and 30d window', () => {
    const now = Date.now();
    const oneDay = 24 * 60 * 60 * 1000;
    const points = [
      makePoint(now - oneDay * 40, 1), // 40d ago
      makePoint(now - oneDay * 20, 2), // 20d ago
      makePoint(now - oneDay * 5, 3),  // 5d ago
      makePoint(now - oneDay * 1, 4),  // 1d ago
      makePoint(now, 5),               // now
    ];
    const out7 = filterPricePoints(points, '7d');
    expect(out7.map(p => p.p)).toEqual([3, 4, 5]);
    const out30 = filterPricePoints(points, '30d');
    expect(out30.map(p => p.p)).toEqual([2, 3, 4, 5]);
  });
});
