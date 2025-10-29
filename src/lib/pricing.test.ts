import { describe, it, expect } from 'vitest';
import { computeInitialPriceFromStats } from './pricing';

describe('pricing.computeInitialPriceFromStats', () => {
  it('gives higher price for QB with passing yards and TDs', () => {
    const qb = computeInitialPriceFromStats({ yards: 300, tds: 3, ints: 0 }, 'QB', 100);
    const wr = computeInitialPriceFromStats({ rec: 8, yards: 120, tds: 1 }, 'WR', 100);
    const rb = computeInitialPriceFromStats({ rush: 80, rec: 3, yards: 50, tds: 1 }, 'RB', 100);
    expect(qb).toBeGreaterThan(wr);
    expect(wr).toBeGreaterThan(rb);
  });

  it('clamps to min and max bounds', () => {
    const tiny = computeInitialPriceFromStats(null, 'WR', 1);
    const huge = computeInitialPriceFromStats({ yards: 100000, tds: 1000 }, 'QB', 5000);
    expect(tiny).toBeGreaterThanOrEqual(5);
    expect(huge).toBeLessThanOrEqual(2000);
  });
});
