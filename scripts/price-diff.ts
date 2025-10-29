import { computeInitialPriceFromStats } from '../src/lib/pricing';

function printSample() {
  const qb = computeInitialPriceFromStats({ yards: 300, tds: 3, ints: 0 }, 'QB', 100);
  const wr = computeInitialPriceFromStats({ rec: 8, yards: 120, tds: 1 }, 'WR', 100);
  const rb = computeInitialPriceFromStats({ rush: 80, rec: 3, yards: 50, tds: 1 }, 'RB', 100);
  console.log('QB sample price:', qb);
  console.log('WR sample price:', wr);
  console.log('RB sample price:', rb);
}

printSample();
