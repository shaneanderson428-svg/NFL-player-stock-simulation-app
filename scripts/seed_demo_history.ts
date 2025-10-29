import fs from 'fs/promises';
import path from 'path';

type Point = { t: string; p: number };

const DATA_DIR = path.join(process.cwd(), 'data');
const OUT = path.join(DATA_DIR, 'price_history.json');

function daysAgo(n: number) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString();
}

function makeSeries(startPrice: number, volatility = 0.03, days = 60) {
  const pts: Point[] = [];
  let p = startPrice;
  for (let i = days - 1; i >= 0; i--) {
    // random-ish but deterministic-ish using sine
    const drift = Math.sin(i * 0.13) * volatility * startPrice;
    p = Math.max(1, p + drift + (Math.cos(i * 0.07) * volatility * startPrice) * 0.5);
    pts.push({ t: daysAgo(i), p: Number(p.toFixed(2)) });
  }
  return pts;
}

async function main() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  const map: Record<string, Point[]> = {};
  // demo espnIds
  map['4241389'] = makeSeries(100, 0.04, 90); // CeeDee
  map['4240603'] = makeSeries(95, 0.035, 90); // Jefferson
  map['4609048'] = makeSeries(120, 0.03, 90); // McCaffrey

  await fs.writeFile(OUT, JSON.stringify(map, null, 2), 'utf8');
  console.log('Wrote', OUT);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
