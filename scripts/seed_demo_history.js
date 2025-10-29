const fs = require('fs').promises;
const path = require('path');

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString();
}

function makeSeries(startPrice, volatility = 0.03, days = 60) {
  const pts = [];
  let p = startPrice;
  for (let i = days - 1; i >= 0; i--) {
    const drift = Math.sin(i * 0.13) * volatility * startPrice;
    p = Math.max(1, p + drift + (Math.cos(i * 0.07) * volatility * startPrice) * 0.5);
    pts.push({ t: daysAgo(i), p: Number(p.toFixed(2)) });
  }
  return pts;
}

async function main() {
  const DATA_DIR = path.join(process.cwd(), 'data');
  const OUT = path.join(DATA_DIR, 'price_history.json');
  await fs.mkdir(DATA_DIR, { recursive: true });
  const map = {};
  map['4241389'] = makeSeries(100, 0.04, 90);
  map['4240603'] = makeSeries(95, 0.035, 90);
  map['4609048'] = makeSeries(120, 0.03, 90);
  await fs.writeFile(OUT, JSON.stringify(map, null, 2), 'utf8');
  console.log('Wrote', OUT);
}

main().catch((e)=>{ console.error(e); process.exit(1); });
