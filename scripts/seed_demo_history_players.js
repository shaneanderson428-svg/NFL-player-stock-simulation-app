const fs = require('fs').promises;
const path = require('path');

function daysAgoISO(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

function mkSeries(base, days = 14) {
  const out = [];
  for (let i = days - 1; i >= 0; i--) {
    const drift = Math.sin(i * 0.3) * 3;
    const noise = (Math.random() - 0.5) * 2;
    out.push({ t: daysAgoISO(i), p: Number((base + drift + noise).toFixed(2)) });
  }
  return out;
}

async function main() {
  const DATA = path.join(process.cwd(), 'data', 'history');
  await fs.mkdir(DATA, { recursive: true });

  const demoPlayers = [
    { id: 3045146, name: 'Geno Smith', base: 85 },
    { id: 4038944, name: 'DK Metcalf', base: 95 },
    { id: 4431728, name: 'Kenneth Walker III', base: 110 }
  ];

  for (const p of demoPlayers) {
    const out = mkSeries(p.base, 30);
    const fp = path.join(DATA, `${p.id}.json`);
    await fs.writeFile(fp, JSON.stringify(out, null, 2), 'utf8');
    console.log('Wrote', fp);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
