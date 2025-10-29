const fs = require('fs');
const path = require('path');

const advancedDir = path.resolve(process.cwd(), 'data', 'advanced');
const indexPath = path.join(advancedDir, 'index.json');

function main() {
  if (!fs.existsSync(advancedDir)) {
    console.error('data/advanced directory not found');
    process.exit(1);
  }

  const files = fs.readdirSync(advancedDir).filter(f => f.endsWith('.json') && f !== 'index.json');

  const seen = new Map();
  const players = [];

  for (const file of files) {
    try {
      const full = path.join(advancedDir, file);
      const txt = fs.readFileSync(full, 'utf8');
      const json = JSON.parse(txt);
      const espnId = json?.espnId ?? json?.player?.espnId ?? json?.id ?? null;
      if (!espnId) continue;
      const idStr = String(espnId).replace(/\.0$/, '');
      if (seen.has(idStr)) continue;
      seen.set(idStr, file);
      players.push({ espnId: Number(idStr), file: file });
    } catch (err) {
      // ignore invalid json files
    }
  }

  const out = {
    lastUpdated: process.cwd(),
    players: players.sort((a,b)=>a.espnId - b.espnId)
  };

  fs.writeFileSync(indexPath, JSON.stringify(out, null, 2));
  console.log('Wrote', players.length, 'entries to', indexPath);
}

main();
