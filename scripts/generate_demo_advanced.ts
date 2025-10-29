import fs from 'fs/promises';
import path from 'path';

async function main() {
  const repo = process.cwd();
  const histDir = path.join(repo, 'data', 'history');
  const advDir = path.join(repo, 'data', 'advanced');
  await fs.mkdir(advDir, { recursive: true });

  const files = await fs.readdir(histDir).catch(() => []);
  const players: Array<{ espnId: number; file: string }> = [];

  for (const f of files) {
    if (!f.endsWith('.json')) continue;
    const espnId = Number(f.replace('.json', ''));
    const raw = await fs.readFile(path.join(histDir, f), 'utf8').catch(() => null);
    if (!raw) continue;
    let history: any[] = [];
    try { history = JSON.parse(raw); } catch (e) { continue; }

    // Synthesize metrics from history: simple stats
    const last = history[history.length - 1];
    const avg = history.length ? (history.reduce((s, it) => s + (it.p || 0), 0) / history.length) : 80;
    const metrics = {
      YardsPerRouteRun: +(1 + (avg % 3)).toFixed(2),
      CatchRateOverExpected: +((avg % 10) / 100).toFixed(3),
      EPA_per_Target: +(((avg % 5) / 20)).toFixed(3),
      receivingYards: Math.round(avg * 10),
      receivingTDs: Math.round((avg % 12) / 2),
      receptions: Math.round(avg / 1.2),
    };

    const out = {
      espnId,
      player: `Demo ${espnId}`,
      position: 'UNK',
      metrics,
    };

    const outPath = path.join(advDir, `${espnId}.json`);
    await fs.writeFile(outPath, JSON.stringify(out, null, 2));
    players.push({ espnId, file: `${espnId}.json` });
  }

  const index = { lastUpdated: new Date().toISOString(), players };
  await fs.writeFile(path.join(advDir, 'index.json'), JSON.stringify(index, null, 2));
  console.log(`Wrote ${players.length} demo advanced files to ${advDir}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
