const cp = require('child_process');
function curl(url){
  try {
    return cp.execSync(`curl -s "${url}"`, { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 });
  } catch (e) {
    console.error('curl failed', e.message);
    process.exit(2);
  }
}

function safeNum(v){
  if (v == null) return 0;
  const cleaned = String(v).replace(/[^0-9.\-]/g, '');
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : 0;
}

function computeInitialPriceFromStats(stats, position, base = 80){
  // simplified mirror of src/lib/pricing.ts
  const POSITION_WEIGHTS = {
    QB: { yards: 0.005, tds: 6, ints: -2, fumbles: -2 },
    WR: { rec: 0.3, yards: 0.1, tds: 6, fumbles: -1 },
    RB: { rush: 0.1, rec: 0.2, yards: 0.05, tds: 6, fumbles: -1 },
    TE: { rec: 0.25, yards: 0.08, tds: 6, fumbles: -1 },
    DEF: { tds: 2, ints: 2 },
  };
  function getWeightsForPosition(pos){
    if (!pos) return { yards: 0.05, tds: 10, ints: -8, fumbles: -8 };
    const p = String(pos).toUpperCase();
    for (const key of Object.keys(POSITION_WEIGHTS)) if (p.includes(key)) return POSITION_WEIGHTS[key];
    return { yards: 0.05, tds: 10, ints: -8, fumbles: -8 };
  }
  function posMultiplier(pos){
    const M = { QB:1.25, WR:1.15, RB:1.1, TE:1.05, DEF:0.9 };
    if (!pos) return 1; const p = String(pos).toUpperCase(); for (const k of Object.keys(M)) if (p.includes(k)) return M[k]; return 1;
  }

  const w = getWeightsForPosition(position);
  const yards = stats?.yards ?? 0;
  const rec = stats?.rec ?? 0;
  const rush = stats?.rush ?? 0;
  const tds = stats?.tds ?? 0;
  const ints = stats?.ints ?? 0;
  const fumbles = stats?.fumbles ?? 0;
  let score = 0;
  if (w.yards) score += yards * w.yards;
  if (w.rec) score += rec * w.rec;
  if (w.rush) score += rush * w.rush;
  if (w.tds) score += tds * w.tds;
  if (w.ints) score += ints * w.ints;
  if (w.fumbles) score += fumbles * w.fumbles;
  const multiplier = posMultiplier(position);
  const raw = base + score * multiplier;
  const clamped = Math.max(5, Math.min(2000, raw));
  return Number(clamped.toFixed(2));
}

function clamp(x,a,b){return Math.max(a,Math.min(b,x));}

// Use three synthetic sample players (QB, RB, WR) with season stats to
// demonstrate how the SEASON_ADJ_MULTIPLIER affects price adjustments.
const samples = [
  {
    id: 'sample-qb',
    name: 'Sample QB',
    position: 'QB',
    stats: { yards: 4000, tds: 30, ints: 8, fumbles: 2 },
  },
  {
    id: 'sample-rb',
    name: 'Sample RB',
    position: 'RB',
    stats: { rush: 1200, rec: 50, yards: 1250, tds: 10, fumbles: 1 },
  },
  {
    id: 'sample-wr',
    name: 'Sample WR',
    position: 'WR',
    stats: { rec: 100, yards: 1400, tds: 12, fumbles: 0 },
  },
];

const multipliers = [0.2, 0.5, 1.0];
const MAX = 0.10; // per-update cap still ±10%
const currentPrice = 100;
const capsToCompare = [0.05, 0.10]; // current cap vs requested larger cap

for (const s of samples) {
  const baseline = computeInitialPriceFromStats(s.stats, s.position, 80);
  const ratio = baseline / currentPrice;
  console.log(`\n=== ${s.name} (${s.position}) ===`);
  console.log('season stats:', s.stats);
  console.log('seasonBaseline:', baseline, 'ratio:', ratio.toFixed(3));
  for (const cap of capsToCompare) {
    console.log(`-- adj cap = ${cap}`);
    for (const m of multipliers) {
      const adj = clamp((ratio - 1) * m, -cap, cap);
      const final = clamp(adj, -MAX, MAX);
      const newPrice = Number((currentPrice * (1 + final)).toFixed(2));
      console.log(`mult=${m.toFixed(1)} -> adj=${adj.toFixed(4)}, finalPct=${final.toFixed(4)}, newPrice=${newPrice}`);
    }
  }
}
