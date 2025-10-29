#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const INDEX = path.join(process.cwd(), 'data', 'advanced', 'index.json');
const OUT_DIR = path.join(process.cwd(), 'data', 'history');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const args = process.argv.slice(2);
const force = args.includes('--force');
const seedArgIndex = args.indexOf('--seed');
const seed = seedArgIndex >= 0 && args[seedArgIndex + 1] ? Number(args[seedArgIndex + 1]) : 1337;

// Small xorshift32-based PRNG for deterministic output. Returns 0..1
function makeRng(s) {
  let x = s >>> 0;
  if (x === 0) x = 1;
  return function() {
    x ^= x << 13;
    x >>>= 0;
    x ^= x >>> 17;
    x ^= x << 5;
    x >>>= 0;
    return (x >>> 0) / 4294967295;
  };
}

function randBetween(a, b, rng) { return a + rng() * (b - a); }

function makeSeries(days = 30, startPrice = 80, rng) {
  const out = [];
  let price = startPrice;
  const today = new Date();
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    // random walk
    const change = randBetween(-1.8, 1.8, rng);
    price = Math.max(1, +(price + change + randBetween(-0.5, 0.5, rng)).toFixed(2));
    out.push({ t: d.toISOString().slice(0,10), p: price, v: Math.round(randBetween(1000, 10000, rng)) });
  }
  // inject 1-3 event spikes
  const events = Math.floor(randBetween(1,4, rng));
  const eventTypes = ['big_game','touchdown','record_performance','highlight_play'];
  for (let e = 0; e < events; e++) {
    const idx = Math.floor(randBetween(0, out.length, rng));
    const ev = eventTypes[Math.floor(randBetween(0, eventTypes.length, rng))];
    const spikePct = randBetween(0.08, 0.35, rng); // 8% - 35%
    out[idx].p = +((out[idx].p * (1 + spikePct))).toFixed(2);
    out[idx].v = Math.round(out[idx].v * randBetween(3, 10, rng));
    out[idx].e = { type: ev, impact: +(spikePct*100).toFixed(1) };
  }
  return out;
}

function loadIndex() {
  if (!fs.existsSync(INDEX)) { console.error('no advanced index found at', INDEX); process.exit(1); }
  const idx = JSON.parse(fs.readFileSync(INDEX, 'utf8') || '{}');
  if (Array.isArray(idx.players)) return idx.players.map(p=>p.espnId || p.id).filter(Boolean);
  if (Array.isArray(idx)) return idx.map(p=>p.espnId || p.id || p);
  if (typeof idx === 'object') return Object.keys(idx).map(k=>Number(k));
  return [];
}

function main() {
  const ids = loadIndex();
  console.log('Generating demo history for', ids.length, 'players. force=', !!force);
  for (const id of ids) {
    const fp = path.join(OUT_DIR, `${id}.json`);
    if (fs.existsSync(fp) && !force) {
      console.log(id, 'exists — skipping');
      continue;
    }
    // pick a base price depending on id (deterministic-ish)
    const base = 60 + (Number(String(id).slice(-2)) % 60);
    // create an rng seeded from global seed and id so each player is deterministic
    const combinedSeed = (Number(seed) ^ Number(id)) >>> 0;
    const rng = makeRng(combinedSeed || Number(seed));
    const series = makeSeries(30, base, rng);
    fs.writeFileSync(fp, JSON.stringify(series, null, 2));
    console.log('Wrote', fp);
  }
}

main();
