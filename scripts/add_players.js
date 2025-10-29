#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const child = require('child_process');
const axios = require('axios');

function usage() {
  console.log('Usage: node scripts/add_players.js <espnId> [espnId2 ...]');
  process.exit(1);
}

const args = process.argv.slice(2);
if (!args.length) usage();

const ADV_DIR = path.join(process.cwd(), 'data', 'advanced');
const PRICE_PATH = path.join(process.cwd(), 'data', 'price_history.json');

if (!fs.existsSync(ADV_DIR)) fs.mkdirSync(ADV_DIR, { recursive: true });
if (!fs.existsSync(PRICE_PATH)) fs.writeFileSync(PRICE_PATH, JSON.stringify({}, null, 2));

async function fetchPlayerMeta(espnId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/players/${espnId}`;
  try {
    const r = await axios.get(url, { timeout: 8000, headers: { Accept: 'application/json' } });
    return r.data;
  } catch (e) {
    return null;
  }
}

function writeAdvanced(espnId, meta) {
  const out = {
    espnId: Number(espnId),
    player: meta?.fullName || meta?.displayName || `player_${espnId}`,
    team: meta?.team?.abbreviation || undefined,
    updatedAt: new Date().toISOString(),
    // minimal metrics placeholder
    metrics: { sample: true }
  };
  const fp = path.join(ADV_DIR, `${espnId}.json`);
  fs.writeFileSync(fp, JSON.stringify(out, null, 2));
  console.log('Wrote advanced JSON:', fp);
}

function appendPriceHistory(espnId) {
  const hist = JSON.parse(fs.readFileSync(PRICE_PATH, 'utf8') || '{}');
  if (!hist[espnId]) hist[espnId] = [];
  hist[espnId].push({ ts: Date.now(), price: 1000 });
  fs.writeFileSync(PRICE_PATH, JSON.stringify(hist, null, 2));
  console.log('Appended price_history for', espnId);
}

async function main() {
  for (const id of args) {
    const meta = await fetchPlayerMeta(id);
    writeAdvanced(id, meta || {});
    appendPriceHistory(id);
  }
  // regenerate index
  child.execSync('node scripts/regenerate_advanced_index.js', { stdio: 'inherit' });
  // download avatars for new ids
  const idsArg = args.join(' ');
  try {
    child.execSync(`node scripts/download_avatars.js`, { stdio: 'inherit' });
  } catch (e) {
    // ignore
  }
  // flush advanced cache
  try {
    await axios.post('http://localhost:3001/api/admin/flush-advanced-cache', null, { timeout: 3000 }).catch(()=>{});
    console.log('Requested advanced cache flush');
  } catch (e) {}
}

main().catch(e => { console.error(e); process.exit(1); });
