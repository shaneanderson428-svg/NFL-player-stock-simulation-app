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

// Load roster backup CSV to help resolve non-numeric espnIds -> numeric ids
const ROSTER_PATH = path.join(process.cwd(), 'data', 'roster_backup.csv');
let rosterByName = new Map();
let rosterBySlug = new Map();
function slugifyName(n){
  return String(n || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '');
}
try{
  if (fs.existsSync(ROSTER_PATH)){
    const txt = fs.readFileSync(ROSTER_PATH, 'utf8');
    const lines = txt.split(/\r?\n/).slice(1).filter(Boolean);
    for (const line of lines){
      const parts = line.split(',');
      const espnId = parts[0];
      const name = parts[1];
      if (!espnId || !name) continue;
      rosterByName.set(String(name).toLowerCase(), espnId);
      rosterBySlug.set(slugifyName(name), espnId);
    }
  }
}catch(e){
  // ignore
}

async function fetchPlayerMeta(espnId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/players/${espnId}`;
  try {
    const r = await axios.get(url, { timeout: 8000, headers: { Accept: 'application/json' } });
    return r.data;
  } catch (e) {
    return null;
  }
}

async function searchEspnByName(name){
  try{
    const url = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/players?search=${encodeURIComponent(name)}`;
    const r = await axios.get(url, { timeout: 8000, headers: { Accept: 'application/json' } });
    const data = r.data || {};
    // Try a few common fields where search results may appear
    const candidates = data?.players || data?.athletes || data?.results || data?.items || [];
    if (Array.isArray(candidates) && candidates.length > 0){
      // try to find first entry with an id
      for (const c of candidates){
        const id = c?.id || c?.athleteId || c?.uid || c?.espnId;
        if (id && String(id).match(/^\d+$/)) return String(id);
        // sometimes nested
        if (c?.athlete && c.athlete.id) return String(c.athlete.id);
      }
    }
  }catch(e){
    // ignore
  }
  return null;
}

async function resolveEspnId(candidate, fallbackName){
  const s = String(candidate || '').trim();
  if (!s) return null;
  if (/^\d+$/.test(s)) return s;
  // check roster by exact name
  const byName = rosterByName.get(s.toLowerCase());
  if (byName) return String(byName);
  // check slug map
  const slug = slugifyName(s);
  const bySlug = rosterBySlug.get(slug);
  if (bySlug) return String(bySlug);
  // try fallbackName if provided
  if (fallbackName){
    const byName2 = rosterByName.get(String(fallbackName).toLowerCase());
    if (byName2) return String(byName2);
    const bySlug2 = rosterBySlug.get(slugifyName(fallbackName));
    if (bySlug2) return String(bySlug2);
  }
  // last resort: search ESPN
  const found = await searchEspnByName(s || fallbackName || '');
  if (found) return found;
  return null;
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
  const added = [];
  for (const rawId of args) {
    const arg = String(rawId || '').trim();
    if (!arg){
      console.warn('Skipping empty argument');
      continue;
    }

    // Resolve numeric espnId if needed
    let resolvedId = await resolveEspnId(arg);
    let meta = null;

    if (!resolvedId){
      // If arg looks like a name, try to fetch meta by search; but fetchPlayerMeta requires numeric id
      console.warn(`⚠️  Could not resolve numeric ESPN id for '${arg}'. Attempting to use as name lookup via ESPN.`);
      const searchId = await resolveEspnId(arg, null);
      if (searchId) resolvedId = searchId;
    }

    if (!resolvedId){
      console.warn(`Skipping '${arg}': missing or invalid espnId and no match found in roster or ESPN.`);
      continue;
    }

    try{
      meta = await fetchPlayerMeta(resolvedId);
    }catch(e){
      meta = null;
    }

    writeAdvanced(resolvedId, meta || {});
    appendPriceHistory(resolvedId);
    added.push(resolvedId);
  }

  // regenerate index
  child.execSync('node scripts/regenerate_advanced_index.js', { stdio: 'inherit' });
  // download avatars for new ids
  try {
    child.execSync(`node scripts/download_avatars.js`, { stdio: 'inherit' });
  } catch (e) {
    // ignore
  }

  // flush advanced cache (POST to dev server port 3000)
  const flushUrl = process.env.FLUSH_URL || 'http://localhost:3000/api/admin/flush-advanced-cache';
  try{
    await axios.post(flushUrl, null, { timeout: 3000 });
    console.log('Requested advanced cache flush');
  }catch(e){
    console.warn('⚠️ Cache flush failed, restart dev server manually');
  }
}

main().catch(e => { console.error(e); process.exit(1); });
