#!/usr/bin/env node
// Bulk downloader for avatars — saves to public/avatars/{espnId}.png
// Uses axios and optionally sharp if installed.

const fs = require('fs');
const path = require('path');
const axios = require('axios');
let sharp = null;
try { sharp = require('sharp'); } catch (e) { sharp = null; }

const DATA_INDEX = path.join(process.cwd(), 'data', 'advanced', 'index.json');
const OUT_DIR = path.join(process.cwd(), 'public', 'avatars');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const TEAM_MAP_PATH = path.join(process.cwd(), 'data', 'team-map.json');

function persistAltId(originalId, altId) {
  try {
    let map = {};
    if (fs.existsSync(TEAM_MAP_PATH)) {
      map = JSON.parse(fs.readFileSync(TEAM_MAP_PATH, 'utf8') || '{}');
    }
    const key = String(originalId);
    const existing = map[key] || {};
    if (String(existing.altEspnId || '') === String(altId)) return; // already set
    existing.altEspnId = String(altId);
    map[key] = existing;
    fs.writeFileSync(TEAM_MAP_PATH, JSON.stringify(map, null, 2), 'utf8');
    console.log(`Persisted altEspnId mapping: ${key} -> ${altId}`);
  } catch (e) {
    // ignore persistence errors
  }
}

function safeName(id) {
  return String(id).replace(/[^a-zA-Z0-9-_\.]/g, '_');
}

async function downloadForId(id, name) {
  const base = safeName(id);
  const outPath = path.join(OUT_DIR, `${base}.png`);
  if (fs.existsSync(outPath)) return { id, ok: true, cached: true };
  const urls = [
    `https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/${id}.png&w=256&h=256`,
    `https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/${id}.png&w=128&h=128`,
    `https://a.espncdn.com/i/headshots/nfl/players/full/${id}.png`,
  ];
  for (const u of urls) {
    try {
      const r = await axios.get(u, { responseType: 'arraybuffer', timeout: 8000, headers: { 'User-Agent': 'my-app-avatar-downloader/1.0' } });
      const ct = (r.headers['content-type'] || '').toLowerCase();
      if (!ct.startsWith('image/')) continue;
      const buf = Buffer.from(r.data);
      if (sharp) {
        await sharp(buf).png().toFile(outPath);
      } else {
        if (ct.includes('png')) fs.writeFileSync(outPath, buf);
        else {
          const ext = ct.includes('jpeg') || ct.includes('jpg') ? 'jpg' : (ct.includes('webp') ? 'webp' : 'bin');
          const fallback = path.join(OUT_DIR, `${base}.${ext}`);
          fs.writeFileSync(fallback, buf);
          return { id, ok: true, url: `/avatars/${base}.${ext}` };
        }
      }
      return { id, ok: true, url: `/avatars/${base}.png` };
    } catch (e) {
      // try next
    }
  }
  // If CDN patterns failed, attempt to query ESPN player API for a headshot URL
  try {
    const playerUrl = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/players/${id}`;
    let pr;
    try {
      pr = await axios.get(playerUrl, { timeout: 8000, headers: { 'User-Agent': 'my-app-avatar-downloader/1.0', Accept: 'application/json' } });
    } catch (err) {
      // If the id-based endpoint 404s, try a search by name (if provided)
      if (name) {
        try {
          const searchUrl = `https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/players?search=${encodeURIComponent(name)}`;
          const sr = await axios.get(searchUrl, { timeout: 8000, headers: { 'User-Agent': 'my-app-avatar-downloader/1.0', Accept: 'application/json' } });
          // try to find a player whose id or displayName matches
          const candidates = (sr.data?.items || []).flatMap((g) => (Array.isArray(g?.items) ? g.items : []));
          const foundPlayer = candidates.find((c) => {
            if (!c) return false;
            const pid = c?.id ?? c?.player?.id ?? c?.athlete?.id;
            const dname = c?.displayName || c?.player?.displayName || c?.athlete?.displayName || '';
            return String(pid) === String(id) || (dname && name && dname.toLowerCase().includes(String(name).toLowerCase()));
          });
          if (foundPlayer) {
            pr = { data: foundPlayer };
          }
        } catch (se) {
          // ignore search errors
        }
      }
    }
    const payload = pr?.data;
    // Deep search for a likely headshot URL in the payload
    const found = findHeadshotUrl(payload);
  if (found) {
      try {
        const rr = await axios.get(found, { responseType: 'arraybuffer', timeout: 8000, headers: { 'User-Agent': 'my-app-avatar-downloader/1.0' } });
        const ct2 = (rr.headers['content-type'] || '').toLowerCase();
        if (ct2.startsWith('image/')) {
          const buf2 = Buffer.from(rr.data);
          if (sharp) {
            await sharp(buf2).png().toFile(outPath);
          } else {
            if (ct2.includes('png')) fs.writeFileSync(outPath, buf2);
            else {
              const ext = ct2.includes('jpeg') || ct2.includes('jpg') ? 'jpg' : (ct2.includes('webp') ? 'webp' : 'bin');
              const fallback = path.join(OUT_DIR, `${base}.${ext}`);
              fs.writeFileSync(fallback, buf2);
              return { id, ok: true, url: `/avatars/${base}.${ext}` };
            }
          }
          return { id, ok: true, url: `/avatars/${base}.png` };
        }
      } catch (e) {
        // ignore and fall through
      }
    }
  } catch (e) {
    // ignore errors from ESPN API
  }

  // If player-level lookup failed, try team-based roster lookup using local team-map
  try {
    const TEAM_MAP_PATH = path.join(process.cwd(), 'data', 'team-map.json');
    if (fs.existsSync(TEAM_MAP_PATH)) {
      const teamMap = JSON.parse(fs.readFileSync(TEAM_MAP_PATH, 'utf8') || '{}');
      const teamObj = teamMap[String(id)] || teamMap[String(id)] === undefined ? null : null;
      // If team not keyed by espnId, try reverse lookup by espnId key already present
      let teamAbbr = null;
      if (teamObj && teamObj.abbreviation) teamAbbr = teamObj.abbreviation;
      // Alternatively, if name provided, search team-map values for matching name (loose)
      if (!teamAbbr && name) {
        for (const [k, v] of Object.entries(teamMap)) {
          if (v && v.name && String(name).toLowerCase().includes(String(v.name).toLowerCase().split(' ')[0])) {
            teamAbbr = v.abbreviation;
            break;
          }
        }
      }
      if (teamAbbr) {
        // fetch teams list and find team id
        try {
          const teamsResp = await axios.get('https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams', { timeout: 8000 });
          const teamsList = (teamsResp.data && (teamsResp.data.teams || teamsResp.data.sports?.[0]?.leagues?.[0]?.teams)) || [];
          let teamId = null;
          for (const t of teamsList) {
            const ab = (t?.abbreviation || t?.team?.abbreviation || '').toString().toUpperCase();
            const idnum = Number(t?.id ?? t?.team?.id ?? NaN);
            if (ab === String(teamAbbr).toUpperCase()) { teamId = idnum; break; }
          }
          if (teamId) {
            const rosterUrl = `https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/${teamId}/roster`;
            const rr = await axios.get(rosterUrl, { timeout: 8000 });
            const payload = rr.data;
            // search payload for headshot href
            const found = findHeadshotUrl(payload) || null;
            if (found) {
              try {
                const resp2 = await axios.get(found, { responseType: 'arraybuffer', timeout: 8000 });
                const ct2 = (resp2.headers['content-type'] || '').toLowerCase();
                if (ct2.startsWith('image/')) {
                  const buf2 = Buffer.from(resp2.data);
                  if (sharp) await sharp(buf2).png().toFile(outPath);
                  else if (ct2.includes('png')) fs.writeFileSync(outPath, buf2);
                  else {
                    const ext = ct2.includes('jpeg') || ct2.includes('jpg') ? 'jpg' : (ct2.includes('webp') ? 'webp' : 'bin');
                    const fallback = path.join(OUT_DIR, `${base}.${ext}`);
                    fs.writeFileSync(fallback, buf2);
                    return { id, ok: true, url: `/avatars/${base}.${ext}` };
                  }
                  return { id, ok: true, url: `/avatars/${base}.png` };
                }
              } catch (e) {
                // ignore
              }
            }
            // If roster payload didn't directly include headshot, iterate roster items for matching player
            const arrays = [];
            if (Array.isArray(payload?.items)) arrays.push(...payload.items.flatMap(g => Array.isArray(g.items) ? g.items : []));
            if (Array.isArray(payload?.athletes)) arrays.push(...payload.athletes);
            if (Array.isArray(payload?.players)) arrays.push(...payload.players);
            for (const it of arrays) {
              // try to match by id or name
              const candidateId = it?.id ?? it?.player?.id ?? it?.athlete?.id ?? null;
              const candidateName = it?.displayName || it?.player?.displayName || it?.athlete?.displayName || it?.fullName || it?.name;
              if (String(candidateId) === String(id) || (name && candidateName && String(candidateName).toLowerCase().includes(String(name).toLowerCase().split(' ')[0]))) {
                const head = it?.headshot?.href || it?.player?.headshot?.href || it?.athlete?.headshot?.href || it?.person?.headshot?.href || null;
                if (head) {
                  try {
                    const r2 = await axios.get(head, { responseType: 'arraybuffer', timeout: 8000 });
                    const ct3 = (r2.headers['content-type'] || '').toLowerCase();
                    if (ct3.startsWith('image/')) {
                      const buf3 = Buffer.from(r2.data);
                      if (sharp) await sharp(buf3).png().toFile(outPath);
                      else if (ct3.includes('png')) fs.writeFileSync(outPath, buf3);
                      else {
                        const ext = ct3.includes('jpeg') || ct3.includes('jpg') ? 'jpg' : (ct3.includes('webp') ? 'webp' : 'bin');
                        const fallback = path.join(OUT_DIR, `${base}.${ext}`);
                        fs.writeFileSync(fallback, buf3);
                        return { id, ok: true, url: `/avatars/${base}.${ext}` };
                      }
                      return { id, ok: true, url: `/avatars/${base}.png` };
                    }
                  } catch (e) {
                    // ignore
                  }
                }
              }
            }
          }
        } catch (e) {
          // ignore teams fetch errors
        }
      }
    }
  } catch (e) {
    // ignore overall team-map lookup errors
  }

  return { id, ok: false };
}

// Helper: recursively search object for a string that looks like an ESPN headshot URL
function findHeadshotUrl(obj) {
  if (!obj) return null;
  const seen = new Set();
  const stack = [obj];
  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur === 'string') {
      if (typeof cur === 'string') {
        const s = cur;
        if (/a\.espncdn\.com/.test(s) && /headshots|headshot|i\/headshots/.test(s)) return s;
        if (/https?:\/\/.+\.(png|jpe?g|webp)/i.test(s) && /headshots/.test(s)) return s;
      }
      continue;
    }
    if (Array.isArray(cur)) {
      for (const v of cur) stack.push(v);
    } else if (typeof cur === 'object') {
      for (const k of Object.keys(cur)) {
        const v = cur[k];
        if (typeof v === 'string') {
          const s = v;
          if (/a\.espncdn\.com/.test(s) && /headshots|headshot|i\/headshots/.test(s)) return s;
          if (/https?:\/\/.+\.(png|jpe?g|webp)/i.test(s) && /headshots/.test(s)) return s;
        } else if (typeof v === 'object' && !seen.has(v)) {
          seen.add(v);
          stack.push(v);
        }
      }
    }
  }
  return null;

}

(async function main() {
  if (!fs.existsSync(DATA_INDEX)) {
    console.error('data/advanced/index.json not found');
    process.exit(1);
  }
  const idx = JSON.parse(fs.readFileSync(DATA_INDEX, 'utf8') || '{}');
  // idx may be array with players or flat map. Normalize to array of { espnId, name }
  let espnItems = [];
  if (Array.isArray(idx.players)) {
    espnItems = idx.players.map(p => ({ espnId: p.espnId || p.id, name: p.name }));
  } else if (Array.isArray(idx)) {
    espnItems = idx.map((p) => ({ espnId: p.espnId || p.id, name: p.name || (p.player || (p.playerName || undefined)) }));
  } else if (typeof idx === 'object') {
    espnItems = Object.keys(idx).map(k => ({ espnId: Number(k), name: undefined }));
  }
  // If some names are missing, try reading corresponding advanced files to extract player name
  const filled = [];
  for (const it of espnItems) {
    let name = it.name;
    if ((!name || name === undefined) && it.espnId) {
      const fp = path.join(process.cwd(), 'data', 'advanced', `${it.espnId}.json`);
      try {
        if (fs.existsSync(fp)) {
          const raw = JSON.parse(fs.readFileSync(fp, 'utf8') || '{}');
          name = raw?.player ?? raw?.playerName ?? raw?.name ?? name;
        }
      } catch (e) {
        // ignore
      }
    }
    if (it.espnId) filled.push({ espnId: it.espnId, name: name });
  }
  const espnIds = [...new Set(filled.map(f => f.espnId))].slice(0, 1000);
  console.log('Attempting to download', espnIds.length, 'avatars');
  for (const id of espnIds) {
    const item = filled.find(f => f.espnId === id) || { name: undefined };
    const res = await downloadForId(id, item.name);
    console.log(id, res.ok ? (res.cached ? 'cached' : (res.url || 'downloaded')) : 'failed');
  }
})();
