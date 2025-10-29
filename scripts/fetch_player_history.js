#!/usr/bin/env node
// Simple script to fetch a single player's data and print priceHistory + trend
// Usage: node scripts/fetch_player_history.js [playerId] [baseUrl]
// Example: node scripts/fetch_player_history.js b-mayfield http://localhost:3004

const playerId = process.argv[2] || 'b-mayfield';
const base = process.argv[3] || process.env.BASE_URL || 'http://localhost:3004';
const url = `${base.replace(/\/$/, '')}/api/nfl/stocks`;

async function main() {
  try {
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) {
      console.error(`Failed to fetch ${url}: ${res.status} ${res.statusText}`);
      process.exit(2);
    }
    const json = await res.json();
    const players = Array.isArray(json?.players) ? json.players : (Array.isArray(json?.rows) ? json.rows : []);
    if (!players || players.length === 0) {
      console.error('No players array found in API response.');
      process.exit(2);
    }

    const idLower = String(playerId).toLowerCase();
    const found = players.find(p => {
      const pids = [p.espnId, p.id, p.uid, p.espn_id, p.espnid].map(x => String(x || '').toLowerCase());
      if (pids.includes(idLower)) return true;
      const name = String(p.player || p.name || '').toLowerCase();
      if (!name) return false;
      return name.includes(idLower) || idLower.includes(name.split(' ').slice(-1)[0]);
    });

    if (!found) {
      console.error(`Player with id/name '${playerId}' not found in /api/nfl/stocks response.`);
      // show a brief listing of candidate ids (first 10)
      console.error('Sample players (first 10):');
      players.slice(0, 10).forEach(p => console.error('-', p.espnId || p.id || p.player || p.name));
      process.exit(3);
    }

    // Print the found player basic info
    console.log('Found player:');
    console.log(JSON.stringify({ player: found.player || found.name || found.fullName || found.id, espnId: found.espnId || found.id }, null, 2));

    // Normalize different possible fields for price history. Accept several shapes:
    // - priceHistory: [{t,p}, ...]
    // - history: [{week,stock,...}, ...] (CSV-produced)
    // - price_history
    let raw = found.priceHistory || found.price_history || found.history || found.priceHistoryPoints || null;
    if (!Array.isArray(raw) || raw.length === 0) {
      console.log('No priceHistory/history found for this player.');
      process.exit(0);
    }

    // Normalize to points { t, p } and order newest -> oldest
    const points = raw.map((pt) => {
      if (pt == null) return null;
      if (typeof pt === 'number') return { t: String(pt), p: Number(pt) };
      if (typeof pt === 'object') {
        // CSV history rows often have { week, stock, confidence }
        if ('stock' in pt) {
          const t = pt.t ?? (pt.week !== undefined ? String(pt.week) : pt.date ?? '?');
          const p = Number(pt.stock ?? pt.p ?? pt.price ?? NaN);
          return { t, p };
        }
        if ('p' in pt || 'price' in pt) {
          const t = pt.t ?? pt.date ?? '?';
          const p = Number(pt.p ?? pt.price ?? NaN);
          return { t, p };
        }
        // fallback: try first two array-like entries
        const vals = Object.values(pt);
        const t = String(vals[0] ?? '?');
        const p = Number(vals[1] ?? vals[0] ?? NaN);
        return { t, p };
      }
      return null;
    }).filter(Boolean);

    if (points.length === 0) {
      console.log('\nCould not extract numeric price points from history.');
      process.exit(0);
    }

    // If points appear oldest->newest (CSV), reverse to newest->oldest for our display logic
    const newestFirst = points.slice().reverse();

    console.log('\npriceHistory (newest -> oldest):');
    newestFirst.forEach((pt, i) => console.log(`${i}: ${pt.t} -> ${pt.p}`));

    const newest = Number(newestFirst[0].p);
    const oldest = Number(newestFirst[newestFirst.length - 1].p);

    if (Number.isNaN(newest) || Number.isNaN(oldest)) {
      console.log('\nCould not determine numeric newest/oldest prices (non-numeric values).');
      process.exit(0);
    }

    console.log(`\nNewest: ${newest}, Oldest: ${oldest}`);
    if (newest > oldest) {
      console.log('UP ✅');
      console.log('Suggested chart color: #4ade80 (green)');
    } else if (newest < oldest) {
      console.log('DOWN 🔻');
      console.log('Suggested chart color: #f87171 (red)');
    } else {
      console.log('UNCHANGED — neutral color');
      console.log('Suggested chart color: #9aa (neutral)');
    }

    process.exit(0);
  } catch (err) {
    console.error('Error fetching or processing data:', err);
    process.exit(1);
  }
}

// Node 18+ has global fetch; if not available, inform the user.
if (typeof fetch !== 'function') {
  console.error('This script requires a Node runtime with global fetch (Node 18+).');
  console.error('Alternatively run with: node --experimental-fetch ... or use the Python version.');
  process.exit(1);
}

main();
