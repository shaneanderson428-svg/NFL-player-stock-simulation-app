import { NextResponse } from 'next/server';
import fs from 'fs/promises';
import path from 'path';

type WeeklyPoint = { week: number; price: number };

function toNumber(v: any): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

async function lookupPlayerInfo(id: string) {
  try {
    const csvPath = path.join(process.cwd(), 'data', 'player_stock_summary.csv');
    const raw = await fs.readFile(csvPath, 'utf8');
    const lines = raw.split(/\r?\n/).filter(Boolean);
    if (lines.length === 0) return { playerName: null, position: null };
    const header = lines[0].split(',');
    const idxPlayer = header.indexOf('player');
    const idxEspn = header.indexOf('espnId');
    const idxPos = header.indexOf('position');
    for (let i = 1; i < lines.length; i++) {
      const parts = lines[i].split(',');
      const espn = parts[idxEspn];
      if (String(espn) === String(id)) {
        return { playerName: parts[idxPlayer] || null, position: parts[idxPos] || null };
      }
    }
  } catch (e) {
    // ignore
  }
  // fallback to roster backup (simpler format)
  try {
    const rb = path.join(process.cwd(), 'data', 'roster_backup.csv');
    const raw2 = await fs.readFile(rb, 'utf8');
    const lines2 = raw2.split(/\r?\n/).filter(Boolean);
    for (const ln of lines2) {
      const parts = ln.split(',');
      if (String(parts[0]) === String(id)) return { playerName: parts[1] || null, position: parts[2] || null };
    }
  } catch (e) {
    // ignore
  }
  return { playerName: null, position: null };
}

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const id = url.searchParams.get('id');
    if (!id) return NextResponse.json({ playerId: null, playerName: null, position: null, currentPrice: null, weeklyHistory: [] });

    const DATA_DIR = path.join(process.cwd(), 'data');
    const file = path.join(DATA_DIR, 'history', `${id}.json`);

    try {
      const raw = await fs.readFile(file, 'utf8');
      if (!raw) {
        const info = await lookupPlayerInfo(id);
        return NextResponse.json({ playerId: Number(id), playerName: info.playerName, position: info.position, currentPrice: null, weeklyHistory: [] });
      }
      const obj = JSON.parse(raw);

      // Points may be stored in different shapes. Normalize all supported shapes
      let points: any[] = [];

      if (Array.isArray(obj)) {
        // legacy: file is an array of points
        points = obj;
      } else if (Array.isArray(obj.points)) {
        points = obj.points;
      } else if (Array.isArray(obj.history)) {
        points = obj.history;
      } else if (Array.isArray(obj.weeks) && Array.isArray(obj.prices) && obj.weeks.length === obj.prices.length) {
        points = obj.weeks.map((w: any, i: number) => ({ week: toNumber(w), price: toNumber(obj.prices[i]) }));
      } else if (obj.points && typeof obj.points === 'object') {
        // sometimes points stored as mapping
        points = Object.values(obj.points);
      }

      // Extract weekly points: prefer explicit {week, price} entries
      const weekly: WeeklyPoint[] = [];
      for (const p of points) {
        if (p == null) continue;
        if (p.week !== undefined && p.price !== undefined) {
          const w = toNumber(p.week);
          const pr = toNumber(p.price);
          if (w !== null && pr !== null) weekly.push({ week: w, price: pr });
        } else if (p.week !== undefined && p.p !== undefined) {
          const w = toNumber(p.week);
          const pr = toNumber(p.p);
          if (w !== null && pr !== null) weekly.push({ week: w, price: pr });
        }
      }

      // If none found, try to recover from last element (some writers append a weekly object at the end)
      if (weekly.length === 0 && points.length > 0) {
        const last = points[points.length - 1];
        if (last && (last.week !== undefined || last.price !== undefined || last.p !== undefined)) {
          const w = toNumber(last.week ?? last.w ?? null);
          const pr = toNumber(last.price ?? last.p ?? null);
          if (w !== null && pr !== null) weekly.push({ week: w, price: pr });
        }
      }

      // Sort by week ascending
      weekly.sort((a, b) => (a.week - b.week));

      const info = await lookupPlayerInfo(id);

      // Determine currentPrice: prefer last weekly price, else try last point p, else null
      let currentPrice: number | null = null;
      if (weekly.length) currentPrice = weekly[weekly.length - 1].price;
      else if (points.length) {
        const last = points[points.length - 1];
        const pf = toNumber(last.price ?? last.p ?? last.p ?? null);
        if (pf !== null) currentPrice = pf;
      }

      return NextResponse.json({ playerId: Number(id), playerName: info.playerName, position: info.position, currentPrice, weeklyHistory: weekly });
    } catch (e) {
      const info = await lookupPlayerInfo(id);
      // missing file or parse error -> return empty history
      return NextResponse.json({ playerId: Number(id), playerName: info.playerName, position: info.position, currentPrice: null, weeklyHistory: [] });
    }
  } catch (e) {
    return NextResponse.json({ playerId: null, playerName: null, position: null, currentPrice: null, weeklyHistory: [] });
  }
}
