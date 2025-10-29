import axios from 'axios';
import { NextResponse } from 'next/server';
import { computeInitialPriceFromStats } from '@/lib/pricing';

// Helper to parse numbers safely
function safeNum(v: any) {
  if (v == null) return 0;
  const s = String(v).replace(/[^0-9.\-]/g, '');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function computeDeltaFromStats(stats: { yards?: number; tds?: number; ints?: number; fumbles?: number }) {
  const yards = stats.yards ?? 0;
  const tds = stats.tds ?? 0;
  const ints = stats.ints ?? 0;
  const fumbles = stats.fumbles ?? 0;
  // demo weights
  const score = yards * 0.02 + tds * 6 - (ints + fumbles) * 3;
  return Number(score.toFixed(2));
}

// Build a season price series by examining athlete statistics splits (per-game)
export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const espnId = url.searchParams.get('espnId');
    const startPrice = Number(url.searchParams.get('startPrice') ?? 100);
    if (!espnId) return NextResponse.json({ ok: false, error: 'expected espnId query param' }, { status: 400 });

    const statsUrl = `https://site.api.espn.com/apis/common/v3/sports/football/nfl/athletes/${encodeURIComponent(espnId)}/statistics`;
    const resp = await axios.get(statsUrl, { timeout: 5000 });
    const data = resp?.data;

    // Look for splits or games arrays
    const candidates: any[] = [];
    if (Array.isArray(data?.splits)) candidates.push(...data.splits);
    // Some endpoints wrap recent games under statistics?.splits or similar
    if (Array.isArray(data?.statistics?.splits)) candidates.push(...data.statistics.splits);
    // Also look for nested arrays
    for (const k of Object.keys(data || {})) {
      const v = (data as any)[k];
      if (Array.isArray(v)) candidates.push(...v);
    }

    // Heuristic: each candidate that contains a date/opponent/game indicates a per-game split
    const games: any[] = [];
    for (const c of candidates) {
      if (!c || typeof c !== 'object') continue;
      // Accept if it has yards/tds or a label like 'game' or 'opponent'
      const hasStats = c.yards || c.yds || c.receptions || c.rushingYards || c.receivingYards || c.touchdowns || c.tds;
      if (hasStats) games.push(c);
    }

    if (games.length === 0) {
      return NextResponse.json({ ok: false, error: 'no per-game splits found for athlete' }, { status: 404 });
    }

    // Sort games by date if possible
    games.sort((a, b) => {
      const ta = Date.parse(a.date ?? a.gameDate ?? a.eventDate ?? a.season ?? '') || 0;
      const tb = Date.parse(b.date ?? b.gameDate ?? b.eventDate ?? b.season ?? '') || 0;
      return ta - tb;
    });

    // Build price series
    const series: Array<{ t: string; p: number; stats?: any }> = [];
    let curPrice = Number(startPrice) || 100;
    for (const g of games) {
      const yards = safeNum(g.yards ?? g.yds ?? g.passingYards ?? g.receivingYards ?? g.rushingYards ?? 0);
      const rec = safeNum(g.receptions ?? g.rec ?? g.receiving ?? 0);
      const rush = safeNum(g.rush ?? g.rushing ?? 0);
      const tds = safeNum(g.tds ?? g.touchdowns ?? g.touchdown ?? 0);
      const ints = safeNum(g.ints ?? g.interceptions ?? 0);
      const fumbles = safeNum(g.fumbles ?? g.fumblesLost ?? 0);
      const absDelta = computeDeltaFromStats({ yards, tds, ints, fumbles });
      const observedPct = curPrice > 0 ? absDelta / curPrice : 0;
      // cap per-week pct to ±10%
      const appliedPct = Math.max(-0.10, Math.min(0.10, observedPct));
      curPrice = Math.max(0.01, Number((curPrice * (1 + appliedPct)).toFixed(2)));
      const dateLabel = g.date ?? g.gameDate ?? g.eventDate ?? (g.season ? String(g.season) : new Date().toISOString());
      series.push({ t: dateLabel, p: curPrice, stats: { yards, rec, rush, tds, ints, fumbles } });
    }

    return NextResponse.json({ ok: true, espnId, startPrice, series });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}

// no default export — Next App Router expects named HTTP method exports (GET/POST/etc.)
