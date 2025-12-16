import fs from 'fs/promises';
import path from 'path';
import { NextResponse } from 'next/server';
import { parse } from 'csv-parse/sync';

type PricePoint = { t: string; p: number; yoe?: number; roe?: number; toe?: number; uer?: number; pis?: number; its?: number };

function synthHistory(base: number): PricePoint[] {
  const now = new Date();
  const days = (n: number) => {
    const d = new Date(now);
    d.setDate(d.getDate() - n);
    return d.toISOString().slice(0, 10);
  };
  return Array.from({ length: 8 }, (_, i) => {
    const drift = Math.sin(i / 2) * 2;
    const noise = (Math.random() * 2 - 1);
    // synth history includes advanced metrics as placeholders (0) so the UI can read fields
    return { t: days(7 - i), p: +(base + drift + noise).toFixed(2), yoe: 0, roe: 0, toe: 0, uer: 0, pis: 0, its: 0 } as PricePoint;
  });
}

// API route: /api/tank01/wr
// Reads the CSV produced by scripts/fetch_tank01_week.py and returns JSON
// The CSV is expected at external/tank01/player_stats_week_13.csv (or change path below)

export async function GET() {
  try {
    const csvPath = path.join(process.cwd(), 'external', 'tank01', 'player_stats_week_13.csv');
    const raw = await fs.readFile(csvPath, 'utf8');
    const records = parse(raw, { columns: true, skip_empty_lines: true }) as any[];

    // Try to load optional history file (external/history/wr_price_history.json)
    const historyPath = path.join(process.cwd(), 'external', 'history', 'wr_price_history.json');
    let historyMap: Record<string, any> = {};
    try {
      const histRaw = await fs.readFile(historyPath, 'utf8');
      historyMap = JSON.parse(histRaw || '{}');
    } catch (e) {
      // missing history is OK; we'll synthesize per-player histories below
      historyMap = {};
    }

    // Attach priceHistory to each record (mimic existing Athlete.priceHistory shape)
    const rows = records.map((r) => {
      const espnId = String(r.playerID ?? r.espnID ?? r.espnid ?? '') || '';
      let priceHistory: PricePoint[] = [];
      if (espnId && historyMap[espnId]) {
        priceHistory = historyMap[espnId];
      } else {
        // Attempt to derive a sensible base price from available fields
        const fp = r['fantasyPoints'] ?? r['fantasyPointsDefault.standard'] ?? r['fantasyPointsDefault'] ?? r.fantasyPointsDefault;
        const base = Number(fp) && !Number.isNaN(Number(fp)) ? Math.max(5, Number(fp) * 4 + 50) : 100;
        priceHistory = synthHistory(base);
      }

      // compute percent change from last two points (latest vs prior)
      let pricePercentChange: number | null = null;
      try {
        if (Array.isArray(priceHistory) && priceHistory.length >= 2) {
          // attempt to sort by timestamp string if present
          const copy = [...priceHistory].slice();
          try {
            copy.sort((a, b) => (String(a.t) < String(b.t) ? -1 : String(a.t) > String(b.t) ? 1 : 0));
          } catch (e) {
            // ignore
          }
          const last = copy[copy.length - 1];
          const prev = copy[copy.length - 2];
          const latest = Number(last.p ?? 0);
          const prior = Number(prev.p ?? 0);
          if (Number.isFinite(latest) && Number.isFinite(prior) && prior !== 0) {
            pricePercentChange = Math.round(((latest - prior) / Math.abs(prior)) * 100 * 100) / 100;
          }
        }
      } catch (e) {
        pricePercentChange = null;
      }

      // attach last advanced metrics (if present)
      const last = Array.isArray(priceHistory) && priceHistory.length ? priceHistory[priceHistory.length - 1] : null;
      const yoe = last?.yoe ?? null;
      const roe = last?.roe ?? null;
      const toe = last?.toe ?? null;
      const uer = last?.uer ?? null;
      const pis = last?.pis ?? null;
      const its = last?.its ?? null;

      return { ...r, priceHistory, pricePercentChange, yoe, roe, toe, uer, pis, its };
    });

    // return as JSON array
    return NextResponse.json({ ok: true, rows });
  } catch (e: any) {
    // Helpful error for local dev when CSV doesn't exist yet
    return NextResponse.json({ ok: false, error: String(e?.message ?? e) }, { status: 500 });
  }
}
