#!/usr/bin/env python3
"""
Compute weekly prices from weekly CSVs and write per-week price CSV to data/prices/{season}/week_{week}.csv

Behavior:
- Reads data/weekly/{season}/week_{week}.csv (or per-position week_{week}_POS.csv)
- Computes z-scores per-position (or across all if position missing)
- Computes delta using same formula as backfill and applies to previous price when available
- Writes CSV with columns: playerId, playerName, position, week, price
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import csv
from collections import defaultdict
from statistics import mean, pstdev
from typing import List


DATA_DIR = os.path.join(os.getcwd(), 'data')
WEEKLY_BASE = os.path.join(DATA_DIR, 'weekly')
PRICES_BASE = os.path.join(DATA_DIR, 'prices')


def safe_num(v):
    try:
        if v is None or v == '':
            return 0.0
        return float(str(v).strip())
    except Exception:
        return 0.0


def compute_zscores(values: List[float]):
    if not values:
        return []
    m = mean(values)
    sd = pstdev(values) if len(values) > 1 else 0.0
    if sd == 0:
        return [0.0 for _ in values]
    return [(v - m) / sd for v in values]


def computeWeeklyDelta(z_epa, z_yards, z_tds, z_vol):
    delta_raw = 0.35 * z_epa + 0.30 * z_yards + 0.25 * z_tds + 0.10 * z_vol
    delta_raw = max(-0.10, min(0.10, delta_raw))
    if abs(delta_raw) < 0.005:
        return 0.0
    return delta_raw


def find_weekly_csv(season: int, week: int):
    # For CSV-only mode prefer the normalized player_stats file and do NOT call external APIs.
    candidate = os.path.join(WEEKLY_BASE, f'player_stats_{season}_week_{week}.csv')
    if os.path.exists(candidate):
        return candidate
    # If the canonical player_stats CSV is not present, do not fallback to other sources in CSV-only mode.
    return None


def read_rows(path):
    rows = []
    with open(path, newline='', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append(r)
    return rows


def write_prices(season: int, week: int, rows):
    outdir = os.path.join(PRICES_BASE, str(season))
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f'week_{week}.csv')
    fieldnames = ['playerId', 'week', 'price']
    with open(outpath, 'w', newline='', encoding='utf8') as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Wrote {len(rows)} prices to {outpath}")
    return outpath


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    ap.add_argument('--week', type=int, required=True)
    args = ap.parse_args()

    path = find_weekly_csv(args.season, args.week)
    if not path:
        print(f"ERROR: No weekly player_stats CSV found for season={args.season} week={args.week}. Expected: {os.path.join(WEEKLY_BASE, f'player_stats_{args.season}_week_{args.week}.csv')}")
        sys.exit(2)

    print(f"Computing prices for season={args.season} week={args.week} from {path}")
    rows = read_rows(path)
    if not rows:
        print(f"ERROR: Weekly CSV at {path} contains no player rows (header-only). Aborting.")
        sys.exit(3)

    # parse rows into numeric stats
    parsed = []
    for r in rows:
        pid = str(r.get('playerId') or r.get('player_id') or r.get('playerID') or r.get('espnId') or '').strip()
        if not pid:
            continue
        epa = safe_num(r.get('epa'))
        yards = safe_num(r.get('yards') or r.get('recYds'))
        tds = safe_num(r.get('tds'))
        vol = safe_num(r.get('targets')) + safe_num(r.get('receptions')) + safe_num(r.get('carries'))
        parsed.append({'playerId': pid, 'epa': epa, 'yards': yards, 'tds': tds, 'vol': vol})

    # Absolute performance scoring (per-player, no cohort normalization)
    # score = (epa * 12) + (tds * 6) + (yards * 0.08) + ((targets + carries) * 0.4)
    # delta_pct = clamp(score / 100, -0.15, +0.15)
    def clamp(v, lo, hi):
        return max(lo, min(hi, v))

    # read prior prices if exist to chain changes
    prev_prices = {}
    prev_path = os.path.join(PRICES_BASE, str(args.season), f'week_{args.week - 1}.csv')
    if os.path.exists(prev_path):
        try:
            prev_rows = read_rows(prev_path)
            for r in prev_rows:
                prev_prices[str(r.get('playerId'))] = safe_num(r.get('price'))
        except Exception:
            prev_prices = {}
    # If previous-week prices file is missing, try to seed from per-player history files
    if not prev_prices:
        hist_dir = os.path.join(os.getcwd(), 'data', 'history')
        for p in parsed:
            pid = str(p['playerId'])
            hist_f = os.path.join(hist_dir, f"{pid}_price_history.json")
            if os.path.exists(hist_f):
                try:
                    import json as _json
                    with open(hist_f, 'r', encoding='utf8') as fh:
                        h = _json.load(fh)
                        if isinstance(h, list) and h:
                            last = h[-1]
                            prev_prices[pid] = safe_num(last.get('price'))
                except Exception:
                    continue

    out_rows = []
    for p in parsed:
        pid = p['playerId']
        # compute raw score
        score = (p['epa'] * 12.0) + (p['tds'] * 6.0) + (p['yards'] * 0.08) + (p['vol'] * 0.4)
        delta_pct = clamp(score / 100.0, -0.15, 0.15)
        # tiny movements under 0.005 treated as no-change
        if abs(delta_pct) < 0.005:
            delta_pct = 0.0
        base = prev_prices.get(pid, 100.0)
        price = round(base * (1.0 + float(delta_pct)), 2)
        out_rows.append({'playerId': pid, 'week': args.week, 'price': price})

    write_prices(args.season, args.week, out_rows)


if __name__ == '__main__':
    main()
