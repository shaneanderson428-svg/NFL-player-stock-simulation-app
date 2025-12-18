#!/usr/bin/env python3
"""
Backfill historical weekly prices for a season into per-player JSONs under
`data/history/{season}/{playerId}.json` and generate `data/history/{season}/index.json`.

Usage:
  python3 scripts/backfill_historical_prices.py --season 2025

This script discovers weekly files under `data/weekly/{season}/week_*.csv`,
processes them in order, computes weekly deltas (same coeffs as other scripts),
and writes per-player histories (overwrites existing). It also writes an index.json
for frontend consumption.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from collections import defaultdict
from statistics import mean, pstdev
from datetime import datetime


DATA_DIR = os.path.join(os.getcwd(), 'data')
WEEKLY_BASE = os.path.join(DATA_DIR, 'weekly')
HISTORY_BASE = os.path.join(DATA_DIR, 'history')


def ensure_dirs(season: int):
    d = os.path.join(HISTORY_BASE, str(season))
    os.makedirs(d, exist_ok=True)
    return d


def safe_num(v):
    try:
        if v is None or v == '':
            return 0.0
        return float(str(v).strip())
    except Exception:
        return 0.0


def compute_zscores(values):
    if not values:
        return []
    m = mean(values)
    sd = pstdev(values) if len(values) > 1 else 0.0
    if sd == 0:
        return [0.0 for _ in values]
    return [(v - m) / sd for v in values]


def computeWeeklyDelta(row, z_epa, z_yards, z_tds, z_vol):
    delta_raw = 0.35 * z_epa + 0.30 * z_yards + 0.25 * z_tds + 0.10 * z_vol
    delta_raw = max(-0.10, min(0.10, delta_raw))
    if abs(delta_raw) < 0.005:
        return 0.0
    return delta_raw


def discover_weeks(season: int):
    pattern = os.path.join(WEEKLY_BASE, str(season), f'week_*.csv')
    paths = glob.glob(pattern)
    weeks = set()
    for p in paths:
        bn = os.path.basename(p)
        try:
            import re

            m = re.search(rf'week_(\d+)(?:_.*)?\.csv$', bn)
            if m:
                w = int(m.group(1))
                weeks.add(w)
        except Exception:
            continue
    return sorted(list(weeks))


def read_stats_csv(path):
    rows = []
    with open(path, newline='', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append(r)
    return rows



def parse_rows(rows, season, week):
    parsed = []
    for r in rows:
        try:
            pid = r.get('playerId') or r.get('player_id') or r.get('playerID') or r.get('id') or r.get('espnId')
            if pid is None or str(pid).strip() == '':
                continue
            playerId = int(str(pid).strip())
            season_v = int(str(r.get('season', season)))
            week_v = int(str(r.get('week', week)))
            if season_v != season or week_v != week:
                continue
            epa = safe_num(r.get('epa'))
            yards = safe_num(r.get('yards'))
            tds = safe_num(r.get('tds'))
            targets = safe_num(r.get('targets'))
            receptions = safe_num(r.get('receptions'))
            carries = safe_num(r.get('carries'))
            position = (r.get('position') or r.get('pos') or '').strip() or None
            parsed.append({
                'playerId': playerId,
                'season': season_v,
                'week': week_v,
                'epa': epa,
                'yards': yards,
                'tds': tds,
                'targets': targets,
                'receptions': receptions,
                'carries': carries,
                'position': position,
            })
        except Exception:
            continue
    return parsed


def backfill(season: int):
    hist_dir = ensure_dirs(season)
    weeks = discover_weeks(season)
    if not weeks:
        print(f"[backfill] no weekly files found for season {season} in {os.path.join(WEEKLY_BASE, str(season))}")
        return
    print(f"[backfill] discovered weeks: {weeks}")

    # persistent structures
    price_map = {}  # playerId -> current price
    histories = defaultdict(list)  # playerId -> list of {week, price}
    positions = {}

    for week in weeks:
        # prefer canonical merged weekly file, fallback to per-position files
        path = os.path.join(WEEKLY_BASE, str(season), f'week_{week}.csv')
        if not os.path.exists(path):
            found = glob.glob(os.path.join(WEEKLY_BASE, str(season), f'week_{week}_*.csv'))
            if not found:
                print(f"[backfill] missing file for week {week}, skipping")
                continue
            path = found[0]
        print(f"[backfill] processing week {week} -> {path}")
        rows = read_stats_csv(path)
        parsed = parse_rows(rows, season, week)

        # grouping
        has_positions = any(bool(p.get('position')) for p in parsed)
        if not has_positions:
            print('[backfill] WARNING: no position column found; computing z-scores across all players')

        groups = defaultdict(list)
        for p in parsed:
            key = p['position'] if (p['position'] and has_positions) else 'ALL'
            groups[key].append(p)
            try:
                positions[p['playerId']] = p.get('position') or positions.get(p['playerId'])
            except Exception:
                pass

        # compute z-scores per group
        zscores = {}
        for key, items in groups.items():
            epas = [it['epa'] for it in items]
            yards = [it['yards'] for it in items]
            tds = [it['tds'] for it in items]
            vols = [(it['targets'] + it['receptions'] + it['carries']) for it in items]
            z_epas = compute_zscores(epas)
            z_yards = compute_zscores(yards)
            z_tds = compute_zscores(tds)
            z_vols = compute_zscores(vols)
            for idx, it in enumerate(items):
                zscores[it['playerId']] = (z_epas[idx], z_yards[idx], z_tds[idx], z_vols[idx])

        # apply deltas
        for p in parsed:
            pid = p['playerId']
            z_epa, z_yards, z_tds, z_vol = zscores.get(pid, (0.0, 0.0, 0.0, 0.0))
            delta = computeWeeklyDelta(p, z_epa, z_yards, z_tds, z_vol)
            prev_price = price_map.get(pid, 100.0)
            new_price = round(prev_price * (1.0 + float(delta)), 2)
            price_map[pid] = new_price
            histories[pid].append({'week': week, 'price': new_price})

    # write per-player histories and index
    written = 0
    for pid, hist in histories.items():
        fout = os.path.join(hist_dir, f"{pid}.json")
        obj = {'playerId': int(pid), 'position': positions.get(pid) or None, 'history': hist}
        with open(fout, 'w', encoding='utf8') as fh:
            json.dump(obj, fh, indent=2)
        written += 1

    print(f"[backfill] Completed. weeks={len(weeks)} players_updated={written}")
    # print sample
    sample = list(histories.items())[:5]
    for pid, hist in sample:
        print(f"[backfill] sample {pid} -> {hist[:5]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    args = ap.parse_args()
    backfill(args.season)


if __name__ == '__main__':
    main()
