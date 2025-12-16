#!/usr/bin/env python3
"""
Run weekly price update from CSV weekly stats.

Usage:
  python3 scripts/run_weekly_price_update.py --season 2025 --week 15

Notes / assumptions:
- CSV is expected at data/weekly/player_stats_{season}_week_{week}.csv
- Required columns: playerId, week, season, epa, yards, tds, targets, receptions, carries
- If CSV contains a 'position' column, z-scores are computed within each position.
  Otherwise, z-scores are computed across all players (logged).
"""
import argparse
import csv
import json
import math
import os
from collections import defaultdict
from datetime import datetime
from statistics import mean, pstdev


DATA_DIR = os.path.join(os.getcwd(), 'data')
HISTORY_DIR = os.path.join(DATA_DIR, 'history')


def ensure_dirs():
    os.makedirs(HISTORY_DIR, exist_ok=True)


def read_stats_csv(season: int, week: int):
    path = os.path.join(DATA_DIR, f'weekly/player_stats_{season}_week_{week}.csv')
    print(f"[weekly-update] Reading stats from {path}")
    rows = []
    with open(path, newline='', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append(r)
    print(f"[weekly-update] Read {len(rows)} rows")
    return rows


def safe_num(v):
    try:
        if v is None or v == '':
            return 0.0
        return float(str(v).strip())
    except Exception:
        return 0.0


def compute_zscores(values):
    # population stdev (pstdev) to be stable for small groups
    if not values:
        return [0.0] * 0
    m = mean(values)
    sd = pstdev(values) if len(values) > 1 else 0.0
    if sd == 0:
        return [0.0 for _ in values]
    return [(v - m) / sd for v in values]


def computeWeeklyDelta(row, z_epa, z_yards, z_tds, z_vol):
    # Weighted score
    delta_raw = 0.35 * z_epa + 0.30 * z_yards + 0.25 * z_tds + 0.10 * z_vol
    # Clamp
    delta_raw = max(-0.10, min(0.10, delta_raw))
    if abs(delta_raw) < 0.005:
        return 0.0
    return delta_raw


def load_history(player_id):
    fp = os.path.join(HISTORY_DIR, f"{player_id}.json")
    if not os.path.exists(fp):
        return {"playerId": int(player_id), "points": []}
    try:
        with open(fp, 'r', encoding='utf8') as fh:
            data = json.load(fh)
            # Backwards-compat: older history files may be plain lists. Normalize to dict.
            if isinstance(data, list):
                # Heuristics: if list of dicts (likely points), keep as points. Otherwise try to
                # interpret as legacy prices/weeks lists; otherwise produce empty canonical fields.
                points = []
                prices = []
                weeks = []
                if data and all(isinstance(x, dict) for x in data):
                    points = data
                    # also populate prices/weeks where available for compatibility
                    for p in points:
                        try:
                            if 'price' in p:
                                prices.append(float(p.get('price') or 0))
                        except Exception:
                            continue
                        try:
                            if 'week' in p:
                                weeks.append(int(p.get('week')))
                        except Exception:
                            continue
                else:
                    # list of primitives — assume numeric prices if possible
                    def _is_number(x):
                        try:
                            float(x)
                            return True
                        except Exception:
                            return False

                    if data and all(_is_number(x) for x in data):
                        prices = [float(x) for x in data]
                    # leave points/weeks empty otherwise

                data = {"playerId": int(player_id), "points": points, "prices": prices, "weeks": weeks}
            # Ensure we always return a dict with .get support
            if not isinstance(data, dict):
                return {"playerId": int(player_id), "points": []}
            return data
    except Exception:
        return {"playerId": int(player_id), "points": []}


def save_history(player_id, obj):
    fp = os.path.join(HISTORY_DIR, f"{player_id}.json")
    with open(fp, 'w', encoding='utf8') as fh:
        json.dump(obj, fh, indent=2)
    print(f"[weekly-update] wrote history for {player_id} -> {fp}")


def run(season: int, week: int):
    ensure_dirs()
    rows = read_stats_csv(season, week)

    # Filter and map rows into structured dicts
    parsed = []
    for r in rows:
        try:
            pid = r.get('playerId') or r.get('player_id') or r.get('playerID') or r.get('id')
            if pid is None or str(pid).strip() == '':
                continue
            playerId = int(str(pid).strip())
            season_v = int(str(r.get('season', season)))
            week_v = int(str(r.get('week', week)))
            # ignore other seasons
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

    # Group by position if available, else by 'ALL'
    groups = defaultdict(list)
    has_positions = any(bool(p.get('position')) for p in parsed)
    if not has_positions:
        print('[weekly-update] WARNING: no position column found; computing z-scores across all players')

    for p in parsed:
        key = p['position'] if (p['position'] and has_positions) else 'ALL'
        groups[key].append(p)

    # Precompute z-scores per group
    zscores = {}
    for key, items in groups.items():
        epas = [it['epa'] for it in items]
        yards = [it['yards'] for it in items]
        tds = [it['tds'] for it in items]
        vols = [ (it['targets'] + it['receptions'] + it['carries']) for it in items]
        z_epas = compute_zscores(epas)
        z_yards = compute_zscores(yards)
        z_tds = compute_zscores(tds)
        z_vols = compute_zscores(vols)
        for idx, it in enumerate(items):
            zscores[it['playerId']] = (z_epas[idx], z_yards[idx], z_tds[idx], z_vols[idx])

    # Apply deltas and write history files
    for p in parsed:
        pid = p['playerId']
        z_epa, z_yards, z_tds, z_vol = zscores.get(pid, (0.0,0.0,0.0,0.0))
        delta = computeWeeklyDelta(p, z_epa, z_yards, z_tds, z_vol)
        if delta != 0.0:
            print(f"[weekly-update] delta != 0 for {pid}: {delta:.4f}")
        # load history
        hist = load_history(pid)
        points = hist.get('points', [])
        prev_price = 100.0
        if points:
            prev_price = float(points[-1].get('price', prev_price))
        new_price = round(prev_price * (1.0 + float(delta)), 2)
        # append only if week not already present
        exists = any(int(pt.get('week', -9999)) == week for pt in points)
        if not exists:
            points.append({ 'week': week, 'price': new_price, 'delta': round(float(delta), 4) })
            hist['playerId'] = int(pid)
            hist['points'] = points
            save_history(pid, hist)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    ap.add_argument('--week', type=int, required=True)
    args = ap.parse_args()
    run(args.season, args.week)


if __name__ == '__main__':
    main()
