#!/usr/bin/env python3
"""Compute and persist weekly player stock price updates.

Rules implemented:
- Each player has currentPrice, previousPrice, weeklyChangePercent
- Compute a score from advanced metrics (epa, yards, tds, usage) and map to
  a percent change capped between -15% and +15%.
- Apply change once per week only (tracked via lastUpdatedWeek in the price file)
- Persist results to `external/history/player_prices.json`.

This script reads the latest `external/tank01/player_stats_week_<WEEK>.csv`
and optional `external/advanced/advanced_metrics_week_<WEEK>.csv` produced by
your fetch/compute scripts. It does not call any live APIs and should be run
by a weekly cron/job after the other scripts produce their outputs.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TANK_DIR = ROOT / "external" / "tank01"
ADV_DIR = ROOT / "external" / "advanced"
HISTORY_DIR = ROOT / "external" / "history"
PRICE_FILE = HISTORY_DIR / "player_prices.json"


def find_latest_stats_csv() -> Path | None:
    if not TANK_DIR.exists():
        return None
    candidates = list(TANK_DIR.glob("player_stats_week_*.csv"))
    if not candidates:
        return None
    # pick highest week number
    best = None
    best_week = -1
    for p in candidates:
        name = p.name
        try:
            wk = int(name.split("player_stats_week_")[1].split(".")[0])
        except Exception:
            continue
        if wk > best_week:
            best_week = wk
            best = p
    return best


def normalize_cols(d: Dict[str, Any], keys: list[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k in keys:
        v = d.get(k)
        if v is None:
            out[k] = 0.0
            continue
        try:
            out[k] = float(v)
        except Exception:
            out[k] = 0.0
    return out


def compute_scores(rows: list[Dict[str, Any]]) -> Dict[str, float]:
    # Compute a position-normalized performance score per player using z-scores
    # across the dataset grouped by position. Return a mapping pid -> performance_score.
    parsed: list[Dict[str, Any]] = []

    def infer_position(r: Dict[str, Any]) -> str:
        p = (r.get('position') or r.get('pos') or '')
        if p:
            return str(p).upper()
        # heuristics
        try:
            if float(r.get('passAttempts') or r.get('pass_attempts') or r.get('pass_attempt') or 0) > 0:
                return 'QB'
        except Exception:
            pass
        try:
            if float(r.get('targets') or r.get('targets_per_game') or r.get('Receiving.targets') or 0) > 0:
                return 'WR'
        except Exception:
            pass
        try:
            if float(r.get('rushYards') or r.get('rush_yards') or r.get('rushing_yards') or 0) > 0:
                return 'RB'
        except Exception:
            pass
        return ''

    # extract per-row canonical metrics and detect EPA presence
    for r in rows:
        # pid lookup
        pid = str(r.get('espnID') or r.get('espnId') or r.get('playerID') or r.get('playerId') or r.get('player') or '')
        if not pid:
            pid = str(r.get('playerName') or r.get('longName') or r.get('displayName') or 'unknown')

        # extract fields
        epa_keys = ['epa', 'EPA', 'epa_per_play', 'avg_epa', 'avgEPA', 'avg_epa']
        has_epa = False
        epa_val = float('nan')
        for k in epa_keys:
            if k in r and r.get(k) not in (None, ''):
                try:
                    epa_val = float(r.get(k))
                    has_epa = True
                    break
                except Exception:
                    continue

        def fget(*cands, default=0.0):
            for c in cands:
                if c in r and r.get(c) not in (None, ''):
                    try:
                        return float(r.get(c))
                    except Exception:
                        continue
            return float(default)

        yards = fget('yds', 'yards', 'recYds', 'receiving_yards', 'Receiving.recYds', default=0.0)
        tds = fget('td', 'tds', 'recTD', 'receiving_tds', default=0.0)
        plays = fget('plays', 'snap_count', 'snapCounts', default=0.0)
        targets = fget('targets', 'rec_targets', 'targets_per_game', default=0.0)
        receptions = fget('receptions', 'rec', default=0.0)

        # volume proxy: prefer plays, fallback to targets+receptions
        volume = plays if plays and plays > 0 else (targets + receptions)

        pos = infer_position(r)

        parsed.append({
            'pid': pid,
            'row': r,
            'position': pos,
            'epa': epa_val if has_epa else float('nan'),
            'has_epa': has_epa,
            'yards': yards,
            'tds': tds,
            'volume': volume,
        })

    # group by position and compute z-scores (mean/std) for metrics
    from collections import defaultdict

    buckets = defaultdict(list)
    for p in parsed:
        buckets[p['position']].append(p)

    perf_map: Dict[str, float] = {}

    # per-position weights (production, efficiency, scoring, volume)
    pos_weights = {
        'QB': {'production': 0.4, 'efficiency': 0.3, 'scoring': 0.2, 'volume': 0.1},
        'RB': {'production': 0.35, 'efficiency': 0.25, 'scoring': 0.2, 'volume': 0.2},
        'WR': {'production': 0.3, 'efficiency': 0.3, 'scoring': 0.2, 'volume': 0.2},
        'TE': {'production': 0.25, 'efficiency': 0.35, 'scoring': 0.2, 'volume': 0.2},
        '':   {'production': 0.3, 'efficiency': 0.3, 'scoring': 0.2, 'volume': 0.2},
    }

    for pos, items in buckets.items():
        # collect arrays
        prod_arr = [it['yards'] for it in items]
        eff_arr = [it['epa'] for it in items]
        score_arr = [it['tds'] for it in items]
        vol_arr = [it['volume'] for it in items]

        def stats(arr):
            try:
                # coerce values to numeric, converting non-convertible entries to NaN
                a = pd.to_numeric(pd.Series(arr), errors='coerce')
                m_val = a.mean()
                s_val = a.std()
                m = float(m_val) if not pd.isna(m_val) else 0.0
                s = float(s_val) if not pd.isna(s_val) else 0.0
                return m, s
            except Exception:
                return 0.0, 0.0

        pm, ps = stats(prod_arr)
        em, es = stats([x for x in eff_arr if not pd.isna(x)])
        sm, ss = stats(score_arr)
        vm, vs = stats(vol_arr)

        w = pos_weights.get(pos, pos_weights[''])

        for it in items:
            # production z
            try:
                prod_z = (it['yards'] - pm) / ps if ps and not pd.isna(ps) and ps != 0 else 0.0
            except Exception:
                prod_z = 0.0
            # efficiency z: prefer EPA, but if missing we'll set NaN here and handle later
            try:
                eff_z = (it['epa'] - em) / es if (it['has_epa'] and es and not pd.isna(es) and es != 0) else float('nan')
            except Exception:
                eff_z = float('nan')
            # scoring z
            try:
                score_z = (it['tds'] - sm) / ss if ss and not pd.isna(ss) and ss != 0 else 0.0
            except Exception:
                score_z = 0.0
            # volume z
            try:
                vol_z = (it['volume'] - vm) / vs if vs and not pd.isna(vs) and vs != 0 else 0.0
            except Exception:
                vol_z = 0.0

            # If EPA missing, substitute efficiency proxy = yards per volume (if volume>0)
            eff_val = None
            if it['has_epa'] and not pd.isna(eff_z):
                eff_val = eff_z
            else:
                # yards per touch proxy
                try:
                    ypt = (it['yards'] / it['volume']) if it['volume'] and it['volume'] > 0 else it['yards']
                except Exception:
                    ypt = it['yards']
                # normalize ypt across position using prod stats (approx)
                try:
                    # use (ypt - pm)/ps as an approximate z
                    eff_val = (ypt - pm) / ps if ps and not pd.isna(ps) and ps != 0 else 0.0
                except Exception:
                    eff_val = 0.0

            perf = (
                w['production'] * prod_z
                + w['efficiency'] * (eff_val if not pd.isna(eff_val) else 0.0)
                + w['scoring'] * score_z
                + w['volume'] * vol_z
            )

            perf_map[it['pid']] = float(perf)

    return perf_map


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="Compute and persist weekly player stock price updates")
    p.add_argument("--season", type=int, help="Season year (optional)")
    p.add_argument("--week", type=int, help="Week number (optional)")
    p.add_argument("--force-append", action="store_true", help="If set, append history rows even if the week already exists (useful for backfills/testing)")
    p.add_argument("--stats-csv", type=str, help="Path to tank01 player_stats CSV (optional)")
    p.add_argument("--fetch", action="store_true", help="If set, run fetch_weekly_all_positions before computing (requires --season and --week)")
    p.add_argument("--max-weekly-pct", type=float, default=25.0, help="Maximum absolute weekly percent change (default: 25)")
    p.add_argument("--momentum-alpha", type=float, default=0.10, help="Momentum carry-in factor for previous week's pct (default: 0.10)")
    p.add_argument("--missing-epa-volatility", type=float, default=0.5, help="Volatility multiplier to use when EPA is missing (default: 0.5)")
    args = p.parse_args()

    if args.stats_csv:
        stats_csv = Path(args.stats_csv)
    elif args.season and args.week:
        # prefer explicit week file when provided
        candidate = TANK_DIR / f"player_stats_week_{args.week}.csv"
        stats_csv = candidate if candidate.exists() else find_latest_stats_csv()
    else:
        stats_csv = find_latest_stats_csv()

    # Optional: allow fetching from API before computing if requested.
    if getattr(args, 'season', None) and getattr(args, 'week', None) and getattr(args, 'fetch', None):
        try:
            import subprocess, sys
            print(f"Fetching stats for season={args.season} week={args.week} via fetch_weekly_all_positions")
            subprocess.run([sys.executable, '-m', 'scripts.fetch_weekly_all_positions', '--season', str(args.season), '--week', str(args.week)], check=False)
            # after fetch, recompute stats_csv candidate
            candidate = TANK_DIR / f"player_stats_week_{args.week}.csv"
            if candidate.exists():
                stats_csv = candidate
        except Exception:
            pass
    if stats_csv is None:
        print("No weekly player stats CSV found in external/tank01 — nothing to do")
        return

    # Determine week to use. If --week provided, always trust it and do not parse
    # the filename. If not provided, attempt to parse week from the filename
    # (legacy behavior) and fall back to 0 when parsing fails.
    if getattr(args, 'week', None) is not None:
        wk = int(args.week)
    else:
        try:
            # try to parse from filename (legacy)
            import re

            m = re.search(r"player_stats_week_(\d+)", stats_csv.name)
            if m:
                wk = int(m.group(1))
            else:
                wk = 0
        except Exception:
            wk = 0

    print(f"Updating prices for week {wk} using {stats_csv}")

    df = pd.read_csv(stats_csv)
    stats_rows = df.to_dict(orient="records")

    # attempt to load advanced metrics for this week
    adv_path = ADV_DIR / f"advanced_metrics_week_{wk}.csv"
    if adv_path.exists():
        try:
            adv_df = pd.read_csv(adv_path)
            adv_map = {str(r.get('playerID') or r.get('espnID') or r.get('espnId') or r.get('player') or r.get('playerName') or ''): r for r in adv_df.to_dict(orient='records')}
        except Exception:
            adv_map = {}
    else:
        adv_map = {}

    # merge adv metrics into stats rows where possible (do not overwrite existing keys)
    merged_rows = []
    for r in stats_rows:
        key = str(r.get('espnID') or r.get('espnId') or r.get('playerID') or r.get('playerId') or r.get('player') or '')
        adv = adv_map.get(key)
        merged = dict(r)
        if adv:
            for k, v in adv.items():
                if k not in merged or merged.get(k) is None:
                    merged[k] = v
        merged_rows.append(merged)

    scores = compute_scores(merged_rows)

    try:
        num_candidates = len(stats_rows)
    except Exception:
        num_candidates = 0

    # ensure history dir exists
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    # load existing prices
    if PRICE_FILE.exists():
        try:
            with PRICE_FILE.open('r', encoding='utf8') as fh:
                price_map = json.load(fh)
        except Exception:
            price_map = {}
    else:
        price_map = {}

    # Debug info: detected week, existing max week in history, number of candidate rows
    try:
        existing_weeks = [int(v.get('lastUpdatedWeek') or 0) for v in price_map.values()]
        max_existing_week = max(existing_weeks) if existing_weeks else 0
    except Exception:
        max_existing_week = 0
    print(f"Debug: detected week={wk}; existing_max_week_in_history={max_existing_week}; candidate_rows={num_candidates}")

    changed = 0
    updated_rows: list[Dict[str, Any]] = []
    skipped_already_updated = 0
    skipped_zero_delta = 0
    for r in merged_rows:
        pid = str(r.get('espnID') or r.get('espnId') or r.get('playerID') or r.get('playerId') or r.get('player') or '')
        if not pid:
            pid = str(r.get('playerName') or r.get('longName') or r.get('displayName') or '')
        if not pid:
            continue

        entry = price_map.get(pid, {})
        last_week = int(entry.get('lastUpdatedWeek') or 0)
        current = float(entry.get('currentPrice') or 0)
        previous = float(entry.get('previousPrice') or 0)

        # derive a base price if not present
        if not current or current <= 0:
            # try fantasyPoints-based heuristic
            fp = None
            for cand in ('fantasyPoints', 'fantasyPoints.total', 'fantasyPointsDefault.standard', 'fantasyPointsDefault'):
                if cand in r and r[cand] not in (None, ''):
                    try:
                        fp = float(r[cand])
                        break
                    except Exception:
                        fp = None
            try:
                base = max(5.0, float(fp) * 4.0 + 50.0) if fp is not None else 100.0
            except Exception:
                base = 100.0
            current = round(base, 2)

        # Only apply once per week unless --force-append is set
        if last_week == wk and not getattr(args, 'force_append', False):
            # already updated for this week
            skipped_already_updated += 1
            continue

        # performance_score computed by compute_scores (position-normalized z-based)
        perf = float(scores.get(pid, 0.0))

        # detect EPA presence for volatility handling
        epa_keys = ['epa', 'EPA', 'epa_per_play', 'avg_epa', 'avgEPA', 'avg_epa']
        has_epa = False
        for k in epa_keys:
            if k in r and r.get(k) not in (None, ''):
                has_epa = True
                break

        # map performance_score to percent change with position-aware volatility
        base_scale = float(getattr(args, 'max_weekly_pct', 25.0))  # maximum magnitude in percent
        volatility_multiplier = 1.0 if has_epa else float(getattr(args, 'missing_epa_volatility', 0.5))
        # small momentum factor from previous week's recorded pct
        prev_pct = 0.0
        try:
            prev_pct = float(entry.get('weeklyChangePercent') or entry.get('weekly_change_percent') or 0.0)
        except Exception:
            prev_pct = 0.0
        momentum_alpha = float(getattr(args, 'momentum_alpha', 0.10))

        pct = perf * base_scale * volatility_multiplier + (momentum_alpha * prev_pct)
        pct = clamp(pct, -base_scale, base_scale)

        # Skip zero-delta cases (no change) to avoid noisy history unless forced
        if abs(pct) < 1e-12 and not getattr(args, 'force_append', False):
            skipped_zero_delta += 1
            continue

        new_price = round(current * (1.0 + pct / 100.0), 2)
        # ensure reasonable bounds
        new_price = max(1.0, new_price)

        price_map[pid] = {
            'playerId': pid,
            'previousPrice': round(current, 2),
            'currentPrice': new_price,
            'weeklyChangePercent': round(pct, 2),
            'lastUpdatedWeek': wk,
        }
        changed += 1
        # Capture a history row for append-only history store; append new columns performance_score and weekly_pct_change
        player_name = str(r.get('playerName') or r.get('longName') or r.get('player') or r.get('displayName') or '')
        updated_rows.append({
            'playerId': pid,
            'playerName': player_name,
            'season': datetime.utcnow().year,
            'week': wk,
            'price': new_price,
            'priceChangePct': round(pct, 2),
            'performance_score': round(perf, 4),
            'weekly_pct_change': round(pct, 2),
            'timestamp': int(datetime.utcnow().timestamp()),
        })

    # persist
    with PRICE_FILE.open('w', encoding='utf8') as fh:
        json.dump(price_map, fh, indent=2, ensure_ascii=False)

    print(f"Updated prices for {changed} players; persisted to {PRICE_FILE}")
    try:
        print(f"Skipped {skipped_already_updated} players because they were already updated for week {wk} (use --force-append to override)")
    except Exception:
        pass
    try:
        print(f"Skipped {skipped_zero_delta} players due to zero delta (no price change)")
    except Exception:
        pass

    # Append to persistent, append-only CSV history. Do not overwrite existing history.
    HISTORY_CSV = HISTORY_DIR / "player_price_history.csv"
    # Ensure history dir exists (already created above)
    try:
        existing_keys = set()
        if HISTORY_CSV.exists():
            try:
                hist_df = pd.read_csv(HISTORY_CSV)
                for row in hist_df.to_dict(orient='records'):
                    key = (str(row.get('playerId') or ''), int(row.get('season') or 0), int(row.get('week') or 0))
                    existing_keys.add(key)
            except Exception:
                existing_keys = set()

        # Prepare rows to append (skip duplicates and skip if updated_rows empty)
        rows_to_append = []
        for hr in updated_rows:
            key = (str(hr.get('playerId') or ''), int(hr.get('season') or 0), int(hr.get('week') or 0))
            if key in existing_keys:
                continue
            rows_to_append.append(hr)

        if rows_to_append:
            import csv

            write_header = not HISTORY_CSV.exists()
            with HISTORY_CSV.open('a', encoding='utf8', newline='') as fh:
                writer = csv.DictWriter(fh, fieldnames=['playerId', 'playerName', 'season', 'week', 'price', 'priceChangePct', 'performance_score', 'weekly_pct_change', 'timestamp'])
                if write_header:
                    writer.writeheader()
                for r in rows_to_append:
                    writer.writerow(r)
            print(f"Appended {len(rows_to_append)} rows to history {HISTORY_CSV}")
        else:
            print("No new history rows to append")
    except Exception as e:
        print(f"Warning: failed to append history: {e}")

        # Also append to the site-side JSON history store (`data/price_history.json`) so
        # frontend components that read from historyStore will see the new points.
        try:
            DATA_HISTORY = ROOT / 'data' / 'price_history.json'
            if rows_to_append:
                # load existing JSON
                try:
                    with DATA_HISTORY.open('r', encoding='utf8') as fh:
                        hist_obj = json.load(fh)
                except Exception:
                    hist_obj = {}

                for hr in rows_to_append:
                    key = str(hr.get('playerId') or '')
                    point = {'t': datetime.utcnow().isoformat(), 'p': float(hr.get('price') or 0.0)}
                    # include performance_score and weekly_pct_change when available for richer tooltips
                    try:
                        if 'performance_score' in hr:
                            point['performance_score'] = float(hr.get('performance_score') or 0.0)
                    except Exception:
                        pass
                    try:
                        if 'weekly_pct_change' in hr:
                            point['weekly_pct_change'] = float(hr.get('weekly_pct_change') or 0.0)
                    except Exception:
                        pass
                    arr = hist_obj.get(key) or []
                    # avoid exact duplicate timestamps/prices
                    if arr and isinstance(arr, list):
                        last = arr[-1]
                        try:
                            if float(last.get('p', 0)) == point['p'] and str(last.get('t', '')) == point['t']:
                                continue
                        except Exception:
                            pass
                    arr.append(point)
                    hist_obj[key] = arr

                # write back (atomic write would be nicer, but keep simple)
                DATA_HISTORY.parent.mkdir(parents=True, exist_ok=True)
                with DATA_HISTORY.open('w', encoding='utf8') as fh:
                    json.dump(hist_obj, fh, indent=2, ensure_ascii=False)
                print(f"Appended {len(rows_to_append)} points to {DATA_HISTORY}")
            else:
                print("No new JSON history points to append")
        except Exception as e:
            print(f"Warning: failed to append JSON history: {e}")


if __name__ == '__main__':
    main()
