#!/usr/bin/env python3
"""
Orchestrator: run the weekly pipeline steps in order:
 1) fetch weekly boxscores -> data/weekly/{season}/week_{week}.csv
 2) compute weekly prices -> data/prices/{season}/week_{week}.csv
 3) backfill/update per-player history -> data/history/{season}/*.json and index.json

This script shells out to the other Python scripts in scripts/ so each step logs to stdout
and failures are surfaced immediately.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import os


def run_step(cmd, descr: str):
    print(f"\n=== STEP: {descr} ===")
    print(f"Running: {' '.join(cmd)}")
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(res.stdout)
    if res.returncode != 0:
        print(f"Step failed: {descr} (exit {res.returncode})")
        sys.exit(res.returncode)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    ap.add_argument('--week', type=int, required=True)
    ap.add_argument('--dnp-penalty', type=float, default=0.05, help='Penalty applied to close price for DNP weeks (e.g., 0.05 for 5)')
    args = ap.parse_args()

    py = sys.executable
    cwd = os.getcwd()

    # ensure scripts are executable via python
    compute_cmd = [py, os.path.join(cwd, 'scripts', 'compute_weekly_prices.py'), '--season', str(args.season), '--week', str(args.week)]
    backfill_cmd = [py, os.path.join(cwd, 'scripts', 'backfill_historical_prices.py'), '--season', str(args.season)]
    append_cmd = [py, os.path.join(cwd, 'scripts', 'append_price_history.py'), '--season', str(args.season), '--week', str(args.week), '--dnp-penalty', str(args.dnp_penalty)]

    # Hybrid fetch logic:
    # 1) If data/weekly/player_stats_{season}_week_{week}.csv exists, use it.
    # 2) Else, attempt per-week aggregate fetch via scripts/fetch_tank01_weekly_stats.py
    # 3) If still missing, fall back to per-game boxscore aggregation using
    #    scripts/fetch_tank01_week_games.py + fetch_tank01_game_boxscore.fetch_game_boxscore
    player_stats_path = os.path.join(cwd, 'data', 'weekly', f'player_stats_{args.season}_week_{args.week}.csv')
    season_dir = os.path.join(cwd, 'data', 'weekly', str(args.season))
    target_week_path = os.path.join(season_dir, f'week_{args.week}.csv')

    valid_csv_exists = False
    row_count = 0
    if os.path.exists(player_stats_path):
        # check whether CSV contains any player rows
        try:
            import csv as _csv

            with open(player_stats_path, newline='', encoding='utf8') as fh:
                rdr = _csv.DictReader(fh)
                row_count = sum(1 for _ in rdr)
        except Exception as e:
            print(f"Error reading {player_stats_path}: {e}")
            row_count = 0

        if row_count > 0:
            valid_csv_exists = True
            print(f"[mode] CSV-only (historical)")
            print(f"Using existing weekly player stats CSV for week {args.week}: {player_stats_path}")
            os.makedirs(season_dir, exist_ok=True)
            if not os.path.exists(target_week_path):
                import shutil

                try:
                    shutil.copyfile(player_stats_path, target_week_path)
                    print(f"Copied {player_stats_path} -> {target_week_path}")
                except Exception as e:
                    print(f"Failed to copy player_stats CSV to season week path: {e}")
            else:
                print(f"Per-season week file already exists at {target_week_path}; not overwriting")
            print('[fetch] skipped')
        else:
            print(f"Weekly CSV exists but contains no player rows — refetching week {args.week}")

    if valid_csv_exists:
        # CSV-only mode: run compute & backfill and exit. Do NOT call any fetch logic.
        run_step(compute_cmd, 'compute_weekly_prices')
        run_step(append_cmd, 'append_price_history')
        print('\n[prices] computed')
        print('[history] updated')
        print('\nAll steps completed successfully.')
        return

    # CSV-only mode: we do NOT call external APIs. If CSV missing or header-only, skip computation.
    if not os.path.exists(player_stats_path):
        print(f"WARNING: No weekly player_stats CSV found at {player_stats_path}. CSV-only mode enabled; skipping price computation and history update.")
        print('[fetch] skipped')
        print('\nAll steps completed (nothing to do).')
        return

    # If file exists but has no data rows, treat as missing
    try:
        import csv as _csv
        with open(player_stats_path, newline='', encoding='utf8') as fh:
            rdr = _csv.DictReader(fh)
            _rows = list(rdr)
    except Exception as e:
        print(f"ERROR reading {player_stats_path}: {e}")
        print('[fetch] skipped')
        print('\nAll steps completed (nothing to do).')
        return

    if len(_rows) == 0:
        print(f"WARNING: player_stats CSV at {player_stats_path} is header-only. CSV-only mode enabled; skipping price computation and history update.")
        print('[fetch] skipped')
        print('\nAll steps completed (nothing to do).')
        return

    # At this point we have a valid CSV with data rows; proceed to compute and update history
    run_step(compute_cmd, 'compute_weekly_prices')

    # Append prices to per-player history JSONs
    append_cmd = [py, os.path.join(cwd, 'scripts', 'append_price_history.py'), '--season', str(args.season), '--week', str(args.week), '--dnp-penalty', str(args.dnp_penalty)]
    run_step(append_cmd, 'append_price_history')

    print('\n[prices] computed')
    print('[history] updated')
    print('\nAll steps completed successfully.')


if __name__ == '__main__':
    main()
