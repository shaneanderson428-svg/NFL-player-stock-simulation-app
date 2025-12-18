#!/usr/bin/env python3
"""
Orchestrate building all weekly stats CSVs for a season.

Usage:
  python3 scripts/build_all_weeks_stats.py --season 2025 [--max-week N]

Behavior:
 - Determine the highest week to process by scanning existing files in:
     data/games/season_{season}/week_{week}_games.json
   and
     data/weekly/player_stats_{season}_week_{week}.csv
   If none found, --max-week is required.
 - For each week from 1..max_week, if data/weekly/player_stats_{season}_week_{week}.csv
   already exists, skip. Otherwise:
     - run: python3 scripts/fetch_tank01_week_games.py --season {season} --week {week}
     - run: python3 scripts/build_weekly_stats_from_games.py --season {season} --week {week}

This script does not modify other scripts; it only orchestrates them.
"""
import argparse
import glob
import os
import re
import subprocess
from typing import List


ROOT = os.getcwd()
DATA_DIR = os.path.join(ROOT, 'data')
WEEKLY_DIR = os.path.join(DATA_DIR, 'weekly')
GAMES_DIR_TEMPLATE = os.path.join(DATA_DIR, 'games', 'season_{season}')


def find_weeks_from_games(season: int) -> List[int]:
    d = GAMES_DIR_TEMPLATE.format(season=season)
    pattern = os.path.join(d, f'week_*_games.json')
    files = glob.glob(pattern)
    weeks = []
    for p in files:
        m = re.search(r'week_(\d+)_games\.json$', p)
        if m:
            weeks.append(int(m.group(1)))
    return sorted(set(weeks))


def find_weeks_from_weekly(season: int) -> List[int]:
    pattern = os.path.join(WEEKLY_DIR, f'player_stats_{season}_week_*.csv')
    files = glob.glob(pattern)
    weeks = []
    for p in files:
        m = re.search(r'player_stats_\d+_week_(\d+)\.csv$', p)
        if m:
            weeks.append(int(m.group(1)))
    return sorted(set(weeks))


def run_cmd(cmd, cwd=ROOT):
    print(f"[orchestrator] running: {cmd}")
    try:
        res = subprocess.run(cmd, shell=True, cwd=cwd, check=False, capture_output=True, text=True)
        if res.stdout:
            print(res.stdout)
        if res.stderr:
            print(res.stderr)
        return res.returncode == 0
    except Exception as e:
        print(f"[orchestrator] command failed: {e}")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    ap.add_argument('--max-week', type=int, default=None, help='Optional explicit max week to process')
    args = ap.parse_args()

    season = args.season
    weeks_games = find_weeks_from_games(season)
    weeks_weekly = find_weeks_from_weekly(season)
    candidate_weeks = sorted(set(weeks_games + weeks_weekly))

    max_week = args.max_week
    if max_week is None:
        if candidate_weeks:
            max_week = max(candidate_weeks)
        else:
            print('[orchestrator] No existing game/weekly files found. Please pass --max-week to specify how many weeks to build.')
            return

    print(f"[orchestrator] season={season} max_week={max_week}")

    generated = []
    skipped = []
    failed = []

    for week in range(1, max_week + 1):
        csv_path = os.path.join(WEEKLY_DIR, f'player_stats_{season}_week_{week}.csv')
        if os.path.exists(csv_path):
            print(f"[orchestrator] week {week}: CSV exists, skipping -> {csv_path}")
            skipped.append(week)
            continue

        # call fetch and build scripts
        ok1 = run_cmd(f"python3 scripts/fetch_tank01_week_games.py --season {season} --week {week}")
        if not ok1:
            print(f"[orchestrator] week {week}: fetch script failed (continuing)")
        ok2 = run_cmd(f"python3 scripts/build_weekly_stats_from_games.py --season {season} --week {week}")
        if ok2:
            print(f"[orchestrator] week {week}: built CSV")
            generated.append(week)
        else:
            print(f"[orchestrator] week {week}: build script failed")
            failed.append(week)

    print(f"[orchestrator] done. generated={len(generated)} skipped={len(skipped)} failed={len(failed)}")
    if generated:
        print(f"[orchestrator] generated files for weeks: {generated[:10]}{'...' if len(generated)>10 else ''}")


if __name__ == '__main__':
    main()
