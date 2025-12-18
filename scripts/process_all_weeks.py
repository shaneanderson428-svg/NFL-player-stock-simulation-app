#!/usr/bin/env python3
"""
Process all available player_stats_{season}_week_{week}.csv files in data/weekly/
and run the orchestrator `run_weekly_update.py` for each week in ascending order per season.

Usage:
  python3 scripts/process_all_weeks.py

This script is CSV-only and will not call external APIs: it simply discovers files
and invokes the existing orchestrator which already enforces CSV-only behavior.
"""
from __future__ import annotations

import os
import re
import subprocess
from collections import defaultdict

cwd = os.getcwd()
weekly_dir = os.path.join(cwd, 'data', 'weekly')
pattern = re.compile(r'player_stats_(\d+)_week_(\d+)\.csv$')

files = []
for root, dirs, filenames in os.walk(weekly_dir):
    for fn in filenames:
        m = pattern.match(fn)
        if m:
            season = int(m.group(1))
            week = int(m.group(2))
            path = os.path.join(root, fn)
            files.append((season, week, path))

if not files:
    print('No player_stats CSVs found under', weekly_dir)
    raise SystemExit(0)

by_season = defaultdict(list)
for season, week, path in files:
    by_season[season].append((week, path))

py = 'python3'
for season in sorted(by_season.keys()):
    weeks = sorted(by_season[season], key=lambda x: x[0])
    print(f'Processing season {season} weeks: {[w for w, _ in weeks]}')
    for week, path in weeks:
        print(f'--- Running week {week} for season {season} ---')
        cmd = [py, os.path.join(cwd, 'scripts', 'run_weekly_update.py'), '--season', str(season), '--week', str(week)]
        print('Running:', ' '.join(cmd))
        res = subprocess.run(cmd)
        if res.returncode != 0:
            print(f'run_weekly_update.py failed for season={season} week={week} (exit {res.returncode}); aborting')
            raise SystemExit(res.returncode)

print('All done')
