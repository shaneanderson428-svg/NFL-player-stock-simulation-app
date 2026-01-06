#!/usr/bin/env python3
"""Generate a simple player metadata map from weekly player stats CSVs.

Writes data/player_meta.json as a map: playerId (string) -> { name, position, team, espnId }

This is intentionally conservative: it prefers non-empty values and does not
overwrite existing values with empty ones. Run this whenever weekly CSVs are
updated to ensure the frontend can look up player names for any playerId.
"""
from __future__ import annotations

import csv
import glob
import json
import os
from collections import defaultdict


def safe_get(d, keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, ''):
            return v
    return None


def main():
    cwd = os.getcwd()
    data_dir = os.path.join(cwd, 'data')
    weekly_dir = os.path.join(data_dir, 'weekly')
    out_path = os.path.join(data_dir, 'player_meta.json')

    files = glob.glob(os.path.join(weekly_dir, '**', '*.csv'), recursive=True)
    files += glob.glob(os.path.join(weekly_dir, '*.csv'))
    # Also scan price CSVs for playerName if weekly CSVs are missing it
    prices_dir = os.path.join(data_dir, 'prices')
    files += glob.glob(os.path.join(prices_dir, '**', '*.csv'), recursive=True)
    files += glob.glob(os.path.join(prices_dir, '*.csv'))

    # Also try loading cleaned player profiles for canonical names
    profiles = []
    prof_path = os.path.join(data_dir, 'player_profiles_cleaned.csv')
    if os.path.exists(prof_path):
        profiles.append(prof_path)
    prof_path2 = os.path.join(data_dir, 'player_profiles.csv')
    if os.path.exists(prof_path2):
        profiles.append(prof_path2)
    files += profiles
    files = sorted(set(files))

    meta = {}
    for fp in files:
        try:
            with open(fp, newline='', encoding='utf8') as fh:
                rdr = csv.DictReader(fh)
                for r in rdr:
                    pid = str(safe_get(r, ['playerId', 'espnId', 'player_id', 'id', 'playerID']) or '').strip()
                    if not pid:
                        continue
                    name = safe_get(r, ['playerName', 'player_name', 'PlayerName', 'player', 'name', 'fullName'])
                    pos = safe_get(r, ['position', 'pos', 'Position', 'positionName'])
                    team = safe_get(r, ['team', 'teamAbbr', 'team_abbr', 'team_name', 'team'])
                    espn = safe_get(r, ['espnId', 'playerId', 'espn_id'])

                    pid_key = str(pid)
                    cur = meta.get(pid_key) or {}
                    # prefer existing values, but fill missing ones
                    if not cur.get('name') and name:
                        cur['name'] = str(name)
                    if not cur.get('position') and pos:
                        cur['position'] = str(pos)
                    if not cur.get('team') and team:
                        cur['team'] = str(team)
                    if not cur.get('espnId') and espn:
                        cur['espnId'] = str(espn)
                    meta[pid_key] = cur
        except Exception:
            # ignore malformed CSVs
            continue

    # write stable, sorted JSON
    try:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        # Coerce keys to strings to avoid mixed-type sorting issues
        new_meta = {str(k): meta[k] for k in meta.keys()}
        def sort_key(x):
            sx = str(x)
            if sx.isdigit():
                return (0, int(sx))
            return (1, sx)
        with open(out_path, 'w', encoding='utf8') as fh:
            json.dump({k: new_meta[k] for k in sorted(new_meta.keys(), key=sort_key)}, fh, indent=2, ensure_ascii=False)
        print(f"Wrote player metadata for {len(meta)} players to {out_path}")
    except Exception as e:
        print('ERROR writing player_meta.json:', e)


if __name__ == '__main__':
    main()
