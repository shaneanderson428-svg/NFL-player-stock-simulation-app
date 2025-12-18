#!/usr/bin/env python3
"""
Append weekly prices from data/prices/{season}/week_{week}.csv to per-player
history files at data/history/{playerId}_price_history.json.

Behavior:
- Reads prices CSV written by compute_weekly_prices.py
- For each row: playerId, week, price
- Appends {"week": week, "price": price} to data/history/{playerId}_price_history.json
- If file does not exist, create it with a list containing the new entry
- Avoid duplicate week entries: if last entry.week == current week, skip and log
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys


data_dir = os.path.join(os.getcwd(), 'data')
prices_base = os.path.join(data_dir, 'prices')
history_dir = os.path.join(data_dir, 'history')


def read_prices(season: int, week: int):
    path = os.path.join(prices_base, str(season), f'week_{week}.csv')
    if not os.path.exists(path):
        print(f"ERROR: prices CSV not found at {path}")
        sys.exit(2)
    rows = []
    with open(path, newline='', encoding='utf8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            rows.append(row)
    return rows


def append_history_for_player(pid: str, week: int, price: float):
    os.makedirs(history_dir, exist_ok=True)
    fout = os.path.join(history_dir, f"{pid}_price_history.json")
    data = []
    if os.path.exists(fout):
        try:
            with open(fout, 'r', encoding='utf8') as fh:
                data = json.load(fh)
                if not isinstance(data, list):
                    print(f"WARNING: existing history for {pid} is not a list; backing up and recreating")
                    os.rename(fout, fout + '.bak')
                    data = []
        except Exception as e:
            print(f"WARNING: failed to read existing history {fout}: {e}; backing up and recreating")
            try:
                os.rename(fout, fout + '.bak')
            except Exception:
                pass
            data = []
    # Ensure ordering and avoid duplicate week
    if data and isinstance(data, list):
        last = data[-1]
        try:
            if int(last.get('week')) == int(week):
                print(f"Skipping append for player {pid} week {week}: already present in history")
                return False
        except Exception:
            pass
    entry = {'week': int(week), 'price': float(price)}
    data.append(entry)
    try:
        with open(fout, 'w', encoding='utf8') as fh:
            json.dump(data, fh, indent=2)
        print(f"Appended price for player {pid}: week={week} price={price} -> {fout}")
        return True
    except Exception as e:
        print(f"ERROR: failed to write history for {pid}: {e}")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    ap.add_argument('--week', type=int, required=True)
    args = ap.parse_args()

    rows = read_prices(args.season, args.week)
    if not rows:
        print(f"No prices found in data/prices/{args.season}/week_{args.week}.csv; nothing to append")
        return

    appended = 0
    for r in rows:
        pid = str(r.get('playerId') or r.get('player_id') or r.get('playerID') or '').strip()
        if not pid:
            continue
        try:
            price = float(r.get('price'))
        except Exception:
            continue
        week = int(r.get('week') or args.week)
        ok = append_history_for_player(pid, week, price)
        if ok:
            appended += 1

    print(f"Appended price points for {appended} players")


if __name__ == '__main__':
    main()
