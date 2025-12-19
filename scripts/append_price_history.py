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


def load_history(pid: str):
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
    return data


def write_history(pid: str, data: list):
    fout = os.path.join(history_dir, f"{pid}_price_history.json")
    try:
        with open(fout, 'w', encoding='utf8') as fh:
            json.dump(data, fh, indent=2)
        return True
    except Exception as e:
        print(f"ERROR: failed to write history for {pid}: {e}")
        return False


def append_entry(pid: str, week: int, entry: dict):
    data = load_history(pid)
    # Avoid duplicate week
    if data and isinstance(data, list):
        last = data[-1]
        try:
            if int(last.get('week')) == int(week):
                print(f"Skipping append for player {pid} week {week}: already present in history")
                return False
        except Exception:
            pass
    data.append(entry)
    ok = write_history(pid, data)
    if ok:
        print(f"Appended entry for player {pid}: week={week} close={entry.get('close')} reason={entry.get('reason')}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    ap.add_argument('--week', type=int, required=True)
    ap.add_argument('--dnp-penalty', type=float, default=-0.07, help='Default DNP penalty as decimal (e.g. -0.07 for -7%%)')
    ap.add_argument('--min-price', type=float, default=10.0, help='Minimum allowed price after adjustments')
    ap.add_argument('--seed-price', type=float, default=100.0, help='Seed price for players with no history')
    args = ap.parse_args()

    rows = read_prices(args.season, args.week)
    if not rows:
        print(f"No prices found in data/prices/{args.season}/week_{args.week}.csv; nothing to append")
        return
    # Map prices from CSV by player id
    price_map = {}
    for r in rows:
        pid = str(r.get('playerId') or r.get('player_id') or r.get('playerID') or '').strip()
        if not pid:
            continue
        try:
            price = float(r.get('price'))
        except Exception:
            continue
        week = int(r.get('week') or args.week)
        price_map[pid] = price

    # Build the set of all known player ids: prices CSV + existing history files + optional summary CSV
    all_pids = set(price_map.keys())
    # existing history files
    if os.path.exists(history_dir):
        for f in os.listdir(history_dir):
            if f.endswith('_price_history.json'):
                pid = f[: -len('_price_history.json')]
                if pid:
                    all_pids.add(pid)

    # optional player list from player_stock_summary.csv (if present)
    summary_csv = os.path.join(os.getcwd(), 'data', 'player_stock_summary.csv')
    if os.path.exists(summary_csv):
        try:
            with open(summary_csv, newline='', encoding='utf8') as fh:
                r = csv.DictReader(fh)
                for row in r:
                    pid = str(row.get('espnId') or row.get('playerId') or row.get('id') or '').strip()
                    if pid:
                        all_pids.add(pid)
        except Exception:
            pass

    appended = 0
    dnp_count = 0
    week = int(args.week)
    for pid in sorted(all_pids):
        # load last known close
        hist = load_history(pid)
        prev_close = None
        if hist and isinstance(hist, list) and len(hist) > 0:
            last = hist[-1]
            try:
                prev_close = float(last.get('price') or last.get('close'))
            except Exception:
                prev_close = None

        if prev_close is None:
            prev_close = float(args.seed_price)

        # skip if history already contains this week
        if hist and isinstance(hist, list) and len(hist) > 0:
            try:
                if int(hist[-1].get('week')) == week:
                    # already present
                    continue
            except Exception:
                pass

        if pid in price_map:
            close = float(price_map[pid])
            reason = 'STATS'
        else:
            # DNP handling: apply penalty
            close = prev_close * (1.0 + float(args.dnp_penalty))
            if close < float(args.min_price):
                close = float(args.min_price)
            reason = 'DNP'
            dnp_count += 1

        open_price = float(prev_close)
        high = max(open_price, float(close)) * 1.05
        low = min(open_price, float(close)) * 0.95

        entry = {
            'week': week,
            'price': float(close),
            'open': float(open_price),
            'high': float(high),
            'low': float(low),
            'close': float(close),
            'reason': reason,
        }

        ok = append_entry(pid, week, entry)
        if ok:
            appended += 1

    print(f"Appended price entries for {appended} players ({dnp_count} DNP entries)")


if __name__ == '__main__':
    main()
