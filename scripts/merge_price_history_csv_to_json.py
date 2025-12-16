#!/usr/bin/env python3
"""
Merge append-only CSV history into a frontend-ready JSON file.
Reads:  external/history/player_price_history.csv
Writes: data/price_history.json (atomic overwrite)

Each output point shape:
  { "t": "<season>-W<week>", "p": <price>, "weekly_pct_change": <optional>, "performance_score": <optional> }

Grouping is by playerId. Points are sorted by (season, week) numeric order. If duplicate (season,week) rows exist for a player, the last one in the CSV wins (CSV is append-only).
"""
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

CSV_PATH = Path("external/history/player_price_history.csv")
OUT_PATH = Path("data/price_history.json")

if not CSV_PATH.exists():
    print(f"ERROR: CSV not found at {CSV_PATH}")
    sys.exit(2)

# Read CSV into grouped dict: playerId -> {(season,week): row_dict}
grouped = defaultdict(dict)
fieldnames = []
with CSV_PATH.open(newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames or []
    # normalize header names to known keys
    for row in reader:
        # robust key access: allow different column names
        def get(k, alt=None):
            for candidate in (k, alt):
                if candidate is None:
                    continue
                if candidate in row and row[candidate] != "":
                    return row[candidate]
            return None

        player_id = get("playerId", "player_id") or get("id")
        if player_id is None:
            # skip rows without player id
            continue
        player_id = str(player_id)

        season = get("season") or get("year") or "0"
        week = get("week") or "0"

        # try to parse season/week ints for sorting; fallback to 0
        try:
            season_i = int(float(season))
        except Exception:
            season_i = 0
        try:
            week_i = int(float(week))
        except Exception:
            week_i = 0

        price_val = get("price") or get("currentPrice") or get("p")
        if price_val is None or price_val == "":
            # skip rows without price
            continue
        try:
            price_f = float(price_val)
        except Exception:
            # try to strip currency
            try:
                price_f = float(price_val.replace("$", "").replace(",", ""))
            except Exception:
                continue

        # optional extras
        weekly_pct = None
        perf_score = None
        for key in ("weekly_pct_change", "weeklyChangePct", "priceChangePct", "weekly_change_pct"):
            v = row.get(key)
            if v not in (None, ""):
                try:
                    weekly_pct = float(v)
                    break
                except Exception:
                    pass
        for key in ("performance_score", "perf_score", "performanceScore"):
            v = row.get(key)
            if v not in (None, ""):
                try:
                    perf_score = float(v)
                    break
                except Exception:
                    pass

        # store last-seen row for this (season,week)
        grouped[player_id][(season_i, week_i)] = {
            "season": season_i,
            "week": week_i,
            "p": price_f,
            # store optional raw values so we include them when present
            **({"weekly_pct_change": weekly_pct} if weekly_pct is not None else {}),
            **({"performance_score": perf_score} if perf_score is not None else {}),
        }

# Convert grouped map into final structure: playerId -> [points sorted]
out = {}
for player_id, points_map in grouped.items():
    # sort by (season, week)
    keys_sorted = sorted(points_map.keys())
    arr = []
    for sk in keys_sorted:
        item = points_map[sk]
        season_i = item["season"]
        week_i = item["week"]
        t = f"{season_i}-W{week_i}"
        point = {"t": t, "p": item["p"]}
        if "weekly_pct_change" in item:
            point["weekly_pct_change"] = item["weekly_pct_change"]
        if "performance_score" in item:
            point["performance_score"] = item["performance_score"]
        arr.append(point)
    out[player_id] = arr

# Ensure output directory exists
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

tmp_path = OUT_PATH.with_suffix(".tmp")
# Write atomically
with tmp_path.open("w", encoding="utf-8") as f:
    json.dump(out, f, indent=2, ensure_ascii=False)
    f.flush()
    os.fsync(f.fileno())

# Rename over target
os.replace(tmp_path, OUT_PATH)

print(f"Wrote {len(out)} players to {OUT_PATH} (atomic)")

# Small verification: print first 3 players and their first/last points
count_shown = 0
for pid, pts in out.items():
    print(pid, "->", len(pts), "points; first:", pts[0] if pts else None, "last:", pts[-1] if pts else None)
    count_shown += 1
    if count_shown >= 3:
        break

print("Done.")
