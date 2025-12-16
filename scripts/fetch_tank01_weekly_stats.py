#!/usr/bin/env python3
"""
Fetch Tank01 weekly player stats via RapidAPI and write a normalized CSV for the pricing pipeline.

Usage:
  python3 scripts/fetch_tank01_weekly_stats.py --season 2025 --week 15

Requirements:
- Read Tank01 RapidAPI key from env var TANK01_API_KEY
- Writes CSV to data/weekly/player_stats_{season}_week_{week}.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

# Ensure project root is on sys.path so scripts._env loads .env.local
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
try:
    import scripts._env  # type: ignore
except Exception:
    # .env.local is optional
    pass

try:
    import requests
except Exception:
    print("The 'requests' package is required. Install with: pip install requests")
    raise


RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"


def flatten(d: dict, parent: str = "") -> dict:
    out: dict = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        key = f"{parent}.{k}" if parent else k
        if isinstance(v, dict):
            out.update(flatten(v, key))
        elif isinstance(v, list):
            try:
                out[key] = json.dumps(v, ensure_ascii=False)
            except Exception:
                out[key] = str(v)
        else:
            out[key] = v
    return out


def safe_get(d: dict, keys: list, default=None):
    for k in keys:
        if k in d:
            return d[k]
    return default


def num_from_candidates(flat: dict, candidates: list) -> float:
    for c in candidates:
        if c in flat:
            v = flat.get(c)
            if v is None:
                continue
            s = str(v).strip()
            if s == "":
                continue
            try:
                return float(s)
            except Exception:
                try:
                    import re

                    cleaned = re.sub(r"[^0-9.\-]", "", s) or "0"
                    return float(cleaned)
                except Exception:
                    continue
    return 0.0


def extract_player_rows(data: dict | list) -> list:
    # data may be dict or list; heuristics: look for common container keys
    candidates = []
    # common keys that have held player arrays in Tank01 variants
    possible_keys = (
        "players",
        "playerList",
        "data",
        "body",
        "playerStats",
        "stats",
        "playerWeekStats",
        "player_stats",
        "playerstats",
        "player_week_stats",
    )
    if isinstance(data, dict):
        for k in possible_keys:
            v = data.get(k)
            if isinstance(v, list) and v:
                candidates = v
                break
        if not candidates:
            # maybe top-level map of players or a dict under 'body' as a map
            # try extracting list-valued entries
            for v in data.values():
                if isinstance(v, list) and v:
                    candidates = v
                    break
    elif isinstance(data, list):
        candidates = data
    return candidates or []


def normalize_and_write(season: int, week: int, payload: dict | list):
    rows = extract_player_rows(payload)
    out_rows = []
    for item in rows:
        try:
            if not isinstance(item, dict):
                continue
            # flatten game/player nested objects to make keys predictable
            flat = flatten(item)
            # If nested player object exists, merge its flattened keys with 'player.' prefix
            nested = item.get("player")
            if isinstance(nested, dict):
                flat.update(flatten(nested, "player"))

            pid = safe_get(flat, ["playerID", "player.playerID", "player.id", "playerId", "id"]) or safe_get(item, ["playerID", "id"])
            if pid is None or str(pid).strip() == "":
                continue
            try:
                playerId = int(str(pid))
            except Exception:
                # try stripping non-digits
                import re

                s = re.sub(r"[^0-9]", "", str(pid))
                if not s:
                    continue
                playerId = int(s)

            # compute numeric fields from many possible keys
            rushing_candidates = [
                "rushing.rushingYards",
                "rushingYards",
                "Rushing.rushYards",
                "rushYds",
                "rushing.yards",
                "rushingYd",
            ]
            receiving_candidates = [
                "receiving.receivingYards",
                "receivingYards",
                "Receiving.recYds",
                "recYds",
                "receiving.yards",
            ]
            rushing_y = num_from_candidates(flat, rushing_candidates)
            receiving_y = num_from_candidates(flat, receiving_candidates)
            yards = rushing_y + receiving_y

            rushing_td_candidates = ["rushingTDs", "rushing.td", "rushTD", "rushTDs"]
            receiving_td_candidates = ["receivingTDs", "receiving.td", "recTD", "recTDs"]
            tds = num_from_candidates(flat, rushing_td_candidates) + num_from_candidates(flat, receiving_td_candidates)

            targets = num_from_candidates(flat, ["targets", "receiving.targets", "Receiving.targets", "target"]) or 0.0
            receptions = num_from_candidates(flat, ["receptions", "receiving.receptions", "Receiving.receptions", "rec"]) or 0.0
            carries = num_from_candidates(flat, ["rushingAttempts", "rushAttempts", "rushing.att", "att", "attempts"]) or 0.0

            # EPA: prefer explicit fields; otherwise compute proxy
            epa_val = num_from_candidates(flat, ["epa", "EPA", "playerEPA", "cpoe", "cpoE"]) or None
            if not epa_val:
                epa = yards * 0.01 + tds * 0.6
            else:
                epa = epa_val

            # Skip players with zero total yards AND zero TDs
            if (yards == 0 or yards == 0.0) and (tds == 0 or tds == 0.0):
                continue

            out_rows.append({
                "playerId": int(playerId),
                "week": int(week),
                "season": int(season),
                "epa": float(epa),
                "yards": float(yards),
                "tds": int(tds),
                "targets": int(targets),
                "receptions": int(receptions),
                "carries": int(carries),
            })
        except Exception:
            continue

    # Ensure output dir
    out_dir = Path("data") / "weekly"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"player_stats_{season}_week_{week}.csv"

    # Write CSV header and rows
    fieldnames = ["playerId", "week", "season", "epa", "yards", "tds", "targets", "receptions", "carries"]
    with out_path.open("w", newline="", encoding="utf8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print(f"[fetch-tank01] wrote {len(out_rows)} players to {out_path}")
    return len(out_rows)


def convert_external_csv_to_weekly(season: int, week: int, external_path: Path):
    import csv as _csv

    out_rows = []
    with external_path.open("r", encoding="utf8") as fh:
        reader = _csv.DictReader(fh)
        for row in reader:
            try:
                # prefer playerID / espnID fields
                pid = None
                for c in ("playerID", "espnID", "espnId", "playerId", "id"):
                    if c in row and row.get(c):
                        pid = row.get(c)
                        break
                if not pid:
                    continue
                try:
                    playerId = int(str(pid))
                except Exception:
                    import re

                    s = re.sub(r"[^0-9]", "", str(pid))
                    if not s:
                        continue
                    playerId = int(s)

                # Use the same numeric extraction helpers as above
                flat = dict(row)
                rushing_y = num_from_candidates(flat, ["rushing.rushingYards", "rushingYards", "rushYds", "rushing.yards", "rushYd", "Rushing.rushYards"]) if isinstance(flat, dict) else 0.0
                receiving_y = num_from_candidates(flat, ["receiving.receivingYards", "receivingYards", "recYds", "receiving.yards"]) if isinstance(flat, dict) else 0.0
                yards = rushing_y + receiving_y
                tds = num_from_candidates(flat, ["rushingTDs", "rushing.td", "rushTD", "rushTDs"]) + num_from_candidates(flat, ["receivingTDs", "receiving.td", "recTD", "recTDs"]) if isinstance(flat, dict) else 0
                targets = int(num_from_candidates(flat, ["targets", "receiving.targets", "target"]) or 0)
                receptions = int(num_from_candidates(flat, ["receptions", "receiving.receptions", "rec"]) or 0)
                carries = int(num_from_candidates(flat, ["rushingAttempts", "rushAttempts", "att", "attempts"]) or 0)
                epa_val = num_from_candidates(flat, ["epa", "EPA", "playerEPA"]) or None
                epa = epa_val if epa_val else (yards * 0.01 + tds * 0.6)

                if (yards == 0 or yards == 0.0) and (tds == 0 or tds == 0.0):
                    continue

                out_rows.append({
                    "playerId": int(playerId),
                    "week": int(week),
                    "season": int(season),
                    "epa": float(epa),
                    "yards": float(yards),
                    "tds": int(tds),
                    "targets": int(targets),
                    "receptions": int(receptions),
                    "carries": int(carries),
                })
            except Exception:
                continue

    # write to data/weekly path
    out_dir = Path("data") / "weekly"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"player_stats_{season}_week_{week}.csv"
    fieldnames = ["playerId", "week", "season", "epa", "yards", "tds", "targets", "receptions", "carries"]
    with out_path.open("w", newline="", encoding="utf8") as fh:
        writer = _csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print(f"[fetch-tank01] converted and wrote {len(out_rows)} players to {out_path} (from {external_path})")


def fetch_and_write(season: int, week: int):
    key = os.getenv("TANK01_API_KEY")
    if not key:
        print("TANK01_API_KEY not set in environment. Set it or create .env.local with TANK01_API_KEY=...")
        raise SystemExit(1)

    host = RAPIDAPI_HOST
    # Use the completed-week endpoint (not the live-in-game stats)
    endpoint = "getNFLPlayerStatsByWeek"
    url = f"https://{host}/{endpoint}?season={season}&week={week}"
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": host,
        "Accept": "application/json",
    }

    print(f"[fetch-tank01] requesting {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except Exception as exc:
        print("Request failed:", exc)
        raise SystemExit(1)

    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = {}

    # Log top-level keys for debugging
    try:
        if isinstance(data, dict):
            print(f"[fetch-tank01] top-level response keys: {list(data.keys())}")
        else:
            print(f"[fetch-tank01] response type: {type(data)}")
    except Exception:
        pass

    # Defensive: if response embeds payload under 'body' or similar, extract it
    payload = data
    if isinstance(data, dict) and "body" in data and isinstance(data["body"], (dict, list)):
        payload = data["body"]

    # Try normalizing directly from payload first
    try:
        written = normalize_and_write(season, week, payload)
    except Exception:
        written = 0

    # If nothing was written, fall back to existing per-player fetch script which
    # performs a more robust per-player aggregation (scripts/fetch_tank01_week.py).
    if not written:
        print("[fetch-tank01] no players written from direct endpoint. Falling back to per-player fetch (scripts/fetch_tank01_week.py)")
        import subprocess
        try:
            subprocess.run([sys.executable, "scripts/fetch_tank01_week.py", "--week", str(week)], check=True)
        except Exception as e:
            print("[fetch-tank01] fallback per-player fetch failed:", e)
            return

        # Convert the external CSV output into the normalized data/weekly CSV expected by pricing pipeline
        ext_path = Path("external") / "tank01" / f"player_stats_week_{week}.csv"
        if ext_path.exists():
            try:
                convert_external_csv_to_weekly(season, week, ext_path)
            except Exception as e:
                print("[fetch-tank01] failed to convert external CSV:", e)
        else:
            print(f"[fetch-tank01] expected fallback CSV not found at {ext_path}")



def main():
    p = argparse.ArgumentParser(description="Fetch Tank01 weekly stats and write normalized CSV for pricing")
    p.add_argument("--season", required=True, type=int)
    p.add_argument("--week", required=True, type=int)
    args = p.parse_args()
    fetch_and_write(args.season, args.week)


if __name__ == "__main__":
    main()
