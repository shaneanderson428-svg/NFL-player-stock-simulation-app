#!/usr/bin/env python3
"""
Fetch weekly player stats (WR/RB/TE/QB) by detecting the latest completed week,
reading the scoreboard to find final games, fetching boxscores by gameID, and
exporting CSVs per position plus a merged ALL CSV.

This script uses only the non-live endpoints:
 - /getNFLScoreboard?season=&week=
 - /getNFLBoxScore?gameID=

Usage:
  export RAPIDAPI_KEY=...
  python3 scripts/fetch_weekly_all_positions.py

"""
from __future__ import annotations

import os
from pathlib import Path
import sys

# Ensure project root (one level above `scripts/`) is on sys.path so
# `import scripts._env` works when this file is executed directly as
# `python3 scripts/fetch_weekly_all_positions.py`.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import scripts._env  # loads .env.local into environment (if present)
import time
from typing import Dict, List, Optional

import requests
import argparse

import scripts._schedule as _schedule
try:
    import scripts.fetch_tank01_weekly_stats as tank_weekly
except Exception:
    tank_weekly = None

API_KEY = os.getenv("RAPIDAPI_KEY")
API_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}"

OUTPUT_BASE = Path('data') / 'weekly'

def output_dir_for(season: int) -> Path:
    p = OUTPUT_BASE / str(season)
    p.mkdir(parents=True, exist_ok=True)
    return p

HEADERS = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": API_HOST,
    "Accept": "application/json",
}

RETRIES = 3
BACKOFF = 1.0


def _get(url: str, params: Optional[Dict] = None) -> Optional[Dict]:
    """Simple GET with retries, returns parsed JSON or None."""
    attempt = 0
    while attempt < RETRIES:
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    return None
            if r.status_code == 403:
                # permission denied / forbidden
                print(f"Request forbidden (403) for {url} params={params}; skipping")
                return None
            if r.status_code == 429:
                # rate limited
                wait = BACKOFF * (2 ** attempt)
                print(f"Rate limited (429); sleeping {wait}s and retrying...")
                time.sleep(wait)
                attempt += 1
                continue
            # other non-200 -> return parsed body if possible for debugging
            try:
                return r.json()
            except Exception:
                return None
        except requests.RequestException as exc:
            print("Request error:", exc)
            time.sleep(BACKOFF)
            attempt += 1
    return None


def fetch_scoreboard(season: int, week: int) -> Optional[Dict]:
    # Deprecated scoreboard endpoint removed. Use schedule endpoint via
    # scripts._schedule.fetch_weekly_schedule instead in the main flow.
    url = f"{BASE_URL}/getNFLScoreboard"
    return _get(url, params={"season": season, "week": week})


def fetch_boxscore_by_gameid(game_id: str) -> Optional[Dict]:
    url = f"{BASE_URL}/getNFLBoxScore"
    return _get(url, params={"gameID": game_id})


# Week auto-detection removed. Use the schedule endpoint helper to verify
# that a requested week is complete before fetching boxscores.


def flatten_player(player: Dict, position: str, game_meta: Dict) -> Dict:
    out: Dict = {}
    # identity
    out["playerID"] = player.get("playerID") or player.get("playerId") or player.get("player_id")
    out["playerName"] = player.get("playerName") or player.get("longName") or player.get("displayName")
    out["team"] = player.get("team")
    out["position"] = position
    out["gameID"] = game_meta.get("gameID") or game_meta.get("gameId") or game_meta.get("id")
    out["opponent"] = game_meta.get("opponent") or game_meta.get("opponentName")
    out["gameDate"] = game_meta.get("gameDate")
    out["gameStatus"] = game_meta.get("gameStatus")

    # stats: Tank01 variants sometimes put stats under 'stats' or top-level keys
    stats = player.get("stats") or player
    if isinstance(stats, dict):
        for k, v in stats.items():
            if k in ("playerID", "playerName", "team", "position"):
                continue
            try:
                out[k] = v
            except Exception:
                out[k] = str(v)
    return out


def safe_num(v):
    try:
        if v is None or v == '':
            return 0.0
        return float(str(v).strip())
    except Exception:
        return 0.0


def extract_players_from_boxscore_body(box_body: Dict) -> List[Dict]:
    players: List[Dict] = []
    if not isinstance(box_body, dict):
        return players
    ps = box_body.get("playerStats")
    if isinstance(ps, dict):
        for pos, arr in ps.items():
            if not isinstance(arr, list):
                continue
            for p in arr:
                players.append(flatten_player(p, pos, box_body))
    elif isinstance(ps, list):
        for p in ps:
            pos = p.get("position") or p.get("pos") or ""
            players.append(flatten_player(p, pos, box_body))
    else:
        for v in box_body.values():
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and ("position" in item or "playerID" in item or "playerId" in item):
                        pos = item.get("position") or item.get("pos") or ""
                        players.append(flatten_player(item, pos, box_body))
    return players


def run_weekly_fetch(season: int, week: int):
    if not API_KEY:
        raise RuntimeError(
            "RAPIDAPI_KEY not set. Create a file named .env.local at the project root with:\n"
            "RAPIDAPI_KEY=your_key_here\n\n"
            "That file is ignored by git and will be loaded automatically. Or export RAPIDAPI_KEY in your shell."
        )

    # New flow: fetch player-level stats directly for the week using the
    # allowed endpoint getNFLPlayerStatsByWeek. Presence of player stats is
    # treated as evidence the week is final. This avoids relying on scoreboard
    # or schedule endpoints which may be restricted.
    outdir = output_dir_for(season)
    all_players: List[Dict] = []

    # Try the direct per-week player stats endpoint first
    endpoint = f"{BASE_URL}/getNFLPlayerStatsByWeek"
    print(f"Requesting player-level stats from {endpoint} for season={season} week={week}")
    payload = _get(endpoint, params={"season": season, "week": week})

    if payload:
        # payload may have 'body' or nested containers — prefer scripts.fetch_tank01_weekly_stats helpers
        try:
            if tank_weekly:
                data_payload = payload.get("body") if isinstance(payload, dict) and payload.get("body") else payload
                data_payload = data_payload or {}
                items = tank_weekly.extract_player_rows(data_payload)
                for item in items:
                    # flatten and extract common fields
                    flat = tank_weekly.flatten(item) if hasattr(tank_weekly, 'flatten') else (item if isinstance(item, dict) else {})
                    # player identity
                    pid = flat.get("playerID") or flat.get("player.id") or flat.get("playerId") or flat.get("id")
                    if pid is None:
                        # try nested player object
                        nested = item.get('player') if isinstance(item, dict) else None
                        if isinstance(nested, dict):
                            pid = nested.get('playerID') or nested.get('id') or nested.get('playerId')
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

                    playerName = flat.get('playerName') or flat.get('player.longName') or flat.get('longName') or ''
                    position = flat.get('position') or flat.get('player.position') or ''
                    # numeric extraction using tank_weekly helpers when available
                    try:
                        epa = tank_weekly.num_from_candidates(flat, ['epa', 'EPA', 'playerEPA', 'cpoe', 'cpoE']) if hasattr(tank_weekly, 'num_from_candidates') else 0.0
                    except Exception:
                        epa = 0.0
                    yards = 0.0
                    try:
                        yards = tank_weekly.num_from_candidates(flat, ['receiving.receivingYards','receivingYards','recYds']) if hasattr(tank_weekly, 'num_from_candidates') else 0.0
                    except Exception:
                        yards = 0.0
                    tds = 0
                    try:
                        tds = int(tank_weekly.num_from_candidates(flat, ['receivingTDs','receiving.td','recTD']) if hasattr(tank_weekly, 'num_from_candidates') else 0)
                    except Exception:
                        tds = 0
                    targets = int(tank_weekly.num_from_candidates(flat, ['targets','receiving.targets']) if hasattr(tank_weekly, 'num_from_candidates') else 0)
                    receptions = int(tank_weekly.num_from_candidates(flat, ['receptions','rec']) if hasattr(tank_weekly, 'num_from_candidates') else 0)
                    carries = int(tank_weekly.num_from_candidates(flat, ['rushingAttempts','att','rushAttempts']) if hasattr(tank_weekly, 'num_from_candidates') else 0)

                    all_players.append({
                        'playerId': playerId,
                        'playerName': playerName,
                        'position': position,
                        'week': week,
                        'season': season,
                        'epa': float(epa or 0.0),
                        'yards': float(yards or 0.0),
                        'tds': int(tds or 0),
                        'targets': int(targets or 0),
                        'receptions': int(receptions or 0),
                        'carries': int(carries or 0),
                    })
            else:
                # no tank_weekly helper: try to handle payload generically
                data_payload = payload.get('body') if isinstance(payload, dict) and payload.get('body') else payload
                if isinstance(data_payload, dict):
                    # try to find list-valued entries
                    items = []
                    for v in data_payload.values():
                        if isinstance(v, list):
                            items = v
                            break
                elif isinstance(data_payload, list):
                    items = data_payload
                else:
                    items = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    pid = item.get('playerID') or item.get('playerId') or item.get('id')
                    if not pid:
                        continue
                    try:
                        playerId = int(str(pid))
                    except Exception:
                        continue
                    playerName = item.get('playerName') or item.get('longName') or ''
                    position = item.get('position') or ''
                    epa = safe_num(item.get('epa'))
                    yards = safe_num(item.get('yards') or item.get('recYds'))
                    tds = int(safe_num(item.get('tds')))
                    targets = int(safe_num(item.get('targets')))
                    receptions = int(safe_num(item.get('receptions')))
                    carries = int(safe_num(item.get('carries')))
                    all_players.append({
                        'playerId': playerId,
                        'playerName': playerName,
                        'position': position,
                        'week': week,
                        'season': season,
                        'epa': float(epa),
                        'yards': float(yards),
                        'tds': int(tds),
                        'targets': int(targets),
                        'receptions': int(receptions),
                        'carries': int(carries),
                    })
        except Exception as e:
            print("Error extracting player rows from payload:", e)
            all_players = []
    else:
        # No direct payload from per-week endpoint. Try the existing tank_weekly script
        if tank_weekly:
            try:
                print("Direct per-week endpoint returned no payload or empty; falling back to scripts/fetch_tank01_weekly_stats.fetch_and_write")
                tank_weekly.fetch_and_write(season, week)
                # read normalized CSV written by fallback
                import csv as _csv
                out_path = Path('data') / 'weekly' / f'player_stats_{season}_week_{week}.csv'
                if out_path.exists():
                    with out_path.open('r', encoding='utf8') as fh:
                        rdr = _csv.DictReader(fh)
                        for r in rdr:
                            try:
                                pid = r.get('playerId')
                                if not pid:
                                    continue
                                all_players.append({
                                    'playerId': int(pid),
                                    'playerName': '',
                                    'position': '',
                                    'week': int(r.get('week', week)),
                                    'season': int(r.get('season', season)),
                                    'epa': float(r.get('epa') or 0.0),
                                    'yards': float(r.get('yards') or 0.0),
                                    'tds': int(r.get('tds') or 0),
                                    'targets': int(r.get('targets') or 0),
                                    'receptions': int(r.get('receptions') or 0),
                                    'carries': int(r.get('carries') or 0),
                                })
                            except Exception:
                                continue
                else:
                    print(f"Fallback normalized CSV not found at {out_path}")
            except SystemExit as se:
                # fetch_and_write uses SystemExit to signal missing env/key; don't crash — just log and continue
                print("Fallback per-week fetch exited early:", se)
            except Exception as e:
                print("Fallback per-week fetch failed:", e)
        else:
            print("No per-week payload and no tank_weekly helper available; nothing to write")

    # write outputs in the same naming scheme as before. Use csv module so pandas is not required.
    if not all_players:
        print(f"No player stats found for season {season} week {week}; nothing to write.")
        return

    # Normalize fieldnames for merged CSV
    fieldnames = ['playerId', 'playerName', 'position', 'week', 'season', 'epa', 'yards', 'tds', 'targets', 'receptions', 'carries']
    import csv as _csv
    # write merged
    merged_path = outdir / f'week_{week}.csv'
    with merged_path.open('w', newline='', encoding='utf8') as fh:
        w = _csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for p in all_players:
            row = {k: p.get(k, '') for k in fieldnames}
            w.writerow(row)
    print(f"Wrote merged ALL ({len(all_players)}) to {merged_path}")

    # write per-position files (attempt to group by position if available)
    positions = ['WR', 'RB', 'TE', 'QB']
    for pos in positions:
        outp = outdir / f'week_{week}_{pos}.csv'
        with outp.open('w', newline='', encoding='utf8') as fh:
            w = _csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            written = 0
            for p in all_players:
                ppos = (p.get('position') or '').upper()
                if ppos == pos:
                    w.writerow({k: p.get(k, '') for k in fieldnames})
                    written += 1
        print(f"Wrote {written} rows to {outp}")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Fetch weekly player stats for a specified season and week")
    p.add_argument("--season", type=int, required=True, help="Season year, e.g. 2025")
    p.add_argument("--week", type=int, required=True, help="Week number, e.g. 1")
    args = p.parse_args(argv)
    run_weekly_fetch(season=args.season, week=args.week)


if __name__ == "__main__":
    main()
