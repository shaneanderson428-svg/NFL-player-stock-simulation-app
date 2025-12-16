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

import pandas as pd
import requests
import argparse

import scripts._schedule as _schedule

API_KEY = os.getenv("RAPIDAPI_KEY")
API_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}"

OUTPUT_DIR = "external/tank01"

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

    # Use the schedule endpoint as the single source of truth for week completeness.
    # First try Tank01 schedule endpoint as a single source of truth. If the
    # schedule endpoint is not accessible (403) we will fall back to the
    # scoreboard endpoint so weekly updates continue.
    schedule_result = _schedule.fetch_weekly_schedule(season=season, week=week)
    finals = []
    if schedule_result is None:
        print("Schedule endpoint not available or permission denied; falling back to scoreboard for finals")
        # fallback: use scoreboard endpoint to find final games
        data = fetch_scoreboard(season, week)
        games: List[Dict] = []
        if isinstance(data, dict):
            body = data.get("body")
            if body is None:
                body = data.get("games") or data.get("data")

            if isinstance(body, list):
                games = body
            elif isinstance(body, dict):
                games = list(body.values())
            else:
                # sometimes the response itself is a single game dict
                if any(k in data for k in ("gameID", "gameId", "game")):
                    games = [data]

        finals = [
            g
            for g in games
            if isinstance(g, dict) and str(g.get("gameStatus", "")).lower() in ("final", "completed", "closed")
        ]
    elif schedule_result == []:
        # schedule explicitly says week is not complete — abort to avoid partial stats
        raise RuntimeError(f"Week {week} season {season} is not complete according to schedule; aborting.")
    else:
        finals = schedule_result

    print(f"Using week {week} (found {len(finals)} final games)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_players: List[Dict] = []

    for g in finals:
        gid = None
        if isinstance(g, dict):
            gid = g.get("gameID") or g.get("gameId") or g.get("id")
        if not gid:
            print("Skipping scoreboard entry missing gameID:", g)
            continue
        print(f"Fetching boxscore for gameID {gid}...")
        box = fetch_boxscore_by_gameid(str(gid))
        if not box:
            print(f"No boxscore returned for {gid}")
            continue
        if not isinstance(box, dict):
            print(f"Boxscore for {gid} not a dict, skipping")
            continue

        body = box.get("body")
        if body is None:
            body = box

        if not isinstance(body, dict):
            print(f"Boxscore for {gid} has no body, skipping")
            continue
        players = extract_players_from_boxscore_body(body)
        print(f"Extracted {len(players)} players from game {gid}")
        all_players.extend(players)

    if not all_players:
        print("No players extracted for week", week)
        return

    df = pd.DataFrame(all_players)
    positions = ["WR", "RB", "TE", "QB"]

    for pos in positions:
        sub = df[df["position"].astype(str).str.upper() == pos]
        outp = os.path.join(OUTPUT_DIR, f"week_{week}_{pos}.csv")
        sub.to_csv(outp, index=False)
        print(f"Wrote {len(sub)} rows to {outp}")

    all_path = os.path.join(OUTPUT_DIR, f"week_{week}_ALL.csv")
    df.to_csv(all_path, index=False)
    print(f"Wrote merged ALL ({len(df)}) to {all_path}")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Fetch weekly player stats for a specified season and week")
    p.add_argument("--season", type=int, required=True, help="Season year, e.g. 2025")
    p.add_argument("--week", type=int, required=True, help="Week number, e.g. 1")
    args = p.parse_args(argv)
    run_weekly_fetch(season=args.season, week=args.week)


if __name__ == "__main__":
    main()
