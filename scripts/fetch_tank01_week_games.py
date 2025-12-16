#!/usr/bin/env python3
"""
Fetch all NFL games for a given season/week and write finished games (status == 'final')
to data/games/season_{season}_week_{week}_games.json

Usage:
  python3 scripts/fetch_tank01_week_games.py --season 2025 --week 15
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

try:
    import requests
except Exception:
    print("The 'requests' package is required. Install with: pip install requests")
    raise


RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"


def build_headers():
    key = os.getenv("TANK01_API_KEY")
    if not key:
        raise RuntimeError("TANK01_API_KEY is not set in environment")
    return {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Accept": "application/json",
    }


def _extract_games(data):
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # common container keys
        for k in ("games", "gameList", "body", "data", "items", "schedule"):
            v = data.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return list(v.values())
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


def fetch_week_games(season: int, week: int):
    # Use the RapidAPI Tank01 endpoint that returns games for a week
    # (getNFLGamesForWeek returns an object with a 'games' list)
    url = f"https://{RAPIDAPI_HOST}/getNFLGamesForWeek"
    headers = build_headers()
    # include seasonType as required by the API; use REG for regular season
    params = {"season": season, "week": week, "seasonType": "REG"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    except Exception as exc:
        print("Schedule request failed:", exc)
        raise

    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = {}

    # The Tank01 /getNFLGamesForWeek endpoint returns { "games": [...] }
    games = []
    if isinstance(data, dict):
        if "message" in data:
            print(f"[fetch-week-games] API message: {data.get('message')}")
        g = data.get("games")
        if isinstance(g, list):
            games = g
        else:
            # fallback to previous heuristics when shape differs
            games = _extract_games(data)
    else:
        games = _extract_games(data)

    print(f"[fetch-week-games] schedule top-level keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
    total = len(games)
    finals = []
    for g in games:
        if not isinstance(g, dict):
            continue
        status = str(g.get("gameStatus") or g.get("status") or "").strip().lower()
        if status == "final" or status == "completed" or status == "closed":
            finals.append(g)

    out_dir = Path("data") / "games" / f"season_{season}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"week_{week}_games.json"
    with out_path.open("w", encoding="utf8") as fh:
        json.dump(finals, fh, indent=2)

    print(f"[fetch-week-games] total games found: {total}")
    print(f"[fetch-week-games] finalized games written: {len(finals)} -> {out_path}")
    return str(out_path)


def main():
    p = argparse.ArgumentParser(description="Fetch finished games for a season/week from Tank01")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--week", type=int, required=True)
    args = p.parse_args()
    fetch_week_games(args.season, args.week)


if __name__ == "__main__":
    main()
