#!/usr/bin/env python3
"""
Fetch finished NFL games for a season/week using the CreativesDev "NFL API Data" (RapidAPI)

Usage:
  export NFL_API_DATA_KEY=...
  python3 scripts/fetch_nflapi_week_games.py --season 2025 --week 15

Writes: data/games/season_{season}_week_{week}_games.json
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


RAPIDAPI_HOST = "nfl-api-data.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}"


def build_headers() -> dict:
    key = os.getenv("NFL_API_DATA_KEY")
    if not key:
        raise RuntimeError("NFL_API_DATA_KEY is not set in environment")
    return {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Accept": "application/json",
    }


def extract_games(data) -> list:
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # commonly the endpoint returns {'games': [...]} or a list directly
        for k in ("games", "data", "body", "items"):
            v = data.get(k)
            if isinstance(v, list):
                return v
        # fallback: any first list value
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


def fetch_week_games(season: int, week: int) -> Path:
    url = f"{BASE_URL}/nfl-games/v1/season/{season}/week/{week}"
    headers = build_headers()
    print(f"[nflapi-week-games] GET {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except Exception as exc:
        print("[nflapi-week-games] request failed:", exc)
        raise

    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = {}

    games = extract_games(data)
    print(f"[nflapi-week-games] top-level keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
    total = len(games)

    finals = []
    for g in games:
        if not isinstance(g, dict):
            continue
        status = str(g.get("gameStatus") or g.get("status") or "").strip().lower()
        if status in ("final", "completed", "closed"):
            # normalize minimal fields
            finals.append({
                "gameId": g.get("gameId") or g.get("gameID") or g.get("id"),
                "homeTeam": g.get("homeTeam") or g.get("home_team") or g.get("home"),
                "awayTeam": g.get("awayTeam") or g.get("away_team") or g.get("away"),
                "gameStatus": g.get("gameStatus") or g.get("status"),
            })

    out_dir = Path("data") / "games"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"season_{season}_week_{week}_games.json"
    with out_path.open("w", encoding="utf8") as fh:
        json.dump(finals, fh, indent=2)

    print(f"[nflapi-week-games] total games from API: {total}")
    print(f"[nflapi-week-games] final games written: {len(finals)} -> {out_path}")
    return out_path


def main():
    p = argparse.ArgumentParser(description="Fetch NFL games for a season/week from NFL API Data (CreativesDev)")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--week", type=int, required=True)
    args = p.parse_args()
    fetch_week_games(args.season, args.week)


if __name__ == "__main__":
    main()
