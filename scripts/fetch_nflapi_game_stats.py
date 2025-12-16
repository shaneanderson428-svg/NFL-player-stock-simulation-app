#!/usr/bin/env python3
"""
Fetch per-game player stats from NFL API Data (CreativesDev RapidAPI).

Provides fetch_game_player_stats(game_id) -> list[dict] and CLI.
"""
from __future__ import annotations

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


def num(v) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        s = str(v)
        import re

        cleaned = re.sub(r"[^0-9.\-]", "", s) or "0"
        try:
            return float(cleaned)
        except Exception:
            return 0.0


def fetch_game_player_stats(game_id: str) -> list:
    """Return list of player stat dicts for a game.

    Each dict contains keys: playerId, rushingYards, rushingTDs, receivingYards,
    receivingTDs, targets, receptions, carries.
    """
    url = f"{BASE_URL}/nfl-games/v1/game/{game_id}/players"
    headers = build_headers()
    print(f"[nflapi-game-stats] GET {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except Exception as exc:
        print(f"[nflapi-game-stats] request failed for {game_id}:", exc)
        return []

    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = {}

    # Defensive extraction: response may be list or dict with 'players' key
    players = []
    if isinstance(data, list):
        players = data
    elif isinstance(data, dict):
        if "players" in data and isinstance(data["players"], list):
            players = data["players"]
        else:
            # try common keys
            for k in ("data", "body", "items"): 
                if k in data and isinstance(data[k], list):
                    players = data[k]
                    break
            # fallback: any first list value
            if not players:
                for v in data.values():
                    if isinstance(v, list):
                        players = v
                        break

    out = []
    count_with_stats = 0
    for p in players:
        if not isinstance(p, dict):
            continue
        pid = p.get("playerId") or p.get("playerID") or p.get("id")
        if pid is None:
            # try nested player object
            nested = p.get("player") or p.get("person")
            if isinstance(nested, dict):
                pid = nested.get("playerId") or nested.get("playerID") or nested.get("id")
        try:
            playerId = int(str(pid))
        except Exception:
            try:
                import re

                playerId = int(re.sub(r"[^0-9]", "", str(pid)))
            except Exception:
                continue

        rushingYards = num(p.get("rushingYards") or p.get("rushYards") or p.get("rushYds") or p.get("rushing_yards"))
        rushingTDs = int(num(p.get("rushingTDs") or p.get("rushTDs") or p.get("rushing_tds")))
        receivingYards = num(p.get("receivingYards") or p.get("recYards") or p.get("receiving_yards") or p.get("recYds"))
        receivingTDs = int(num(p.get("receivingTDs") or p.get("recTDs") or p.get("receiving_tds")))
        targets = int(num(p.get("targets") or p.get("target") or p.get("receivingTargets") or p.get("receiving.targets")))
        receptions = int(num(p.get("receptions") or p.get("rec") or p.get("receiving.receptions")))
        carries = int(num(p.get("carries") or p.get("rushingAttempts") or p.get("rushAttempts") or p.get("att")))

        # Skip players with no numeric stats (all zeros)
        if rushingYards == 0 and receivingYards == 0 and rushingTDs == 0 and receivingTDs == 0 and targets == 0 and receptions == 0 and carries == 0:
            continue

        count_with_stats += 1
        out.append({
            "playerId": int(playerId),
            "rushingYards": float(rushingYards),
            "rushingTDs": int(rushingTDs),
            "receivingYards": float(receivingYards),
            "receivingTDs": int(receivingTDs),
            "targets": int(targets),
            "receptions": int(receptions),
            "carries": int(carries),
        })

    print(f"[nflapi-game-stats] players with stats in game {game_id}: {count_with_stats}")
    return out


def main():
    import argparse

    p = argparse.ArgumentParser(description="Fetch per-game player stats from NFL API Data (CreativesDev)")
    p.add_argument("--gameId", required=True)
    args = p.parse_args()
    rows = fetch_game_player_stats(args.gameId)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
