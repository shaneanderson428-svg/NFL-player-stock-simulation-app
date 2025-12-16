#!/usr/bin/env python3
"""
Fetch a single game's boxscore from Tank01 and return normalized per-player stats.

Provides an importable function `fetch_game_boxscore(game_id)` and a CLI interface.
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


def num_from_candidates(flat: dict, candidates: list) -> float:
    import re

    def _clean(v):
        try:
            return float(v)
        except Exception:
            try:
                s = str(v).strip()
                cleaned = re.sub(r"[^0-9.\-]", "", s) or "0"
                return float(cleaned)
            except Exception:
                return 0.0

    # direct matches first
    for c in candidates:
        if c in flat:
            v = flat.get(c)
            if v is None:
                continue
            return _clean(v)

    # try matching on key name endings or contained substrings (case-insensitive)
    low_flat = {k.lower(): v for k, v in flat.items()}
    for c in candidates:
        lc = c.lower()
        # exact key match in lowercase
        if lc in low_flat:
            return _clean(low_flat[lc])
        # suffix or contained match
        for k, v in low_flat.items():
            if k.endswith(lc) or lc in k:
                return _clean(v)

    return 0.0


def extract_players_from_body(body: dict) -> list:
    # Use similar extraction heuristics as other scripts
    players = []
    if not isinstance(body, dict):
        return players
    ps = body.get("playerStats")
    if isinstance(ps, dict):
        # tank01 sometimes returns a mapping of playerId->playerDict
        # or a mapping of position->list[playerDict]. Handle both.
        vals_all_dict = all(isinstance(v, dict) for v in ps.values())
        if vals_all_dict:
            for pid, p in ps.items():
                pos = p.get("position") or p.get("pos") or ""
                players.append((p, pos))
        else:
            for pos, arr in ps.items():
                if not isinstance(arr, list):
                    continue
                for p in arr:
                    players.append((p, pos))
    elif isinstance(ps, list):
        for p in ps:
            pos = p.get("position") or p.get("pos") or ""
            players.append((p, pos))
    else:
        # fallback: scan for dicts keyed by playerID OR lists of player dicts
        for v in body.values():
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and ("playerID" in item or "playerId" in item or "position" in item):
                        pos = item.get("position") or item.get("pos") or ""
                        players.append((item, pos))
            elif isinstance(v, dict):
                # some boxscore responses structure players as a dict mapping playerId -> playerObj
                # detect that and include each player dict
                possible_players = []
                for kk, vv in v.items():
                    if isinstance(vv, dict) and ("playerID" in vv or "playerId" in vv or "longName" in vv or "playerID" in vv):
                        possible_players.append(vv)
                for item in possible_players:
                    pos = item.get("position") or item.get("pos") or ""
                    players.append((item, pos))
    return players


def fetch_game_boxscore(game_id: str) -> list:
    """Return list of player stat dicts for the given game_id.

    Each dict contains keys: playerId, rushingYards, rushingTDs, receivingYards,
    receivingTDs, targets, receptions, carries (all numeric, default 0).
    """
    # Use Tank01 non-live boxscore endpoint
    url = f"https://{RAPIDAPI_HOST}/getNFLBoxScore"
    headers = build_headers()
    params = {"gameID": game_id}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    except Exception as exc:
        print(f"[fetch-game-box] request failed for {game_id}:", exc)
        return []

    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = {}

    # If the API returned a message (e.g. subscription error), log it and return []
    if isinstance(data, dict) and "message" in data:
        print(f"[fetch-game-box] API message for game {game_id}: {data.get('message')}")

    body = data.get("body") if isinstance(data, dict) and "body" in data else data
    if not isinstance(body, dict):
        return []

    player_tuples = extract_players_from_body(body)
    out = []
    for p, pos in player_tuples:
        try:
            flat = flatten(p)
            # if nested 'player' exists, merge
            if isinstance(p.get("player"), dict):
                flat.update(flatten(p.get("player"), "player"))

            pid = flat.get("playerID") or flat.get("playerId") or flat.get("player.playerID") or flat.get("id")
            if pid is None:
                continue
            try:
                playerId = int(str(pid))
            except Exception:
                import re

                s = re.sub(r"[^0-9]", "", str(pid))
                if not s:
                    continue
                playerId = int(s)

            # extract a few stat candidates including passing
            passing_y = num_from_candidates(flat, ["passingYards", "passing.passingYards", "passYds", "passYd"]) or 0.0
            passing_td = num_from_candidates(flat, ["passingTDs", "passing.td", "passTD"]) or 0.0
            rushing_y = num_from_candidates(flat, ["rushingYards", "rushing.rushingYards", "rushYds", "rushYd", "rushYards"]) or 0.0
            rushing_td = num_from_candidates(flat, ["rushingTDs", "rushing.td", "rushTD"]) or 0.0
            receiving_y = num_from_candidates(flat, ["receivingYards", "receiving.receivingYards", "recYds"]) or 0.0
            receiving_td = num_from_candidates(flat, ["receivingTDs", "receiving.td", "recTD"]) or 0.0
            targets = num_from_candidates(flat, ["targets", "receiving.targets"]) or 0.0
            receptions = num_from_candidates(flat, ["receptions", "receiving.receptions", "rec"]) or 0.0
            carries = num_from_candidates(flat, ["rushingAttempts", "rushAttempts", "att", "attempts"]) or 0.0

            # name & position candidates
            name = flat.get("playerName") or flat.get("player.fullName") or flat.get("name") or flat.get("player.playerName") or flat.get("displayName") or ""
            position = pos or flat.get("position") or flat.get("pos") or ""

            out.append({
                "playerId": int(playerId),
                "playerName": str(name),
                "position": str(position),
                "passingYards": float(passing_y),
                "passingTDs": int(passing_td),
                "rushingYards": float(rushing_y),
                "rushingTDs": int(rushing_td),
                "receivingYards": float(receiving_y),
                "receivingTDs": int(receiving_td),
                "targets": int(targets),
                "receptions": int(receptions),
                "carries": int(carries),
            })
        except Exception:
            continue

    return out


def main():
    import argparse

    p = argparse.ArgumentParser(description="Fetch boxscore for a single game and print normalized player stats as JSON")
    p.add_argument("--gameId", required=True)
    args = p.parse_args()
    rows = fetch_game_boxscore(args.gameId)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
