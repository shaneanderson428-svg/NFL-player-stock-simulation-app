#!/usr/bin/env python3
"""
Aggregate weekly stats from per-game boxscores (fetched from Tank01) and write
the normalized CSV consumed by the pricing pipeline.

Usage:
  python3 scripts/build_weekly_stats_from_games.py --season 2025 --week 15
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path
import sys

# Prefer Tank01 boxscore fetcher; fall back to other providers if needed
try:
    from fetch_tank01_game_boxscore import fetch_game_boxscore
    from fetch_tank01_week_games import fetch_week_games
    provider = "tank01"
except Exception:
    # allow running from scripts/ directory directly and fallback
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    try:
        from fetch_tank01_game_boxscore import fetch_game_boxscore  # type: ignore
        from fetch_tank01_week_games import fetch_week_games  # type: ignore
        provider = "tank01"
    except Exception:
        try:
            from fetch_nflapi_game_stats import fetch_game_player_stats as fetch_game_boxscore  # type: ignore
            provider = "nflapi"
        except Exception:
            raise RuntimeError("No game-stats provider available. Install Tank01 or NFLAPI fetch scripts.")


def build(season: int, week: int):
    games_path = Path("data") / "games" / f"season_{season}" / f"week_{week}_games.json"
    if not games_path.exists():
        print(f"[build-week] games file not found: {games_path}; attempting to fetch schedule from provider ({provider})")
        try:
            # fetch_week_games should write the file and return its path
            new_path = fetch_week_games(season, week)
            games_path = Path(new_path)
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch week schedule: {exc}")

    with games_path.open("r", encoding="utf8") as fh:
        games = json.load(fh)

    # games should be list of game dicts with at least gameID
    per_player = defaultdict(lambda: {
        "playerName": "",
        "position": "",
        "passingYards": 0.0,
        "passingTDs": 0,
        "rushingYards": 0.0,
        "rushingTDs": 0,
        "receivingYards": 0.0,
        "receivingTDs": 0,
        "receptions": 0,
        "targets": 0,
        "carries": 0,
    })
    total_games = 0
    finalized_games = len(games)
    boxscores_fetched = 0
    api_calls = 0
    for g in games:
        gid = g.get("gameId") or g.get("gameID") or g.get("gameId") or g.get("id")
        if not gid:
            print("Skipping game entry missing id:", g)
            continue
        total_games += 1
        print(f"[build-week] Fetching boxscore for game {gid}...")
        try:
            players = fetch_game_boxscore(str(gid))
            api_calls += 1
        except Exception as exc:
            print(f"[build-week] Failed to fetch game stats for {gid}:", exc)
            continue
        if not players:
            print(f"[build-week] No player stats returned for game {gid}")
            continue
        boxscores_fetched += 1
        for p in players:
            try:
                pid = int(p.get("playerId"))
            except Exception:
                continue
            # accumulate all stat categories requested
            per_player[pid]["playerName"] = per_player[pid]["playerName"] or p.get("playerName") or ""
            per_player[pid]["position"] = per_player[pid]["position"] or p.get("position") or ""
            per_player[pid]["passingYards"] = float(per_player[pid].get("passingYards") or 0) + float(p.get("passingYards") or 0)
            per_player[pid]["passingTDs"] = int(per_player[pid].get("passingTDs") or 0) + int(p.get("passingTDs") or 0)
            per_player[pid]["rushingYards"] = float(per_player[pid].get("rushingYards") or 0) + float(p.get("rushingYards") or 0)
            per_player[pid]["rushingTDs"] = int(per_player[pid].get("rushingTDs") or 0) + int(p.get("rushingTDs") or 0)
            per_player[pid]["receivingYards"] = float(per_player[pid].get("receivingYards") or 0) + float(p.get("receivingYards") or 0)
            per_player[pid]["receivingTDs"] = int(per_player[pid].get("receivingTDs") or 0) + int(p.get("receivingTDs") or 0)
            per_player[pid]["targets"] = int(per_player[pid].get("targets") or 0) + int(p.get("targets") or 0)
            per_player[pid]["receptions"] = int(per_player[pid].get("receptions") or 0) + int(p.get("receptions") or 0)
            per_player[pid]["carries"] = int(per_player[pid].get("carries") or 0) + int(p.get("carries") or 0)

    print(f"[build-week] Processed {total_games} games (finalized in file: {finalized_games}); boxscores fetched: {boxscores_fetched}")
    print(f"[build-week] API calls made (games): {api_calls}")

    # Build CSV rows
    out_rows = []
    for pid, stats in per_player.items():
        # ensure numeric values
        passing_y = float(stats.get("passingYards") or 0)
        passing_tds = int(stats.get("passingTDs") or 0)
        rushing_y = float(stats.get("rushingYards") or 0)
        rushing_tds = int(stats.get("rushingTDs") or 0)
        receiving_y = float(stats.get("receivingYards") or 0)
        receiving_tds = int(stats.get("receivingTDs") or 0)
        receptions = int(stats.get("receptions") or 0)
        targets = int(stats.get("targets") or 0)
        carries = int(stats.get("carries") or 0)

        # skip players with all-zero stats
        if (
            passing_y == 0
            and passing_tds == 0
            and rushing_y == 0
            and rushing_tds == 0
            and receiving_y == 0
            and receiving_tds == 0
            and receptions == 0
            and targets == 0
            and carries == 0
        ):
            continue

        # compute compatibility fields used by older pipeline
        yards_total = passing_y + rushing_y + receiving_y
        tds_total = passing_tds + rushing_tds + receiving_tds
        epa = yards_total * 0.01 + tds_total * 0.6

        out_rows.append({
            # compatibility: include both espnId and playerId and pricing fields
            "espnId": int(pid),
            "playerId": int(pid),
            "playerName": stats.get("playerName") or "",
            "position": stats.get("position") or "",
            "passingYards": float(passing_y),
            "passingTDs": int(passing_tds),
            "rushingYards": float(rushing_y),
            "rushingTDs": int(rushing_tds),
            "receivingYards": float(receiving_y),
            "receivingTDs": int(receiving_tds),
            "receptions": int(receptions),
            "targets": int(targets),
            "carries": int(carries),
            # legacy pricing columns
            "week": int(week),
            "season": int(season),
            "epa": float(epa),
            "yards": float(yards_total),
            "tds": int(tds_total),
        })

    if not out_rows:
        print(f"[build-week] No player rows to write for season={season} week={week}; not writing CSV")
        return None

    out_dir = Path("data") / "weekly"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"player_stats_{season}_week_{week}.csv"
    import csv

    # prefer the full schema including legacy pricing fields for compatibility
    fieldnames = [
        "espnId",
        "playerId",
        "playerName",
        "position",
        "passingYards",
        "passingTDs",
        "rushingYards",
        "rushingTDs",
        "receivingYards",
        "receivingTDs",
        "receptions",
        "targets",
        "carries",
        "week",
        "season",
        "epa",
        "yards",
        "tds",
    ]
    with out_path.open("w", newline="", encoding="utf8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"[build-week] Wrote {len(out_rows)} players to {out_path}")
    print(f"[build-week] players aggregated: {len(out_rows)}")
    return out_path


def main():
    p = argparse.ArgumentParser(description="Build weekly stats CSV from finished game boxscores")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--week", type=int, required=True)
    args = p.parse_args()
    build(args.season, args.week)


if __name__ == "__main__":
    main()
