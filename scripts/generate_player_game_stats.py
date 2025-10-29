"""
Aggregate play-by-play into per-player-per-game stats needed by compute_player_stock.py

The script reads a play-by-play CSV (gz) at data/play_by_play_{year}.csv.gz and
produces data/player_game_stats.csv with columns: player, week, epa_per_play, cpoe, plays

Usage:
    python scripts/generate_player_game_stats.py --year 2025

If the play-by-play file isn't present, the script will exit with an error.
"""

import argparse
from pathlib import Path
import sys
import pandas as pd


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", "-y", type=int, default=2025)
    parser.add_argument(
        "--pbp", type=str, default=None, help="Path to play-by-play gz CSV"
    )
    parser.add_argument(
        "--output", "-o", type=str, default="data/player_game_stats.csv"
    )
    args = parser.parse_args(argv)

    pbp_path = (
        Path(args.pbp) if args.pbp else Path(f"data/play_by_play_{args.year}.csv.gz")
    )
    out_path = Path(args.output)

    if not pbp_path.exists():
        print(f"Play-by-play file not found: {pbp_path}", file=sys.stderr)
        sys.exit(2)

    # Only read the columns we need
    # Include pass/rush columns needed for QB stock
    usecols = [
        "game_id",
        "week",
        "passer_player_name",
        "epa",
        "cpoe",
        "play_id",
        "passing_yards",
        "pass_touchdown",
        "interception",
        "rushing_yards",
        "rush_touchdown",
        "fumble",
        "pass_attempt",
    ]
    try:
        df = pd.read_csv(
            pbp_path,
            compression="gzip",
            usecols=usecols,
            dtype={"passer_player_name": "string"},
        )
    except Exception as e:
        print("Failed to read play-by-play:", e, file=sys.stderr)
        sys.exit(2)

    # Drop plays without a passer name
    df = df.dropna(subset=["passer_player_name"])

    # Convert types
    df["week"] = pd.to_numeric(df["week"], errors="coerce")
    df = df.dropna(subset=["week"])

    # Aggregate per player x week
    # Normalize indicator columns to numeric
    for c in [
        "pass_touchdown",
        "interception",
        "rush_touchdown",
        "fumble",
        "pass_attempt",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        else:
            df[c] = 0

    # Aggregate per player x week
    grp = (
        df.groupby(["passer_player_name", "week"], dropna=True)
        .agg(
            pass_attempts=("pass_attempt", "sum"),
            pass_yards=("passing_yards", "sum"),
            pass_tds=("pass_touchdown", "sum"),
            ints=("interception", "sum"),
            rush_yards=("rushing_yards", "sum"),
            rush_tds=("rush_touchdown", "sum"),
            fumbles=("fumble", "sum"),
            plays=("play_id", "count"),
            epa_total=("epa", "sum"),
            cpoe_total=("cpoe", "sum"),
        )
        .reset_index()
    )

    # Compute per-play averages where appropriate
    grp["epa_per_play"] = grp["epa_total"] / grp["plays"]
    grp["cpoe"] = grp["cpoe_total"] / grp["plays"]

    out = grp.rename(columns={"passer_player_name": "player"})[
        [
            "player",
            "week",
            "pass_yards",
            "pass_tds",
            "ints",
            "rush_yards",
            "rush_tds",
            "fumbles",
            "epa_per_play",
            "cpoe",
            "pass_attempts",
        ]
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
