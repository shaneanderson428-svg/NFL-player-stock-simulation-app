#!/usr/bin/env python3
"""
Download the play-by-play CSV from the nflverse GitHub release, compute
average EPA/CPOE by passer (filtering by minimum plays), and write a CSV
to data/epa_cpoe_summary_2025.csv.

This script is intended to be run in CI (GitHub Actions). It is intentionally
minimal and uses only pandas.
"""
from pathlib import Path
import pandas as pd
import sys


def main():
    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / "epa_cpoe_summary_2025.csv"

    url = (
        "https://github.com/nflverse/nflverse-data/releases/download/pbp/"
        "play_by_play_2025.csv.gz"
    )

    print("Fetching live 2025 play-by-play data...")
    df = pd.read_csv(url, compression="gzip", low_memory=False)
    print(f"Loaded {len(df):,} plays")

    summary = (
        df.groupby("passer_player_name", dropna=True)
        .agg(
            avg_epa=("epa", "mean"),
            avg_cpoe=("cpoe", "mean"),
            plays=("play_id", "count"),
        )
        .loc[lambda d: d["plays"] >= 50]
        .sort_values("avg_epa", ascending=False)
    )

    summary.head(10).to_csv(out_file, index=True)
    print(f"Wrote summary to {out_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise
