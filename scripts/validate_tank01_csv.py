#!/usr/bin/env python3
"""Validate Tank01 weekly CSV.

Usage: python scripts/validate_tank01_csv.py --week 1

Prints row count, sample rows, and validates that `player_id` exists and has
at least one non-empty value.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Tank01 weekly CSV")
    parser.add_argument("--week", type=int, required=True, help="week number (integer)")
    args = parser.parse_args()

    week = int(args.week)
    path = os.path.join("external", "tank01", f"player_stats_week_{week}.csv")

    if not os.path.exists(path):
        print(f"Missing Tank01 CSV: {path}")
        return 2

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"Failed to read CSV {path}: {e}")
        return 3

    row_count = len(df)
    print(f"Loaded {path}: {row_count} rows")

    if row_count == 0:
        print("CSV is empty")
        return 4

    # Print a small sample of rows
    print("\nSample rows:\n")
    try:
        print(df.head(5).to_string(index=False))
    except Exception:
        # Fallback: print first 5 rows raw
        print(df.head(5))

    # Check player_id column
    if "player_id" not in df.columns:
        print("FAIL: 'player_id' column not found in CSV")
        return 5

    # Count non-empty player_id values
    try:
        non_empty = df["player_id"].astype(str).str.strip().replace({"nan": ""})
        populated = (non_empty != "").sum()
    except Exception:
        populated = 0

    print(f"player_id populated values: {populated}")
    if populated == 0:
        print("FAIL: 'player_id' column exists but contains no values")
        return 6

    # Print a few distinct player ids as a quick sanity check
    try:
        sample_ids = df["player_id"].dropna().astype(str).str.strip().unique()[:10]
        print("Sample player_id values:", ", ".join(sample_ids.astype(str)))
    except Exception:
        pass

    print("OK: Tank01 CSV looks valid for basic checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
