#!/usr/bin/env python3
"""Clean player CSVs:

- Loads `data/player_stock_summary.csv` and `data/player_profiles_cleaned.csv`.
- Drops rows where `player` or `espnId` are missing/blank/NaN.
- Prints how many rows were removed per file.
- Saves cleaned DataFrames back to the same file paths.

Usage: python3 scripts/clean_player_csvs.py
"""
from pathlib import Path
import pandas as pd


def is_blank_series(s: pd.Series) -> pd.Series:
    # Treat NaN, empty string, whitespace-only, and literal 'nan' (case-insensitive) as blank
    return (
        s.isna()
        | s.astype(str).str.strip().eq("")
        | s.astype(str).str.strip().str.lower().eq("nan")
    )


def clean_file(path: Path) -> int:
    if not path.exists():
        print(f"[skip] {path}: file not found")
        return 0
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[error] failed to read {path}: {e}")
        return 0

    before = len(df)
    missing_cols = [c for c in ("player", "espnId") if c not in df.columns]
    if missing_cols:
        print(f"[skip] {path}: missing required columns: {', '.join(missing_cols)}")
        return 0

    bad_mask = is_blank_series(df["player"]) | is_blank_series(df["espnId"])
    removed = int(bad_mask.sum())
    if removed > 0:
        cleaned = df.loc[~bad_mask].copy()
        try:
            cleaned.to_csv(path, index=False)
        except Exception as e:
            print(f"[error] failed to write cleaned CSV to {path}: {e}")
            return 0
    else:
        # still write back to ensure consistent formatting (optional)
        try:
            df.to_csv(path, index=False)
        except Exception:
            pass

    after = before - removed
    print(f"{path}: removed {removed} rows (from {before} -> {after})")
    return removed


def main():
    base = Path("data")
    paths = [base / "player_stock_summary.csv", base / "player_profiles_cleaned.csv"]
    totals = {}
    for p in paths:
        totals[str(p)] = clean_file(p)

    print("\nSummary:")
    for k, v in totals.items():
        print(f" - {k}: {v} rows removed")


if __name__ == "__main__":
    main()
