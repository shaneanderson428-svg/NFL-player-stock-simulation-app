#!/usr/bin/env python3
"""Calculate lightweight advanced metrics from RapidAPI box-score CSV.

Reads a RapidAPI-produced CSV (defaults to external/rapid/player_stats_live.csv
or external/rapidapi/player_stats_2025.csv if present), computes several
derived columns (pass_efficiency, yards_per_target, rush_success_rate,
estimated_epa, cpoe_estimate) and writes an enriched CSV.

The formulas are directional approximations and intended to be stable across
different providers. Division-by-zero and NaNs are handled gracefully.

Usage:
  python3 scripts/calculate_advanced_metrics.py --input path/to/input.csv --output path/to/output.csv
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import List

try:
    import pandas as pd
except Exception as e:
    print("pandas is required for calculate_advanced_metrics.py; please install it", file=sys.stderr)
    raise

try:
    import numpy as np
except Exception:
    # numpy is a lightweight dependency used only for NaN constants; fall back to math.nan
    import math
    np = None


DEFAULT_INPUTS = [Path("external/rapidapi/player_stats_2025.csv"), Path("external/rapid/player_stats_live.csv")]


def pick_first_existing(paths: List[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


def safe_div(numer, denom):
    # works with pandas Series
    with pd.option_context("mode.use_inf_as_na", True):
        try:
            out = numer / denom
            # coerce to numeric (NaN) so downstream CSVs are friendly and consistent
            out = pd.to_numeric(out, errors="coerce")
        except Exception:
            # return a numeric series filled with NaN
            try:
                idx = numer.index
            except Exception:
                idx = None
            if np is not None:
                nanv = np.nan
            else:
                nanv = float("nan")
            if idx is not None:
                out = pd.Series([nanv] * len(numer), index=idx)
            else:
                out = pd.Series([nanv] * len(numer))
    return out


def map_column(df: pd.DataFrame, candidates: List[str], default=None):
    for c in candidates:
        if c in df.columns:
            return df[c]
    # return a Series of default values
    return pd.Series([default] * len(df), index=df.index)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", help="Input CSV file", default=None)
    p.add_argument("--output", "-o", help="Output enriched CSV file", default="external/rapidapi/player_stats_enriched_2025.csv")
    # escape percent sign in help string for argparse formatting compatibility
    p.add_argument("--cpoe-baseline", type=float, default=0.62, help="League-average completion %% baseline used in CPOE proxy")
    p.add_argument("--season", type=int, default=2025, help="Season year for naming output when using defaults")
    args = p.parse_args(argv)

    inp = None
    if args.input:
        ip = Path(args.input)
        if not ip.exists():
            print(f"Input {ip} not found", file=sys.stderr)
            return 2
        inp = ip
    else:
        cand = pick_first_existing(DEFAULT_INPUTS)
        if cand is None:
            print("No default input found. Provide --input", file=sys.stderr)
            return 2
        inp = cand

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)

    print(f"Reading input {inp}")
    df = pd.read_csv(inp)

    # map common provider column names to canonical short names used in formulas
    # passing
    passing_yards = map_column(df, ["passing_yards", "pass_yards", "passingYds", "passYards", "yards_passed", "yds_pass"], 0).astype(float, errors="ignore")
    passing_attempts = map_column(df, ["passing_attempts", "pass_attempts", "passAtt", "passingAtt", "attempts_passed", "att_pass"], 0).astype(float, errors="ignore")
    completions = map_column(df, ["completions", "pass_completions", "passing_completions", "comp"], 0).astype(float, errors="ignore")
    passing_tds = map_column(df, ["passing_tds", "pass_tds", "passing_td", "passTD"], 0).astype(float, errors="ignore")
    interceptions = map_column(df, ["interceptions", "int", "ints"], 0).astype(float, errors="ignore")

    # receiving
    receiving_yards = map_column(df, ["receiving_yards", "rec_yards", "recYds", "receivingYds"], 0).astype(float, errors="ignore")
    targets = map_column(df, ["targets", "target", "tgts"], 0).astype(float, errors="ignore")

    # rushing
    rushing_attempts = map_column(df, ["rushing_attempts", "rush_attempts", "rushAtt", "rush_attempt"], 0).astype(float, errors="ignore")
    rushing_first_downs = map_column(df, ["rushing_first_downs", "rush_first_downs", "rush_fd", "rush_first_down"], 0).astype(float, errors="ignore")

    # compute derived columns safely
    df = df.copy()

    df["pass_efficiency"] = safe_div(passing_yards, passing_attempts)
    df["yards_per_target"] = safe_div(receiving_yards, targets)
    df["rush_success_rate"] = safe_div(rushing_first_downs, rushing_attempts)

    # estimated EPA proxy
    df["estimated_epa"] = (
        0.04 * passing_yards.fillna(0).astype(float)
        + 0.5 * passing_tds.fillna(0).astype(float)
        - 0.7 * interceptions.fillna(0).astype(float)
    )

    # estimated CPOE proxy (as percentage points)
    # use completions / attempts, but protect divide by zero
    comp_pct = safe_div(completions, passing_attempts)
    df["cpoe_estimate"] = (comp_pct.fillna(0) - args.cpoe_baseline) * 100

    # Replace infinite and NaN with empty for CSV readability
    # prefer numpy.nan for CSV output when available
    if np is not None:
        _nan = np.nan
    else:
        _nan = float("nan")

    for c in ["pass_efficiency", "yards_per_target", "rush_success_rate", "estimated_epa", "cpoe_estimate"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            # ensure any pandas NA/NaT/NaN are converted to a numeric NaN for CSV
            df[c] = df[c].fillna(_nan)

    # Log average values per position if available
    stats_to_log = ["pass_efficiency", "yards_per_target", "rush_success_rate", "estimated_epa", "cpoe_estimate"]
    if "position" in df.columns:
        try:
            grp = df.groupby("position")[stats_to_log].mean(numeric_only=True)
            print("Average derived stats by position:")
            print(grp.round(3).to_string())
        except Exception:
            print("Could not compute averages by position (unexpected data types)")
    else:
        # overall averages
        try:
            overall = df[stats_to_log].mean(numeric_only=True)
            print("Overall averages:")
            print(overall.round(3).to_string())
        except Exception:
            pass

    # Write output CSV
    df.to_csv(outp, index=False)
    print(f"Wrote enriched CSV to {outp}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
