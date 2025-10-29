#!/usr/bin/env python3
"""
Scaffold script to compute per-player advanced metrics from nflfastR pbp files.

Usage:
  python3 scripts/compute_advanced_metrics.py --input data/pbp --output data/advanced

Notes:
- If pyarrow and pandas are installed, this script will read Parquet files; otherwise it will
    look for CSV.gz files.
- The actual advanced metric computations are placeholders — fill them in with your
    preferred formulas or use existing analytics code.
"""
import argparse
import json
import os
import gzip
import csv
from pathlib import Path
from typing import Any

pd: Any
try:
    import pandas as pd
except Exception:
    pd = None


def read_pbp_files(input_dir):
    p = Path(input_dir)
    files = list(p.glob("*.parquet"))
    if not files:
        files = list(p.glob("*.csv.gz"))
    if not files:
        print("No pbp files found in", input_dir)
        return None

    # If pandas is available, use it for faster parsing; otherwise fallback to csv
    if pd is not None:
        frames = []
        for f in files:
            print("Reading", f)
            if f.suffix == ".parquet":
                frames.append(pd.read_parquet(f))
            else:
                frames.append(pd.read_csv(f, compression="gzip"))
        df = pd.concat(frames, ignore_index=True)
        return df

    # Fallback: parse CSV.GZ into a list of dicts
    rows = []
    for f in files:
        print("Reading", f)
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                # normalize numeric-ish fields where possible
                for k, v in list(r.items()):
                    if v is None or v == "":
                        r[k] = None
                        continue
                    try:
                        if "." in v:
                            r[k] = float(v)
                        else:
                            r[k] = int(v)
                    except Exception:
                        # keep as string
                        r[k] = v
                rows.append(r)
    return rows


def compute_metrics(df):
    # Placeholder: group by player id and compute trivial aggregates.
    # Replace this with real advanced metric calculations (EPA per play, CPOE, ROE, YPRR, etc.)
    players = {}
    if df is None:
        return players

    # If pandas is not available, we may receive a list of dict rows from read_pbp_files
    is_list_rows = isinstance(df, list)
    if is_list_rows:
        # convert list of dicts into a simple column set and grouping map
        cols = set()
        for r in df:
            cols.update(r.keys())
        cols = list(cols)
        # build groups by id (we'll detect id_col later)
    else:
        pass

    # Attempt to pick an id column; nflfastR uses several possible columns
    candidate_ids = [
        "rusher_player_id",
        "receiver_player_id",
        "passer_player_id",
        "runner_player_id",
        "player_id",
    ]
    id_col = None
    if is_list_rows:
        for c in candidate_ids:
            if any((c in (r or {})) for r in df):
                id_col = c
                break
    else:
        for c in candidate_ids:
            if c in df.columns:
                id_col = c
                break
    if id_col is None:
        print("No recognized player id column found; skipping metric compute")
        return players

    cols = df.columns if not is_list_rows else list(cols)

    # Precompute league-level baselines for fallback expected values
    league_completion_rate = None
    if "complete_pass" in cols and "pass_attempt" in cols:
        try:
            league_completion_rate = (
                df["complete_pass"].sum() / df["pass_attempt"].sum()
            )
        except Exception:
            league_completion_rate = None

    avg_rush_yards = None
    if "rush_yards" in cols:
        try:
            avg_rush_yards = df["rush_yards"].mean()
        except Exception:
            avg_rush_yards = None

    # Build simple expected completion & expected YAC tables by air_yards bucket (best-effort)
    expected_comp_by_air = None
    expected_yac_by_air = None
    if "air_yards" in cols and "pass_attempt" in cols:
        try:
            df["_air_bucket"] = df["air_yards"].fillna(0).astype(int).clip(-10, 60)
            pass_plays = df[df["pass_attempt"] == 1]
            grouped_air = pass_plays.groupby("_air_bucket")
            expected_comp_by_air = (
                grouped_air["complete_pass"].mean().to_dict()
                if "complete_pass" in pass_plays.columns
                else None
            )
            if "yards_after_catch" in pass_plays.columns:
                expected_yac_by_air = grouped_air["yards_after_catch"].mean().to_dict()
            elif "yac" in pass_plays.columns:
                expected_yac_by_air = grouped_air["yac"].mean().to_dict()
            else:
                expected_yac_by_air = None
        except Exception:
            expected_comp_by_air = None
            expected_yac_by_air = None

    # Group rows by id_col
    grouped_map = {}
    if is_list_rows:
        for r in df:
            pid = r.get(id_col)
            if pid is None:
                continue
            grouped_map.setdefault(pid, []).append(r)
        iter_groups = grouped_map.items()
    else:
        grouped = df.groupby(id_col)
        iter_groups = grouped

    for pid, g in iter_groups:
        pid_str = str(pid)
        total_plays = len(g)

        # EPA per play: mean of 'epa' for plays involving the player
        EPA_per_play = None
        if "epa" in cols:
            try:
                if is_list_rows:
                    vals = [float(x["epa"]) for x in g if x.get("epa") is not None]
                    EPA_per_play = float(sum(vals) / len(vals)) if vals else None
                else:
                    EPA_per_play = float(g["epa"].mean())
            except Exception:
                EPA_per_play = None

        # CPOE: completion percentage over expected for passers
        CPOE = None
        passer_attempts = 0
        passer_completions = 0
        expected_available = False
        if "pass_attempt" in cols:
            try:
                if is_list_rows:
                    passer_attempts = int(sum(1 for x in g if x.get("pass_attempt")))
                else:
                    passer_attempts = int(g["pass_attempt"].sum())
            except Exception:
                passer_attempts = 0
        if "complete_pass" in cols:
            try:
                if is_list_rows:
                    passer_completions = int(
                        sum(1 for x in g if x.get("complete_pass"))
                    )
                else:
                    passer_completions = int(g["complete_pass"].sum())
            except Exception:
                passer_completions = 0

        for col_name in [
            "complete_pass_prob",
            "complete_pass_expected",
            "exp_completion",
            "cp_prob",
        ]:
            if col_name in cols:
                try:
                    # We only care whether an expected/probability column exists
                    _ = float(g[col_name].sum())
                    expected_available = True
                    break
                except Exception:
                    expected_available = False

        if passer_attempts > 0 and expected_comp_by_air is not None:
            try:
                play_buckets = g["_air_bucket"].fillna(0).astype(int)
                expected_rates = [
                    expected_comp_by_air.get(
                        int(b),
                        (
                            league_completion_rate
                            if league_completion_rate is not None
                            else 0
                        ),
                    )
                    for b in play_buckets
                ]
                expected_rate = sum(expected_rates) / max(1, len(expected_rates))
                actual_rate = (
                    passer_completions / passer_attempts if passer_attempts > 0 else 0
                )
                CPOE = actual_rate - expected_rate
                expected_available = True
            except Exception:
                expected_available = expected_available

        if passer_attempts > 0 and not expected_available:
            if league_completion_rate is not None:
                expected_rate = league_completion_rate
            else:
                expected_rate = 0
            actual_rate = (
                passer_completions / passer_attempts if passer_attempts > 0 else 0
            )
            CPOE = actual_rate - expected_rate

        # ROE: RushYardsOverExpected_per_Att (for rushers)
        ROE = None
        rush_attempts = 0
        rush_yards_total = 0.0
        if "rush_attempt" in cols or "rushing_attempt" in cols or "rush" in cols:
            try:
                if "rush_attempt" in cols:
                    rush_attempts = int(g["rush_attempt"].sum())
                elif "rushing_attempt" in cols:
                    rush_attempts = int(g["rushing_attempt"].sum())
                else:
                    if "play_type" in cols:
                        rush_attempts = int((g["play_type"] == "run").sum())
            except Exception:
                rush_attempts = 0
        if "rush_yards" in cols:
            try:
                if is_list_rows:
                    rush_yards_total = float(
                        sum(float(x.get("rush_yards") or 0) for x in g)
                    )
                else:
                    rush_yards_total = float(g["rush_yards"].sum())
            except Exception:
                rush_yards_total = 0.0

        if rush_attempts > 0:
            exp_yards_sum = None
            for col_name in [
                "exp_rush_yards",
                "rush_yards_expected",
                "expected_rush_yards",
            ]:
                if col_name in cols:
                    try:
                        exp_yards_sum = float(g[col_name].sum())
                        break
                    except Exception:
                        exp_yards_sum = None
            if exp_yards_sum is not None:
                ROE = (rush_yards_total - exp_yards_sum) / rush_attempts
            elif avg_rush_yards is not None:
                ROE = (
                    rush_yards_total - (avg_rush_yards * rush_attempts)
                ) / rush_attempts
            else:
                ROE = None

        # YPRR: Yards per route run — estimate using targets as proxy for route runs
        YPRR = None
        targets = 0
        receiving_yards = 0.0
        if "target_player_id" in cols:
            try:
                targets = int((g["target_player_id"].notnull()).sum())
            except Exception:
                targets = 0
        if targets == 0 and "receiver_player_id" in cols:
            try:
                targets = int((g["receiver_player_id"].notnull()).sum())
            except Exception:
                targets = 0
        if "receiving_yards" in cols:
            try:
                if is_list_rows:
                    receiving_yards = float(
                        sum(float(x.get("receiving_yards") or 0) for x in g)
                    )
                else:
                    receiving_yards = float(g["receiving_yards"].sum())
            except Exception:
                receiving_yards = 0.0
        elif "yards_gained" in cols and "target_player_id" in cols:
            try:
                if is_list_rows:
                    receiving_yards = float(
                        sum(
                            float(x.get("yards_gained") or 0)
                            for x in g
                            if x.get("target_player_id")
                        )
                    )
                else:
                    receiving_yards = float(
                        g.loc[g["target_player_id"].notnull(), "yards_gained"].sum()
                    )
            except Exception:
                receiving_yards = 0.0

        if targets > 0:
            YPRR = receiving_yards / targets

        # YAC over expected: if we have expected_yac_by_air, compute average expected YAC for target plays and compare
        YAC_over_expected = None
        if (
            targets > 0
            and expected_yac_by_air is not None
            and "target_player_id" in cols
        ):
            try:
                target_rows = (
                    g[g["target_player_id"].notnull()]
                    if "target_player_id" in cols
                    else g[g["receiver_player_id"].notnull()]
                )
                if not target_rows.empty:
                    buckets = target_rows["_air_bucket"].fillna(0).astype(int)
                    actual_yac = 0.0
                    if "yards_after_catch" in target_rows.columns:
                        actual_yac = float(target_rows["yards_after_catch"].sum())
                    elif "yac" in target_rows.columns:
                        actual_yac = float(target_rows["yac"].sum())
                    else:
                        actual_yac = 0.0
                    expected_vals = [
                        expected_yac_by_air.get(int(b), 0.0) for b in buckets
                    ]
                    expected_total = sum(expected_vals)
                    YAC_over_expected = (actual_yac - expected_total) / max(
                        1, len(buckets)
                    )
            except Exception:
                YAC_over_expected = None

        # WPA: Win Probability Added per play (if wp or wp_before/wp_after exist) — average per play
        WPA_per_play = None
        if "wp" in cols and "wp_before" in cols:
            try:
                if is_list_rows:
                    diffs = [
                        float((x.get("wp") or 0) - (x.get("wp_before") or 0))
                        for x in g
                        if x.get("wp") is not None and x.get("wp_before") is not None
                    ]
                    WPA_per_play = float(sum(diffs) / len(diffs)) if diffs else None
                else:
                    wpa_vals = (g["wp"] - g["wp_before"]).dropna()
                    if len(wpa_vals):
                        WPA_per_play = float(wpa_vals.mean())
            except Exception:
                WPA_per_play = None
        elif "wp" in cols and "wp_post" in cols:
            try:
                if is_list_rows:
                    diffs = [
                        float((x.get("wp_post") or 0) - (x.get("wp") or 0))
                        for x in g
                        if x.get("wp_post") is not None and x.get("wp") is not None
                    ]
                    WPA_per_play = float(sum(diffs) / len(diffs)) if diffs else None
                else:
                    wpa_vals = (g["wp_post"] - g["wp"]).dropna()
                    if len(wpa_vals):
                        WPA_per_play = float(wpa_vals.mean())
            except Exception:
                WPA_per_play = None

        # SuccessRate: percent of player's plays with EPA > 0 (simple definition)
        SuccessRate = None
        if "epa" in cols:
            try:
                if is_list_rows:
                    success_count = int(sum(1 for x in g if (x.get("epa") or 0) > 0))
                else:
                    success_count = int((g["epa"] > 0).sum())
                SuccessRate = success_count / total_plays if total_plays > 0 else None
            except Exception:
                SuccessRate = None

        stats = {
            "espnId": pid_str,
            "plays": int(total_plays),
            "totalYards": (
                float(sum(float(x.get("yards_gained") or 0) for x in g))
                if is_list_rows and "yards_gained" in cols
                else (float(g["yards_gained"].sum()) if "yards_gained" in cols else 0.0)
            ),
            "EPA_per_play": EPA_per_play,
            "CPOE": CPOE,
            "RushYardsOverExpected_per_Att": ROE,
            "YardsPerRouteRun": YPRR,
            "YAC_over_expected": YAC_over_expected,
            "WPA_per_play": WPA_per_play,
            "SuccessRate": SuccessRate,
        }
        players[pid_str] = stats

    return players


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/pbp")
    parser.add_argument("--output", default="data/advanced")
    args = parser.parse_args()

    df = read_pbp_files(args.input)
    players = compute_metrics(df)

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    index = {"lastUpdated": str(os.environ.get("ADVANCED_INDEX_TS", "")), "players": []}
    for pid, stats in players.items():
        # normalize pid to integer-like string to avoid keys like '3045146.0'
        try:
            pid_int = int(float(pid))
        except Exception:
            pid_int = pid
        stats_out = dict(stats)
        # ensure espnId string is normalized
        stats_out["espnId"] = str(pid_int)
        # add a demo player name if none provided
        if not stats_out.get("player"):
            stats_out["player"] = f"Demo {pid_int}"

        out_filename = f"{pid_int}.json"
        out = out_dir / out_filename
        with open(out, "w") as f:
            json.dump(stats_out, f, indent=2)
        index["players"].append({"espnId": pid_int, "file": out_filename})

    index["lastUpdated"] = (
        str(Path(".").absolute())
        if not index.get("lastUpdated")
        else index["lastUpdated"]
    )
    with open(out_dir / "index.json", "w") as f:
        json.dump(index, f, indent=2)

    print("Wrote", len(index["players"]), "player metric files to", out_dir)


if __name__ == "__main__":
    main()
