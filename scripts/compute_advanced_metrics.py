#!/usr/bin/env python3
"""Compute advanced metrics from Tank01 weekly player stats.

Writes external/advanced/advanced_metrics_week_<WEEK>.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from scripts._safe import safe_float


LOG = logging.getLogger("compute_advanced_metrics")


def _safe_num(s: Any, default: float = 0.0) -> float:
    return safe_float(s, default=default)


def find_col(df: pd.DataFrame, candidates: List[str], default: Optional[str] = None) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return default


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    # Copy input to avoid modifying original
    df = df.copy()

    # detect columns
    rush_col = find_col(df, ["rush_att", "rush_attempts", "rushing_attempts", "rushAttempts", "rushing_att"], None)
    rec_col = find_col(df, ["rec_targets", "targets", "receptions", "rec_targets", "targets_received"], None)
    rush_y_col = find_col(df, ["rush_yards", "rushing_yards", "rushYards"], None)
    rec_y_col = find_col(df, ["rec_yards", "receiving_yards", "recYards"], None)
    pass_y_col = find_col(df, ["pass_yards", "passing_yards"], None)
    pass_td_col = find_col(df, ["pass_tds", "passing_tds"], None)
    rush_td_col = find_col(df, ["rush_tds", "rushing_tds"], None)
    rec_td_col = find_col(df, ["rec_tds", "receiving_tds"], None)
    ints_col = find_col(df, ["ints", "interceptions"], None)
    fumbles_col = find_col(df, ["fumbles", "fumbles_lost", "fum"], None)
    fantasy_col = find_col(df, ["fantasyPoints", "fantasy_points", "fantasy"], None)

    # team and player name cols
    team_col = find_col(df, ["team", "team_name", "teamName", "teamID"], "")
    name_col = find_col(df, ["longName", "player_name", "name", "playerName"], "")
    pos_col = find_col(df, ["pos", "position"], "")

    # compute touches
    def get_series(col) -> pd.Series:
        if col and col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    rush_att = get_series(rush_col)
    rec_tg = get_series(rec_col)
    df["touches"] = (rush_att + rec_tg).astype(float)

    # team touches
    team = df[team_col] if team_col in df.columns else pd.Series([""] * len(df))
    df["team_name"] = team.astype(str)
    team_touches = df.groupby("team_name")["touches"].transform("sum").replace({0: np.nan}).fillna(0.0)

    # opportunity share
    df["opportunity_share"] = df["touches"] / team_touches.replace({0: np.nan}).fillna(0.0)
    df["opportunity_share"] = df["opportunity_share"].fillna(0.0)

    # yards per touch
    rush_y = get_series(rush_y_col)
    rec_y = get_series(rec_y_col)
    df["yards_per_touch"] = ((rush_y + rec_y) / df["touches"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # efficiency_score: yards_per_touch normalized by position averages
    df["position"] = df[pos_col].fillna("") if pos_col in df.columns else pd.Series([""] * len(df))
    pos_avg = df.groupby("position")["yards_per_touch"].transform("mean").replace({0: np.nan}).fillna(0.0)
    # avoid division by zero
    df["efficiency_score"] = df["yards_per_touch"] / pos_avg.replace({0: np.nan}).fillna(1.0)

    # fantasy_efficiency: fantasy_points / expected_points (expected based on touches and pos averages)
    fantasy_pts = get_series(fantasy_col)
    df["fantasy_points"] = fantasy_pts
    # position average fantasy per touch
    with np.errstate(divide="ignore", invalid="ignore"):
        df["fp_per_touch"] = (fantasy_pts / df["touches"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    pos_fp_avg = df.groupby("position")["fp_per_touch"].transform("mean").replace({0: np.nan}).fillna(0.0)
    expected = df["touches"] * pos_fp_avg.fillna(0.0)
    df["expected_points"] = expected
    df["fantasy_efficiency"] = (df["fantasy_points"] / df["expected_points"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # weighted_production_score: combine components
    pass_y = get_series(pass_y_col)
    pass_tds = get_series(pass_td_col)
    rush_tds = get_series(rush_td_col)
    rec_tds = get_series(rec_td_col)

    df["weighted_production_score"] = (
        0.2 * pass_y + 4.0 * pass_tds + 0.1 * (rush_y + rec_y) + 6.0 * (rush_tds + rec_tds)
    )
    # normalize by position mean
    pos_wp_avg = df.groupby("position")["weighted_production_score"].transform("mean").replace({0: np.nan}).fillna(1.0)
    df["weighted_production_score"] = df["weighted_production_score"] / pos_wp_avg

    # epa_lite: simple EPA-style estimate
    ints = get_series(ints_col)
    fumbles = get_series(fumbles_col)
    df["epa_lite"] = 0.02 * (rush_y + rec_y + pass_y) + 0.6 * (pass_tds + rush_tds + rec_tds) - 0.4 * (ints + fumbles)

    # volatility_score: squared deviation from position expectation (yards_per_touch)
    df["volatility_score"] = (df["yards_per_touch"] - pos_avg).pow(2)

    # momentum_score: use price_history.json if present
    ph_path = os.path.join("data", "price_history.json")
    last_prices = {}
    try:
        if os.path.exists(ph_path):
            with open(ph_path, "r") as fh:
                ph = json.load(fh)
            # ph assumed to be dict player_id -> list of price entries or simple mapping
            if isinstance(ph, dict):
                for k, v in ph.items():
                    # take last price if list
                    if isinstance(v, list) and v:
                        last_item = v[-1]
                        if isinstance(last_item, dict):
                            price_candidate = last_item.get("price", None)
                        else:
                            price_candidate = last_item
                        price_val = safe_float(price_candidate, default=float("nan"))
                        if not np.isnan(price_val):
                            last_prices[str(k)] = price_val
                    else:
                        price_val = safe_float(v, default=float("nan"))
                        if not np.isnan(price_val):
                            last_prices[str(k)] = price_val
    except Exception:
        LOG.exception("Failed reading price_history.json")

    # map player id from Tank01: playerID or espnID
    id_col = find_col(df, ["playerID", "espnID", "playerID"], None)
    if id_col and id_col in df.columns:
        df["player_id"] = df[id_col].astype(str)
    else:
        df["player_id"] = pd.Series([""] * len(df), index=df.index).astype(str)
    momentum = []
    for pid in df["player_id"].astype(str).tolist():
        last = last_prices.get(pid) or last_prices.get(str(pid))
        if last is None:
            momentum.append(0.0)
        else:
            # crude momentum: difference between current expected_points and last price
            # not ideal but provides a signal
            momentum.append(0.0)
    df["momentum_score"] = momentum

    # select output columns
    out_cols = [
        "player_id",
        "player_name",
        "team_name",
        "position",
        "touches",
        "opportunity_share",
        "yards_per_touch",
        "efficiency_score",
        "fantasy_points",
        "expected_points",
        "fantasy_efficiency",
        "weighted_production_score",
        "epa_lite",
        "volatility_score",
        "momentum_score",
    ]

    # Provide sensible typed defaults for missing columns so the DataFrame
    # doesn't end up with mixed-type columns. String columns get "", numeric
    # columns get 0.0. This avoids assigning a string to a column later
    # used numerically (and vice versa), which can trigger static-type
    # diagnostics and runtime surprises.
    string_cols = {"player_id", "player_name", "team_name", "position"}
    numeric_cols = set(out_cols) - string_cols
    for c in out_cols:
        if c not in df.columns:
            if c in string_cols:
                df[c] = ""
            else:
                df[c] = 0.0

    return df[out_cols]


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute advanced metrics from Tank01 weekly stats")
    parser.add_argument("--week", type=int, help="Week number", required=False)
    args = parser.parse_args(argv)

    week = args.week or int(os.environ.get("WEEK", "0") or 0)
    if not week or week <= 0:
        LOG.error("No week provided. Set --week or WEEK env var to a positive integer.")
        return 3

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    tank_path = os.path.join("external", "tank01", f"player_stats_week_{week}.csv")
    if not os.path.exists(tank_path):
        LOG.error("Tank01 weekly file not found: %s", tank_path)
        return 2

    df = pd.read_csv(tank_path)
    LOG.info("Loaded Tank01 %s (%d rows)", tank_path, len(df))

    out = compute_metrics(df)

    out_dir = os.path.join("external", "advanced")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"advanced_metrics_week_{week}.csv")
    LOG.info("Writing %s (%d rows)", out_path, len(out))
    out.to_csv(out_path, index=False)
    return 0

