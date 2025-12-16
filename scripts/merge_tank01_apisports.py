#!/usr/bin/env python3
"""Merge Tank01 basic weekly stats with API-Sports advanced weekly stats.

Writes to external/combined/week_<WEEK>_merged.csv
"""
from __future__ import annotations

import argparse
import logging
import os
from typing import Optional

import pandas as pd


LOG = logging.getLogger("merge_tank01_apisports")


def _norm_name(s: Optional[str]) -> str:
    if s is None:
        return ""
    try:
        return "".join(c for c in str(s).lower() if c.isalnum() or c.isspace()).strip()
    except Exception:
        return str(s).lower()


def load_csv_safe(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        LOG.warning("Missing file: %s", path)
        return None
    try:
        df = pd.read_csv(path)
        LOG.info("Loaded %s (%d rows)", path, len(df))
        return df
    except Exception:
        LOG.exception("Failed to read %s", path)
        return None


def merge_week(week: int) -> int:
    tank_path = os.path.join("external", "tank01", f"player_stats_week_{week}.csv")
    apis_path = os.path.join("external", "apisports", f"advanced_week_{week}.csv")
    out_dir = os.path.join("external", "combined")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"week_{week}_merged.csv")

    tank_df = load_csv_safe(tank_path)
    apis_df = load_csv_safe(apis_path)

    if tank_df is None and apis_df is None:
        LOG.error("Both Tank01 and API-Sports advanced CSVs are missing. Nothing to merge.")
        return 2

    # Ensure we at least have an empty DataFrame to merge into
    if tank_df is None:
        tank_df = pd.DataFrame()
    if apis_df is None:
        apis_df = pd.DataFrame()

    # Normalize tank01 espn id column (common names: playerID, espnID)
    if "playerID" in tank_df.columns:
        tank_df["espnId"] = tank_df["playerID"].astype(str)
    elif "espnID" in tank_df.columns:
        tank_df["espnId"] = tank_df["espnID"].astype(str)
    else:
        tank_df["espnId"] = ""

    # Normalize player name and team/position in both frames
    # Tank01: longName or player.longName
    tank_name_cols = [c for c in ("longName", "player.longName", "name", "player_name") if c in tank_df.columns]
    if tank_name_cols:
        tank_df["player_name"] = tank_df[tank_name_cols[0]]
    else:
        tank_df["player_name"] = ""

    # API-Sports: player_name is expected from fetch_apisports_adv_week
    if "player_name" not in apis_df.columns and "playerName" in apis_df.columns:
        apis_df["player_name"] = apis_df["playerName"]

    # team normalization
    def pick_team(df, candidates):
        for c in candidates:
            if c in df.columns:
                return df[c]
        return pd.Series([""] * len(df))

    tank_df["team_name"] = pick_team(tank_df, ["team", "team_name", "teamName", "team.name"]).astype(str)
    apis_df["team_name"] = pick_team(apis_df, ["team_name", "team", "teamName"]).astype(str)

    tank_df["position"] = pick_team(tank_df, ["pos", "position", "player.position"]).astype(str)
    apis_df["position"] = pick_team(apis_df, ["position", "pos"]).astype(str)

    # Compute a merge key: normalized name + team + position
    tank_df["merge_key"] = (
        tank_df["player_name"].fillna("").apply(_norm_name)
        + "|"
        + tank_df["team_name"].fillna("").astype(str).str.lower()
        + "|"
        + tank_df["position"].fillna("")
    )
    apis_df["merge_key"] = (
        apis_df["player_name"].fillna("").apply(_norm_name)
        + "|"
        + apis_df["team_name"].fillna("").astype(str).str.lower()
        + "|"
        + apis_df["position"].fillna("")
    )

    # Perform outer merge on merge_key
    merged = pd.merge(tank_df, apis_df, on="merge_key", how="outer", suffixes=("_tank", "_apis"))

    # If tank espnId present, expose as espnId column
    if "espnId" in merged.columns:
        merged["espnId"] = merged["espnId"].fillna("")

    # Keep deterministic column ordering
    cols = sorted(merged.columns.tolist())
    merged = merged.reindex(columns=cols)

    LOG.info("Writing merged output to %s (%d rows)", out_path, len(merged))
    merged.to_csv(out_path, index=False)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge Tank01 basic stats with API-Sports advanced stats")
    parser.add_argument("--week", type=int, help="Week number", required=False)
    args = parser.parse_args(argv)

    week = args.week or int(os.environ.get("WEEK", "0") or 0)
    if not week or week <= 0:
        LOG.error("No week provided. Set --week or WEEK env var to a positive integer.")
        return 3

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    return merge_week(week)


if __name__ == "__main__":
    raise SystemExit(main())
