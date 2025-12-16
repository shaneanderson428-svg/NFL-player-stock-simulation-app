#!/usr/bin/env python3
"""Merge API-Sports weekly CSV with nflfastR master CSV.

Produces external/combined/player_stats_merged_week_<WEEK>.csv

Usage:
  python external/merge/merge_week.py --week 1
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

import pandas as pd


LOG = logging.getLogger("merge_week")


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
    repo_root = os.path.dirname(os.path.dirname(__file__))
    apis_path = os.path.join(repo_root, "apisports", f"player_stats_week_{week}.csv")
    tank_path = os.path.join(repo_root, "tank01", f"player_stats_week_{week}.csv")
    nflfast_path = os.path.join(repo_root, "..", "nflfastR", "player_stats_2025.csv")
    # canonical output location
    out_dir = os.path.join(repo_root, "..", "combined")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"player_stats_merged_week_{week}.csv")

    apis_df = load_csv_safe(apis_path)
    tank_df = load_csv_safe(tank_path)
    nfl_df = load_csv_safe(nflfast_path)

    if nfl_df is None and apis_df is None:
        LOG.error("Both API-Sports and nflfastR CSV files are missing. Nothing to merge.")
        return 2

    if nfl_df is not None:
        # Normalize nflfastR `espn_id` -> `player_id` if present
        if "espn_id" in nfl_df.columns:
            nfl_df = nfl_df.rename(columns={"espn_id": "player_id"})
        # Ensure player_id is present as string for merging
        if "player_id" in nfl_df.columns:
            nfl_df["player_id"] = nfl_df["player_id"].astype(str)

    if apis_df is not None:
        if "player_id" in apis_df.columns:
            apis_df["player_id"] = apis_df["player_id"].astype(str)
        else:
            # attempt to find common id columns
            for cand in ("playerId", "id", "player_id_id"):
                if cand in apis_df.columns:
                    apis_df = apis_df.rename(columns={cand: "player_id"})
                    apis_df["player_id"] = apis_df["player_id"].astype(str)
                    break

    # Tank01 CSV handling: normalize playerID -> player_id and prefix tank01_ to its stats
    if tank_df is not None:
        # normalize id
        if "playerID" in tank_df.columns:
            tank_df = tank_df.rename(columns={"playerID": "player_id"})
        if "player_id" in tank_df.columns:
            tank_df["player_id"] = tank_df["player_id"].astype(str)

        # identify a player name column to keep unprefixed if present
        name_candidates = ["longName", "fullName", "player_name", "name", "playerName", "player.longName", "long_name"]
        keep_names = [c for c in name_candidates if c in tank_df.columns]
        # Build rename map: prefix everything except player_id and any keep_names
        rename_map = {}
        for c in tank_df.columns:
            if c == "player_id" or c in keep_names:
                continue
            rename_map[c] = f"tank01_{c}"
        if rename_map:
            tank_df = tank_df.rename(columns=rename_map)

    # If one side is missing, write the other as merged output with a source column
    # If only nflfastR present (both apis and tank absent), write nflfastR-only
    if apis_df is None and tank_df is None:
        LOG.info("API-Sports and Tank01 missing; writing nflfastR-only merged file")
        assert nfl_df is not None
        out_df = nfl_df.copy()
        out_df["_merge_source"] = "nflfastR_only"
        out_df.to_csv(out_path, index=False)
        LOG.info("Wrote %s", out_path)
        return 0

    # If nflfastR missing but we have either apis or tank01, write a combined source-only file
    if nfl_df is None:
        # Prefer to merge available external sources together by outer-joining on player_id
        LOG.info("nflfastR missing; merging available external sources only")
        sources = []
        if apis_df is not None:
            sources.append(apis_df.copy())
        if tank_df is not None:
            sources.append(tank_df.copy())
        if not sources:
            LOG.error("No external source available to write output")
            return 2
        # Merge all external sources sequentially on player_id
        out_df = sources[0]
        for s in sources[1:]:
            out_df = pd.merge(out_df, s, on="player_id", how="outer")
        out_df["_merge_source"] = "external_only"
        out_df.to_csv(out_path, index=False)
        LOG.info("Wrote %s", out_path)
        return 0

    # Both nflfastR and at least one external source present: perform outer merge
    LOG.info(
        "Merging sources: nflfastR (%d), API-Sports (%s), Tank01 (%s)",
        len(nfl_df),
        len(apis_df) if apis_df is not None else "-",
        len(tank_df) if tank_df is not None else "-",
    )

    # Start with nfl_df and merge external sources (apis_df and tank_df) into it
    merged = nfl_df.copy()
    if apis_df is not None:
        merged = pd.merge(merged, apis_df, on="player_id", how="outer", indicator=False)
    if tank_df is not None:
        merged = pd.merge(merged, tank_df, on="player_id", how="outer", indicator=False)

    # Provide canonical epa_per_play and cpoe preferring nflfastR values when available
    for col in ("epa_per_play", "cpoe"):
        if col in nfl_df.columns:
            merged[col] = merged[col]
        else:
            tank_col = f"tank01_{col}"
            apis_col = f"{col}_apis"
            if tank_col in merged.columns:
                merged[col] = merged[tank_col]
            elif apis_col in merged.columns:
                merged[col] = merged[apis_col]

    # Create a merge source column: prefer explicit _merge if available, otherwise infer
    merged["_merge_source"] = "nflfastR_with_external"

    # Drop duplicated/unprefixed external columns if they duplicate canonical columns
    # (keep canonical epa_per_play and cpoe only)
    drop_cols = []
    for c in merged.columns:
        if c.startswith("tank01_"):
            # keep tank01_ prefixed columns
            continue
        if c.endswith("_apis"):
            # api-sports suffix columns: drop to avoid duplication
            drop_cols.append(c)
    if drop_cols:
        merged.drop(columns=drop_cols, inplace=True, errors="ignore")

    # Ensure deterministic alphabetical ordering of columns
    cols = sorted(merged.columns.tolist())
    merged = merged.reindex(columns=cols)

    LOG.info("Writing merged output to %s (%d rows)", out_path, len(merged))
    merged.to_csv(out_path, index=False)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Merge API-Sports with nflfastR for a given week")
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
