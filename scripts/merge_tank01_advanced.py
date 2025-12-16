#!/usr/bin/env python3
"""Merge Tank01 weekly stats with our computed advanced metrics.

Writes external/combined/week_<WEEK>_merged.csv
"""
from __future__ import annotations

import argparse
import logging
import os
from typing import Optional

import pandas as pd
from pandas.errors import EmptyDataError


LOG = logging.getLogger("merge_tank01_advanced")


def load_csv_safe(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        LOG.warning("Missing file: %s", path)
        return None
    try:
        df = pd.read_csv(path)
        LOG.info("Loaded %s (%d rows)", path, len(df))
        return df
    except EmptyDataError:
        LOG.warning("Empty CSV (no columns) at %s", path)
        return None
    except Exception:
        LOG.exception("Failed to read %s", path)
        return None


def _norm_name(s: Optional[str]) -> str:
    if s is None:
        return ""
    try:
        return "".join(c for c in str(s).lower() if c.isalnum() or c.isspace()).strip()
    except Exception:
        return str(s).lower()


def merge_week(week: int) -> int:
    tank_path = os.path.join("external", "tank01", f"player_stats_week_{week}.csv")
    adv_path = os.path.join("external", "advanced", f"advanced_metrics_week_{week}.csv")
    out_dir = os.path.join("external", "combined")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"week_{week}_merged.csv")

    tank = load_csv_safe(tank_path)
    adv = load_csv_safe(adv_path)

    if tank is None and adv is None:
        LOG.error("Both Tank01 and advanced metrics CSVs are missing. Nothing to merge.")
        return 2

    if tank is None:
        LOG.info("Tank01 missing; writing advanced-only merged file")
        assert adv is not None
        adv["_merge_source"] = "advanced_only"
        adv.to_csv(out_path, index=False)
        return 0

    if adv is None:
        LOG.info("Advanced metrics missing; writing Tank01-only merged file")
        tank["_merge_source"] = "tank01_only"
        tank.to_csv(out_path, index=False)
        return 0

    # Normalize espn/player ids
    if "playerID" in tank.columns:
        tank["espnId"] = tank["playerID"].astype(str)
    elif "espnID" in tank.columns:
        tank["espnId"] = tank["espnID"].astype(str)
    else:
        tank["espnId"] = ""

    # advanced metrics use player_id column
    if "player_id" in adv.columns:
        adv["espnId"] = adv["player_id"].astype(str)

    # prepare fallback merge key name|team|position
    tank["player_name"] = tank.get("longName", tank.get("player_name", ""))
    adv["player_name"] = adv.get("player_name", adv.get("player_name", ""))
    tank["team_name"] = tank.get("team", tank.get("team_name", ""))
    adv["team_name"] = adv.get("team_name", adv.get("team", ""))
    tank["position"] = tank.get("pos", tank.get("position", ""))
    adv["position"] = adv.get("position", "")

    tank["merge_key"] = (
        tank["player_name"].fillna("").apply(_norm_name)
        + "|"
        + tank["team_name"].fillna("").astype(str).str.lower()
        + "|"
        + tank["position"].fillna("")
    )
    adv["merge_key"] = (
        adv["player_name"].fillna("").apply(_norm_name)
        + "|"
        + adv["team_name"].fillna("").astype(str).str.lower()
        + "|"
        + adv["position"].fillna("")
    )

    # Prefer joining on espnId when available, otherwise use merge_key
    if adv["espnId"].notna().any() and tank["espnId"].notna().any():
        merged = pd.merge(tank, adv, on="espnId", how="outer", suffixes=("_tank", "_adv"))
    else:
        merged = pd.merge(tank, adv, on="merge_key", how="outer", suffixes=("_tank", "_adv"))

    merged["_merge_source"] = "tank01_advanced"
    # deterministic column ordering
    merged = merged.reindex(columns=sorted(merged.columns.tolist()))
    LOG.info("Writing merged output to %s (%d rows)", out_path, len(merged))
    merged.to_csv(out_path, index=False)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Merge Tank01 with computed advanced metrics")
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
