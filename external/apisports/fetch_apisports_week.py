#!/usr/bin/env python3
"""Fetch weekly player stats from API-Sports (american-football.api-sports.io)

Saves a flattened CSV to external/apisports/player_stats_week_<WEEK>.csv

Usage:
  python external/apisports/fetch_apisports_week.py --week 1
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

import pandas as pd
import requests


LOG = logging.getLogger("fetch_apisports")


def fetch_week_stats(week: int, season: int = 2025) -> pd.DataFrame:
    """Fetch player statistics for the given week and season from API-Sports.

    Returns a pandas DataFrame with flattened columns.
    """
    key = os.environ.get("APISPORTS_KEY")
    if not key:
        LOG.error("Environment variable APISPORTS_KEY is not set. Aborting.")
        raise SystemExit(2)

    url = "https://v1.american-football.api-sports.io/players/statistics"
    headers = {"x-apisports-key": key}
    params = {
        "league": 1,
        "season": season,
        "week": week,
        # request a larger page size if supported; API may ignore
        "page": 1,
    }

    all_rows: List[Dict[str, Any]] = []

    while True:
        LOG.info("Requesting %s params=%s", url, params)
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            LOG.error("API returned %s: %s", r.status_code, r.text[:200])
            r.raise_for_status()

        data = r.json()
        # API returns a `response` list of player-stat blocks
        resp = data.get("response") or []
        LOG.info("Received %d records (page=%s)", len(resp), params.get("page"))

        for item in resp:
            # Each item usually contains 'player', 'team', and 'statistics' (list)
            row: Dict[str, Any] = {}
            player = item.get("player") or {}
            team = item.get("team") or {}
            # canonical ids/names
            row["player_id"] = player.get("id")
            row["player_name"] = player.get("name") or player.get("fullname")
            row["team_id"] = team.get("id")
            row["team_name"] = team.get("name") or team.get("abbreviation")
            row["position"] = player.get("position") or item.get("position")

            # statistics is often a list per team/league, flatten the first element
            stats_list = item.get("statistics") or []
            if isinstance(stats_list, list) and len(stats_list) > 0:
                # stats may be nested dicts like {'games': {...}, 'passing': {...}}
                stats = stats_list[0]
                # flatten nested dicts
                for k, v in stats.items():
                    if isinstance(v, dict):
                        for subk, subv in v.items():
                            keyname = f"{k}_{subk}" if subk else k
                            row[keyname] = subv
                    else:
                        row[k] = v
            else:
                # no stats available, still keep base fields
                pass

            all_rows.append(row)

        # Pagination: API may include paging info under 'paging' or 'meta'
        paging = data.get("paging") or {}
        total_pages = paging.get("total") or paging.get("pages") or None
        current_page = params.get("page", 1)

        # Try the standard paging fields first
        try:
            if total_pages is not None and int(current_page) < int(total_pages):
                params["page"] = int(current_page) + 1
                time.sleep(0.3)
                continue
        except Exception:
            # Fall through to meta-based paging below
            pass

        # Some APIs include meta->page / meta->total_pages instead
        meta = data.get("meta") or {}
        try:
            meta_page = meta.get("page")
            meta_total = meta.get("total_pages") or meta.get("total")
            if meta_page and meta_total and int(meta_page) < int(meta_total):
                params["page"] = int(meta_page) + 1
                time.sleep(0.3)
                continue
        except Exception:
            pass

        # Otherwise break after first page
        break

    df = pd.DataFrame(all_rows)
    return df


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch API-Sports weekly player stats")
    parser.add_argument("--week", type=int, help="Week number to fetch", required=False)
    parser.add_argument("--season", type=int, default=2025, help="Season year")
    args = parser.parse_args(argv)

    week = args.week or int(os.environ.get("WEEK", "0") or 0)
    if not week or week <= 0:
        LOG.error("No week provided. Set --week or WEEK env var to a positive integer.")
        return 3

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    LOG.info("Fetching API-Sports stats for season=%s week=%s", args.season, week)

    try:
        df = fetch_week_stats(week=week, season=args.season)
    except SystemExit:
        return 2
    except Exception:
        LOG.exception("Failed to fetch API-Sports data")
        return 4

    out_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "apisports")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"player_stats_week_{week}.csv")
    LOG.info("Writing %s (%d rows)", out_path, len(df))
    # Ensure deterministic column ordering - sort columns
    if not df.empty:
        df = df.reindex(sorted(df.columns), axis=1)
    df.to_csv(out_path, index=False)
    LOG.info("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
