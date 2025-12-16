#!/usr/bin/env python3
"""Fetch advanced weekly player statistics from API-Sports and write a flattened CSV.

Usage: python scripts/fetch_apisports_adv_week.py --week 1

Writes to external/apisports/advanced_week_<WEEK>.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any, Dict, List

import pandas as pd
import requests


LOG = logging.getLogger("fetch_apisports_adv")


def fetch_week_stats(week: int, season: int = 2025) -> pd.DataFrame:
    key = os.environ.get("APISPORTS_KEY")
    if not key:
        LOG.error("Environment variable APISPORTS_KEY is not set. Aborting.")
        raise SystemExit(2)

    url = "https://v1.american-football.api-sports.io/players/statistics"
    headers = {"x-apisports-key": key}
    params = {"league": 1, "season": season, "week": week, "page": 1}

    all_rows: List[Dict[str, Any]] = []

    while True:
        LOG.info("Requesting %s params=%s", url, params)
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            LOG.error("API returned %s: %s", r.status_code, r.text[:200])
            r.raise_for_status()

        data = r.json()
        resp = data.get("response") or []
        LOG.info("Received %d records (page=%s)", len(resp), params.get("page"))

        for item in resp:
            # Each item contains 'player', 'team', and 'statistics' list
            row: Dict[str, Any] = {}
            player = item.get("player") or {}
            team = item.get("team") or {}
            row["player_id"] = player.get("id")
            row["player_name"] = player.get("name") or player.get("fullname")
            row["team_id"] = team.get("id")
            row["team_name"] = team.get("name") or team.get("abbreviation")
            row["position"] = player.get("position") or item.get("position")

            stats_list = item.get("statistics") or []
            if isinstance(stats_list, list) and len(stats_list) > 0:
                stats = stats_list[0]
                # flatten nested dicts recursively
                def _flatten(prefix: str, obj: Any) -> None:
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            key = f"{prefix}_{k}" if prefix else k
                            _flatten(key, v)
                    else:
                        row[prefix] = obj

                _flatten("", stats)

            all_rows.append(row)

        # pagination
        paging = data.get("paging") or {}
        total_pages = paging.get("total") or paging.get("pages") or None
        current_page = params.get("page", 1)

        try:
            if total_pages is not None and int(current_page) < int(total_pages):
                params["page"] = int(current_page) + 1
                time.sleep(0.3)
                continue
        except Exception:
            pass

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

        break

    df = pd.DataFrame(all_rows)
    return df


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch API-Sports advanced weekly stats")
    parser.add_argument("--week", type=int, help="Week number to fetch", required=False)
    parser.add_argument("--season", type=int, default=2025, help="Season year")
    args = parser.parse_args(argv)

    week = args.week or int(os.environ.get("WEEK", "0") or 0)
    if not week or week <= 0:
        LOG.error("No week provided. Set --week or WEEK env var to a positive integer.")
        return 3

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    LOG.info("Fetching API-Sports ADV stats for season=%s week=%s", args.season, week)

    try:
        df = fetch_week_stats(week=week, season=args.season)
    except SystemExit:
        return 2
    except Exception:
        LOG.exception("Failed to fetch API-Sports advanced data")
        return 4

    out_dir = os.path.join("external", "apisports")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"advanced_week_{week}.csv")
    LOG.info("Writing %s (%d rows)", out_path, len(df))
    if not df.empty:
        df = df.reindex(sorted(df.columns), axis=1)
    df.to_csv(out_path, index=False)
    LOG.info("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
