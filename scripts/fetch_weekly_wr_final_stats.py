#!/usr/bin/env python3
"""
Fetch FINAL WR stats for the current NFL week using Tank01 (RapidAPI).

Runs by calling the scoreboard endpoint to discover final games, then
fetches box scores for each final game and extracts WR stats.

To run weekly via cron (Tuesday 9 AM EST):
# 0 9 * * TUE

Requirements:
- Uses RAPIDAPI_KEY environment variable for auth.
- Only uses /getNFLScoreboard and /getNFLBoxScore endpoints (no live endpoints).
- Handles network errors, rate limits (429), missing gameIDs, and empty weeks.

Usage:
  python3 scripts/fetch_weekly_wr_final_stats.py

Provides `run_weekly_fetch()` so it can be invoked programmatically.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from pathlib import Path
import sys
# Make `scripts` package importable when running this file directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import scripts._env  # loads .env.local into environment (if present)
import argparse
import scripts._schedule as _schedule


RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
MAX_WORKERS = 6
RETRIES = 5
BACKOFF_BASE = 1.0
FINAL_STATUS_SUBSTRS = ("final", "completed", "complete", "closed")


def request_with_retries(url: str, headers: Dict[str, str], params: Optional[Dict[str, str]] = None) -> requests.Response:
    """Perform GET with retries for transient errors and 429 rate limits."""
    attempt = 0
    while True:
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        except requests.RequestException as exc:
            attempt += 1
            if attempt > RETRIES:
                raise
            sleep = BACKOFF_BASE * (2 ** (attempt - 1)) + (0.1 * attempt)
            logging.warning("Request exception, retrying in %.1fs: %s", sleep, exc)
            time.sleep(sleep)
            continue

        if resp.status_code == 429:
            attempt += 1
            if attempt > RETRIES:
                resp.raise_for_status()
            # If Retry-After header present, honor it
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra is not None else BACKOFF_BASE * (2 ** (attempt - 1))
            except Exception:
                wait = BACKOFF_BASE * (2 ** (attempt - 1))
            logging.warning("Rate limited (429). Sleeping %.1fs before retry (attempt %d)", wait, attempt)
            time.sleep(wait)
            continue

        # other non-200s are returned to caller to handle
        return resp


def _extract_list_from_response(data: Any) -> List[Any]:
    """Defensive extraction of arrays from Tank01 responses."""
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("games", "gameList", "body", "data", "items", "scoreboard", "gamesList"):
            v = data.get(k)
            if isinstance(v, list):
                return v
            # body sometimes is a dict map of id->game
            if isinstance(v, dict):
                # convert map->list
                return list(v.values())
        # fallback: any first list value inside dict
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


def _is_final_status(status_raw: Optional[str]) -> bool:
    if not status_raw:
        return False
    s = str(status_raw).lower()
    return any(sub in s for sub in FINAL_STATUS_SUBSTRS)


def detect_current_season_and_week(headers: Dict[str, str]) -> Optional[Dict[str, int]]:
    """Detect current season and most recent week that has final games.

    Strategy: try current calendar year, probe weeks from 18 down to 1 and pick
    the highest week that has at least one final game. If none found and it's
    early in the year, attempt prior year.
    """
    today = date.today()
    years_to_try = [today.year]
    # If it's early in the year, also try previous season (e.g., Jan before new season)
    if today.month < 3:
        years_to_try.append(today.year - 1)

    for yr in years_to_try:
        # Typical NFL weeks up to 18 (including playoffs maybe higher); probe 18->1
        for wk in range(18, 0, -1):
            url = f"https://{RAPIDAPI_HOST}/getNFLScoreboard"
            params = {"season": str(yr), "week": str(wk)}
            try:
                resp = request_with_retries(url, headers, params=params)
            except Exception as exc:
                logging.debug("Scoreboard request failed for %s wk %s: %s", yr, wk, exc)
                continue

            if resp.status_code != 200:
                logging.debug("Scoreboard %s wk %s returned %d", yr, wk, resp.status_code)
                continue

            try:
                data = resp.json()
            except Exception:
                try:
                    data = json.loads(resp.text or "{}")
                except Exception:
                    data = None

            games = _extract_list_from_response(data)
            if not games:
                continue

            # check if any game is final
            any_final = False
            for g in games:
                status = None
                if isinstance(g, dict):
                    status = g.get("gameStatus") or g.get("status") or g.get("gameStatusText")
                if _is_final_status(status):
                    any_final = True
                    break

            if any_final:
                logging.info("Detected season=%s week=%s (has final games)", yr, wk)
                return {"season": yr, "week": wk}

    logging.warning("Could not automatically detect a season/week with final games")
    return None


def fetch_final_game_ids_for_week(season: int, week: int, headers: Dict[str, str]) -> List[str]:
    url = f"https://{RAPIDAPI_HOST}/getNFLScoreboard"
    params = {"season": str(season), "week": str(week)}
    resp = request_with_retries(url, headers, params=params)
    if resp.status_code != 200:
        logging.error("Scoreboard request failed: %s", resp.status_code)
        return []

    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = None

    games = _extract_list_from_response(data)
    final_ids: List[str] = []
    for g in games:
        if not isinstance(g, dict):
            continue
        status = g.get("gameStatus") or g.get("status") or g.get("gameStatusText")
        if not _is_final_status(status):
            continue
        gid = g.get("gameID") or g.get("gameId") or g.get("id")
        if gid:
            final_ids.append(str(gid))
        else:
            logging.warning("Final game missing gameID in scoreboard entry: %s", g)
    logging.info("Found %d final games for season=%s week=%s", len(final_ids), season, week)
    return final_ids


def extract_wr_players_from_box(box: Any) -> List[Dict[str, Any]]:
    """Search the box score JSON for WR player entries and return flattened stats dicts."""
    found: List[Dict[str, Any]] = []

    def _scan(obj: Any, parent: Optional[Dict[str, Any]] = None):
        if isinstance(obj, dict):
            # detect a player-like dict
            keys = set(k.lower() for k in obj.keys())
            if ("position" in keys or "pos" in keys) and ("playerid" in keys or "playerid" in keys or "player" in keys):
                pos = obj.get("position") or obj.get("pos") or None
                # sometimes nested under 'player' key
                player_info = obj
                if "player" in obj and isinstance(obj.get("player"), dict):
                    player_info = obj.get("player")
                ppos = (player_info.get("position") or player_info.get("pos") or "").upper() if isinstance(player_info, dict) else ""
                if ppos == "WR" or (isinstance(pos, str) and str(pos).upper() == "WR"):
                    # flatten: merge player_info and obj (player-level stats)
                    rec = {}
                    if isinstance(player_info, dict):
                        for k, v in player_info.items():
                            rec[f"player.{k}"] = v
                    for k, v in obj.items():
                        if k == "player":
                            continue
                        rec[k] = v
                    found.append(rec)
                    return

            # otherwise recurse
            for v in obj.values():
                _scan(v, obj)
        elif isinstance(obj, list):
            for it in obj:
                _scan(it, parent)

    _scan(box)
    return found


def fetch_box_and_extract_wr(game_id: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"https://{RAPIDAPI_HOST}/getNFLBoxScore"
    params = {"gameID": game_id}
    try:
        resp = request_with_retries(url, headers, params=params)
    except Exception as exc:
        logging.error("Boxscore request failed for %s: %s", game_id, exc)
        return []

    if resp.status_code != 200:
        logging.error("Boxscore %s returned %d", game_id, resp.status_code)
        return []

    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = None

    wrs = extract_wr_players_from_box(data)
    # attach gameID and some box-level metadata if available
    for r in wrs:
        r.setdefault("gameID", game_id)
    return wrs


def run_weekly_fetch() -> None:
    """Main entrypoint: fetch final games for a specified season/week, collect WR stats, and write CSV.

    This function no longer auto-detects the week; call via `main()` which requires
    explicit --season and --week arguments.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("fetch_weekly_wr_final_stats")

    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        logger.error(
            "RAPIDAPI_KEY is not set. Create a file named .env.local at the project root with:\n"
            "RAPIDAPI_KEY=your_key_here\n\n"
            "That file is ignored by git and will be loaded automatically. Or export RAPIDAPI_KEY in your shell."
        )
        raise SystemExit(1)

    headers = {"X-RapidAPI-Key": key, "X-RapidAPI-Host": RAPIDAPI_HOST, "Accept": "application/json"}

    # Expect caller to supply season/week via command-line. We'll check args in main().
    # For backwards-compatibility when invoked programmatically, require that
    # `SEASON` and `WEEK` be provided as environment variables if not passed.
    season = int(os.environ.get("SEASON", "0") or 0)
    week = int(os.environ.get("WEEK", "0") or 0)
    if not season or not week:
        logger.error("Please run via the CLI with --season and --week (or set SEASON and WEEK env vars). Exiting.")
        return

    # Try schedule endpoint first; if permission denied (None) fall back to scoreboard
    schedule_result = _schedule.fetch_weekly_schedule(season=season, week=week)
    final_game_ids: List[str] = []
    if schedule_result is None:
        logger.warning("Schedule endpoint unavailable or permission denied; falling back to scoreboard for finals")
        final_game_ids = fetch_final_game_ids_for_week(season, week, headers)
    elif schedule_result == []:
        logger.info("Schedule indicates week %s/%s is not complete; aborting.", season, week)
        return
    else:
        for g in schedule_result:
            if isinstance(g, dict):
                gid = g.get("gameID") or g.get("gameId") or g.get("id")
                if gid:
                    final_game_ids.append(str(gid))

    if not final_game_ids:
        logger.info("No final games for season=%s week=%s; nothing to do.", season, week)
        return

    all_wr_records: List[Dict[str, Any]] = []
    missing_gameids: List[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exc:
        futs = {exc.submit(fetch_box_and_extract_wr, gid, headers): gid for gid in final_game_ids}
        for fut in as_completed(futs):
            gid = futs[fut]
            try:
                wrs = fut.result()
            except Exception as excp:
                logger.exception("Error fetching/parsing box for %s: %s", gid, excp)
                missing_gameids.append(gid)
                continue

            if not wrs:
                logger.warning("No WRs extracted for game %s", gid)
                # still consider game processed but empty
                continue

            all_wr_records.extend(wrs)

    df = pd.DataFrame(all_wr_records)

    outdir = Path("external/tank01")
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"player_stats_week_{week}.csv"

    if df.empty:
        logger.warning("No WR records collected for season=%s week=%s", season, week)
    else:
        # ensure deterministic column ordering
        cols = sorted(df.columns.tolist())
        df = df.reindex(columns=cols)
        df.to_csv(outpath, index=False)
        logger.info("Wrote %d WR rows to %s", len(df), outpath)

    logger.info("Processed %d final games, extracted %d WRs. Missing gameIDs: %s", len(final_game_ids), len(all_wr_records), missing_gameids)


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch FINAL WR stats for a specified season and week")
    p.add_argument("--season", type=int, required=False, help="Season year, e.g. 2025")
    p.add_argument("--week", type=int, required=False, help="Week number, e.g. 1")
    args = p.parse_args()

    # Allow explicit CLI args (preferred), otherwise fall back to environment vars.
    if args.season:
        os.environ["SEASON"] = str(args.season)
    if args.week:
        os.environ["WEEK"] = str(args.week)

    try:
        run_weekly_fetch()
    except Exception as e:
        logging.exception("Unexpected failure: %s", e)
        raise


if __name__ == "__main__":
    main()
