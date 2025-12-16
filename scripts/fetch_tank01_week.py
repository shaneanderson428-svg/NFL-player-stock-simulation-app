#!/usr/bin/env python3
"""Fetch weekly player stats from Tank01 (RapidAPI) and write a flattened CSV.

Usage: python scripts/fetch_tank01_week.py --week 1

Reads RAPIDAPI_KEY from the environment.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any, Dict, List
from pathlib import Path
import sys
# Ensure project root is on sys.path so `import scripts._env` works when run as
# a script directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import scripts._env  # loads .env.local into environment (if present)

import pandas as pd
import requests
import concurrent.futures
import random


RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"

# Only fetch offensive wide receivers (WR) with a valid ESPN ID to reduce API calls
ALLOWED_POSITIONS = {"WR"}
MAX_WORKERS = 8
RETRIES = 3
BACKOFF_BASE = 0.5


def flatten(d: Dict[str, Any], parent: str = "") -> Dict[str, Any]:
    """Recursively flatten a dict using dot notation for nested keys.

    Lists are JSON-dumped. Non-dict values are returned as-is.
    """
    out: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        key = f"{parent}.{k}" if parent else k
        if isinstance(v, dict):
            out.update(flatten(v, key))
        elif isinstance(v, list):
            try:
                out[key] = json.dumps(v, ensure_ascii=False)
            except Exception:
                out[key] = str(v)
        else:
            out[key] = v
    return out


def safe_get(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d:
            return d[k]
    return default


def _game_has_stats_for_week(game: Dict[str, Any], week: int) -> bool:
    """Return True if the game dict contains stats for the requested week.

    Heuristic: try to extract the gameWeek and match against requested week.
    If gameWeek is missing, flatten the dict and look for common stat-like keys
    with non-empty/non-zero values (fantasyPoints, rushing, receiving, passing, yards, td, etc.).
    """
    try:
        flat = flatten(game)
    except Exception:
        flat = {}

    game_week = safe_get(flat, ["gameWeek", "game.week", "week", "gameWeekNumber"], None)
    try:
        gw = int(game_week) if game_week is not None and str(game_week).strip() != "" else None
    except Exception:
        gw = None
    if gw is not None and gw == week:
        return True

    # Look for stat-like keys with non-empty/nonnull values
    stat_substrings = (
        "fantasypoints",
        "fantasyPoints",
        "rushing",
        "receiv",
        "pass",
        "yards",
        "yds",
        "recept",
        "target",
        "attempt",
        "td",
        "tackle",
        "sack",
    )
    for k, v in (flat or {}).items():
        if not k:
            continue
        kl = k.lower()
        if any(sub.lower() in kl for sub in stat_substrings):
            try:
                if v is None:
                    continue
                s = str(v).strip()
                if s == "" or s == "0" or s.lower() == "none":
                    continue
                return True
            except Exception:
                continue
    return False


def _game_has_any_stats(game: Dict[str, Any]) -> bool:
    """Return True if the game dict contains stat-like fields (ignore gameWeek).

    This is a relaxed check used when we don't want to rely on an explicit
    `gameWeek` field from the provider. It uses the same stat substrings
    heuristic as `_game_has_stats_for_week` but ignores the week comparison.
    """
    try:
        flat = flatten(game)
    except Exception:
        flat = {}

    stat_substrings = (
        "fantasypoints",
        "fantasyPoints",
        "rushing",
        "receiv",
        "pass",
        "yards",
        "yds",
        "recept",
        "target",
        "attempt",
        "td",
        "tackle",
        "sack",
    )
    for k, v in (flat or {}).items():
        if not k:
            continue
        kl = k.lower()
        if any(sub.lower() in kl for sub in stat_substrings):
            try:
                if v is None:
                    continue
                s = str(v).strip()
                if s == "" or s == "0" or s.lower() == "none":
                    continue
                return True
            except Exception:
                continue
    return False


def _game_has_stats(game: Dict[str, Any]) -> bool:
    """Robust stat detector: scan nested dicts/lists and flattened keys (lowercase).

    Return True if any key contains stat-like substrings and the value is non-empty/non-zero.
    """
    try:
        flat = flatten(game)
    except Exception:
        flat = {}

    # normalize keys to lowercase
    norm = {str(k).lower(): v for k, v in (flat or {}).items()}

    stat_substrings = (
        "rec",
        "receptions",
        "targets",
        "yds",
        "yards",
        "td",
        "touchdown",
        "long",
        "receiving",
        "rushing",
        "rush",
        "att",
        "attempt",
        "attempts",
        "fantasy",
        "points",
        "epa",
        "cpoe",
    )

    for k, v in norm.items():
        if not k:
            continue
        if any(sub in k for sub in stat_substrings):
            try:
                if v is None:
                    continue
                s = str(v).strip()
                if s == "" or s == "0" or s.lower() == "none":
                    continue
                return True
            except Exception:
                continue
    # also scan nested lists/dicts values recursively for stat-like keys
    def _scan_value(val: Any) -> bool:
        if isinstance(val, dict):
            for kk, vv in val.items():
                if _scan_value({str(kk): vv}):
                    return True
        elif isinstance(val, list):
            for it in val:
                if _scan_value(it):
                    return True
        elif isinstance(val, str):
            ls = val.lower()
            for sub in stat_substrings:
                if sub in ls and ls.strip() != "" and ls != "0" and ls != "none":
                    return True
        elif isinstance(val, (int, float)):
            if val != 0:
                return True
        return False

    for v in (flat or {}).values():
        try:
            if _scan_value(v):
                return True
        except Exception:
            continue

    return False


def fetch_player_list(session: requests.Session, headers: Dict[str, str]):
    # Try the working Tank01 endpoint (trailing slash). If it returns 404,
    # fall back to the alternative query form.
    url = f"https://{RAPIDAPI_HOST}/getNFLPlayerList/"
    resp = session.get(url, headers=headers, timeout=15)
    # Debug: print the raw response body (truncated) for inspection before parsing
    try:
        print('[RAW RESPONSE]', resp.text[:2000])
    except Exception:
        print('[RAW RESPONSE] <unprintable>')
    if resp.status_code == 404:
        fallback_url = f"https://{RAPIDAPI_HOST}/getNFLPlayerList?format=json"
        try:
            resp = session.get(fallback_url, headers=headers, timeout=15)
            try:
                print('[RAW RESPONSE - FALLBACK]', resp.text[:2000])
            except Exception:
                print('[RAW RESPONSE - FALLBACK] <unprintable>')
        except Exception:
            # leave resp as-is; we'll handle status checks below
            pass

    # Ensure we have a successful response before attempting to parse JSON.
    try:
        resp.raise_for_status()
    except Exception:
        # If the endpoint fails, return an empty list so callers can continue.
        return []

    # Parse JSON defensively: some endpoints may return text that still can be
    # parsed as JSON, so try both resp.json() and json.loads(resp.text).
    try:
        data = resp.json()
    except Exception:
        try:
            data = json.loads(resp.text or "{}")
        except Exception:
            data = {}

    # Response shape may vary; try a few heuristics
    list_candidates = []
    if isinstance(data, dict):
        # common: {'players': [...]} or {'body': [...]} (Tank01 wraps payload in 'body')
        list_candidates = safe_get(data, ["players", "playerList", "data", "body"], []) or []
    elif isinstance(data, list):
        list_candidates = data
    else:
        list_candidates = []

    # Return the raw candidates and let caller decide how to extract ids and diagnostics
    return list_candidates


def fetch_games_for_player(
    session: requests.Session, headers: Dict[str, str], player_id: str, week: int | None = None
    ) -> List[Dict[str, Any]]:
    url = f"https://{RAPIDAPI_HOST}/getNFLGamesForPlayer"
    params = {
        "playerID": player_id,
        "itemFormat": "map",
        "numberOfGames": 10,
        "fantasyPoints": "true",
    }
    if week is not None and week > 0:
        params["gameWeek"] = week
    resp = session.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    # The endpoint may return dict with 'games' or a list directly
    games = []
    if isinstance(data, dict):
        # Common shapes:
        #  - { 'games': [ ... ] }
        #  - { 'body': { 'GAMEID': { ... }, ... } }
        #  - { 'body': [ ... ] }
        games = safe_get(data, ["games", "gameList", "data"], []) or []
        if not games:
            body = data.get("body")
            if isinstance(body, dict):
                # Tank01 sometimes returns a map of gameID -> game dict
                games = list(body.values())
            elif isinstance(body, list):
                games = body
        # Sometimes the response includes a single game dict (top-level)
        if not games and any(k in data for k in ("gameWeek", "gameID", "game")):
            games = [data]
    elif isinstance(data, list):
        games = data
    return games


def _fetch_games_with_retries(session: requests.Session, headers: Dict[str, str], player_id: str, week: int | None = None, retries: int = RETRIES) -> List[Dict[str, Any]]:
    """Fetch games for player with simple retry/backoff. Returns list or empty list on failure/no-data."""
    for attempt in range(retries):
        try:
            return fetch_games_for_player(session, headers, player_id, week)
        except requests.HTTPError as e:
            # On HTTP error, backoff and retry
            try:
                status = e.response.status_code  # type: ignore[attr-defined]
            except Exception:
                status = None
            sleep = BACKOFF_BASE * (2 ** attempt) + random.random() * 0.1
            time.sleep(sleep)
            continue
        except Exception:
            time.sleep(BACKOFF_BASE * (2 ** attempt) + random.random() * 0.1)
            continue
    return []


def main() -> None:
    # Temporary debug: indicate whether RAPIDAPI_KEY is set (do not print the key)
    print("DEBUG: RAPIDAPI_KEY set?", "yes" if os.getenv("RAPIDAPI_KEY") else "no")
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", type=int, required=True, help="week number (integer)")
    args = parser.parse_args()
    week = int(args.week)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("fetch_tank01_week")

    rapid_key = os.getenv("RAPIDAPI_KEY")
    if not rapid_key:
        logger.error("RAPIDAPI_KEY not set in environment; aborting.")
        raise SystemExit(1)

    # Use the exact case-sensitive RapidAPI headers so the service returns
    # the full player list payload (some hosts are strict about header casing).
    headers = {
        "X-RapidAPI-Key": rapid_key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Accept": "application/json",
    }

    session = requests.Session()

    try:
        player_list = fetch_player_list(session, headers)
    except Exception as e:
        logger.exception("Failed to fetch player list: %s", e)
        raise SystemExit(1)

    # Diagnostics: ensure diagnostics dir exists and save a small snapshot of the player list
    diag_dir = os.path.join("external", "tank01", "diagnostics")
    os.makedirs(diag_dir, exist_ok=True)
    try:
        with open(os.path.join(diag_dir, f"player_list_week_{week}.json"), "w", encoding="utf-8") as fh:
            # write just first 200 items to keep file small
            import json as _json

            _json.dump(player_list[:200], fh, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("Failed to write diagnostics file for player list")

    # Build minimal player info (id + pos) and apply position whitelist
    players_info: List[Dict[str, str]] = []
    for item in player_list:
        try:
            if isinstance(item, dict):
                # Require an ESPN ID specifically (espnID/espnId/espn_id). Do NOT accept generic playerID.
                pid_val = safe_get(item, ["espnID", "espnId", "espn_id"], None)
                if pid_val is None:
                    nested = item.get("player")
                    if isinstance(nested, dict):
                        pid_val = safe_get(nested, ["espnID", "espnId", "espn_id"], None)
                # position may be at top-level or nested under 'player'
                pos = safe_get(item, ["pos", "position", "player.pos", "player.position"], None)
                nested_player = item.get("player")
                if pos is None and isinstance(nested_player, dict):
                    pos = safe_get(nested_player, ["pos", "position"], None)
                # Only include entries that have an ESPN ID
                if pid_val is not None:
                    players_info.append({"player_id": str(pid_val), "pos": (str(pos) if pos is not None else "").upper()})
        except Exception:
            continue

    total_players = len(player_list)
    logger.info("Found %d player list entries (before filtering).", total_players)
    # Position filter: only WRs (and we already required ESPN ID above)
    players_after_pos = [p for p in players_info if p.get("pos") in ALLOWED_POSITIONS]
    logger.info("Players after position filter (%s): %d", ",".join(sorted(ALLOWED_POSITIONS)), len(players_after_pos))
    logger.info("Total WRs after ESPN-ID filtering: %d", len(players_after_pos))
    logger.info("Will fetch per-player data for %d WRs with ESPN ID", len(players_after_pos))

    rows: List[Dict[str, Any]] = []
    skipped = 0
    skipped_no_games = 0
    skipped_wrong_week = 0
    skipped_errors = 0
    total = 0
    kept_after_played = 0

    # Use a thread pool to parallelize per-player fetches while respecting retries
    pids = [p.get("player_id") for p in players_after_pos if p.get("player_id")]
    futures_map: Dict[concurrent.futures.Future, str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exc:
        for pid in pids:
            pid_str = str(pid)
            futures_map[exc.submit(_fetch_games_with_retries, session, headers, pid_str, week, RETRIES)] = pid_str

        for fut in concurrent.futures.as_completed(futures_map):
            pid = futures_map[fut]
            total += 1
            try:
                games = fut.result()
            except Exception as e:
                logger.warning("Error fetching games for player %s: %s", pid, e)
                skipped += 1
                skipped_errors += 1
                continue

            # If the endpoint returned no games at all, skip this player
            if not games:
                skipped += 1
                skipped_no_games += 1
                continue

            # Accept any game that includes stat fields (ignore gameWeek entirely).
            games_with_stats = []
            try:
                for g in games:
                    if _game_has_stats(g):
                        games_with_stats.append(g)
            except Exception:
                logger.exception("Error while inspecting games for player %s", pid)

            if not games_with_stats:
                # No stat-containing games found for this player
                skipped += 1
                skipped_no_games += 1
                continue

            # At least one game with stats exists for this player
            kept_after_played += 1

            # Choose the latest stat-containing game by parsing YYYYMMDD from gameID
            def _extract_game_date_int(game_obj: Dict[str, Any]) -> int:
                try:
                    flatg = flatten(game_obj)
                except Exception:
                    flatg = {}
                gid = safe_get(flatg, ["gameID", "game.gameID", "game.id"], None)
                if not gid:
                    return 0
                s = str(gid)
                # first 8 chars should be YYYYMMDD
                if len(s) >= 8 and s[:8].isdigit():
                    try:
                        return int(s[:8])
                    except Exception:
                        return 0
                return 0

            try:
                latest_game = max(games_with_stats, key=_extract_game_date_int)
            except Exception:
                latest_game = games_with_stats[0]

            try:
                # Flatten only the selected latest game and include player nested info
                flat = flatten(latest_game)
                if isinstance(latest_game, dict) and "player" in latest_game and isinstance(latest_game["player"], dict):
                    flat.update(flatten(latest_game["player"], "player"))

                game_week = safe_get(flat, ["gameWeek", "game.week", "week", "gameWeekNumber"], None)
                try:
                    gw = int(game_week) if game_week is not None and str(game_week).strip() != "" else None
                except Exception:
                    gw = None

                out: Dict[str, Any] = {}
                out["playerID"] = safe_get(flat, ["playerID", "player.playerID", "player.id", "playerID"], pid)
                out["longName"] = safe_get(flat, ["longName", "player.longName", "player.name", "player.fullName"], None)
                out["gameWeek"] = gw
                out["gameID"] = safe_get(flat, ["gameID", "game.gameID", "game.id"], None)
                out["team"] = safe_get(flat, ["team", "team.name", "player.team"], None)
                out["teamID"] = safe_get(flat, ["teamID", "team.teamID", "team.id"], None)
                out["fantasyPoints"] = safe_get(flat, ["fantasyPoints", "fantasyPoints.total", "player.fantasyPoints"], None)

                for k, v in flat.items():
                    if k in out:
                        continue
                    out[k] = v

                rows.append(out)
            except Exception:
                logger.exception("Failed to process latest game record for player %s", pid)
                skipped += 1
                skipped_errors += 1
                continue

    logger.info(
        "Completed fetching. total players processed=%d, rows=%d, skipped=%d (no_games=%d, wrong_week=%d, errors=%d)",
        total,
        len(rows),
        skipped,
        skipped_no_games,
        skipped_wrong_week,
        skipped_errors,
    )

    # Log how many WRs we kept that played and had stats for the requested week
    logger.info("WRs that had stat-containing games (accepted as 'played'): %d", kept_after_played)

    # Log how many WRs will be written to the final CSV (one row per WR)
    logger.info("WRs written to the final CSV (one row per WR): %d", len(rows))

    # Save a small sample of processed rows for diagnostics
    try:
        import json as _json

        sample_path = os.path.join(diag_dir, f"player_rows_sample_week_{week}.json")
        with open(sample_path, "w", encoding="utf-8") as fh:
            _json.dump(rows[:200], fh, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("Failed to write diagnostics sample of processed rows")

    # Build DataFrame and write CSV with stable alphabetical columns
    if not rows:
        logger.warning("No rows to write for week %s", week)
    df = pd.DataFrame(rows)
    # Ensure deterministic column ordering
    sorted_cols = sorted(df.columns.tolist())
    df = df.reindex(columns=sorted_cols)

    out_dir = os.path.join("external", "tank01")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"player_stats_week_{week}.csv")
    try:
        df.to_csv(out_path, index=False)
        logger.info("Wrote %d rows to %s", len(df), out_path)
    except Exception:
        logger.exception("Failed to write CSV to %s", out_path)
        raise SystemExit(1)

    # --- Append this week's price points into external/history/wr_price_history.json ---
    try:
        import glob

        history_dir = os.path.join("external", "history")
        os.makedirs(history_dir, exist_ok=True)
        history_path = os.path.join(history_dir, "wr_price_history.json")
        try:
            with open(history_path, "r", encoding="utf-8") as fh:
                history = json.load(fh) or {}
        except Exception:
            history = {}

        # Build season averages from existing CSVs (exclude the CSV we just wrote)
        csvs = sorted(glob.glob(os.path.join("external", "tank01", "player_stats_week_*.csv")))
        csvs = [c for c in csvs if os.path.abspath(c) != os.path.abspath(out_path)]

        # helper to parse numeric candidates
        def _num_from_row_map(r: dict, candidates: list) -> float:
            """Try a list of candidate keys from a row dict and coerce the first valid numeric-looking value to float."""
            for c in candidates:
                if c in r:
                    val = r.get(c)
                    if val is None:
                        continue
                    s = str(val).strip()
                    if s == "":
                        continue
                    # try direct numeric types first
                    if isinstance(val, (int, float)):
                        try:
                            return float(val)
                        except Exception:
                            continue
                    # try to parse string-ish values
                    try:
                        return float(s)
                    except Exception:
                        try:
                            import re

                            cleaned = re.sub(r"[^0-9.\-]", "", s) or "0"
                            return float(cleaned)
                        except Exception:
                            return 0.0
            return 0.0

        from collections import defaultdict

        agg = defaultdict(lambda: {"recs": 0.0, "yds": 0.0, "tds": 0.0, "fp": 0.0, "count": 0})
        for c in csvs:
            try:
                import csv as _csv

                with open(c, "r", encoding="utf-8") as fh:
                    reader = _csv.DictReader(fh)
                    for row in reader:
                        # find espn id
                        espn_id = None
                        for col in ("playerID", "espnID", "espnId", "espnid"):
                            if col in row and row.get(col):
                                espn_id = str(row.get(col))
                                break
                        if not espn_id:
                            continue
                        recs = _num_from_row_map(row, ["Receiving.receptions", "receiving.receptions", "receptions", "rec"])
                        yds = _num_from_row_map(row, ["Receiving.recYds", "receiving.recYds", "yards", "yds", "recYds"])
                        tds = _num_from_row_map(row, ["Receiving.recTD", "receiving.recTD", "td", "recTD"]) 
                        fp = _num_from_row_map(row, ["fantasyPoints", "fantasyPoints.total", "fantasyPointsDefault.standard", "fantasyPointsDefault"])
                        agg[espn_id]["recs"] += recs
                        agg[espn_id]["yds"] += yds
                        agg[espn_id]["tds"] += tds
                        agg[espn_id]["fp"] += fp
                        agg[espn_id]["count"] += 1
            except Exception:
                logger.exception("Failed to scan CSV %s for averages", c)

        averages = {}
        for k, v in agg.items():
            cnt = v.get("count") or 1
            averages[k] = {"recs": v["recs"] / cnt, "yds": v["yds"] / cnt, "tds": v["tds"] / cnt, "fp": v["fp"] / cnt}

        def _extract_date_from_gameid(gid: str | None) -> str | None:
            if not gid:
                return None
            s = str(gid)
            if len(s) >= 8 and s[:8].isdigit():
                try:
                    from datetime import datetime

                    return datetime.strptime(s[:8], "%Y%m%d").date().isoformat()
                except Exception:
                    return None
            return None

        def _price_from_deviation(out_row: dict) -> float:
            # today's stats
            recs = _num_from_row_map(out_row, ["Receiving.receptions", "receiving.receptions", "receptions", "rec"]) 
            yds = _num_from_row_map(out_row, ["Receiving.recYds", "receiving.recYds", "yards", "yds", "recYds"]) 
            tds = _num_from_row_map(out_row, ["Receiving.recTD", "receiving.recTD", "td", "recTD"]) 
            fp_today = _num_from_row_map(out_row, ["fantasyPoints", "fantasyPoints.total", "fantasyPointsDefault.standard", "fantasyPointsDefault"]) 

            espnid = str(out_row.get("playerID") or out_row.get("espnID") or out_row.get("espnId") or out_row.get("espnid") or "").strip()
            avg = averages.get(espnid)
            # base derived from season-average fantasy points; if missing, fall back to today's fp
            base_fp = (avg.get("fp") if avg and avg.get("fp") is not None else fp_today) if avg else fp_today
            try:
                base = round(max(5.0, float(base_fp) * 4.0 + 50.0), 2)
            except Exception:
                base = 100.0

            # compute deltas
            eps = 1e-6
            delta_recs = (recs - (avg.get("recs") if avg else 0.0)) / max(abs(avg.get("recs") if avg else 0.0), eps)
            delta_yds = (yds - (avg.get("yds") if avg else 0.0)) / max(abs(avg.get("yds") if avg else 0.0), eps)
            delta_tds = (tds - (avg.get("tds") if avg else 0.0)) / max(abs(avg.get("tds") if avg else 0.0), eps)

            w_yds = 0.4
            w_recs = 0.35
            w_tds = 0.25

            weighted_delta = delta_yds * w_yds + delta_recs * w_recs + delta_tds * w_tds
            weighted_delta = max(-0.5, min(0.5, weighted_delta))

            price = round(max(5.0, base * (1.0 + weighted_delta)), 2)
            return price

        added = 0
        today = None
        for out in rows:
            try:
                espnid = str(out.get("playerID") or out.get("espnID") or out.get("espnId") or out.get("espnid") or "").strip()
                if not espnid:
                    continue
                price = _price_from_deviation(out)
                gid = out.get("gameID")
                t = _extract_date_from_gameid(gid)
                if not t:
                    if today is None:
                        from datetime import date

                        today = date.today().isoformat()
                    t = today

                history.setdefault(espnid, [])
                # avoid duplicates for same date
                if any((pt.get("t") == t) for pt in history[espnid]):
                    continue

                # compute advanced metrics for this week's entry
                try:
                    recs = _num_from_row_map(out, ["Receiving.receptions", "receiving.receptions", "receptions", "rec"]) 
                    yds = _num_from_row_map(out, ["Receiving.recYds", "receiving.recYds", "yards", "yds", "recYds"]) 
                    tds = _num_from_row_map(out, ["Receiving.recTD", "receiving.recTD", "td", "recTD"]) 
                    fp_today = _num_from_row_map(out, ["fantasyPoints", "fantasyPoints.total", "fantasyPointsDefault.standard", "fantasyPointsDefault"])

                    avg = averages.get(espnid, {})
                    avg_recs = avg.get("recs", 0.0)
                    avg_yds = avg.get("yds", 0.0)
                    avg_tds = avg.get("tds", 0.0)
                    avg_fp = avg.get("fp", 0.0)

                    yoe = round(yds - avg_yds, 2)
                    roe = round(recs - avg_recs, 2)
                    toe = round(tds - avg_tds, 2)
                    try:
                        denom = (float(avg_fp) + 1.0) if avg_fp is not None else 1.0
                        if denom == 0:
                            denom = 1.0
                        uer = round(float(fp_today) / denom, 4)
                    except Exception:
                        uer = 0.0
                    pis = round((0.5 * yoe) + (0.3 * roe) + (0.2 * toe), 4)

                    # ITS computed against last up-to-4 prior prices
                    prev_prices = []
                    for pt in history.get(espnid, []):
                        try:
                            pv = pt.get("p")
                            if pv is None:
                                continue
                            prev_prices.append(float(str(pv)))
                        except Exception:
                            continue
                    if prev_prices:
                        last4 = prev_prices[-4:]
                        avg_last4 = sum(last4) / len(last4)
                        its = round(price - avg_last4, 4)
                    else:
                        its = 0.0
                except Exception:
                    logger.exception("Failed to compute advanced metrics for %s", espnid)
                    yoe = roe = toe = uer = pis = its = 0.0

                history[espnid].append({"t": t, "p": price, "yoe": yoe, "roe": roe, "toe": toe, "uer": uer, "pis": pis, "its": its})
                added += 1
            except Exception:
                logger.exception("Failed to append price for row: %s", out)
                continue

        # sort each player's history by date
        try:
            for k, arr in history.items():
                try:
                    arr.sort(key=lambda x: x.get("t") or "")
                except Exception:
                    pass
        except Exception:
            pass

        # write back
        try:
            with open(history_path, "w", encoding="utf-8") as fh:
                json.dump(history, fh, ensure_ascii=False, indent=2)
            logger.info("Appended %d price points to %s", added, history_path)
        except Exception:
            logger.exception("Failed to write history to %s", history_path)
    except Exception:
        logger.exception("Failed to update WR price history")


if __name__ == "__main__":
    main()
