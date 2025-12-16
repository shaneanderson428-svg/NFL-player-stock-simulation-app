"""Helpers to fetch weekly NFL schedule from Tank01 and determine completeness.

This module centralizes the logic for calling Tank01's weekly schedule
endpoint and deciding whether a given week is complete (all games Final).

Usage:
    from scripts._schedule import fetch_weekly_schedule
    games = fetch_weekly_schedule(season=2025, week=1)
    # raises RuntimeError if week is not complete or request fails
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import requests


RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"


def _build_headers() -> Dict[str, str]:
    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        raise RuntimeError(
            "RAPIDAPI_KEY is not set. Create a file named .env.local at the project root with:\n"
            "RAPIDAPI_KEY=your_key_here\n\n"
            "That file is ignored by git and will be loaded automatically. Or export RAPIDAPI_KEY in your shell."
        )
    return {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Accept": "application/json",
    }


def _extract_games_from_response(data: Any) -> List[Dict]:
    """Defensive extractor: return list of game dicts from varied response shapes."""
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("games", "gameList", "body", "data", "items", "schedule"):
            v = data.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return list(v.values())
        # fallback: any first list in values
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


def fetch_weekly_schedule(season: int, week: int, seasonType: str = "reg") -> Optional[List[Dict]]:
    """Fetch the weekly schedule for season/week and verify completeness.

    Returns:
      - list[dict]: games list when schedule is available and ALL games are Final.
      - []: when schedule is available but week is not complete (some games not Final).
      - None: when the schedule call failed due to permission (403) or a network/error
        situation — callers should fall back to other sources and must NOT abort the
        weekly update because of schedule permission errors.
    """
    url = f"https://{RAPIDAPI_HOST}/getNFLSchedule"
    headers = _build_headers()
    params = {"season": str(season), "week": str(week), "seasonType": seasonType}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    except requests.RequestException as exc:
        logging.warning("Failed to call schedule endpoint for %s week %s: %s", season, week, exc)
        return None

    # If we get a 403 (no permission) or other non-200, do not raise — fall back.
    if resp.status_code == 403:
        logging.warning("Schedule endpoint returned 403 Forbidden for %s week %s — falling back to trusting provided week", season, week)
        return None
    if resp.status_code != 200:
        logging.warning("Schedule endpoint returned status %s for %s week %s — falling back to trusting provided week", resp.status_code, season, week)
        return None

    try:
        data = resp.json()
    except Exception:
        try:
            import json as _json

            data = _json.loads(resp.text or "{}")
        except Exception:
            logging.warning("Failed to parse schedule response for %s week %s — falling back", season, week)
            return None

    games = _extract_games_from_response(data)
    if not games:
        # schedule returned empty — treat as week not complete
        logging.info("Schedule returned no games for season=%s week=%s", season, week)
        return []

    # Consider week complete only if every game's status is Final (case-insensitive)
    not_final = [g for g in games if str(g.get("gameStatus") or "").strip().lower() != "final"]
    if not_final:
        logging.info("Week %s season %s is not complete: %d games not Final", week, season, len(not_final))
        return []

    return games
