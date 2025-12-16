#!/usr/bin/env python3
"""Normalize player profile CSVs (split out from clean_player_profiles.py).

Reads an input CSV (default: data/player_profiles.csv). If that file does not exist,
it will attempt to construct profiles from `data/player_game_stats.csv` by extracting
unique player names and any id/team/position columns available.

Output: data/player_profiles_cleaned.csv with columns: espnId,player,team,position

Duplicate rows are removed (prefer espnId when present), player names are title-cased,
and missing espnId/team/position are filled with sensible defaults (espnId: slug).
"""

from pathlib import Path
from typing import Optional
import argparse
import pandas as pd
import re
import json
from collections import Counter


def slugify(name: str) -> str:
    s = name or ""
    s = re.sub(r"[^0-9a-zA-Z]+", "-", s).strip("-").lower()
    if not s:
        return "unknown"
    return s


def title_case(name: str) -> str:
    if not isinstance(name, str):
        return ""
    # Basic title casing
    return name.title().strip()


def detect_input_file() -> Optional[Path]:
    # Prefer an explicit `data/player_profiles.csv` file if present.
    p = Path("data/player_profiles.csv")
    if p.exists():
        return p
    # historic fallback: some projects used `data/players.csv`; prefer it only
    # if the explicit `player_profiles.csv` is not present.
    p2 = Path("data/players.csv")
    if p2.exists():
        return p2
    # fall back to None; caller will handle (and may synthesize from game stats)
    return None


def normalize_profiles(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize common profile column names
    col_map = {c: c for c in df.columns}
    lower = {c.lower(): c for c in df.columns}
    # espn id
    for cand in ["espnid", "player_id", "playerid", "id", "espn_id"]:
        if cand in lower:
            col_map[lower[cand]] = "espnId"
            break
    # player name
    for cand in ["player_name", "player", "name", "playerfull", "player_full_name"]:
        if cand in lower:
            col_map[lower[cand]] = "player"
            break
    # team
    for cand in ["team", "team_name", "team_abbr", "team_abbreviation"]:
        if cand in lower:
            col_map[lower[cand]] = "team"
            break
    # position
    for cand in ["position", "pos", "position_name"]:
        if cand in lower:
            col_map[lower[cand]] = "position"
            break

    df = df.rename(columns=col_map)

    # Ensure required columns exist
    if "player" not in df.columns:
        df["player"] = df.index.map(lambda i: f"player_{i}")
    if "espnId" not in df.columns:
        # try to use a player-based slug
        df["espnId"] = df["player"].fillna("").apply(lambda s: slugify(s))
    if "team" not in df.columns:
        df["team"] = ""
    if "position" not in df.columns:
        df["position"] = ""

    # Clean values
    df["player"] = df["player"].astype(str).map(title_case)
    df["team"] = (
        df["team"].astype(str).str.upper().map(lambda s: s if s != "NAN" else "")
    )
    df["position"] = (
        df["position"].astype(str).str.upper().map(lambda s: s if s != "NAN" else "")
    )
    df["espnId"] = df["espnId"].astype(str).map(lambda s: s.strip())

    # If espnId looks numeric but with .0 (from CSV floats), normalize
    df["espnId"] = df["espnId"].str.replace(r"\.0+$", "", regex=True)

    # Remove exact duplicates by espnId (keeping first), then by player name
    df = df.drop_duplicates(subset=["espnId"], keep="first")
    df = df.drop_duplicates(subset=["player"], keep="first")

    # Reorder columns
    out_cols = ["espnId", "player", "team", "position"]
    for c in out_cols:
        if c not in df.columns:
            df[c] = ""
    return df[out_cols]


def build_from_game_stats() -> pd.DataFrame:
    # Try to synthesize profiles from data/player_game_stats.csv
    p = Path("data/player_game_stats.csv")
    if not p.exists():
        return pd.DataFrame(columns=["espnId", "player", "team", "position"])
    df = pd.read_csv(p)
    # try to find espn id column
    lower = {c.lower(): c for c in df.columns}
    espn_col = None
    for cand in ["espnid", "player_id", "playerid", "id", "espn_id"]:
        if cand in lower:
            espn_col = lower[cand]
            break
    player_col = None
    for cand in ["player", "name", "player_name"]:
        if cand in lower:
            player_col = lower[cand]
            break
    team_col = None
    for cand in ["team", "team_name"]:
        if cand in lower:
            team_col = lower[cand]
            break
    pos_col = None
    for cand in ["position", "pos"]:
        if cand in lower:
            pos_col = lower[cand]
            break

    out = pd.DataFrame()
    if player_col:
        out["player"] = df[player_col].astype(str)
    else:
        out["player"] = (
            df["player"].astype(str)
            if "player" in df.columns
            else df.index.map(lambda i: f"player_{i}")
        )
    if espn_col:
        out["espnId"] = df[espn_col].astype(str)
    else:
        out["espnId"] = out["player"].map(lambda s: slugify(s))
    if team_col:
        out["team"] = df[team_col].astype(str)
    else:
        out["team"] = ""
    if pos_col:
        out["position"] = df[pos_col].astype(str)
    else:
        out["position"] = ""

    # Deduplicate
    out = out.drop_duplicates(subset=["espnId"], keep="first")
    out = out.drop_duplicates(subset=["player"], keep="first")
    out = normalize_profiles(out)
    return out


def enrich_profiles(df: pd.DataFrame) -> pd.DataFrame:
    """Attempt to fill missing position and team values.

    This function tries to infer missing `team` and `position` values by
    using the most common non-empty value for the same `player` or `espnId`.
    It normalizes casing and then delegates to `normalize_profiles` to
    ensure consistent output columns and formatting.

    Returns a DataFrame with columns: espnId, player, team, position
    """
    # work on a copy to avoid mutating caller's dataframe
    df = df.copy()

    # Ensure required columns exist
    for c in ("team", "position"):
        if c not in df.columns:
            df[c] = ""

    # Normalize values
    df["team"] = (
        df["team"].astype(str).fillna("").str.upper().map(lambda s: s if s != "NAN" else "")
    )
    df["position"] = (
        df["position"].astype(str).fillna("").str.upper().map(lambda s: s if s != "NAN" else "")
    )

    # Build most-common mappings by player and by espnId
    try:
        team_by_player = (
            df[df["team"] != ""].groupby("player")["team"].agg(lambda s: Counter(s).most_common(1)[0][0]).to_dict()
        )
    except Exception:
        team_by_player = {}
    try:
        pos_by_player = (
            df[df["position"] != ""].groupby("player")["position"].agg(lambda s: Counter(s).most_common(1)[0][0]).to_dict()
        )
    except Exception:
        pos_by_player = {}
    try:
        team_by_espn = (
            df[df["team"] != ""].groupby("espnId")["team"].agg(lambda s: Counter(s).most_common(1)[0][0]).to_dict()
        )
    except Exception:
        team_by_espn = {}
    try:
        pos_by_espn = (
            df[df["position"] != ""].groupby("espnId")["position"].agg(lambda s: Counter(s).most_common(1)[0][0]).to_dict()
        )
    except Exception:
        pos_by_espn = {}


    # Fill missing values using the mappings (robust to NaN and missing keys)
    def _fill_row(row):
        # Extract raw values defensively
        team = row.get("team", "")
        if pd.isna(team):
            team = ""
        position = row.get("position", "")
        if pd.isna(position):
            position = ""
        player = row.get("player", "") or ""
        espn = row.get("espnId", "") or ""

        if not team:
            if player and player in team_by_player:
                team = team_by_player[player]
            elif espn and espn in team_by_espn:
                team = team_by_espn[espn]
        if not position:
            if player and player in pos_by_player:
                position = pos_by_player[player]
            elif espn and espn in pos_by_espn:
                position = pos_by_espn[espn]

        # Assign back to the series
        row["team"] = team
        row["position"] = position
        return row

    filled = df.apply(_fill_row, axis=1)
    # ensure the result is a DataFrame for the type-checker
    filled_df = pd.DataFrame(list(filled), index=filled.index)

    # Normalize and return
    return normalize_profiles(filled_df)