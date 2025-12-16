#!/usr/bin/env python3
"""Clean player profiles (cleaned copy).

This is a single, clean implementation saved as a separate file. If you
approve, I can replace `scripts/clean_player_profiles.py` with this file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, List
import argparse
import io
import sys

import pandas as pd
import requests


SUMMARY_PATH = Path("data/player_stock_summary.csv")
DEFAULT_PRICE = 100.0
ROSTER_URLS: List[str] = [
    "https://raw.githubusercontent.com/nflverse/nflfastR-roster/main/data/roster.csv",
    "https://raw.githubusercontent.com/nflverse/nflfastR-roster/master/data/roster.csv",
]


def fetch_roster(urls: Optional[List[str]] = None, timeout: int = 10) -> pd.DataFrame:
    urls = urls or ROSTER_URLS
    last_exc: Optional[Exception] = None
    for u in urls:
        try:
            resp = requests.get(u, timeout=timeout)
            resp.raise_for_status()
            return pd.read_csv(io.StringIO(resp.text), dtype=str)
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"Unable to fetch roster from known URLs: {last_exc}")


def build_from_game_stats() -> pd.DataFrame:
    p = Path("data/player_game_stats.csv")
    if not p.exists():
        return pd.DataFrame(columns=["espnId", "player", "position", "team"])
    df = pd.read_csv(p, dtype=str)
    lower = {c.lower(): c for c in df.columns}
    espn_col = next((lower[c] for c in ("espnid", "player_id", "playerid", "id", "espn_id") if c in lower), None)
    player_col = next((lower[c] for c in ("player", "name", "player_name") if c in lower), None)
    team_col = next((lower[c] for c in ("team", "team_name") if c in lower), None)
    pos_col = next((lower[c] for c in ("position", "pos") if c in lower), None)

    out = pd.DataFrame()
    out["player"] = df[player_col].astype(str) if player_col else df.index.map(lambda i: f"player_{i}")
    out["espnId"] = df[espn_col].astype(str) if espn_col else out["player"].map(lambda s: s.replace(" ", "-").lower())
    out["team"] = df[team_col].astype(str) if team_col else ""
    out["position"] = df[pos_col].astype(str) if pos_col else ""
    out = out.drop_duplicates(subset=["espnId"], keep="first")
    out = out.drop_duplicates(subset=["player"], keep="first")
    for c in ("espnId", "player", "position", "team"):
        if c not in out.columns:
            out[c] = ""
    return out[["espnId", "player", "position", "team"]]


def build_from_roster(roster: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in roster.iterrows():
        rows.append({
            "player": r.get("player", ""),
            "espnId": r.get("espnId", ""),
            "position": r.get("position", ""),
            "team": r.get("team", ""),
            "currentPrice": DEFAULT_PRICE,
        })
    df = pd.DataFrame(rows, columns=["player", "espnId", "position", "team", "currentPrice"]) if rows else pd.DataFrame(columns=["player", "espnId", "position", "team", "currentPrice"]) 
    return df.sort_values([c for c in ("team", "position", "player") if c in df.columns], ignore_index=True)


def clean_roster(df: pd.DataFrame) -> pd.DataFrame:
    name_cols = ["full_name", "player", "displayName", "name"]
    id_cols = ["gsis_id", "gsis", "player_id", "espnId", "id"]
    pos_cols = ["position", "pos"]
    team_cols = ["team", "team_abbr", "team_name"]

    def choose(cands: List[str]) -> Optional[str]:
        for c in cands:
            if c in df.columns:
                return c
        return None

    name_col = choose(name_cols)
    id_col = choose(id_cols)
    pos_col = choose(pos_cols)
    team_col = choose(team_cols)

    if not name_col or not pos_col or not team_col:
        raise RuntimeError("Roster input missing required columns (name/position/team).")

    cols = [name_col, pos_col, team_col] + ([id_col] if id_col else [])
    roster = df.loc[:, [c for c in cols if c in df.columns]].copy()
    roster = roster.rename(columns={name_col: "player", pos_col: "position", team_col: "team"})
    if id_col and id_col in roster.columns:
        roster = roster.rename(columns={id_col: "espnId"})
    else:
        roster["espnId"] = ""

    roster = roster.dropna(subset=["position", "team"]) if not roster.empty else roster
    for c in ("player", "position", "team", "espnId"):
        if c in roster.columns:
            roster[c] = roster[c].astype(str).str.strip().replace({"nan": ""}).fillna("")
        else:
            roster[c] = ""

    return roster[["espnId", "player", "position", "team"]]


def build_default_row(template: pd.DataFrame, player_row: pd.Series) -> dict:
    row: dict = {}
    for col in template.columns:
        if col in ("player", "espnId", "position", "team"):
            row[col] = player_row.get(col, "")
        elif "price" in col.lower() or col.lower() == "currentprice":
            row[col] = float(DEFAULT_PRICE)
        else:
            dtype = template[col].dtype
            if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
                row[col] = 0
            elif pd.api.types.is_bool_dtype(dtype):
                row[col] = False
            else:
                row[col] = ""
    return row


def merge_rosters(existing: Optional[pd.DataFrame], roster: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if existing is None or existing.empty:
        out = build_from_roster(roster)
        return out, len(out)

    existing_players = set(existing.get("player", pd.Series(dtype=str)).astype(str).str.strip())
    existing_ids = set(existing.get("espnId", pd.Series(dtype=str)).astype(str).str.strip())

    new_rows: List[dict] = []
    for _, r in roster.iterrows():
        name = str(r.get("player", "")).strip()
        eid = str(r.get("espnId", "")).strip()
        if (name in existing_players) or (eid and eid in existing_ids):
            continue
        new_rows.append(build_default_row(existing, r))

    if not new_rows:
        return existing, 0

    new_df = pd.DataFrame(new_rows, columns=existing.columns)
    merged = pd.concat([existing, new_df], ignore_index=True)
    sort_cols = [c for c in ["team", "position", "player"] if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols, ignore_index=True)
    return merged, len(new_rows)


def save_summary(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Clean and append player profiles from roster")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--local", type=str)
    args = parser.parse_args(argv)

    try:
        raw = fetch_roster()
        roster_df = clean_roster(raw)
    except Exception:
        if args.local:
            try:
                raw_local = pd.read_csv(args.local, dtype=str)
                roster_df = clean_roster(raw_local)
            except Exception:
                roster_df = build_from_game_stats()
        else:
            roster_df = build_from_game_stats()

    existing = None
    if SUMMARY_PATH.exists():
        existing = pd.read_csv(SUMMARY_PATH)

    if args.reset or existing is None:
        out = build_from_roster(roster_df)
        added = len(out)
    else:
        out, added = merge_rosters(existing, roster_df)
        if "currentPrice" not in out.columns:
            out["currentPrice"] = DEFAULT_PRICE

    save_summary(out, SUMMARY_PATH)
    print(f"✅ Added {added} new players, total {len(out):,} active players")


if __name__ == "__main__":
    main()
