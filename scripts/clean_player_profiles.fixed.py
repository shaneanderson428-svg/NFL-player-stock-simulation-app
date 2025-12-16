#!/usr/bin/env python3
"""Fixed copy of clean_player_profiles.py used for smoke-testing.

This file is intentionally separate to avoid touching the corrupted original
while we verify behavior and outputs.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROSTER_URLS = [
    "https://raw.githubusercontent.com/nflverse/nflfastR-roster/main/data/roster.csv",
]
SUMMARY_PATH = "data/player_stock_summary.csv"


def fetch_roster(urls: list[str] | None = None) -> pd.DataFrame:
    urls = urls or ROSTER_URLS
    last_err = None
    for u in urls:
        try:
            return pd.read_csv(u, dtype=str)
        except Exception as e:
            last_err = e
            print(f"Failed to fetch roster from {u}: {e}")
    raise RuntimeError(f"Unable to fetch roster from known URLs: {last_err}")


def clean_roster(df: pd.DataFrame) -> pd.DataFrame:
    name_cols = ["full_name", "player", "displayName", "name"]
    id_cols = ["gsis_id", "gsis", "player_id", "espnId", "id"]
    position_cols = ["position"]
    team_cols = ["team", "team_abbr", "team_name"]

    def choose(cands):
        for c in cands:
            if c in df.columns:
                return c
        return None

    name_col = choose(name_cols)
    id_col = choose(id_cols)
    pos_col = choose(position_cols)
    team_col = choose(team_cols)

    if not name_col or not pos_col or not team_col:
        raise RuntimeError("Roster input missing required columns (name/position/team).")

    cols = [name_col, pos_col, team_col]
    if id_col:
        cols.append(id_col)
    roster = df[cols].copy()
    roster = roster.rename(columns={name_col: "player", pos_col: "position", team_col: "team"})
    if id_col:
        roster = roster.rename(columns={id_col: "espnId"})
    else:
        roster["espnId"] = ""

    roster = roster.dropna(subset=["position", "team"]).copy()
    roster["player"] = roster["player"].astype(str).str.strip()
    roster["position"] = roster["position"].astype(str).str.strip()
    roster["team"] = roster["team"].astype(str).str.strip()
    roster["espnId"] = roster["espnId"].astype(str).str.strip().replace({"nan": ""}).fillna("")

    return roster[["espnId", "player", "position", "team"]]


def load_existing(path: str) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return None


def build_from_game_stats() -> pd.DataFrame:
    p = Path("data/player_game_stats.csv")
    if not p.exists():
        return pd.DataFrame(columns=["espnId", "player", "team", "position"])
    df = pd.read_csv(p)
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
        out["player"] = df["player"].astype(str) if "player" in df.columns else df.index.map(lambda i: f"player_{i}")
    if espn_col:
        out["espnId"] = df[espn_col].astype(str)
    else:
        out["espnId"] = out["player"].map(lambda s: s.replace(" ", "-").lower())
    out["team"] = df[team_col].astype(str) if team_col else ""
    out["position"] = df[pos_col].astype(str) if pos_col else ""

    out = out.drop_duplicates(subset=["espnId"], keep="first")
    out = out.drop_duplicates(subset=["player"], keep="first")
    for c in ["espnId", "player", "team", "position"]:
        if c not in out.columns:
            out[c] = ""
    return out[["espnId", "player", "position", "team"]]


def build_from_roster(roster: pd.DataFrame) -> pd.DataFrame:
    columns = ["player", "espnId", "position", "team", "currentPrice"]
    rows = []
    for _, r in roster.iterrows():
        rows.append({
            "player": r.get("player", ""),
            "espnId": r.get("espnId", ""),
            "position": r.get("position", ""),
            "team": r.get("team", ""),
            "currentPrice": 100.0,
        })
    df = pd.DataFrame(rows, columns=columns)
    df = df.sort_values(["team", "position", "player"], ignore_index=True)
    return df


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test: clean_player_profiles.fixed")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--local", type=str)
    args = parser.parse_args(argv)

    roster = None
    try:
        roster_raw = fetch_roster()
        roster = clean_roster(roster_raw)
    except Exception:
        if args.local:
            try:
                roster_raw = pd.read_csv(args.local, dtype=str)
                roster = clean_roster(roster_raw)
            except Exception:
                roster = build_from_game_stats()
        else:
            roster = build_from_game_stats()

    existing = load_existing(SUMMARY_PATH)
    if args.reset or existing is None:
        out = build_from_roster(roster)
        added = len(out)
    else:
        # simple append-only behavior for smoke test: add missing players
        existing_players = set(existing["player"].astype(str).str.strip().fillna(""))
        rows = []
        for _, r in roster.iterrows():
            if r["player"] not in existing_players:
                rows.append({
                    "player": r.get("player", ""),
                    "espnId": r.get("espnId", ""),
                    "position": r.get("position", ""),
                    "team": r.get("team", ""),
                    "currentPrice": 100.0,
                })
        if rows:
            new_df = pd.DataFrame(rows)
            out = pd.concat([existing, new_df], ignore_index=True)
            added = len(new_df)
        else:
            out = existing
            added = 0

    out.to_csv(SUMMARY_PATH, index=False)
    print(f"Wrote {SUMMARY_PATH}; new players added: {added}")


if __name__ == "__main__":
    main()
