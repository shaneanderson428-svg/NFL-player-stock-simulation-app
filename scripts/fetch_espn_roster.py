#!/usr/bin/env python3
"""Fetch full NFL rosters from ESPN's public API and save offensive players.

This script iterates team_id 1..32 and fetches:
  https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster

It parses the JSON `athletes` list and extracts:
  espnId = athlete["id"]
  player = athlete["fullName"]
  position = athlete["position"]["abbreviation"]
  team = response["team"]["displayName"]

Only offensive positions (QB, RB, WR, TE) are kept. The result is
saved to `data/roster_backup.csv` with columns:
  espnId,player,position,team,currentPrice

Prints a short summary on completion.
"""

from pathlib import Path
import sys
import time
from typing import List, Dict, Optional, Tuple
import io

import requests
import pandas as pd

OUT_PATH = Path("data/roster_backup.csv")
OFFENSIVE = {"QB", "RB", "WR", "TE"}
TEAM_IDS = range(1, 33)
BASE_URL = "https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster"
NFLFASTR_URLS = [
    "https://raw.githubusercontent.com/nflverse/nflfastR-data/master/data/roster.csv",
    "https://raw.githubusercontent.com/nflverse/nflfastR-data/master/data/players.csv",
]


def fetch_team_roster(team_id: int, timeout: int = 10) -> Dict:
    url = BASE_URL.format(team_id=team_id)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def build_roster() -> Tuple[pd.DataFrame, List[tuple]]:
    """Build roster using nflfastR remote CSVs first, then local backup fallback.

    Returns a DataFrame of offensive players with columns:
      espnId, player, position, team, currentPrice
    """
    last_exc: Optional[Exception] = None
    for url in NFLFASTR_URLS:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text), dtype=str)
            # Prefer gsis_id/full_name layout, else try common mappings
            prefer = ["gsis_id", "full_name", "position", "team"]
            if all(c in df.columns for c in prefer):
                roster = df.loc[:, prefer].copy()
                roster = roster.rename(columns={"gsis_id": "espnId", "full_name": "player"})
            else:
                # map common names to our schema
                cols_map = {}
                if "gsis_id" in df.columns:
                    cols_map["gsis_id"] = "espnId"
                if "full_name" in df.columns:
                    cols_map["full_name"] = "player"
                if "player" in df.columns and "player" not in cols_map:
                    cols_map["player"] = "player"
                if "position" in df.columns:
                    cols_map["position"] = "position"
                if "team" in df.columns:
                    cols_map["team"] = "team"
                roster = df.rename(columns=cols_map)

            for c in ("espnId", "player", "position", "team"):
                if c in roster.columns:
                    roster[c] = roster[c].astype(str).str.strip().fillna("")
                else:
                    roster[c] = ""

            roster = roster[roster["position"].isin(OFFENSIVE)].copy()
            exclude_status = {"Reserve/Injured", "Retired", "Practice Squad", "Suspended"}
            if "status" in roster.columns:
                roster = roster[~roster["status"].isin(exclude_status)].copy()

            roster["currentPrice"] = 100.0
            roster = roster.drop_duplicates(subset=["espnId"], keep="first")
            roster = roster.drop_duplicates(subset=["player"], keep="first")
            return roster[["espnId", "player", "position", "team", "currentPrice"]], []
        except Exception as exc:
            last_exc = exc

    # try local backup
    backup = OUT_PATH
    if backup.exists():
        try:
            local = pd.read_csv(backup, dtype=str)
            for c in ("espnId", "player", "position", "team"):
                if c not in local.columns:
                    local[c] = ""
            local["currentPrice"] = local.get("currentPrice", 100.0)
            # If the local backup exists but is empty, fall through to ESPN per-team fetch
            if local.empty:
                # continue to ESPN per-team fallback
                pass
            else:
                return local[["espnId", "player", "position", "team", "currentPrice"]], []
        except Exception as exc:
            raise RuntimeError(f"Unable to fetch nflfastR roster: tried all known URLs and local fallback; local read error: {exc}")
    # As a final fallback, attempt to build roster from ESPN's per-team roster API
    rows: List[Dict] = []
    errors: List[tuple] = []
    for tid in TEAM_IDS:
        try:
            data = fetch_team_roster(tid)
        except Exception as exc:
            errors.append((tid, str(exc)))
            time.sleep(0.05)
            continue

        team_name = None
        try:
            if isinstance(data, dict):
                team_name = data.get("team", {}).get("displayName") or data.get("team", {}).get("shortDisplayName")
        except Exception:
            team_name = None

        athletes = data.get("athletes") or []
        for group in athletes:
            items = group.get("items") or []
            for item in items:
                try:
                    espn_id = item.get("id") or (item.get("uid") or "")
                    full_name = item.get("fullName") or item.get("displayName") or (f"{item.get('firstName','')} {item.get('lastName','')}").strip()
                    # position can be nested or at item level
                    pos = ""
                    if isinstance(item.get("position"), dict):
                        pos = item.get("position", {}).get("abbreviation") or ""
                    elif isinstance(item.get("position"), str):
                        pos = item.get("position")
                    else:
                        pos = item.get("displayPosition") or item.get("positionAbbr") or ""

                    team = team_name or (item.get("team", {}) or {}).get("displayName") or ""
                    if pos in OFFENSIVE:
                        rows.append({
                            "espnId": str(espn_id),
                            "player": str(full_name).strip(),
                            "position": pos,
                            "team": str(team).strip(),
                            "currentPrice": 100.0,
                        })
                except Exception:
                    continue

    df = pd.DataFrame(rows, columns=["espnId", "player", "position", "team", "currentPrice"]) if rows else pd.DataFrame(columns=["espnId", "player", "position", "team", "currentPrice"]) 
    if not df.empty:
        df["espnId"] = df["espnId"].astype(str).str.strip()
        df["player"] = df["player"].astype(str).str.strip()
        df = df.drop_duplicates(subset=["espnId"], keep="first")
        df = df.drop_duplicates(subset=["player"], keep="first")

    return df, errors


def save_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> None:
    df, errors = build_roster()
    save_df(df, OUT_PATH)
    n = len(df)
    teams_tried = 32
    print(f"✅ Downloaded {n} offensive players from ESPN ({teams_tried} teams).")
    if errors:
        print(f"⚠️ Encountered errors for {len(errors)} teams; first few: {errors[:5]}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Failed to fetch roster:", e, file=sys.stderr)
        sys.exit(1)
