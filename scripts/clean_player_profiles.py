#!/usr/bin/env python3
"""Clean player profiles — single canonical implementation.

This script fetches a roster CSV (from known nflfastR raw URLs), normalizes
it to columns (espnId, player, position, team) and writes/updates
`data/player_stock_summary.csv`. Helpers are defined once (no duplicates),
and we only call Series methods on actual Series objects to avoid editor
diagnostics like "Cannot access attribute 'astype' for class 'str'".
"""

from pathlib import Path
import json
from typing import Optional, List, Tuple
import argparse
import io
import sys

import pandas as pd
import requests


SUMMARY_PATH = Path("data/player_stock_summary.csv")
DEFAULT_PRICE = 100.0
ROSTER_URLS: List[str] = [
    # New canonical players list (nflfastR-data)
    "https://raw.githubusercontent.com/nflverse/nflfastR-data/master/data/players.csv",
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
    # Remote fetch failed — try a local backup file if present
    backup = Path("data/roster_backup.csv")
    if backup.exists():
        try:
            return pd.read_csv(backup, dtype=str)
        except Exception as exc:
            raise RuntimeError(f"Failed to read local roster backup '{backup}': {exc}")
    raise RuntimeError(f"Unable to fetch roster from known URLs: {last_exc}")


def build_from_game_stats() -> pd.DataFrame:
    p = Path("data/player_game_stats.csv")
    if not p.exists():
        return pd.DataFrame(columns=["espnId", "player", "position", "team"])
    df = pd.read_csv(p, dtype=str)
    lower = {c.lower(): c for c in df.columns}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c in lower:
                return lower[c]
        return None

    espn_col = pick("espnid", "player_id", "playerid", "id", "espn_id")
    player_col = pick("player", "name", "player_name")
    team_col = pick("team", "team_name")
    pos_col = pick("position", "pos")

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


def enrich_profiles(df: pd.DataFrame, add_missing: bool = True) -> pd.DataFrame:
    """Attempt to fill missing position and team values and optionally add missing players.

    - If add_missing is True, synthesize profiles from `build_from_game_stats()` and
      append any players not already present in `df` (by espnId or player name).
    - For any row missing `position` (or `team`), attempt to read
      `data/advanced/<espnId>.json` and use its `position`/`team` fields when available.

    Returns a normalized DataFrame with columns: espnId, player, position, team.
    """
    df = df.copy()
    # Ensure required columns
    for c in ("espnId", "player", "position", "team"):
        if c not in df.columns:
            df[c] = ""

    # Optionally append missing players from game stats
    if add_missing:
        try:
            synth = build_from_game_stats()
        except Exception:
            synth = pd.DataFrame(columns=["espnId", "player", "position", "team"])

        existing_ids = set(df["espnId"].astype(str).str.strip())
        existing_players = set(df["player"].astype(str).str.strip())

        new_rows: list[dict] = []
        for _, r in synth.iterrows():
            eid = str(r.get("espnId", "")).strip()
            pname = str(r.get("player", "")).strip()
            if (eid and eid not in existing_ids) and (pname and pname not in existing_players):
                new_rows.append({"espnId": eid, "player": pname, "position": r.get("position", ""), "team": r.get("team", "")})

        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # Fill from advanced JSON when available
    adv_dir = Path("data/advanced")
    for idx in df.index:
        row = df.loc[idx]
        pos = str(row.get("position", "") or "").strip()
        team = str(row.get("team", "") or "").strip()
        eid = str(row.get("espnId", "") or "").strip()
        if (not pos or pos == "") and eid:
            adv_path = adv_dir / f"{eid}.json"
            if adv_path.exists():
                try:
                    data = json.loads(adv_path.read_text())
                    adv_pos = str(data.get("position", "") or "").strip()
                    adv_team = str(data.get("team", "") or "").strip()
                    if adv_pos:
                        df.loc[idx, "position"] = adv_pos
                    if adv_team and not team:
                        df.loc[idx, "team"] = adv_team
                except Exception:
                    # ignore malformed advanced files
                    pass

    # Deduplicate and normalize formatting
    # prefer espnId uniqueness then player
    df["espnId"] = df["espnId"].astype(str).map(lambda s: s.strip())
    df["player"] = df["player"].astype(str).map(lambda s: s.title().strip())
    df = df.drop_duplicates(subset=["espnId"], keep="first")
    df = df.drop_duplicates(subset=["player"], keep="first")

    # Ensure columns order
    for c in ("espnId", "player", "position", "team"):
        if c not in df.columns:
            df[c] = ""

    return df[["espnId", "player", "position", "team"]]


def clean_roster(df: pd.DataFrame) -> pd.DataFrame:
    # Prefer the canonical nflfastR roster columns: gsis_id, full_name, position, team
    want = ["gsis_id", "full_name", "position", "team"]
    present = [c for c in want if c in df.columns]
    if len(present) == len(want):
        roster = df.loc[:, want].copy()
        # rename to our schema: full_name -> player, gsis_id -> espnId
        roster = roster.rename(columns={"full_name": "player", "gsis_id": "espnId"})
    else:
        # Fallback: try to pick best matching columns from what was provided
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

        cols = [c for c in (name_col, pos_col, team_col, id_col) if c]
        if not name_col or not pos_col or not team_col:
            # If essential columns are missing, try to build minimal roster from available fields
            # and ultimately fall back to an empty DataFrame handled upstream.
            roster = pd.DataFrame(columns=["espnId", "player", "position", "team"])
        else:
            roster = df.loc[:, cols].copy()
            roster = roster.rename(columns={name_col: "player", pos_col: "position", team_col: "team"})
            if id_col and id_col in roster.columns:
                roster = roster.rename(columns={id_col: "espnId"})
            else:
                roster["espnId"] = ""

    # Normalize string columns and ensure final schema
    for c in ("player", "position", "team", "espnId"):
        if c in roster.columns:
            roster[c] = roster[c].astype(str).str.strip().replace({"nan": ""}).fillna("")
        else:
            roster[c] = ""
    # Ensure every row has a normalized offensive position when possible.
    # We'll try to fill missing/blank positions from any ESPN-like position
    # columns present in the source `df` (e.g., 'position', 'pos', 'espn_position', etc.)
    offensive = {"QB", "RB", "WR", "TE"}

    def _normalize_pos(p: str) -> str:
        if not p:
            return ""
        s = str(p).upper().strip()
        # direct match
        if s in offensive:
            return s
        # token match (e.g., 'RB-S', 'WR/TE', 'Running Back')
        for code in offensive:
            if code in s:
                return code
        return ""

    # Build candidate position columns from the original roster input `df`.
    pos_candidates = [c for c in df.columns if "position" in c.lower() or c.lower() == "pos" or c.lower().startswith("pos")]

    # Build lookup maps by espn id and by player name from the original df
    espn_pos_map: dict[str, str] = {}
    name_pos_map: dict[str, str] = {}
    for _, r in df.iterrows():
        # try common id/name fields
        # coerce to string when using as keys (don't annotate; use str() at use-sites)
        eid_raw = r.get("gsis_id") or r.get("espnId") or r.get("player_id") or r.get("id") or ""
        eid = str(eid_raw).strip()
        pname = str(r.get("full_name") or r.get("player") or r.get("name") or "").strip().lower()
        for pc in pos_candidates:
            try:
                val = str(r.get(pc, "") or "").strip()
            except Exception:
                val = ""
            if not val:
                continue
            norm = _normalize_pos(val)
            if not norm:
                continue
            if eid:
                espn_pos_map[eid] = norm
            if pname:
                name_pos_map[pname] = norm
            break

    # Fill missing roster positions using maps (prefer espnId, fallback to name)
    for idx in roster.index:
        cur = roster.at[idx, "position"] if "position" in roster.columns else ""
        if cur:
            # normalize existing value to canonical short code when possible
            n = _normalize_pos(str(cur))
            roster.at[idx, "position"] = n
            continue
        # try to fill
        eid_raw = roster.at[idx, "espnId"] if "espnId" in roster.columns else ""
        eid = str(eid_raw).strip()
        pname = str(roster.at[idx, "player"]).strip().lower() if "player" in roster.columns else ""
        filled = ""
        if eid and eid in espn_pos_map:
            filled = espn_pos_map[eid]
        elif pname and pname in name_pos_map:
            filled = name_pos_map[pname]
        roster.at[idx, "position"] = filled

    # Finally, filter to offensive positions only (QB, RB, WR, TE)
    if "position" in roster.columns:
        roster = roster[roster["position"].isin(offensive)].copy()

    # Exclude inactive/retired/practice/suspended players when 'status' is present in the source
    exclude_status = {"Reserve/Injured", "Retired", "Practice Squad", "Suspended"}
    # if original df had a 'status' column, it would have been preserved only in fallback cases
    if "status" in df.columns and "status" in roster.columns:
        roster = roster[~roster["status"].isin(exclude_status)].copy()

    return roster[["espnId", "player", "position", "team"]]


def build_from_roster(roster: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict] = []
    for _, r in roster.iterrows():
        rows.append({
            "player": r.get("player", ""),
            "espnId": r.get("espnId", ""),
            "position": r.get("position", ""),
            "team": r.get("team", ""),
            "currentPrice": DEFAULT_PRICE,
        })
    df = pd.DataFrame(rows, columns=["player", "espnId", "position", "team", "currentPrice"]) if rows else pd.DataFrame(columns=["player", "espnId", "position", "team", "currentPrice"]) 
    sort_cols = [c for c in ("team", "position", "player") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ignore_index=True)
    return df


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
    # If no existing summary, build fresh from roster
    if existing is None or existing.empty:
        out = build_from_roster(roster)
        return out, len(out)

    # Normalize existing identifiers and players
    # at this point existing is guaranteed not None
    assert existing is not None
    # Narrow type for static checkers
    from typing import cast

    existing_df: pd.DataFrame = cast(pd.DataFrame, existing.copy())
    if "espnId" in existing_df.columns:
        existing_df["espnId"] = existing_df["espnId"].astype(str).str.strip()
    else:
        existing_df["espnId"] = ""
    if "player" in existing_df.columns:
        existing_df["player"] = existing_df["player"].astype(str).str.strip()
    else:
        existing_df["player"] = ""

    existing_ids = set([s for s in existing_df["espnId"] if s])
    existing_players = set([s for s in existing_df["player"] if s])

    new_rows: List[dict] = []
    for _, r in roster.iterrows():
        name = str(r.get("player", "")).strip()
        eid = str(r.get("espnId", "")).strip()

        # Use gsis_id (espnId) primarily as unique identifier. If absent, fall back to full_name.
        already_present = False
        if eid and eid in existing_ids:
            already_present = True
        elif (not eid) and name and name in existing_players:
            already_present = True

        if already_present:
            continue

        # Build a new row compatible with the existing summary columns
        new_rows.append(build_default_row(existing, r))

    if not new_rows:
        return existing_df, 0

    new_df = pd.DataFrame(new_rows, columns=existing_df.columns)
    merged = pd.concat([existing_df, new_df], ignore_index=True)
    sort_cols = [c for c in ["team", "position", "player"] if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols, ignore_index=True)
    return merged, len(new_rows)


def save_summary(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Clean and append player profiles from roster")
    parser.add_argument("--reset", action="store_true", help="Rebuild summary from roster (drop existing)")
    parser.add_argument("--local", type=str, help="Optional local roster CSV to use if remote fetch fails")
    args = parser.parse_args(argv)

    try:
        raw = fetch_roster()
        roster_df = clean_roster(raw)
    except Exception as exc:
        # If fetching the remote roster fails, prefer a local CSV if provided.
        if args.local:
            try:
                raw_local = pd.read_csv(args.local, dtype=str)
                roster_df = clean_roster(raw_local)
            except Exception:
                raise RuntimeError(f"Failed to load local roster '{args.local}': {exc}")
        else:
            # Do not silently fall back to game-stats-only roster — the user requested
            # the canonical nflfastR roster. Surface the error so the caller can provide
            # a local file or fix network access.
            raise RuntimeError(f"Unable to fetch nflfastR roster: {exc}. Provide --local <file> to use a local roster CSV.")

    existing: Optional[pd.DataFrame] = None
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
    print(f"✅ Added {added} new players, total {len(out):,} offensive players")


if __name__ == "__main__":
    main()
