"""Clean and normalize player profile CSVs.

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

    Sources (in order):
    - local `data/advanced/*.json` files (index.json -> individual player files)
    - `data/player_game_stats.csv` (most common team for a player)
    - optional ESPN fetch (only if requests and bs4 are available)

    Do not overwrite existing non-empty values. Empty is treated as '' or NaN.
    """
    # Prepare maps
    pos_by_espn = {}
    pos_by_player = {}
    # Load advanced index if available
    adv_index = Path("data/advanced/index.json")
    if adv_index.exists():
        try:
            idx = json.loads(adv_index.read_text())
            players = idx.get("players") or []
            for p in players:
                espn = str(p.get("espnId"))
                f = p.get("file")
                if not f:
                    continue
                pfile = adv_index.parent / f
                if not pfile.exists():
                    continue
                try:
                    rec = json.loads(pfile.read_text())
                except Exception:
                    continue
                # position can be present
                pos = rec.get("position") or rec.get("pos")
                name = rec.get("player")
                if pos:
                    pos_by_espn[espn] = str(pos).upper()
                if name:
                    pos_by_player[title_case(name)] = str(pos).upper() if pos else ""
        except Exception:
            # ignore parse errors in advanced index
            pass

    # Build team map from player_game_stats.csv (most common team per player)
    team_by_player = {}
    pstats = Path("data/player_game_stats.csv")
    if pstats.exists():
        try:
            gdf = pd.read_csv(pstats)
            # find possible player and team columns
            cols = {c.lower(): c for c in gdf.columns}
            player_col = None
            team_col = None
            for cand in ["player", "player_name", "name"]:
                if cand in cols:
                    player_col = cols[cand]
                    break
            for cand in ["team", "team_name", "team_abbr"]:
                if cand in cols:
                    team_col = cols[cand]
                    break
            # espn id detection is unnecessary for building team map; ignore
            if player_col and team_col:
                # normalize player names
                gdf[player_col] = gdf[player_col].astype(str).map(title_case)
                # build most common team per player
                for name, grp in gdf.groupby(player_col):
                    teams = grp[team_col].astype(str).str.upper().replace("NAN", "")
                    most = Counter(teams[teams != ""]).most_common(1)
                    if most:
                        team_by_player[name] = most[0][0]
        except Exception:
            # don't fail enrichment on parse errors
            pass

    # Optional ESPN fetch helpers (only if packages available)
    def fetch_from_espn(espn_id: str, player_name: str) -> dict[str, str]:
        out: dict[str, str] = {}
        try:
            import requests
            from bs4 import BeautifulSoup
        except Exception:
            return out
        # Try by ESPN numeric id if it looks numeric
        url = None
        if espn_id and espn_id.isdigit():
            url = f"https://www.espn.com/nfl/player/_/id/{espn_id}"
        else:
            # fallback: search by name
            q = player_name.replace(" ", "%20")
            url = f"https://www.espn.com/search/results?q={q}"
        try:
            r = requests.get(url, timeout=5)
            if r.status_code != 200:
                return out
            soup = BeautifulSoup(r.text, "html.parser")
            # Try to find position/team in meta or specific selectors
            # ESPN player pages often have a data-overview with position/team
            # This is best-effort; avoid brittle scraping.
            # Look for spans with class 'pos-col' or similar
            text = soup.get_text(separator="|")
            # crude extraction: look for patterns like 'Position: WR' or 'WR •'
            m = re.search(r"Position[:\s]+([A-Z]{1,3})", text)
            if m:
                out["position"] = m.group(1)
            # team: look for 2-4 letter uppercase near team name
            m2 = re.search(r"([A-Z]{2,4})\s+•", text)
            if m2:
                out["team"] = m2.group(1)
        except Exception:
            return {}
        return out

    # Now fill missing values without overwriting existing non-empty ones
    def is_empty_val(v):
        return (v is None) or (str(v).strip() == "")

    for idx, row in df.iterrows():
        player = row.get("player")
        espn = str(row.get("espnId") or "")
        # position
        if is_empty_val(row.get("position")):
            filled = None
            # try espn map
            if espn and espn in pos_by_espn:
                filled = pos_by_espn[espn]
            # try player name map
            if not filled and player and player in pos_by_player:
                filled = pos_by_player[player]
            # try optional espn fetch
            if not filled:
                info = fetch_from_espn(espn, player or "")
                filled = info.get("position")
            if filled:
                mask = df.index == idx
                df.loc[mask, "position"] = filled

        # team
        if is_empty_val(row.get("team")):
            filled = None
            if player and player in team_by_player:
                filled = team_by_player[player]
            # try espn fetch if still empty
            if not filled:
                info = fetch_from_espn(espn, player or "")
                if info:
                    filled = info.get("team")
            if filled:
                mask = df.index == idx
                df.loc[mask, "team"] = filled

    # Final normalize (upper-case team/position)
    df["team"] = (
        df["team"].astype(str).str.upper().map(lambda s: s if s != "NAN" else "")
    )
    df["position"] = (
        df["position"].astype(str).str.upper().map(lambda s: s if s != "NAN" else "")
    )
    return df


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default=None,
        help="Input CSV path (player profiles).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="data/player_profiles_cleaned.csv",
        help="Output CSV path",
    )
    args = parser.parse_args(argv)

    # Determine input: explicit --input overrides, otherwise prefer
    # data/player_profiles.csv if present. If neither exist we synthesize
    # from data/player_game_stats.csv with a clear, single-line message.
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(
                f"Provided input '{input_path}' not found; synthesizing from data/player_game_stats.csv instead"
            )
            df = build_from_game_stats()
        else:
            df = pd.read_csv(input_path)
            df = normalize_profiles(df)
    else:
        detected = detect_input_file()
        if detected and detected.exists():
            # Found an explicit profiles CSV (prefer data/player_profiles.csv)
            print(f"Loading player profiles from {detected}")
            df = pd.read_csv(detected)
            df = normalize_profiles(df)
        else:
            # Clean, user-facing message when we must synthesize
            print(
                "data/player_profiles.csv not found — synthesizing profiles from data/player_game_stats.csv"
            )
            df = build_from_game_stats()
    # Enrich missing team/position values where possible
    try:
        df = enrich_profiles(df)
    except Exception:
        # Keep original behaviour on any enrichment error
        pass
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outp, index=False)
    print(f"Wrote {len(df)} cleaned player profiles to {outp}")


if __name__ == "__main__":
    main()
