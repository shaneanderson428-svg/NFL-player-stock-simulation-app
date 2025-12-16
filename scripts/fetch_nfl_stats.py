#!/usr/bin/env python3
"""Fetch latest nflfastR CSVs (player_stats) for the current season and cache locally.

Saves files under external/nflfastR/ (creates directory if needed). Uses HEAD
Last-Modified header when available to skip re-downloads. Prints number of
rows and unique players loaded.

No API key required.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import sys
import requests
import time
import json
import pandas as pd
import numpy as np
from typing import Optional, List

BASE_RAW = "https://raw.githubusercontent.com/nflverse/nflfastR-data/master/data"
DEFAULT_SEASON = 2025
OUT_DIR = Path("external/nflfastR")


def fetch_file(remote_url: str, out_path: Path, meta_path: Optional[Path] = None) -> tuple[bool, Optional[int]]:
    """Fetch remote_url and save to out_path. Use meta_path to store Last-Modified.
    Returns True if file was downloaded/updated, False if skipped.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {}
    # check existing meta
    existing_meta = None
    if meta_path and meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text())
            lm = existing_meta.get("Last-Modified")
            if lm:
                headers["If-Modified-Since"] = lm
        except Exception:
            existing_meta = None
    try:
        # Try HEAD first to avoid large downloads when not needed
        head = requests.head(remote_url, timeout=10)
        if head.status_code == 200:
            remote_lm = head.headers.get("Last-Modified")
            if remote_lm and existing_meta and existing_meta.get("Last-Modified") == remote_lm and out_path.exists():
                print(f"Up-to-date: {out_path} (Last-Modified matches)")
                return (False, 200)
        # GET the content
        resp = requests.get(remote_url, stream=True, timeout=30)
        if resp.status_code == 304:
            print(f"Not modified: {out_path}")
            return (False, 304)
        if resp.status_code != 200:
            print(f"Failed to fetch {remote_url}: HTTP {resp.status_code}")
            return (False, resp.status_code)
        # write to temp then move
        tmp = out_path.with_suffix(".tmp")
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
        tmp.replace(out_path)
        # save meta
        if meta_path:
            meta = {"fetched_at": time.time(), "url": remote_url}
            if resp.headers.get("Last-Modified"):
                meta["Last-Modified"] = resp.headers.get("Last-Modified")
            if resp.headers.get("ETag"):
                meta["ETag"] = resp.headers.get("ETag")
            try:
                meta_path.write_text(json.dumps(meta))
            except Exception:
                pass
        print(f"Fetched: {out_path}")
        return (True, 200)
    except requests.RequestException as e:
        print(f"Network error while fetching {remote_url}: {e}")
        return (False, None)
    except Exception as e:
        print(f"Error while saving {out_path}: {e}")
        return (False, None)


def default_player_stats_url(season: int) -> str:
    # Try the top-level player_stats CSV first (live data location)
    return f"{BASE_RAW}/player_stats_{season}.csv"


def summarize_play_by_play(season: int, out_dir: Path) -> bool:
    """Read play_by_play_{season}.csv.gz from out_dir, summarize per player,
    and write external/nflfastR/player_stats_{season}_derived.csv.
    Aggregates: mean epa, mean cpoe, play count (play_id)."""
    # support either gz or plain csv filenames
    gz_path = out_dir / f"play_by_play_{season}.csv.gz"
    csv_path = out_dir / f"play_by_play_{season}.csv"
    out_csv = out_dir / f"player_stats_{season}_derived.csv"
    if not gz_path.exists() and not csv_path.exists():
        print(f"play-by-play file not found at {gz_path} or {csv_path}")
        return False
    try:
        # read compressed CSV if gz, otherwise plain CSV
        if gz_path.exists():
            df = pd.read_csv(gz_path, compression="gzip", low_memory=False)
        else:
            df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        print(f"Failed to read {gz_path}: {e}")
        return False

    # Ensure we have a player_id column; try to coalesce common player id columns
    if "player_id" not in df.columns or df["player_id"].isnull().all():
        candidates = [
            "rusher_player_id",
            "passer_player_id",
            "receiver_player_id",
            "returner_player_id",
        ]
        df["player_id"] = df.get("player_id") if "player_id" in df.columns else pd.Series([None] * len(df))
        for c in candidates:
            if c in df.columns:
                df["player_id"] = df["player_id"].fillna(df[c])

    if "player_id" not in df.columns or df["player_id"].isnull().all():
        print("Could not determine player_id column from play-by-play; derived summary not created.")
        return False

    # Ensure epa and cpoe exist
    if "epa" not in df.columns:
        df["epa"] = np.nan
    if "cpoe" not in df.columns:
        df["cpoe"] = np.nan

    # Determine play identifier column for counts
    count_col = "play_id" if "play_id" in df.columns else None
    if count_col is None:
        # fallback to counting rows per player
        summary = df.groupby("player_id").agg({"epa": "mean", "cpoe": "mean"})
        summary = summary.rename(columns={"epa": "avg_epa", "cpoe": "avg_cpoe"})
        summary["plays"] = df.groupby("player_id").size()
    else:
        summary = df.groupby("player_id").agg({"epa": "mean", "cpoe": "mean", "play_id": "count"})
        summary = summary.rename(columns={"epa": "avg_epa", "cpoe": "avg_cpoe", "play_id": "plays"})

    # Write derived CSV
    try:
        summary.reset_index().to_csv(out_csv, index=False)
        print(f"Wrote derived player stats to {out_csv}")
        return True
    except Exception as e:
        print(f"Failed to write derived CSV {out_csv}: {e}")
        return False


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch nflfastR player stats CSV for a season")
    parser.add_argument("--season", "-s", type=int, default=DEFAULT_SEASON)
    parser.add_argument("--dataset", "-d", type=str, default="player_stats", choices=["player_stats", "play_by_play"], help="Which dataset to fetch")
    args = parser.parse_args(argv)

    season = int(args.season)
    ds = args.dataset
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if ds == "player_stats":
        # Try several common locations for the player_stats CSV, preferring the
        # top-level path first per user's request, then the nested player_stats/
        # folder which some releases use.
        candidates = [
            default_player_stats_url(season),
            f"{BASE_RAW}/player_stats/player_stats_{season}.csv",
        ]

        found = False
        last_status = None
        for remote in candidates:
            filename = Path(remote).name
            out_path = OUT_DIR / filename
            meta_path = OUT_DIR / (filename + ".meta.json")
            ok, status = fetch_file(remote, out_path, meta_path=meta_path)
            last_status = status
            if ok or out_path.exists():
                found = True
                break

        # If not found as a player_stats CSV, fall back to play-by-play gz variants
        if not found and last_status == 404:
            print(f"player_stats_{season}.csv not found — attempting play_by_play_{season}.csv.gz instead (live data).")
            pb_candidates = [
                f"{BASE_RAW}/play_by_play_{season}.csv.gz",
                f"{BASE_RAW}/play_by_play/play_by_play_{season}.csv.gz",
                f"{BASE_RAW}/play_by_play/play_by_play_{season}.csv",
            ]
            pb_found = False
            for remote in pb_candidates:
                filename = Path(remote).name
                out_path = OUT_DIR / filename
                meta_path = OUT_DIR / (filename + ".meta.json")
                ok2, status2 = fetch_file(remote, out_path, meta_path=meta_path)
                last_status = status2
                if ok2 or out_path.exists():
                    # Summarize the play-by-play file into a derived player_stats CSV
                    pb_found = True
                    summed = summarize_play_by_play(season, OUT_DIR)
                    if not summed:
                        print("Failed to create derived player stats from play-by-play data.")
                    break
            # If still not found, try GitHub API repo-tree discovery to locate file paths
            if not pb_found:
                print("Direct candidate fetches failed; attempting GitHub repo discovery for play-by-play files...")
                try:
                    # Use GitHub API to list repository tree and find matching play_by_play files
                    api_url = f"https://api.github.com/repos/nflverse/nflfastR-data/git/trees/master?recursive=1"
                    r = requests.get(api_url, timeout=15)
                    if r.status_code == 200:
                        data = r.json()
                        tree = data.get("tree", [])
                        match_path = None
                        # prefer gz then csv
                        for ext in (".csv.gz", ".csv"):
                            target_name = f"play_by_play_{season}{ext}"
                            for entry in tree:
                                p = entry.get("path", "")
                                if p.endswith(target_name):
                                    match_path = p
                                    break
                            if match_path:
                                break
                        if match_path:
                            raw_url = f"https://raw.githubusercontent.com/nflverse/nflfastR-data/master/{match_path}"
                            filename = Path(match_path).name
                            out_path = OUT_DIR / filename
                            meta_path = OUT_DIR / (filename + ".meta.json")
                            ok3, status3 = fetch_file(raw_url, out_path, meta_path=meta_path)
                            if ok3 or out_path.exists():
                                summed = summarize_play_by_play(season, OUT_DIR)
                                if not summed:
                                    print("Failed to create derived player stats after GitHub-discovered download.")
                                pb_found = True
                        else:
                            print("GitHub repo tree scanned but no play_by_play file matched for season.")
                    else:
                        print(f"GitHub API tree request failed: HTTP {r.status_code}")
                except Exception as e:
                    print(f"Error during GitHub discovery: {e}")
            if not pb_found:
                print(f"Failed to fetch play-by-play from any candidate (last status {last_status}).")
    else:
        # play_by_play path (gz) requested explicitly — try both top-level and nested
        pb_candidates = [
            f"{BASE_RAW}/play_by_play_{season}.csv.gz",
            f"{BASE_RAW}/play_by_play/play_by_play_{season}.csv.gz",
        ]
        ok = False
        last_status = None
        for remote in pb_candidates:
            filename = Path(remote).name
            out_path = OUT_DIR / filename
            meta_path = OUT_DIR / (filename + ".meta.json")
            ok, status = fetch_file(remote, out_path, meta_path=meta_path)
            last_status = status
            if ok or out_path.exists():
                break
    # Load the CSV (either freshly fetched or existing) to report counts
    if not out_path.exists():
        print(f"No file at {out_path} — nothing to report.")
        sys.exit(1)

    try:
        df = pd.read_csv(out_path)
        rows = len(df)
        # try common player id/name columns
        player_cols = [c for c in ("player_id", "playerId", "player", "name") if c in df.columns]
        if player_cols:
            unique_players = df[player_cols[0]].nunique()
        else:
            unique_players = df.shape[0]
        print(f"Loaded {rows} rows, {unique_players} unique players from {out_path}")
        return 0
    except Exception as e:
        print(f"Failed to read CSV {out_path}: {e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
