#!/usr/bin/env python3
"""Probe several Tank01 endpoints and save HTTP response snippets for debugging.

Usage: python scripts/probe_tank01_endpoints.py --week 13
Reads RAPIDAPI_KEY from the environment.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

# Ensure project root is importable when running this script directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import scripts._env  # loads .env.local into environment (if present)
import textwrap
from typing import List

import requests


RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"


ENDPOINT_CANDIDATES: List[str] = [
    "/getNFLPlayerList/",
    "/getNFLPlayerList",
    "/getPlayers",
    "/players",
    "/playerList",
    "/players/list",
    "/getNFLGamesForPlayer",
]


def probe_week(week: int) -> int:
    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        raise RuntimeError(
            "RAPIDAPI_KEY is not set. Create a file named .env.local at the project root with:\n"
            "RAPIDAPI_KEY=your_key_here\n\n"
            "That file is ignored by git and will be loaded automatically. Or export RAPIDAPI_KEY in your shell."
        )

    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Accept": "application/json",
    }

    out_dir = os.path.join("external", "tank01", "diagnostics")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"endpoint_probe_week_{week}.txt")

    with open(out_file, "w", encoding="utf-8") as fh:
        for ep in ENDPOINT_CANDIDATES:
            url = f"https://{RAPIDAPI_HOST}{ep}"
            fh.write("""""".strip())
            fh.write(f"\n=== URL: {url}\n")
            try:
                resp = requests.get(url, headers=headers, timeout=15)
                fh.write(f"Status: {resp.status_code}\n")
                # write some headers
                fh.write("Response headers:\n")
                for k in ("content-type", "content-length", "x-rapidapi-error", "x-rapidapi-quota"):
                    if k in resp.headers:
                        fh.write(f"  {k}: {resp.headers.get(k)}\n")
                fh.write("Body (truncated 8000 chars):\n")
                text = resp.text or ""
                fh.write(textwrap.shorten(text, width=8000, placeholder='\n...TRUNCATED...\n'))
                fh.write("\n\n")
            except Exception as e:
                fh.write(f"Request failed: {e}\n\n")

    print(f"Wrote probe output to {out_file}")
    return 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", type=int, required=False, help="week number")
    args = parser.parse_args(argv)
    week = args.week or int(os.environ.get("WEEK", "0") or 0)
    if not week:
        print("Please provide --week or set WEEK env var")
        return 3
    return probe_week(week)


if __name__ == "__main__":
    raise SystemExit(main())
