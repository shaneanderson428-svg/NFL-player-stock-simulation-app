#!/usr/bin/env python3
"""
Fetch Tank01 WR stats for a given season and week and save to external/tank01.

Usage:
  ./scripts/fetch_tank01_wr_week.py --season 2025 --week 14 [--position WR]

Requires the environment variable RAPIDAPI_KEY to be set.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from pathlib import Path as _Path
import sys as _sys
# Ensure the project root is on sys.path so `import scripts._env` works when
# running this file directly (python3 scripts/fetch_tank01_wr_week.py)
_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))
import scripts._env  # loads .env.local into environment (if present)

try:
    import requests
except Exception:  # pragma: no cover - requests may not be installed in CI
    print("The 'requests' package is required. Install with: pip install requests")
    raise


def fetch(season: str, week: str, position: str = "WR") -> int:
    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        raise RuntimeError(
            "RAPIDAPI_KEY is not set. Create a file named .env.local at the project root with:\n"
            "RAPIDAPI_KEY=your_key_here\n\n"
            "That file is ignored by git and will be loaded automatically. Or export RAPIDAPI_KEY in your shell."
        )

    host = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    endpoint = "getNFLPlayerLiveGameStats"
    url = f"https://{host}/{endpoint}?season={season}&week={week}&position={position}"

    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": host,
    }

    print(f"Requesting: {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except Exception as exc:
        print("Request failed:", exc)
        return 3

    outdir = Path("external/tank01")
    outdir.mkdir(parents=True, exist_ok=True)

    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    result = {"ts": ts, "endpoint": endpoint, "status_code": resp.status_code, "url": url}

    try:
        result["raw"] = resp.json()
    except Exception:
        result["raw_text"] = resp.text

    outfile = outdir / f"live_wr_{season}_w{week}.json"
    with outfile.open("w") as fh:
        json.dump(result, fh, indent=2)

    print(f"Wrote {outfile} (status {resp.status_code})")
    return resp.status_code


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch Tank01 WR stats for a season/week and save locally")
    p.add_argument("--season", required=True, help="Season year, e.g. 2025")
    p.add_argument("--week", required=True, help="Week number, e.g. 14")
    p.add_argument("--position", default="WR", help="Position filter (default WR)")
    args = p.parse_args()

    code = fetch(args.season, args.week, args.position)
    if code == 200:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
