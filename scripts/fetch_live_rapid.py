#!/usr/bin/env python3
"""Fetch live player stats from RapidAPI proxy and write compact JSON/CSV.

This script expects a list of numeric player IDs. By default it will try to
read IDs from `data/player_stock_summary.csv` (column espnId) if present and
numeric; otherwise pass --ids-file path with one ID per line.

It calls the local proxy endpoint by default at http://localhost:3000/api/rapid-proxy
and writes outputs to external/rapid/player_stats_live.json and .csv
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import subprocess
import sys

try:
    # prefer pandas.json_normalize for flattening nested structures
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore

OUT_DIR = Path("external/rapid")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_ids_from_summary(path: Path) -> List[int]:
    ids: List[int] = []
    if not path.exists():
        return ids
    with path.open() as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            v = r.get("espnId") or r.get("id") or ""
            try:
                s = str(v).strip()
                if s.isdigit():
                    ids.append(int(s))
            except Exception:
                continue
    return ids


def _safe_float_csv(v: Any):
    try:
        if v is None or v == "":
            return ""
        return float(v)
    except Exception:
        return ""


def _safe_int_csv(v: Any):
    try:
        if v is None or v == "":
            return 0
        return int(float(v))
    except Exception:
        return 0


def _normalize_raw_player(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map RapidAPI player object to canonical fields.

    Canonical keys: player, espnId, epa_per_play, cpoe, plays, week
    """
    out: Dict[str, Any] = {}

    def pick(keys):
        for k in keys:
            if k in raw and raw.get(k) not in (None, "", []):
                return raw.get(k)
        return None

    # player name
    name = pick(["player", "player_name", "name", "full_name", "athlete_name"])
    if name is None:
        # try nested athlete.name or similar
        athlete = raw.get("athlete") or raw.get("playerObj") or {}
        if isinstance(athlete, dict):
            name = athlete.get("displayName") or athlete.get("name") or athlete.get("full_name")
    out["player"] = str(name).strip() if name is not None else ""

    # espnId / id
    eid = pick(["espnId", "playerId", "player_id", "id", "espnid"])
    if eid is None:
        # try nested
        athlete = raw.get("athlete") or {}
        if isinstance(athlete, dict):
            eid = athlete.get("id") or athlete.get("playerId")
    out["espnId"] = str(eid).strip() if eid is not None else ""

    # EPA / CPOE
    epa = pick(["epa_per_play", "avg_epa", "epa", "avgEpa", "epaPerPlay"]) or None
    cpoe = pick(["cpoe", "avg_cpoe", "cpoe_pct"]) or None
    plays = pick(["plays", "play_count", "n_plays", "plays_count"]) or None
    week = pick(["week"]) or None

    # coerce numeric values safely
    def to_float(v):
        try:
            if v is None or v == "":
                return None
            return float(v)
        except Exception:
            return None

    def to_int(v):
        try:
            if v is None or v == "":
                return None
            return int(float(v))
        except Exception:
            return None

    out["epa_per_play"] = to_float(epa)
    out["cpoe"] = to_float(cpoe)
    out["plays"] = to_int(plays) or 0
    out["week"] = to_int(week) or 0

    return out


def fetch_one(base: str, player_id: int, timeout: int = 15, max_retries: int = 5):
    path = f"/nfl-ath-stats?id={player_id}"
    url = f"{base.rstrip('/')}/api/rapid-proxy?path={requests.utils.requote_uri(path)}"

    attempt = 0
    while attempt < max_retries:
        try:
            r = requests.get(url, timeout=timeout)
        except requests.RequestException as e:
            attempt += 1
            backoff = min(10, (2 ** attempt) + random.random())
            print(f"  Network error on attempt {attempt} for {player_id}: {e}. Retrying in {backoff:.1f}s...")
            time.sleep(backoff)
            continue

        if r.status_code == 200:
            try:
                return r.json(), None
            except Exception as e:
                return None, f"Invalid JSON: {e}"

        if r.status_code == 429:
            # rate limited: honor Retry-After if present
            ra = r.headers.get("Retry-After")
            wait = None
            try:
                if ra is not None:
                    wait = float(ra)
            except Exception:
                wait = None
            attempt += 1
            if wait is None:
                wait = min(10, (2 ** attempt) + random.random())
            print(f"  429 rate-limited on attempt {attempt} for {player_id}. Sleeping {wait:.1f}s before retry.")
            time.sleep(wait)
            continue

        # other HTTP errors: don't retry for 4xx except 429; for 5xx, retry a few times
        if 500 <= r.status_code < 600:
            attempt += 1
            backoff = min(10, (2 ** attempt) + random.random())
            print(f"  Server error {r.status_code} on attempt {attempt} for {player_id}. Retrying in {backoff:.1f}s...")
            time.sleep(backoff)
            continue

        # non-retriable error
        return None, f"HTTP {r.status_code}: {r.text[:200]}"

    return None, f"Giving up after {max_retries} attempts"


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://localhost:3000", help="Base URL for local app proxy")
    p.add_argument("--ids-file", help="File with one player id per line")
    p.add_argument("--summary", default="data/player_stock_summary.csv", help="CSV to read espnId from")
    p.add_argument("--out-json", default=str(OUT_DIR / "player_stats_live.json"))
    p.add_argument("--out-csv", default=str(OUT_DIR / "player_stats_live.csv"))
    p.add_argument("--delay", type=float, default=0.25, help="Seconds between requests to avoid rate limits")
    p.add_argument("--max-retries", type=int, default=5, help="Max retries per request on transient errors")
    args = p.parse_args(argv)

    ids: List[int] = []
    if args.ids_file:
        f = Path(args.ids_file)
        if f.exists():
            for line in f.read_text().splitlines():
                s = line.strip()
                if s.isdigit():
                    ids.append(int(s))
    else:
        ids = load_ids_from_summary(Path(args.summary))

    if not ids:
        print("No numeric player IDs found. Provide --ids-file or ensure data/player_stock_summary.csv has numeric espnId column.")
        return 2

    players: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    unique_ids = sorted(set(ids))
    for i, pid in enumerate(unique_ids):
        print(f"[{i+1}/{len(unique_ids)}] Fetching player {pid}...")
        data, err = fetch_one(args.base, pid, max_retries=args.max_retries)
        if err:
            print("  Error:", err)
            errors.append({"id": pid, "error": err})
        else:
            # Normalize/flatten response into canonical player record
            record = None
            try:
                # response might be { 'data': {...} } or {...}
                if isinstance(data, dict) and "data" in data and (isinstance(data["data"], dict) or isinstance(data["data"], list)):
                    payload = data["data"]
                else:
                    payload = data

                # If payload is a list, take first element or treat each as separate record
                if isinstance(payload, list):
                    # prefer first dict-looking element
                    candidate = None
                    for item in payload:
                        if isinstance(item, dict):
                            candidate = item
                            break
                    payload = candidate or (payload[0] if payload else {})

                if isinstance(payload, dict):
                    # try json_normalize to flatten nested structures where available
                    if pd is not None:
                        try:
                            jdf = pd.json_normalize(payload)
                            flat = jdf.to_dict(orient="records")[0] if not jdf.empty else payload
                        except Exception:
                            flat = payload
                    else:
                        flat = payload
                    # Ensure dict keys are strings for typing safety
                    if isinstance(flat, dict):
                        flat2 = {str(k): v for k, v in flat.items()}
                    else:
                        flat2 = {}
                    record = _normalize_raw_player(flat2)
                else:
                    record = {"player": "", "espnId": str(pid), "epa_per_play": None, "cpoe": None, "plays": 0, "week": 0}
            except Exception as e:
                errors.append({"id": pid, "error": f"normalize_failed: {e}"})
                record = {"player": "", "espnId": str(pid), "epa_per_play": None, "cpoe": None, "plays": 0, "week": 0}

            # log missing fields
            missing = [k for k in ("player", "epa_per_play", "cpoe", "plays") if not record.get(k)]
            if missing:
                print(f"  Warning: player {pid} missing/empty fields: {missing}")

            players.append(record)

        time.sleep(args.delay)

    # Write JSON: canonical structure with players list
    outj = Path(args.out_json)
    outj.parent.mkdir(parents=True, exist_ok=True)
    payload = {"fetched_at": time.time(), "players": players, "errors": errors}
    outj.write_text(json.dumps(payload, indent=2, default=str))
    print("Wrote", outj)

    # Write CSV with canonical columns
    outc = Path(args.out_csv)
    fieldnames = ["player", "espnId", "epa_per_play", "cpoe", "plays", "week"]
    # ensure numeric coercion/format in CSV
    rows: List[Dict[str, Any]] = []
    for p in players:
        rows.append({
            "player": p.get("player", ""),
            "espnId": p.get("espnId", ""),
            "epa_per_play": _safe_float_csv(p.get("epa_per_play")),
            "cpoe": _safe_float_csv(p.get("cpoe")),
            "plays": _safe_int_csv(p.get("plays")),
            "week": _safe_int_csv(p.get("week")),
        })

    if rows:
        with outc.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        print("Wrote", outc)

    # Attempt to run local enrichment script if present
    try:
        calc = Path("scripts/calculate_advanced_metrics.py")
        if calc.exists():
            out_enriched = Path("external/rapidapi/player_stats_enriched_2025.csv")
            out_enriched.parent.mkdir(parents=True, exist_ok=True)
            print("Running calculate_advanced_metrics.py to enrich RapidAPI CSV...")
            proc = subprocess.run([sys.executable, str(calc), "--input", str(outc), "--output", str(out_enriched)], check=False)
            if proc.returncode == 0:
                print("Enriched CSV written to", out_enriched)
            else:
                print("calculate_advanced_metrics.py exited with", proc.returncode, file=sys.stderr)
    except Exception as e:
        print("Failed to run calculate_advanced_metrics.py:", e, file=sys.stderr)

    print("Done. Errors:", len(errors))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
