#!/usr/bin/env python3
"""Generate WR price history by scanning weekly Tank01 CSV outputs.

Scans external/tank01/player_stats_week_*.csv, converts fantasyPoints -> price
using: price = max(5, fantasyPoints * 4 + 50), and writes external/history/wr_price_history.json

Date for each point is taken from the row's gameID (prefix YYYYMMDD) when available,
otherwise falls back to the CSV file modification date.

Usage: python scripts/generate_wr_history.py
"""
from __future__ import annotations

import glob
import json
import os
import re
from datetime import datetime
from typing import Dict, List

import pandas as pd


OUT_PATH = os.path.join("external", "history", "wr_price_history.json")


def extract_date_from_gameid(gameid: str) -> str | None:
    if not gameid:
        return None
    s = str(gameid)
    if len(s) >= 8 and s[:8].isdigit():
        try:
            return datetime.strptime(s[:8], "%Y%m%d").date().isoformat()
        except Exception:
            return None
    return None


def price_from_fp(fp: float) -> float:
    try:
        v = float(fp) if fp is not None else 0.0
    except Exception:
        v = 0.0
    price = max(5.0, v * 4.0 + 50.0)
    return round(price, 2)


def main() -> None:
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    files = sorted(glob.glob(os.path.join("external", "tank01", "player_stats_week_*.csv")))
    history: Dict[str, List[Dict[str, object]]] = {}

    if not files:
        print("No player_stats_week_*.csv files found under external/tank01; nothing to backfill.")
        return

    # Read all CSVs into a unified dataframe to compute season averages per player
    dfs = []
    file_dates: Dict[str, str] = {}
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str)
            dfs.append((f, df))
            try:
                mtime = os.path.getmtime(f)
                file_dates[f] = datetime.fromtimestamp(mtime).date().isoformat()
            except Exception:
                file_dates[f] = datetime.utcnow().date().isoformat()
        except Exception as e:
            print(f"Failed to read {f}: {e}")
            continue

    if not dfs:
        print("No valid CSV rows to process.")
        return

    # Build a master rows list to compute averages across all numeric columns
    master_rows = []
    numeric_columns = set()
    for f, df in dfs:
        file_date = file_dates.get(f)
        for _, row in df.iterrows():
            espn_id = None
            for col in ("playerID", "espnID", "espnId", "espnid"):
                if col in df.columns and pd.notna(row.get(col)):
                    espn_id = str(row.get(col))
                    break
            if not espn_id:
                for col in df.columns:
                    if col.lower().startswith("player") and pd.notna(row.get(col)):
                        espn_id = str(row.get(col))
                        break
            if not espn_id:
                continue

            # Identify numeric columns in this dataframe row and collect numeric values
            row_vals = {}
            for col in df.columns:
                v = row.get(col)
                if pd.isna(v):
                    continue
                s = str(v).strip()
                if s == "":
                    continue
                # try to parse as float after stripping non-numeric characters
                try:
                    num = float(s)
                except Exception:
                    try:
                        cleaned = re.sub(r"[^0-9.\-]", "", s)
                        if cleaned == "":
                            continue
                        num = float(cleaned)
                    except Exception:
                        continue
                # record numeric column
                numeric_columns.add(col)
                row_vals[col] = num

            # derive a date for the point: prefer gameID in the row
            date_from_game = None
            for gid_col in ("gameID", "game.gameID", "game.id"):
                if gid_col in df.columns and pd.notna(row.get(gid_col)):
                    date_from_game = extract_date_from_gameid(str(row.get(gid_col)))
                    if date_from_game:
                        break

            t = date_from_game or file_date

            master_rows.append({"espn_id": espn_id, "t": t, "values": row_vals})

    # compute per-player season averages from master_rows
    from collections import defaultdict

    # helper: safely extract numeric values from the parsed `values` dict with candidate keys
    def _num_from_values(vals: dict, candidates: list) -> float:
        if not isinstance(vals, dict):
            return 0.0
        for c in candidates:
            if c in vals and vals.get(c) is not None:
                try:
                    return float(str(vals.get(c)))
                except Exception:
                    try:
                        return float(re.sub(r"[^0-9.\-]", "", str(vals.get(c) or "0")))
                    except Exception:
                        continue
        return 0.0

    agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"recs": 0.0, "yds": 0.0, "tds": 0.0, "fp": 0.0, "count": 0})
    for r in master_rows:
        k = r["espn_id"]
        vals = r.get("values", {}) or {}
        recs = _num_from_values(vals, ["Receiving.receptions", "receiving.receptions", "receiving.receptions", "receptions", "rec", "Receiving.recReceptions", "receivingRec"])
        yds = _num_from_values(vals, ["Receiving.recYds", "receiving.recYds", "yards", "yds", "recYds", "Receiving.yards"])
        tds = _num_from_values(vals, ["Receiving.recTD", "receiving.recTD", "td", "recTD", "Receiving.recTD"])
        fp = _num_from_values(vals, ["fantasyPoints", "fantasyPoints.total", "fantasyPointsDefault.standard", "fantasyPointsDefault", "fp"]) 
        agg[k]["recs"] += recs
        agg[k]["yds"] += yds
        agg[k]["tds"] += tds
        agg[k]["fp"] += fp
        agg[k]["count"] += 1

    averages: Dict[str, Dict[str, float]] = {}
    for k, v in agg.items():
        cnt = v.get("count", 1) or 1
        averages[k] = {"recs": v["recs"] / cnt, "yds": v["yds"] / cnt, "tds": v["tds"] / cnt, "fp": v["fp"] / cnt}

    # compute per-player season averages for all numeric columns
    from collections import defaultdict

    stats_agg: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0})
    for r in master_rows:
        k = r["espn_id"]
        stats_agg.setdefault(k, {"count": 0})
        stats_agg[k]["count"] += 1
        for col, val in r["values"].items():
            stats_agg[k].setdefault(col, 0.0)
            stats_agg[k][col] += float(val)

    averages: Dict[str, Dict[str, float]] = {}
    for k, v in stats_agg.items():
        cnt = v.get("count", 1) or 1
        averages[k] = {}
        for col, total in v.items():
            if col == "count":
                continue
            averages[k][col] = total / cnt

    # now build history points using deviation from season averages across all numeric stats
    for r in master_rows:
        espn_id = r["espn_id"]
        t = r["t"]
        avg = averages.get(espn_id, {})

        # base price derived from season-average fantasy points if available
        base_fp = avg.get("fantasyPoints") or avg.get("fantasypoints") or avg.get("fantasyPoints.total") or avg.get("fp") or 0.0
        base = price_from_fp(base_fp if base_fp is not None else 0.0)

        # compute normalized deviations across all available numeric stats for this player
        eps = 1e-6
        deviations = []
        for col, val in r["values"].items():
            avg_val = avg.get(col)
            if avg_val is None:
                avg_val = val if val != 0 else eps
            try:
                dev = (float(val) - float(avg_val)) / max(abs(float(avg_val)), eps)
                deviations.append(dev)
            except Exception:
                continue

        if not deviations:
            weighted_delta = 0.0
        else:
            # simple average of deviations
            weighted_delta = sum(deviations) / len(deviations)

        # cap the effect
        weighted_delta = max(-0.5, min(0.5, weighted_delta))

        price = round(max(5.0, base * (1.0 + weighted_delta)), 2)

        # compute advanced metrics (yoe, roe, toe, uer, pis, its)
        def _get_val(dvals, keys):
            for k in keys:
                if k in dvals:
                    return float(dvals[k])
            return 0.0

        vals = r.get("values", {})
        # canonical field candidates
        yards = _get_val(vals, ["Receiving.recYds", "receiving.recYds", "yards", "yds", "recYds"])
        recs = _get_val(vals, ["Receiving.receptions", "receiving.receptions", "receptions", "rec"])
        tds = _get_val(vals, ["Receiving.recTD", "receiving.recTD", "td", "recTD"]) 
        fp = _get_val(vals, ["fantasyPoints", "fantasyPoints.total", "fantasyPointsDefault.standard", "fantasyPointsDefault", "fp"]) 

        avg_yards = _get_val(avg, ["Receiving.recYds", "receiving.recYds", "yards", "yds", "recYds"])
        avg_recs = _get_val(avg, ["Receiving.receptions", "receiving.receptions", "receptions", "rec"])
        avg_tds = _get_val(avg, ["Receiving.recTD", "receiving.recTD", "td", "recTD"]) 
        avg_fp = _get_val(avg, ["fantasyPoints", "fantasyPoints.total", "fantasyPointsDefault.standard", "fantasyPointsDefault", "fp"]) 

        yoe = round(yards - avg_yards, 2)
        roe = round(recs - avg_recs, 2)
        toe = round(tds - avg_tds, 2)
        try:
            denom = (avg_fp + 1.0) if (avg_fp is not None) else 1.0
            if denom == 0:
                denom = 1.0
            uer = round(fp / denom, 4)
        except Exception:
            uer = round(fp, 4)
        pis = round((0.5 * yoe) + (0.3 * roe) + (0.2 * toe), 4)

        # ITS = current_price - average_price_last_4_weeks (use available weeks; if none, ITS = 0)
        prev_prices = []
        for x in history.get(espn_id, []):
            if not isinstance(x, dict):
                continue
            pval = x.get("p")
            if pval is None:
                continue
            try:
                prev_prices.append(float(str(pval)))
            except Exception:
                continue

        if prev_prices:
            last4 = prev_prices[-4:]
            avg_last4 = sum(last4) / len(last4)
            its = round(price - avg_last4, 4)
        else:
            its = 0.0

        entry = {"t": t, "p": price, "yoe": yoe, "roe": roe, "toe": toe, "uer": uer, "pis": pis, "its": its}

        history.setdefault(espn_id, [])
        if any(pt.get("t") == t for pt in history[espn_id]):
            continue
        history[espn_id].append(entry)

    # sort each series by date
    for k, arr in history.items():
        try:
            arr.sort(key=lambda x: str(x.get("t") or ""))
        except Exception:
            pass

    # write out
    try:
        with open(OUT_PATH, "w", encoding="utf-8") as fh:
            json.dump(history, fh, ensure_ascii=False, indent=2)
        print(f"Wrote history to {OUT_PATH} with {len(history)} players")
    except Exception as e:
        print(f"Failed to write history: {e}")


if __name__ == "__main__":
    main()
