#!/usr/bin/env python3
"""
Compute weekly prices from weekly CSVs and write per-week price CSV to data/prices/{season}/week_{week}.csv

Behavior:
- Reads data/weekly/{season}/week_{week}.csv (or per-position week_{week}_POS.csv)
- Computes z-scores per-position (or across all if position missing)
- Computes delta using same formula as backfill and applies to previous price when available
- Writes CSV with columns: playerId, playerName, position, week, price
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import csv
from collections import defaultdict
from statistics import mean, pstdev
from typing import List


DATA_DIR = os.path.join(os.getcwd(), 'data')
WEEKLY_BASE = os.path.join(DATA_DIR, 'weekly')
PRICES_BASE = os.path.join(DATA_DIR, 'prices')


def safe_num(v):
    try:
        if v is None or v == '':
            return 0.0
        return float(str(v).strip())
    except Exception:
        return 0.0


def compute_zscores(values: List[float]):
    if not values:
        return []
    m = mean(values)
    sd = pstdev(values) if len(values) > 1 else 0.0
    if sd == 0:
        return [0.0 for _ in values]
    return [(v - m) / sd for v in values]


def computeWeeklyDelta(z_epa, z_yards, z_tds, z_vol):
    delta_raw = 0.35 * z_epa + 0.30 * z_yards + 0.25 * z_tds + 0.10 * z_vol
    delta_raw = max(-0.10, min(0.10, delta_raw))
    if abs(delta_raw) < 0.005:
        return 0.0
    return delta_raw


# --- Market Reaction Engine v1 helpers -------------------------------------------------
def compute_performance_score(epa: float, yards: float, tds: float, vol: float) -> float:
    """Compute a compact weekly performance score used as the 'this_week_performance_score'.

    This is intentionally similar to the absolute scoring used below but returned
    as a normalized ratio so it can be compared to recent history. The scale is
    arbitrary but consistent: scores around ~0..200 are reasonable depending on
    position/usage. We do not add external APIs; this uses the same inputs as
    the prior absolute score.
    """
    try:
        score = (epa * 12.0) + (tds * 6.0) + (yards * 0.08) + (vol * 0.4)
    except Exception:
        score = 0.0
    return float(score)


def load_recent_perf_from_history(pid: str, weeks: int = 4):
    """Load the last `weeks` performance scores for a player from data/history/{pid}_price_history.json.

    We expect the per-player history JSON (used by other scripts) to contain
    dicts with fields including 'week' and 'price'. If no file exists, return
    an empty list. This is a lightweight helper to avoid adding new backends.
    """
    hist_dir = os.path.join(os.getcwd(), 'data', 'history')
    hist_f = os.path.join(hist_dir, f"{pid}_price_history.json")
    out = []
    if not os.path.exists(hist_f):
        return out
    try:
        import json as _json

        with open(hist_f, 'r', encoding='utf8') as fh:
            h = _json.load(fh)
            if isinstance(h, list) and h:
                # return up to `weeks` most recent entries
                recent = h[-weeks:]
                return recent
    except Exception:
        return []
    return out


def compute_expectation_gap(this_week_score: float, recent_scores: list[dict]) -> float:
    """Expectation gap = this_week_performance_score - rolling_avg_performance_score.

    recent_scores: list of history dicts produced elsewhere. If recent_scores is
    empty we treat the rolling average as this_week_score so expectation_gap=0.
    The function returns a fractional change-like number suitable for blending
    with momentum (i.e., typical values in range roughly -0.5..+0.5).
    """
    if not recent_scores:
        return 0.0
    vals = []
    for it in recent_scores:
        # attempt to reconstruct a performance-like value: prefer stored 'score' if
        # present, otherwise try to synthesize from price/close (we avoid inventing
        # stats; best-effort fallback is to assume no surprise previously)
        if isinstance(it, dict) and ('score' in it):
            try:
                vals.append(float(it.get('score') or 0.0))
                continue
            except Exception:
                pass
        # fallback: 0 (neutral) to avoid creating artificial gaps
        vals.append(0.0)

    if len(vals) == 0:
        return 0.0
    roll = sum(vals) / float(len(vals))
    # Normalize gap relative to rolling average; avoid divide-by-zero
    try:
        if abs(roll) < 1e-6:
            return 0.0
        return (this_week_score - roll) / abs(roll)
    except Exception:
        return 0.0


def compute_momentum(pid: str, prev_prices_map: dict) -> float:
    """Compute simple momentum = (price_last_week - price_2_weeks_ago) / price_2_weeks_ago.

    prev_prices_map is a mapping of playerId -> last known prices (if available).
    We attempt to load two last prices from per-player history if prev_prices_map
    lacks them. If insufficient data, momentum=0.0.
    """
    # Try prev_prices_map first: expect prev_prices_map[pid] = [price_t-1, price_t-2,...]
    try:
        entry = prev_prices_map.get(pid)
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            p1 = float(entry[0])
            p2 = float(entry[1])
            if abs(p2) < 1e-6:
                return 0.0
            return (p1 - p2) / abs(p2)
    except Exception:
        pass

    # Fallback: read per-player history file
    try:
        recent = load_recent_perf_from_history(pid, weeks=3)
        prices = []
        for it in reversed(recent):
            # recent is oldest..newest slice; collect last two closing prices if present
            if isinstance(it, dict):
                v = it.get('price') or it.get('close') or it.get('price_close')
                # Skip empty/None values to avoid passing None into float()
                if v is None or v == "":
                    continue
                try:
                    prices.append(float(v))
                except Exception:
                    continue
        if len(prices) >= 2:
            p1 = float(prices[-1])
            p2 = float(prices[-2])
            if abs(p2) < 1e-6:
                return 0.0
            return (p1 - p2) / abs(p2)
    except Exception:
        pass
    return 0.0

# -------------------------------------------------------------------------------------


def find_weekly_csv(season: int, week: int):
    # For CSV-only mode prefer the normalized player_stats file and do NOT call external APIs.
    candidate = os.path.join(WEEKLY_BASE, f'player_stats_{season}_week_{week}.csv')
    if os.path.exists(candidate):
        return candidate
    # If the canonical player_stats CSV is not present, do not fallback to other sources in CSV-only mode.
    return None


def read_rows(path):
    rows = []
    with open(path, newline='', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append(r)
    return rows


def write_prices(season: int, week: int, rows):
    outdir = os.path.join(PRICES_BASE, str(season))
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f'week_{week}.csv')
    # include diagnostics so append_price_history can persist score and other diagnostics
    fieldnames = [
        'playerId',
        'week',
        'price',
        'score',
        'expectation_gap',
        'momentum',
        'price_change_pct',
    ]
    with open(outpath, 'w', newline='', encoding='utf8') as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            # ensure all fields present (avoid KeyError when older code writes simple rows)
            out = {k: r.get(k, '') for k in fieldnames}
            w.writerow(out)
    print(f"Wrote {len(rows)} prices to {outpath}")
    return outpath


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', type=int, required=True)
    ap.add_argument('--week', type=int, required=True)
    args = ap.parse_args()

    path = find_weekly_csv(args.season, args.week)
    if not path:
        print(f"ERROR: No weekly player_stats CSV found for season={args.season} week={args.week}. Expected: {os.path.join(WEEKLY_BASE, f'player_stats_{args.season}_week_{args.week}.csv')}")
        sys.exit(2)

    print(f"Computing prices for season={args.season} week={args.week} from {path}")
    rows = read_rows(path)
    if not rows:
        # If a weekly CSV exists but contains only a header (no player rows),
        # log a warning and skip processing this week. This makes full-season
        # backfills resilient to weeks where data isn't available.
        print(f"WARNING: Weekly CSV at {path} contains no player rows (header-only). Skipping week {args.week}.")
        return

    # ensure seed_map exists even if preseason seeding block is skipped
    seed_map = {}

    # parse rows into numeric stats
    parsed = []
    for r in rows:
        pid = str(r.get('playerId') or r.get('player_id') or r.get('playerID') or r.get('espnId') or '').strip()
        if not pid:
            continue
        epa = safe_num(r.get('epa'))
        yards = safe_num(r.get('yards') or r.get('recYds'))
        tds = safe_num(r.get('tds'))
        vol = safe_num(r.get('targets')) + safe_num(r.get('receptions')) + safe_num(r.get('carries'))
        # capture position when available to allow position-level normalization
        pos = (r.get('position') or r.get('pos') or r.get('Position') or '').strip().upper()
        parsed.append({'playerId': pid, 'epa': epa, 'yards': yards, 'tds': tds, 'vol': vol, 'position': pos, 'row': r})

    # Absolute performance scoring (per-player, no cohort normalization)
    # score = (epa * 12) + (tds * 6) + (yards * 0.08) + ((targets + carries) * 0.4)
    # delta_pct = clamp(score / 100, -0.15, +0.15)
    def clamp(v, lo, hi):
        return max(lo, min(hi, v))

    # read prior prices if exist to chain changes
    prev_prices = {}
    prev_path = os.path.join(PRICES_BASE, str(args.season), f'week_{args.week - 1}.csv')
    if os.path.exists(prev_path):
        try:
            prev_rows = read_rows(prev_path)
            for r in prev_rows:
                prev_prices[str(r.get('playerId'))] = safe_num(r.get('price'))
        except Exception:
            prev_prices = {}
    # If previous-week prices file is missing, try to seed from per-player history files
    if not prev_prices:
        hist_dir = os.path.join(os.getcwd(), 'data', 'history')
        for p in parsed:
            pid = str(p['playerId'])
            hist_f = os.path.join(hist_dir, f"{pid}_price_history.json")
            if os.path.exists(hist_f):
                try:
                    import json as _json
                    with open(hist_f, 'r', encoding='utf8') as fh:
                        h = _json.load(fh)
                        if isinstance(h, list) and h:
                            last = h[-1]
                            prev_prices[pid] = safe_num(last.get('price'))
                except Exception:
                    continue

            # --- Preseason price seeding -------------------------------------------------
            # Build a seed price for players that have no prior history using available
            # preseason/prior-season metrics. Prefer `data/player_stock_summary.csv` when
            # present (it contains per-player season-level metrics like 'stock'), else
            # fall back to a computed performance score from available weekly inputs.
            seed_map = {}
            summary_map = {}
            summary_path = os.path.join(os.getcwd(), 'data', 'player_stock_summary.csv')
            if os.path.exists(summary_path):
                try:
                    with open(summary_path, newline='', encoding='utf8') as fh:
                        rdr = csv.DictReader(fh)
                        for r in rdr:
                            sid = str(r.get('espnId') or r.get('playerId') or r.get('id') or '').strip()
                            if not sid:
                                continue
                            summary_map[sid] = r
                except Exception:
                    summary_map = {}

            # Compute a base_score per player (fundamental): prefer summary 'stock' if present,
            # otherwise use the computed performance_score from available preseason/proxy stats.
            pos_buckets = {}
            base_scores = {}
            for p in parsed:
                pid = p['playerId']
                pos = p.get('position', '') or ''
                # try summary stock
                base = None
                srow = summary_map.get(pid)
                if srow:
                    try:
                        # prefer a numeric 'stock' column if present
                        if srow.get('stock') not in (None, ''):
                            base = float(srow.get('stock'))
                    except Exception:
                        base = None
                if base is None:
                    # fallback: use computed performance_score from available inputs
                    base = compute_performance_score(p['epa'], p['yards'], p['tds'], p['vol'])
                base_scores[pid] = base
                pos_buckets.setdefault(pos or 'UNK', []).append(base)

            # Normalize per-position (min-max) and map to $20..$220, then apply position multipliers
            MIN_PRICE = 20.0
            MAX_PRICE = 220.0
            RANGE = MAX_PRICE - MIN_PRICE
            pos_multipliers = {'QB': 1.2, 'WR': 1.05, 'RB': 1.0, 'TE': 0.95}

            for p in parsed:
                pid = p['playerId']
                pos = p.get('position', '') or 'UNK'
                bucket = pos_buckets.get(pos, pos_buckets.get('UNK', []))
                if bucket:
                    try:
                        lo = float(min(bucket))
                        hi = float(max(bucket))
                    except Exception:
                        lo = hi = float(base_scores.get(pid, 0.0))
                else:
                    lo = hi = float(base_scores.get(pid, 0.0))

                raw = float(base_scores.get(pid, 0.0))
                if hi - lo < 1e-6:
                    norm = 0.5
                else:
                    norm = (raw - lo) / (hi - lo)
                    # clamp
                    norm = max(0.0, min(1.0, norm))

                seed_price = MIN_PRICE + norm * RANGE
                mult = pos_multipliers.get(pos, 1.0)
                seed_price = float(seed_price) * float(mult)
                # safety clamps
                seed_price = max(1.0, min(500.0, seed_price))
                seed_map[pid] = round(seed_price, 2)

            # End preseason seeding ------------------------------------------------------

    out_rows = []

    # Build a simple prev_prices_map for momentum: prefer previous-week CSV prices
    # but also attempt to collect an extra older price from per-player history files.
    prev_prices_map = {}
    for p in parsed:
        pid = p['playerId']
        # initialize with previous-week price if available
        if pid in prev_prices:
            prev = prev_prices.get(pid)
            prev_prices_map[pid] = [prev]
        else:
            prev_prices_map[pid] = []

    # Try to seed a second-oldest price for momentum from history files
    hist_dir = os.path.join(os.getcwd(), 'data', 'history')
    for p in parsed:
        pid = p['playerId']
        if len(prev_prices_map.get(pid, [])) >= 2:
            continue
        hist_f = os.path.join(hist_dir, f"{pid}_price_history.json")
        if os.path.exists(hist_f):
            try:
                import json as _json

                with open(hist_f, 'r', encoding='utf8') as fh:
                    h = _json.load(fh)
                    if isinstance(h, list) and len(h) >= 2:
                        # last entry is most recent; extract last two prices
                        last = h[-1]
                        prev = h[-2]
                        p_last = float(last.get('price') or last.get('close') or 0.0)
                        p_prev = float(prev.get('price') or prev.get('close') or 0.0)
                        prev_prices_map[pid] = [p_last, p_prev]
            except Exception:
                pass

    for p in parsed:
        pid = p['playerId']
        # Compute this week's performance score (stat-driven proxy)
        this_score = compute_performance_score(p['epa'], p['yards'], p['tds'], p['vol'])

        # Load recent performance score objects (best-effort) to compute expectation gap.
        # Use only prior stored 'score' values from history to compute the rolling average.
        recent_perf = load_recent_perf_from_history(pid, weeks=4)
        hist_scores = []
        for it in recent_perf:
            if isinstance(it, dict) and it.get('score') is not None and it.get('score') != "":
                raw = it.get('score')
                if raw is None or raw == "":
                    continue
                try:
                    hist_scores.append(float(raw))
                except Exception:
                    continue
        if hist_scores:
            roll = sum(hist_scores) / float(len(hist_scores))
            # avoid divide-by-zero
            if abs(roll) < 1e-6:
                expectation_gap = 0.0
            else:
                expectation_gap = (this_score - roll) / abs(roll)
        else:
            expectation_gap = 0.0

        # compute momentum from prev_prices_map
        momentum = compute_momentum(pid, prev_prices_map)

        # Pricing formula v1: blend expectation gap and momentum
        price_change_pct = 0.6 * float(expectation_gap) + 0.4 * float(momentum)

        # Clamp extreme changes to +/-20% to avoid explosions
        price_change_pct = max(-0.2, min(0.2, price_change_pct))

        # If change is very small, treat as zero to avoid noise
        if abs(price_change_pct) < 0.0005:
            price_change_pct = 0.0

        # Use prior price if available, otherwise use preseason seed price if present
        if pid in prev_prices:
            base = prev_prices.get(pid, 100.0)
        else:
            base = seed_map.get(pid, 100.0)
        new_price = round(base * (1.0 + float(price_change_pct)), 2)

        out_rows.append(
            {
                'playerId': pid,
                'week': args.week,
                'price': new_price,
                'score': round(this_score, 4),
                'expectation_gap': round(float(expectation_gap), 6),
                'momentum': round(float(momentum), 6),
                'price_change_pct': round(float(price_change_pct), 6),
            }
        )

    write_prices(args.season, args.week, out_rows)


if __name__ == '__main__':
    main()
