#!/usr/bin/env python3
"""Generate small sparkline PNGs for players using the local API JSON.

Usage:
  python scripts/generate_player_charts.py --api-url http://localhost:3001/api/nfl/stocks?all=1 --out tmp/player_charts --limit 20

This reads the API JSON, extracts `priceHistory` (array of {t,p}) or `priceHistory` fallback,
and draws a small sparkline for each player, saving PNGs named <id>__<slug>.png.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import json
from typing import Any

# requests may not be installed in some environments; annotate as Any so
# static checkers don't treat it as Optional[module]. We'll try to import it
# and fall back to urllib if unavailable.
requests: Any = None
try:
    import requests as _requests  # type: ignore
    requests = _requests
    _HAS_REQUESTS = True
except Exception:
    requests = None
    _HAS_REQUESTS = False
    # we'll fallback to urllib when requests isn't installed
    import urllib.request
    import urllib.error
    from http.client import HTTPResponse
import math
import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def slugify(name: str) -> str:
    return ''.join(c for c in name.lower().strip().replace(' ', '-') if (c.isalnum() or c in '-'))


def fetch_api(url: str, retries: int = 3, backoffs=(1, 2, 4)):
    """Fetch API JSON with retries on transient errors or unexpected content.

    Retries up to `retries` times with exponential backoff (seconds specified
    in `backoffs`). On repeated failure, returns an empty dict and logs a
    warning instead of raising.
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            if _HAS_REQUESTS:
                resp = requests.get(url, timeout=15)
                status = resp.status_code
                headers = resp.headers
                text = resp.text
                def _json():
                    return resp.json()
            else:
                # urllib fallback
                req = urllib.request.Request(url, headers={"User-Agent": "python-urllib/3"})
                with urllib.request.urlopen(req, timeout=15) as rr:
                    status = rr.getcode()
                    # headers -> dict
                    headers = {k: v for k, v in rr.getheaders()}
                    text = rr.read().decode('utf8', errors='replace')
                def _json():
                    return json.loads(text)

        except Exception as e:
            last_exc = e
            if attempt < retries:
                import time
                time.sleep(backoffs[min(attempt - 1, len(backoffs) - 1)])
                continue
            print(f"Warning: API fetch failed ({e}) after {attempt} attempts. Proceeding with empty dataset.")
            return {}

        # If status is 5xx or other non-OK, retry
        if status >= 500:
            last_exc = Exception(f"Server error {status}")
            if attempt < retries:
                import time
                time.sleep(backoffs[min(attempt - 1, len(backoffs) - 1)])
                continue
            print(f"Warning: API returned server error {status} after {attempt} attempts. Proceeding with empty dataset.")
            return {}

        # If not a JSON content-type, try to parse but handle gracefully
        content_type = headers.get('Content-Type', headers.get('content-type', '')) or ''
        if 'application/json' not in content_type.lower():
            # attempt to parse anyway but catch ValueError
            try:
                return _json()
            except Exception:
                last_exc = Exception(f"Non-JSON response with Content-Type={content_type}")
                if attempt < retries:
                    import time
                    time.sleep(backoffs[min(attempt - 1, len(backoffs) - 1)])
                    continue
                print(f"Warning: API returned non-JSON response after {attempt} attempts (Content-Type={content_type}). Proceeding with empty dataset.")
                return {}

        # OK JSON
        try:
            return _json()
        except Exception as e:
            last_exc = e
            if attempt < retries:
                import time
                time.sleep(backoffs[min(attempt - 1, len(backoffs) - 1)])
                continue
            print(f"Warning: Failed to decode JSON from API after {attempt} attempts: {e}. Proceeding with empty dataset.")
            return {}
    # fallback
    print(f"Warning: API fetch ultimately failed: {last_exc}. Proceeding with empty dataset.")
    return {}


def extract_price_history(player: dict):
    # priceHistory is expected as [{t:..., p:...}, ...] newest->oldest
    ph = player.get('priceHistory') or player.get('price_history') or []
    if isinstance(ph, list) and len(ph) > 0 and isinstance(ph[0], dict) and 'p' in ph[0]:
        # map to (t,p) list sorted oldest->newest
        pts = [ (str(x.get('t','')), float(x.get('p') or 0.0)) for x in ph ]
        # if newest->oldest flip
        # detect if data seems newest first by checking timestamps increasing vs not
        return list(reversed(pts))
    # try history field
    hist = player.get('history') or []
    if isinstance(hist, list) and len(hist) > 0:
        pts = []
        for h in hist:
            if not isinstance(h, dict):
                continue
            p = None
            if 'stock' in h:
                p = h.get('stock')
            elif 'p' in h:
                p = h.get('p')
            elif 'price' in h:
                p = h.get('price')
            t = h.get('t') or h.get('timestamp') or h.get('date') or h.get('week')
            if p is None:
                continue
            try:
                pv = float(p)
            except Exception:
                continue
            pts.append((str(t or ''), pv))
        return list(reversed(pts))
    return []


def draw_sparkline(values, out_path: Path, title: str = '', figsize=(3, 0.8)):
    # Normalize types for static checkers
    title = '' if title is None else str(title)
    out_path_str = str(out_path)
    if not values:
        # draw a blank placeholder
        fig = plt.figure(figsize=figsize)
        # use tuple of floats for rect
        ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
        ax.text(0.5, 0.5, 'no data', ha='center', va='center', fontsize=8, color='gray')
        ax.set_axis_off()
        fig.savefig(out_path_str, dpi=150, bbox_inches='tight')
        plt.close(fig)
        return
    xs = list(range(len(values)))
    ys = [v for v in values]
    fig = plt.figure(figsize=figsize)
    ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
    ax.plot(xs, ys, color='#1f77b4', linewidth=1.2)
    ax.fill_between(xs, ys, [min(ys)]*len(ys), color='#1f77b4', alpha=0.08)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    # tiny title
    if title:
        ax.text(0, 1.02, title, transform=ax.transAxes, fontsize=7, ha='left')
    fig.savefig(out_path_str, dpi=150, bbox_inches='tight', pad_inches=0.02)
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-url', type=str, default='http://localhost:3001/api/nfl/stocks?all=1')
    parser.add_argument('--out', type=str, default='tmp/player_charts')
    parser.add_argument('--limit', type=int, default=40)
    parser.add_argument('--all', action='store_true', help='Ignore --limit and generate for all players')
    parser.add_argument('--min-points', type=int, default=3)
    args = parser.parse_args(argv)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = fetch_api(args.api_url)
    players = data.get('players') or []

    # If API returned empty or no players, fallback to reading local weekly CSV
    csv_fallback = False
    if not players:
        csv_path = Path('data/player_weekly_stock.csv')
        if csv_path.exists():
            import csv

            # Build per-player history map keyed by espnId or player_id
            hist_map = {}
            # prefer espnId, then player_id, then player name
            with csv_path.open('r', encoding='utf8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    # normalize id
                    pid = (row.get('espnId') or row.get('player_id') or row.get('playerId') or row.get('player_id') or '').strip()
                    if not pid:
                        # fallback to quoted player_id header
                        pid = (row.get('player_id') or row.get('player') or '').strip()
                    # use player name when present
                    pname = (row.get('player') or '').strip()
                    try:
                        wk = int(row.get('week') or 0)
                    except Exception:
                        wk = 0
                    try:
                        sv = float(row.get('stock_value') or 0.0)
                    except Exception:
                        sv = 0.0
                    key = pid or pname or f"player_{len(hist_map)+1}"
                    if key not in hist_map:
                        hist_map[key] = { 'id': key, 'name': pname, 'rows': [] }
                    hist_map[key]['rows'].append((wk, sv))
            # convert to players list
            players = []
            for key, v in hist_map.items():
                rows = sorted(v['rows'], key=lambda x: x[0])
                history = [ { 't': r[0], 'p': r[1] } for r in rows ]
                players.append({ 'espnId': key, 'name': v.get('name','') or '', 'history': history })
            csv_fallback = True
        else:
            players = []

    count = 0
    api_all = False
    try:
        api_all = 'all=1' in (args.api_url or '').lower()
    except Exception:
        api_all = False

    for p in players:
        # when using CSV fallback or when --all/api-url has all=1, generate for every player; otherwise respect --limit
        if (not csv_fallback) and not (args.all or api_all) and args.limit and count >= args.limit:
            break

        name = p.get('name') or p.get('player') or ''
        # normalize to native python str for slugify/title and filename building
        name_str = '' if name is None else str(name)
        pid = p.get('espnId') or p.get('id') or None
        pid_str = '' if pid is None else str(pid)

        ph = extract_price_history(p)
        points = [float(pp[1]) for pp in ph]
        if len(points) < args.min_points:
            # still generate placeholder to show player exists
            # prefer a filename based on espnId/player id for uniqueness
            file_name = pid_str if pid_str else (slugify(name_str) or f'p{count}')
            outp = out_dir / f"{file_name}.png"
            draw_sparkline([], outp, title=name_str)
            count += 1
            continue

        file_name = pid_str if pid_str else (slugify(name_str) or f'p{count}')
        outp = out_dir / f"{file_name}.png"
        draw_sparkline(points, outp, title=name_str)
        count += 1
    print(f"Wrote {count} charts to {out_dir}")

if __name__ == '__main__':
    main()
