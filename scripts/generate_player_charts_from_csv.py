#!/usr/bin/env python3
"""Generate sparkline PNGs from local data/player_weekly_stock.csv.

Usage:
  python scripts/generate_player_charts_from_csv.py --csv data/player_weekly_stock.csv --out tmp/player_charts --limit 40
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def slugify(name: str) -> str:
    return ''.join(c for c in name.lower().strip().replace(' ', '-') if (c.isalnum() or c in '-'))


def draw_sparkline(values, out_path: Path, title: str = '', figsize=(3, 0.8)):
    # Normalize types for static checkers: ensure title is a str and out_path is passed as a str to matplotlib
    title = '' if title is None else str(title)
    out_path_str = str(out_path)
    if not values:
        fig = plt.figure(figsize=figsize)
        # use a tuple for the rect to satisfy type-checkers
        ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
        ax.text(0.5, 0.5, 'no data', ha='center', va='center', fontsize=8, color='gray')
        ax.set_axis_off()
        fig.savefig(out_path_str, dpi=150, bbox_inches='tight')
        plt.close(fig)
        return
    xs = list(range(len(values)))
    ys = list(values)
    fig = plt.figure(figsize=figsize)
    # tuple of floats expected by matplotlib; avoid passing lists or int-tuples
    ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
    ax.plot(xs, ys, color='#1f77b4', linewidth=1.2)
    ax.fill_between(xs, ys, [min(ys)]*len(ys), color='#1f77b4', alpha=0.08)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ('top','right','left','bottom'):
        ax.spines[spine].set_visible(False)
    if title:
        ax.text(0, 1.02, title, transform=ax.transAxes, fontsize=7, ha='left')
    fig.savefig(out_path_str, dpi=150, bbox_inches='tight', pad_inches=0.02)
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', type=str, default='data/player_weekly_stock.csv')
    parser.add_argument('--out', type=str, default='tmp/player_charts')
    parser.add_argument('--limit', type=int, default=40)
    args = parser.parse_args(argv)

    df = pd.read_csv(args.csv)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for pname, group in df.groupby('player'):
        if args.limit and count >= args.limit:
            break
        try:
            grp = group.sort_values('week')
        except Exception:
            grp = group
        vals = pd.to_numeric(grp['stock_value'], errors='coerce').fillna(0.0).tolist()
        # ensure pname is a native str (pandas may give numpy/bytes scalars)
        pname_str = '' if pname is None else str(pname)
        outp = out_dir / f"{slugify(pname_str)}__{count}.png"
        draw_sparkline(vals, outp, title=pname_str)
        count += 1
    print(f"Wrote {count} charts to {out_dir}")

if __name__ == '__main__':
    main()
