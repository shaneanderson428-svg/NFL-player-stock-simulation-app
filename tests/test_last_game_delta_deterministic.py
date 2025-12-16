import csv
import os
import tempfile
from pathlib import Path

import pandas as pd

from scripts.compute_player_stock import compute_player_stock_summary


def test_last_game_delta_deterministic(tmp_path):
    # Create a small per-game CSV with explicit z_epa_per_play values so last_game_delta is deterministic.
    inp = tmp_path / "input_games.csv"
    outp = tmp_path / "out_summary.csv"

    # Two games for same player; week 1 z_epa = 1.0, week 2 z_epa = 0.5
    rows = [
        {"player": "Test Player", "week": 1, "position": "QB", "z_epa_per_play": 1.0, "pass_attempts": 20},
        {"player": "Test Player", "week": 2, "position": "QB", "z_epa_per_play": 0.5, "pass_attempts": 20},
    ]

    # Write CSV
    df = pd.DataFrame(rows)
    df.to_csv(inp, index=False)

    # Run computation
    compute_player_stock_summary(str(inp), str(outp))

    # The compute writes player_weekly_stock.csv next to outp
    weekly = outp.parent / "player_weekly_stock.csv"
    assert weekly.exists(), f"Expected {weekly} to be created"

    wdf = pd.read_csv(weekly)
    # Ensure there are two rows for our player (one per week)
    p_rows = wdf[wdf["player"] == "Test Player"]
    assert len(p_rows) == 2

    # Find week 1 and week 2 rows and assert last_game_delta
    # last_game_delta for week1 should be 0.0 (no previous game)
    # last_game_delta for week2 should be prior z_epa_per_play * 0.3 = 1.0 * 0.3 = 0.3
    row_w1 = p_rows[p_rows["week"] == 1].iloc[0]
    row_w2 = p_rows[p_rows["week"] == 2].iloc[0]

    # coerce numeric
    lg1 = float(row_w1.get("last_game_delta") or 0.0)
    lg2 = float(row_w2.get("last_game_delta") or 0.0)

    assert abs(lg1 - 0.0) < 1e-6
    assert abs(lg2 - 0.3) < 1e-6
