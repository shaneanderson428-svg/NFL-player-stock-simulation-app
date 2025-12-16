import os
import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_compute_qb_stock_basic():
    # import compute_qb_stock from scripts/compute_player_stock.py
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from compute_player_stock import compute_qb_stock

    # create a minimal row-like dict/Series with pass_attempts >= 10
    row = pd.Series({
        "pass_attempts": 20,
        "z_epa_per_play": 1.0,
        "z_cpoe": 0.5,
        "z_pass_yards": 0.8,
        "z_pass_tds": 0.2,
        "z_rush_yards": 0.0,
        "z_rush_tds": 0.0,
    })
    s = compute_qb_stock(row)
    assert s is not None
    assert isinstance(s, float)


def test_calculate_advanced_metrics_cli(tmp_path):
    # create a small CSV input that the enrichment script can read
    csv_in = tmp_path / "in.csv"
    df = pd.DataFrame([
        {
            "player": "Test Player",
            "passing_yards": 200,
            "passing_attempts": 25,
            "completions": 18,
            "passing_tds": 2,
            "interceptions": 0,
            "receiving_yards": 50,
            "targets": 6,
            "rushing_attempts": 5,
            "rushing_first_downs": 2,
        }
    ])
    df.to_csv(csv_in, index=False)

    outp = tmp_path / "out.csv"
    cmd = [sys.executable, "scripts/calculate_advanced_metrics.py", "--input", str(csv_in), "--output", str(outp)]
    proc = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parents[1]), check=False)
    assert proc.returncode == 0
    assert outp.exists()
    outdf = pd.read_csv(outp)
    # derived columns should exist
    assert "pass_efficiency" in outdf.columns
    assert "estimated_epa" in outdf.columns
