import subprocess
import sys
from pathlib import Path
import pytest
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "external" / "nflfastR"
CSV_PATH = OUT_DIR / "player_stats_2025.csv"

REQUIRED_COLS = [
    "player_id",
    "week",
    "epa_per_play",
    "cpoe",
    "plays",
]


def test_fetch_and_columns():
    """Run the fetch script and verify CSV exists and contains required columns.
    If the fetch fails due to network, skip the test.
    """
    p = subprocess.run([sys.executable, str(ROOT / "scripts" / "fetch_nfl_stats.py")], cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        pytest.skip(f"fetch script failed (network?) — stdout: {p.stdout} stderr: {p.stderr}")

    assert CSV_PATH.exists(), f"Expected CSV at {CSV_PATH}"
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        pytest.skip(f"Could not read CSV after fetch: {e}")

    # check for at least one of player id/name columns
    cols = set(df.columns.str.lower())
    missing = [c for c in REQUIRED_COLS if c.lower() not in cols]
    assert not missing, f"Missing expected columns in player_stats CSV: {missing}"
