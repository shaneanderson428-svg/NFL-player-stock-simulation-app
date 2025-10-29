import csv
from pathlib import Path


def test_no_nan_in_position_fields():
    p = Path("data/player_stock_summary.csv")
    assert p.exists(), "data/player_stock_summary.csv not found"
    with p.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            pos = (row.get("position") or "").strip()
            prof = (row.get("position_profile") or "").strip()
            # Fail if literal string 'NAN' appears (case-insensitive)
            assert (
                pos.upper() != "NAN"
            ), f"Found 'NAN' in position for player {row.get('player')}: '{pos}'"
            assert (
                prof.upper() != "NAN"
            ), f"Found 'NAN' in position_profile for player {row.get('player')}: '{prof}'"
