import pandas as pd
import os


def test_player_stock_summary_contains_columns_and_attempts():
    path = os.path.join("data", "player_stock_summary.csv")
    if not os.path.exists(path):
        # If the summary hasn't been generated in this environment, skip.
        import pytest

        pytest.skip(f"{path} not found; skipping")
    df = pd.read_csv(path)
    # columns
    assert "espnId" in df.columns or any(
        c.lower() in ("espnid", "espn_id", "espn") for c in df.columns
    )
    assert "pass_attempts" in df.columns or any(
        c.lower() in ("pass_attempts",) for c in df.columns
    )

    # at least one player with >=20 pass attempts
    if "pass_attempts" in df.columns:
        vals = pd.to_numeric(df["pass_attempts"], errors="coerce")
        assert (
            vals.fillna(0).ge(20).any()
        ), "No player with >=20 pass_attempts found in summary"
