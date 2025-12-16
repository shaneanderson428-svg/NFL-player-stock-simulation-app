import pandas as pd
from scripts.compute_player_stock import apply_volatility_multiplier


def test_beating_projection_increases_price():
    # Player beats projections -> projection_delta positive -> price should increase
    df = pd.DataFrame([
        {
            "player": "Beat",
            "pass_yards": 200,
            "pass_tds": 2,
            "ints": 0,
            "proj_yards": 100,
            "proj_tds": 1,
            "proj_ints": 1,
            "weekly_change": 0.0,
            "week": 0,
            "__game_date": pd.NaT,
            "is_playoff": False,
            "is_gameday": False,
            "price": 100.0,
        }
    ])

    out = apply_volatility_multiplier(df.copy(), price_col="price")
    # newPrice should be > original price due to positive projection_delta
    assert float(out.loc[0, "newPrice"].item()) > 100.0  # type: ignore


def test_missing_projection_decreases_price():
    # Missing projection fields -> penalty -> price should decrease
    df = pd.DataFrame([
        {
            "player": "Missing",
            "pass_yards": 50,
            "pass_tds": 0,
            "ints": 0,
            # no proj_yards/proj_tds/proj_ints
            "weekly_change": 0.0,
            "week": 0,
            "__game_date": pd.NaT,
            "is_playoff": False,
            "is_gameday": False,
            "price": 120.0,
        }
    ])

    out = apply_volatility_multiplier(df.copy(), price_col="price")
    assert float(out.loc[0, "newPrice"].item()) < 120.0  # type: ignore


def test_capped_change_respects_bounds():
    # Create a row with very large projection beat so applied change > 0.2 but capped
    df = pd.DataFrame([
        {
            "player": "Big",
            "pass_yards": 1000,
            "pass_tds": 10,
            "ints": 0,
            "proj_yards": 10,
            "proj_tds": 0.1,
            "proj_ints": 1,
            "weekly_change": 0.0,
            "week": 1,
            "__game_date": pd.Timestamp("2025-09-09"),
            "is_playoff": True,  # playoff multiplier 2.0
            "is_gameday": True,
            "price": 50.0,
        }
    ])

    out = apply_volatility_multiplier(df.copy(), price_col="price")
    # cappedChange should be <= 0.20

    assert float(out.loc[0, "cappedChange"].item()) <= 0.20  # type: ignore
    assert float(out.loc[0, "cappedChange"].item()) >= -0.20  # type: ignore
