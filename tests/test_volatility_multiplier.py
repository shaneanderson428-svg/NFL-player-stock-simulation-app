import importlib.util
from pathlib import Path
import pandas as pd
import math

# Import compute_player_stock by file location so tests work regardless of PYTHONPATH
proj_root = Path(__file__).resolve().parents[1]
script_path = proj_root / "scripts" / "compute_player_stock.py"
spec = importlib.util.spec_from_file_location("compute_player_stock", str(script_path))
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load compute_player_stock from {script_path}")
compute_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compute_mod)
apply_volatility_multiplier = compute_mod.apply_volatility_multiplier


def test_volatility_multipliers_and_capping():
    # Construct controllable DataFrame with three rows:
    #  - row 0: regular gameday (week>0) -> multiplier 1.5
    #  - row 1: primetime (Monday) -> multiplier 1.75
    #  - row 2: playoff (is_playoff True) -> multiplier 2.0 and should be capped
    df = pd.DataFrame(
        [
            {
                "player": "A",
                "weekly_change": 0.05,
                "week": 1,
                "__game_date": pd.Timestamp("2025-09-10"),
                "is_playoff": False,
                "is_primetime": False,
                "price": 100.0,
            },
            {
                "player": "B",
                "weekly_change": 0.06,
                "week": 2,
                "__game_date": pd.Timestamp(
                    "2025-09-08"
                ),  # Monday -> primetime heuristic
                "is_playoff": False,
                "is_primetime": True,
                "price": 100.0,
            },
            {
                "player": "C",
                "weekly_change": 0.12,
                "week": 3,
                "__game_date": pd.Timestamp("2025-09-09"),
                "is_playoff": True,
                "is_primetime": False,
                "price": 100.0,
            },
            {
                "player": "D",
                "weekly_change": 0.02,
                "week": 0,
                "__game_date": pd.NaT,
                "is_playoff": False,
                "is_primetime": False,
                "is_gameday": False,
                "sentiment_score": 0.8,
                "trading_volume": 5000,
                "price": 100.0,
            },
        ]
    )

    out = apply_volatility_multiplier(df.copy(), price_col="price")

    # Row 0: gameday -> multiplier 1.5
    assert float(out.loc[0, "multiplier"]) == 1.5
    # applied_weekly_change now reflects a 70/30 split between performance
    # and amplified market activity. Recompute expectation from diagnostics.
    expected0 = (
        0.7 * float(out.loc[0, "performance_change"]) 
        + 0.3 * (float(out.loc[0, "market_change"]) * float(out.loc[0, "multiplier"]))
    )
    assert abs(float(out.loc[0, "applied_weekly_change"]) - expected0) < 1e-8
    # cappedChange is the tanh-smoothed version of the applied change
    expected0_capped = 0.25 * math.tanh(2.5 * expected0)
    assert abs(float(out.loc[0, "cappedChange"]) - expected0_capped) < 1e-6
    expected0_price = round(100.0 * (1.0 + round(expected0_capped, 6)), 4)
    assert abs(float(out.loc[0, "newPrice"]) - expected0_price) < 1e-8

    # Row 1: primetime -> multiplier >= 1.75
    assert float(out.loc[1, "multiplier"]) >= 1.75
    expected1 = (
        0.7 * float(out.loc[1, "performance_change"]) 
        + 0.3 * (float(out.loc[1, "market_change"]) * float(out.loc[1, "multiplier"]))
    )
    assert abs(float(out.loc[1, "applied_weekly_change"]) - expected1) < 1e-8
    expected1_capped = 0.25 * math.tanh(2.5 * expected1)
    assert abs(float(out.loc[1, "cappedChange"]) - expected1_capped) < 1e-6

    # Row 2: playoff -> multiplier 2.0 -- ensure multiplier is set and smoothing applied
    assert float(out.loc[2, "multiplier"]) == 2.0
    expected2 = (
        0.7 * float(out.loc[2, "performance_change"]) 
        + 0.3 * (float(out.loc[2, "market_change"]) * float(out.loc[2, "multiplier"]))
    )
    assert abs(float(out.loc[2, "applied_weekly_change"]) - expected2) < 1e-8
    expected2_capped = 0.25 * math.tanh(2.5 * expected2)
    assert abs(float(out.loc[2, "cappedChange"]) - expected2_capped) < 1e-6
    expected2_price = round(100.0 * (1.0 + round(expected2_capped, 6)), 4)
    assert abs(float(out.loc[2, "newPrice"]) - expected2_price) < 1e-8

    # Row 3: off-season (is_gameday False) with positive sentiment
    assert float(out.loc[3, "multiplier"]) == 1.0
    # sentiment_factor should be 1 + 0.1*0.8 = 1.08 (clamped between 0.8 and 1.2)
    assert abs(float(out.loc[3, "sentiment_factor"]) - 1.08) < 1e-8
    expected3 = (
        0.7 * float(out.loc[3, "performance_change"]) 
        + 0.3 * (float(out.loc[3, "market_change"]) * float(out.loc[3, "multiplier"]))
    )
    assert abs(float(out.loc[3, "applied_weekly_change"]) - expected3) < 1e-8
    expected3_capped = 0.25 * math.tanh(2.5 * expected3)
    expected_price_pre_decay = 100.0 * (1.0 + round(expected3_capped, 6))
    expected_price_after_decay = expected_price_pre_decay * 0.995
    expected_price_after_decay = round(expected_price_after_decay, 4)
    assert abs(float(out.loc[3, "newPrice"]) - expected_price_after_decay) < 1e-6
    # trading_volume should be present and equal to 5000
    assert int(out.loc[3, "trading_volume"]) == 5000
