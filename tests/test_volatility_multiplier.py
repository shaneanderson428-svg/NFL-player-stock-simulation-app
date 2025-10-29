import importlib.util
from pathlib import Path
import pandas as pd

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
    assert (
        abs(
            float(out.loc[0, "rawChange"]) * 1.5
            - float(out.loc[0, "applied_weekly_change"])
        )
        < 1e-8
    )
    assert float(out.loc[0, "cappedChange"]) == float(
        out.loc[0, "applied_weekly_change"]
    )
    assert (
        abs(
            float(out.loc[0, "newPrice"])
            - (100.0 * (1.0 + float(out.loc[0, "cappedChange"])))
        )
        < 1e-8
    )

    # Row 1: primetime -> multiplier >= 1.75 (we set primetime True)
    assert float(out.loc[1, "multiplier"]) >= 1.75
    assert (
        abs(
            float(out.loc[1, "rawChange"]) * float(out.loc[1, "multiplier"])
            - float(out.loc[1, "applied_weekly_change"])
        )
        < 1e-8
    )
    assert float(out.loc[1, "cappedChange"]) == float(
        out.loc[1, "applied_weekly_change"]
    )

    # Row 2: playoff -> multiplier 2.0 and capping to 0.20
    assert float(out.loc[2, "multiplier"]) == 2.0
    # applied raw change before cap should be 0.12 * 2 = 0.24
    assert abs(float(out.loc[2, "applied_weekly_change"]) - 0.24) < 1e-8
    # cappedChange should be clamped to 0.20
    assert abs(float(out.loc[2, "cappedChange"]) - 0.20) < 1e-8
    # newPrice should reflect the capped change
    assert abs(float(out.loc[2, "newPrice"]) - (100.0 * 1.20)) < 1e-8

    # Row 3: off-season (is_gameday False) with positive sentiment ->
    # - multiplier should remain 1.0 (no game)
    # - sentiment_factor should be 1 + 0.1*0.8 = 1.08 (clamped between 0.8 and 1.2)
    # - applied_weekly_change should be weekly_change * multiplier * sentiment_factor
    assert float(out.loc[3, "multiplier"]) == 1.0
    assert abs(float(out.loc[3, "sentiment_factor"]) - 1.08) < 1e-8
    # compute expected: base raw = weekly_change * multiplier = 0.02
    expected_after_sentiment = 0.02 * 1.08
    assert abs(float(out.loc[3, "applied_weekly_change"]) - expected_after_sentiment) < 1e-8
    # newPrice should be computed with cappedChange and then off-season decay 0.995 applied
    # capped change equals applied change (well under 20%)
    expected_price_pre_decay = 100.0 * (1.0 + float(out.loc[3, "cappedChange"]))
    expected_price_after_decay = expected_price_pre_decay * 0.995
    assert abs(float(out.loc[3, "newPrice"]) - expected_price_after_decay) < 1e-8
    # trading_volume should be present and equal to 5000
    assert int(out.loc[3, "trading_volume"]) == 5000
