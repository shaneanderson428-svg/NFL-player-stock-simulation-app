import importlib.util
from pathlib import Path

import pandas as pd

# Load compute_player_stock.py by path so tests don't depend on PYTHONPATH
proj_root = Path(__file__).resolve().parents[1]
script_path = proj_root / "scripts" / "compute_player_stock.py"
spec = importlib.util.spec_from_file_location("compute_player_stock", str(script_path))
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load compute_player_stock from {script_path}")
compute_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compute_mod)
apply_volatility_multiplier = compute_mod.apply_volatility_multiplier


def test_sentiment_positive():
    df = pd.DataFrame(
        [
            {
                "player": "P",
                "weekly_change": 0.05,
                "week": 0,
                "__game_date": pd.NaT,
                "is_playoff": False,
                "is_gameday": False,
                "sentiment_score": 1.0,
                "price": 100.0,
            }
        ]
    )

    out = apply_volatility_multiplier(df.copy(), price_col="price")

    # sentiment_factor = 1 + 0.1*1 = 1.1
    assert abs(float(out.loc[0, "sentiment_factor"]) - 1.1) < 1e-6

    # applied_weekly_change = weekly_change * multiplier(1.0) * sentiment_factor
    expected_applied = 0.05 * 1.0 * 1.1
    assert abs(float(out.loc[0, "applied_weekly_change"]) - expected_applied) < 1e-6

    # newPrice = price * (1 + cappedChange) * decay(0.995) since offseason
    expected_price_pre_decay = 100.0 * (1.0 + float(out.loc[0, "cappedChange"]))
    expected_price = expected_price_pre_decay * 0.995
    assert abs(float(out.loc[0, "newPrice"]) - expected_price) < 1e-6


def test_sentiment_negative():
    df = pd.DataFrame(
        [
            {
                "player": "P",
                "weekly_change": 0.05,
                "week": 0,
                "__game_date": pd.NaT,
                "is_playoff": False,
                "is_gameday": False,
                "sentiment_score": -1.0,
                "price": 200.0,
            }
        ]
    )

    out = apply_volatility_multiplier(df.copy(), price_col="price")

    # sentiment_factor = 1 + 0.1*(-1) = 0.9
    assert abs(float(out.loc[0, "sentiment_factor"]) - 0.9) < 1e-6

    expected_applied = 0.05 * 1.0 * 0.9
    assert abs(float(out.loc[0, "applied_weekly_change"]) - expected_applied) < 1e-6

    expected_price_pre_decay = 200.0 * (1.0 + float(out.loc[0, "cappedChange"]))
    expected_price = expected_price_pre_decay * 0.995
    assert abs(float(out.loc[0, "newPrice"]) - expected_price) < 1e-6


def test_sentiment_zero_and_clamp():
    # zero sentiment
    df0 = pd.DataFrame(
        [
            {
                "player": "Z",
                "weekly_change": 0.01,
                "week": 0,
                "__game_date": pd.NaT,
                "is_playoff": False,
                "is_gameday": False,
                "sentiment_score": 0.0,
                "price": 50.0,
            }
        ]
    )
    out0 = apply_volatility_multiplier(df0.copy(), price_col="price")
    assert abs(float(out0.loc[0, "sentiment_factor"]) - 1.0) < 1e-6

    # large positive sentiment clamps to 1.2
    df_hi = df0.copy()
    df_hi.loc[0, "sentiment_score"] = 10.0
    out_hi = apply_volatility_multiplier(df_hi.copy(), price_col="price")
    assert abs(float(out_hi.loc[0, "sentiment_factor"]) - 1.2) < 1e-6

    # large negative sentiment clamps to 0.8
    df_lo = df0.copy()
    df_lo.loc[0, "sentiment_score"] = -10.0
    out_lo = apply_volatility_multiplier(df_lo.copy(), price_col="price")
    assert abs(float(out_lo.loc[0, "sentiment_factor"]) - 0.8) < 1e-6


def test_gameday_multiplier():
    # Regular gameday, primetime, playoff rows
    df = pd.DataFrame(
        [
            {
                "player": "G1",
                "weekly_change": 0.02,
                "week": 1,
                "__game_date": pd.Timestamp("2025-09-10"),
                "is_playoff": False,
                "is_primetime": False,
                "price": 10.0,
            },
            {
                "player": "G2",
                "weekly_change": 0.03,
                "week": 2,
                "__game_date": pd.Timestamp("2025-09-08"),
                "is_playoff": False,
                "is_primetime": True,
                "price": 10.0,
            },
            {
                "player": "G3",
                "weekly_change": 0.25,
                "week": 3,
                "__game_date": pd.Timestamp("2025-09-09"),
                "is_playoff": True,
                "is_primetime": False,
                "price": 10.0,
            },
        ]
    )

    out = apply_volatility_multiplier(df.copy(), price_col="price")

    assert abs(float(out.loc[0, "multiplier"]) - 1.5) < 1e-6
    assert float(out.loc[1, "multiplier"]) >= 1.75
    assert abs(float(out.loc[2, "multiplier"]) - 2.0) < 1e-6

    # ensure capping for the large weekly_change in playoff
    assert float(out.loc[2, "cappedChange"]) <= 0.20 + 1e-12


def test_capped_change_limits():
    df = pd.DataFrame(
        [
            {
                "player": "C1",
                "weekly_change": 0.2,
                "week": 3,
                "__game_date": pd.Timestamp("2025-09-09"),
                "is_playoff": True,
                "price": 100.0,
            },
            {
                "player": "C2",
                "weekly_change": -0.3,
                "week": 1,
                "__game_date": pd.Timestamp("2025-09-10"),
                "is_playoff": False,
                "price": 100.0,
            },
        ]
    )
    out = apply_volatility_multiplier(df.copy(), price_col="price")
    # C1: 0.2 * 2.0 = 0.4 -> capped to 0.2
    assert abs(float(out.loc[0, "cappedChange"]) - 0.20) < 1e-6
    # C2: -0.3 * 1.5 = -0.45 -> capped to -0.2
    assert abs(float(out.loc[1, "cappedChange"]) + 0.20) < 1e-6


def test_decay_applies_only_offseason():
    # Two rows: identical applied change but one is gameday
    base = {
        "player": "D",
        "weekly_change": 0.04,
        "week": 0,
        "__game_date": pd.NaT,
        "is_playoff": False,
        "sentiment_score": 0.0,
        "price": 50.0,
    }
    row_off = dict(base)
    row_off["is_gameday"] = False

    row_on = dict(base)
    row_on["is_gameday"] = True

    df = pd.DataFrame([row_off, row_on])
    out = apply_volatility_multiplier(df.copy(), price_col="price")

    # applied change same
    assert abs(float(out.loc[0, "applied_weekly_change"]) - float(out.loc[1, "applied_weekly_change"])) < 1e-6

    # row_off newPrice should have decay (0.995)
    price_pre_off = 50.0 * (1.0 + float(out.loc[0, "cappedChange"]))
    expected_off = price_pre_off * 0.995
    assert abs(float(out.loc[0, "newPrice"]) - expected_off) < 1e-6

    # row_on newPrice should NOT have decay
    price_pre_on = 50.0 * (1.0 + float(out.loc[1, "cappedChange"]))
    assert abs(float(out.loc[1, "newPrice"]) - price_pre_on) < 1e-6
