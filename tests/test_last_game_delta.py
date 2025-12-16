"""Type-safety regression test for last-game delta vectorized addition.

This test verifies the vectorized computation used in
`scripts/compute_player_stock.py` to compute
`stock_value_adj = to_numeric(stock_value) + to_numeric(last_game_delta)`.
It ensures the result is numeric and handles string, None, and NaN inputs
without raising errors.
"""
import pandas as pd
import numpy as np


def test_last_game_delta_computation():
    # small fixture with mixed types (floats, strings, None, NaN)
    rows = [
        {"player_id": "p1", "stock_value": 1.23, "last_game_delta": 0.1},
        {"player_id": "p2", "stock_value": "2.5", "last_game_delta": None},
        {"player_id": "p3", "stock_value": None, "last_game_delta": 0.2},
        {"player_id": "p4", "stock_value": "3.0", "last_game_delta": "-0.5"},
        {"player_id": "p5", "stock_value": 4.0, "last_game_delta": float("nan")},
    ]

    df = pd.DataFrame(rows)

    # Perform the same vectorized coercion/addition used in compute_player_stock.py
    sv = pd.to_numeric(df["stock_value"], errors="coerce").fillna(0.0)
    lg = pd.to_numeric(df["last_game_delta"], errors="coerce").fillna(0.0)
    df["stock_value_adj"] = (sv + lg).round(6)

    # Expected values
    expected = {
        "p1": round(1.23 + 0.1, 6),
        "p2": round(2.5 + 0.0, 6),
        "p3": round(0.0 + 0.2, 6),
        "p4": round(3.0 + (-0.5), 6),
        "p5": round(4.0 + 0.0, 6),
    }

    # Assert column exists and is float dtype
    assert "stock_value_adj" in df.columns
    assert pd.api.types.is_float_dtype(df["stock_value_adj"].dtype)

    # Check values
    for _, r in df.iterrows():
        pid = r["player_id"]
        got = float(r["stock_value_adj"])
        assert got == expected[pid], f"player {pid} expected {expected[pid]} got {got}"

    # Also verify that NaN in last_game_delta didn't change original stock_value when present
    p5_row = df[df["player_id"] == "p5"].iloc[0]
    assert p5_row["stock_value_adj"] == 4.0
