# flake8: noqa
import importlib.util
from pathlib import Path
import pandas as pd
import tempfile

# Load compute_player_stock by file location
proj_root = Path(__file__).resolve().parents[1]
script_path = proj_root / "scripts" / "compute_player_stock.py"
spec = importlib.util.spec_from_file_location("compute_player_stock", str(script_path))
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load compute_player_stock from {script_path}")
compute_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compute_mod)
compute_summary = compute_mod.compute_player_stock_summary


def write_and_run(df: pd.DataFrame):
    # helper to write a temp CSV and run the full pipeline
    with tempfile.TemporaryDirectory() as td:
        inp = Path(td) / "input.csv"
        out = Path(td) / "out.csv"
        df.to_csv(inp, index=False)
        summary = compute_summary(str(inp), str(out))
        return summary


def test_positive_stats_increase_price():
    # Create three players across one game each so z-scores are meaningful
    df = pd.DataFrame([
        {"player": "A", "pass_yards": 300, "pass_tds": 3, "ints": 0, "rush_yards": 20, "rush_tds": 0, "epa_per_play": 0.5, "cpoe": 5.0, "week": 1, "price": 100.0},
        {"player": "B", "pass_yards": 100, "pass_tds": 0, "ints": 1, "rush_yards": 5, "rush_tds": 0, "epa_per_play": -0.2, "cpoe": -2.0, "week": 1, "price": 100.0},
        {"player": "C", "pass_yards": 150, "pass_tds": 1, "ints": 0, "rush_yards": 0, "rush_tds": 0, "epa_per_play": 0.0, "cpoe": 0.0, "week": 1, "price": 100.0},
    ])

    summary = write_and_run(df)
    # Player A has clearly superior stats -> rawChange should be positive
    a_row = summary[summary["player"] == "A"].iloc[0]
    assert float(a_row["rawChange"]) > 0
    assert float(a_row["applied_weekly_change"]) > 0


def test_interceptions_decrease_price():
    df = pd.DataFrame([
        {"player": "X", "pass_yards": 200, "pass_tds": 2, "ints": 3, "rush_yards": 0, "rush_tds": 0, "epa_per_play": 0.3, "cpoe": 2.0, "week": 1, "price": 50.0},
        {"player": "Y", "pass_yards": 180, "pass_tds": 2, "ints": 0, "rush_yards": 0, "rush_tds": 0, "epa_per_play": 0.3, "cpoe": 2.0, "week": 1, "price": 50.0},
        {"player": "Z", "pass_yards": 170, "pass_tds": 1, "ints": 0, "rush_yards": 0, "rush_tds": 0, "epa_per_play": 0.1, "cpoe": 0.5, "week": 1, "price": 50.0},
    ])

    summary = write_and_run(df)
    x_row = summary[summary["player"] == "X"].iloc[0]
    y_row = summary[summary["player"] == "Y"].iloc[0]
    # X has multiple interceptions; rawChange should be lower than Y
    assert float(x_row["rawChange"]) < float(y_row["rawChange"])


def test_capped_change_never_exceeds_bounds():
    # Create an extreme player to provoke large rawChange which should be capped
    df = pd.DataFrame([
        {"player": "Big", "pass_yards": 1000, "pass_tds": 10, "ints": 0, "rush_yards": 100, "rush_tds": 5, "epa_per_play": 5.0, "cpoe": 20.0, "week": 1, "price": 100.0},
        {"player": "Other1", "pass_yards": 100, "pass_tds": 0, "ints": 0, "rush_yards": 0, "rush_tds": 0, "epa_per_play": 0.0, "cpoe": 0.0, "week": 1, "price": 100.0},
        {"player": "Other2", "pass_yards": 90, "pass_tds": 0, "ints": 0, "rush_yards": 0, "rush_tds": 0, "epa_per_play": 0.0, "cpoe": 0.0, "week": 1, "price": 100.0},
    ])

    summary = write_and_run(df)
    big = summary[summary["player"] == "Big"].iloc[0]
    assert abs(float(big["cappedChange"])) <= 0.200001


def test_sentiment_and_gameday_apply_correctly():
    # Sentiment positive and primetime/gameday should amplify change
    df = pd.DataFrame([
        {"player": "S1", "pass_yards": 250, "pass_tds": 2, "ints": 0, "rush_yards": 10, "rush_tds": 0, "epa_per_play": 0.4, "cpoe": 3.0, "week": 1, "is_primetime": True, "sentiment_score": 1.0, "is_gameday": True, "price": 100.0},
        {"player": "S2", "pass_yards": 250, "pass_tds": 2, "ints": 0, "rush_yards": 10, "rush_tds": 0, "epa_per_play": 0.4, "cpoe": 3.0, "week": 1, "is_primetime": False, "sentiment_score": 0.0, "is_gameday": True, "price": 100.0},
        {"player": "S3", "pass_yards": 250, "pass_tds": 2, "ints": 0, "rush_yards": 10, "rush_tds": 0, "epa_per_play": 0.4, "cpoe": 3.0, "week": 1, "is_primetime": False, "sentiment_score": 0.0, "is_gameday": False, "price": 100.0},
    ])

    summary = write_and_run(df)
    s1 = summary[summary["player"] == "S1"].iloc[0]
    s2 = summary[summary["player"] == "S2"].iloc[0]
    s3 = summary[summary["player"] == "S3"].iloc[0]

    # s1 should have larger applied change than s2 due to primetime + positive sentiment
    assert float(s1["applied_weekly_change"]) >= float(s2["applied_weekly_change"]) - 1e-6
    # s3 is offseason (is_gameday False) so newPrice should include decay (smaller than s2 pre-decay)
    pre_s2 = 100.0 * (1.0 + float(s2["cappedChange"]))
    post_s3 = float(s3["newPrice"])
    assert post_s3 <= pre_s2 + 1e-6
