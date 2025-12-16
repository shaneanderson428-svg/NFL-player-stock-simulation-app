import importlib.util
from pathlib import Path
import pandas as pd

# Load the compute_player_stock module by file path so pytest doesn't need
# the top-level `scripts` package on sys.path during collection.
proj_root = Path(__file__).resolve().parents[1]
script_path = proj_root / "scripts" / "compute_player_stock.py"
spec = importlib.util.spec_from_file_location("compute_player_stock", str(script_path))
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load module spec for {script_path}")
compute_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compute_mod)  # type: ignore[attr-defined]
compute_player_stock_summary = compute_mod.compute_player_stock_summary


def test_espnid_alias_detection(tmp_path):
    aliases = ["espn_id", "ESPN", " playerId ", "espnid"]
    for alias in aliases:
        # Create a small sample CSV with alias column
        df = pd.DataFrame(
            {
                alias: ["1234", "5678"],
                "name": ["Player A", "Player B"],
                "epa": [1.2, 0.8],
                "cpoe": [0.1, -0.2],
            }
        )
        input_file = tmp_path / f"sample_{alias.strip()}.csv"
        output_file = tmp_path / f"out_{alias.strip()}.csv"
        df.to_csv(input_file, index=False)

        # Run compute
        compute_player_stock_summary(str(input_file), str(output_file))

        # Read back and assert espnId column exists
        out_df = pd.read_csv(output_file)
        assert "espnId" in out_df.columns, f"espnId missing for alias {alias}"
        # Cast to string before comparison to avoid pandas numeric inference
        assert list(out_df["espnId"].astype(str)) == ["1234", "5678"]


def test_qb_stock_basic():
    from scripts.compute_player_stock import compute_qb_stock

    row = {
        "pass_attempts": 25,
        "z_epa_per_play": 0.8,
        "z_cpoe": 0.5,
        "z_pass_yards": 1.0,
        "z_pass_tds": 0.7,
        "z_rush_yards": 0.4,
        "z_rush_tds": 0.3,
    }
    score = compute_qb_stock(row)
    assert isinstance(score, (float, int))
    assert score is not None


def test_qb_stock_low_volume_returns_none():
    from scripts.compute_player_stock import compute_qb_stock

    row = {"pass_attempts": 5}
    assert compute_qb_stock(row) is None
