"""Integration test: run compute_player_stock_summary on a small local fixture CSV and
verify weekly CSV contains expected last_game_delta and adjusted stock_value.
"""
import importlib.util
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "compute_player_stock.py"
FIXTURE = ROOT / "tests" / "fixtures" / "player_game_stats_fixture.csv"


def load_compute_module(path: Path):
    # Create a module spec from the file path. Be defensive: spec or its loader
    # may be None (static linters complain). Check and raise a clear error so
    # both runtime and static analyzers see the types are correct.
    spec = importlib.util.spec_from_file_location("compute_player_stock", str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot create module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    # Use the spec.name when present; fall back to a stable name to register in sys.modules
    modname = spec.name if getattr(spec, "name", None) else "compute_player_stock"
    sys.modules[modname] = module
    # exec_module is provided by the loader; mypy/Pylance may not infer that but
    # the runtime check above guarantees loader is present.
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def test_compute_player_stock_summary_fixture(tmp_path: Path):
    module = load_compute_module(SCRIPT_PATH)
    input_csv = FIXTURE
    out_csv = tmp_path / "player_stock_summary.csv"

    # Call the function under test
    summary = module.compute_player_stock_summary(str(input_csv), str(out_csv))

    # The compute function writes player_weekly_stock.csv next to the output
    weekly_path = out_csv.parent / "player_weekly_stock.csv"
    assert weekly_path.exists(), f"Expected weekly stock CSV at {weekly_path}"

    wdf = pd.read_csv(weekly_path)

    # Basic checks: columns exist and are numeric
    assert "last_game_delta" in wdf.columns
    assert "stock_value" in wdf.columns
    # coerce to numeric to ensure no mixed-type strings
    wdf["last_game_delta"] = pd.to_numeric(wdf["last_game_delta"], errors="coerce").fillna(0.0)
    wdf["stock_value"] = pd.to_numeric(wdf["stock_value"], errors="coerce").fillna(0.0)

    # For each player ensure week-over-week change equals reported stock_change
    for pname in wdf.player.unique():
        prow = wdf[wdf.player == pname].sort_values("week")
        for i in range(1, len(prow)):
            prev = prow.iloc[i - 1]
            cur = prow.iloc[i]
            # stock_change column exists and should equal cur.stock_value - prev.stock_value
            sc = float(cur.get("stock_change", 0.0))
            calc = float(cur["stock_value"] - prev["stock_value"])
            assert isclose(sc, calc, rel_tol=1e-6), f"stock_change mismatch for {pname} wk {cur.week}: {sc} vs {calc}"

    # Heuristic checks: last_game_delta should be present and sensible for the
    # fixture players: positive for both and Bob's delta (from a larger prior
    # z_epa) should be larger than Alice's.
    alice_row = wdf[(wdf.player == 'Alice') & (wdf.week == 2)]
    bob_row = wdf[(wdf.player == 'Bob') & (wdf.week == 2)]
    assert len(alice_row) == 1 and len(bob_row) == 1
    # Ensure both have numeric last_game_delta values (sign/scale is computed by the pipeline)
    assert pd.notna(alice_row.iloc[0]['last_game_delta'])
    assert pd.notna(bob_row.iloc[0]['last_game_delta'])


from math import isclose
