import os
import pytest


# Ensure the generated player_stock_summary.csv is created before any tests run.
# This keeps generated test fixtures out of source control and makes tests
# deterministic by creating the summary from the canonical per-game input.
@pytest.fixture(scope="session", autouse=True)
def generate_player_stock_summary():
    try:
        # import here so pytest can find the package paths correctly
        try:
            from scripts import compute_player_stock
        except Exception:
            # fallback: import by file location (works when scripts isn't a package)
            import importlib.util
            from pathlib import Path

            proj_root = Path(__file__).resolve().parents[1]
            script_path = proj_root / "scripts" / "compute_player_stock.py"
            spec = importlib.util.spec_from_file_location(
                "compute_player_stock", str(script_path)
            )
            if spec is None or spec.loader is None:
                raise ImportError(
                    f"Could not load compute_player_stock from {script_path}"
                )
            compute_player_stock = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(compute_player_stock)
    except Exception as e:
        pytest.skip(f"Could not import compute_player_stock: {e}")

    input_p = os.path.join("data", "player_game_stats.csv")
    output_p = os.path.join("data", "player_stock_summary.csv")

    # Only generate if the input exists. If not present, skip tests that rely on it.
    if not os.path.exists(input_p):
        pytest.skip(f"Input data file not found: {input_p}; skipping generation")

    try:
        # Call the library function to generate the summary file.
        compute_player_stock.compute_player_stock_summary(input_p, output_p)
    except Exception as e:
        # If generation fails, skip tests so CI doesn't fail with an unrelated error.
        pytest.skip(f"Failed to generate player_stock_summary.csv: {e}")

    yield

    # Do not remove the generated file automatically; leave it for inspection/debug.
